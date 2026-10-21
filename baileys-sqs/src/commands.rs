//! Commands from INPUT_QUEUE or the HTTP endpoint, sent through WhatsApp.
use crate::{
    ephemeral::EphemeralCache,
    js,
    output::Output,
    polls::{self, PollCache},
    whatsapp::{User, WhatsApp},
};
use baileys_rs::{
    messages::generation::{Content, Modifiers, PollVote, content_from_json},
    proto,
    proto_json::JsonField,
    socket::{SendOptions, presence::Presence},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Map, Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CommandError(pub String);

type Result<T> = std::result::Result<T, CommandError>;

fn fail(message: impl Into<String>) -> CommandError {
    CommandError(message.into())
}

fn from_error(error: impl std::fmt::Display) -> CommandError {
    CommandError(error.to_string())
}

/// A phone number becomes a user JID; anything with "@" is kept as is.
pub fn normalize_jid(to: &str) -> String {
    if to.contains('@') {
        return to.into();
    }
    let clean: String = to
        .chars()
        .filter(|c| *c != '+' && !js::is_space(*c))
        .collect();
    format!("{clean}@s.whatsapp.net")
}

fn to_jid(cmd: &Value) -> Result<String> {
    match cmd.get("to") {
        Some(Value::String(to)) => Ok(normalize_jid(to)),
        None | Some(Value::Null) => Err(fail(
            "Cannot read properties of undefined (reading 'includes')",
        )),
        Some(_) => Err(fail("to.includes is not a function")),
    }
}

/// What a command produced, for its acknowledgement.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Outcome {
    pub to: Option<String>,
    pub message_id: Option<String>,
    pub extra: Map<String, Value>,
}

/// The current socket; replaced on every reconnect.
pub type SharedSocket = Arc<RwLock<Option<Arc<dyn WhatsApp>>>>;

#[derive(Clone)]
pub struct Commands {
    pub socket: SharedSocket,
    pub ephemeral: EphemeralCache,
    pub polls: PollCache,
    pub output: Output,
    pub http: reqwest::Client,
}

fn message_id(message: &Option<proto::WebMessageInfo>) -> Option<String> {
    message.as_ref()?.key.as_ref()?.id.clone()
}

fn object(value: Option<&Value>) -> Map<String, Value> {
    match value {
        Some(Value::Object(fields)) if js::truthy(value) => fields.clone(),
        _ => Map::new(),
    }
}

/// A message key as the TS handlers build it: `remoteJid` defaults to the
/// chat, `fromMe` to the given default, `participant` is kept when set.
fn message_key(key: &Value, jid: &str, from_me_default: bool) -> Value {
    let mut out = json!({
        "remoteJid": js::get(key, "remoteJid").cloned().unwrap_or_else(|| json!(jid)),
        "fromMe": match key.get("fromMe") {
            None => from_me_default,
            Some(v) => js::truthy(Some(v)),
        },
        "id": key["id"].clone(),
    });
    if let Some(participant) = key.get("participant").filter(|v| !v.is_null()) {
        out["participant"] = participant.clone();
    }
    out
}

fn require_key(cmd: &Value, command: &str) -> Result<Value> {
    let key = cmd.get("message_key").cloned().unwrap_or(Value::Null);
    if !js::truthy(key.get("id")) {
        return Err(fail(format!(
            "{command} requires \"message_key\" with at least an \"id\" field"
        )));
    }
    Ok(key)
}

impl Commands {
    /// Fetch an outgoing media URL, failing after ten seconds.
    async fn fetch(&self, url: &str) -> Result<Vec<u8>> {
        let response = tokio::time::timeout(Duration::from_secs(10), async {
            let response = self.http.get(url).send().await.map_err(from_error)?;
            if !response.status().is_success() {
                return Err(fail(format!(
                    "Failed to fetch media: {}",
                    response.status().as_u16()
                )));
            }
            response.bytes().await.map_err(from_error)
        })
        .await
        .map_err(|_| fail("This operation was aborted"))??;
        Ok(response.to_vec())
    }

    async fn media_bytes(&self, media: &Value) -> Result<Vec<u8>> {
        if let Some(data) = js::get(media, "data_base64") {
            return Ok(js::base64_lenient(&js::string(data)));
        }
        if let Some(url) = js::get(media, "url") {
            return self.fetch(&js::string(url)).await;
        }
        Err(fail("Media must include either data_base64 or url"))
    }

    /// Strip a quoted message without content, which would fail the send.
    fn sanitize_quoted(options: &mut Map<String, Value>) {
        let quoted = options.get("quoted");
        if js::truthy(quoted) && !js::truthy(quoted.and_then(|q| q.get("message"))) {
            let quoted = &options["quoted"];
            tracing::warn!(
                quoted_keys = ?quoted.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()),
                has_key = js::truthy(quoted.get("key")),
                has_message = false,
                "quoted object missing message property - stripping to prevent crash"
            );
            options.remove("quoted");
        }
    }

    /// The cache is keyed by the chat id events carry, which for most 1:1
    /// chats is the LID, while commands usually address a phone number (or
    /// the other way round), so a chat is looked up under its other form too.
    async fn cached_ephemeral(&self, whatsapp: &dyn WhatsApp, jid: &str) -> Option<Value> {
        if let Some(expiration) = self.ephemeral.get(jid) {
            return Some(expiration);
        }
        match whatsapp.alternate_jid(jid).await {
            Ok(alternate) => alternate.and_then(|alternate| self.ephemeral.get(&alternate)),
            Err(err) => {
                tracing::warn!(%err, jid, "failed to resolve the chat for the ephemeral cache");
                None
            }
        }
    }

    /// Attach the chat's known disappearing-messages duration unless the
    /// caller set `ephemeralExpiration` (even to null).
    async fn apply_ephemeral(
        &self,
        whatsapp: &dyn WhatsApp,
        jid: &str,
        options: &mut Map<String, Value>,
    ) {
        if options.contains_key("ephemeralExpiration") {
            return;
        }
        if let Some(expiration) = self.cached_ephemeral(whatsapp, jid).await {
            tracing::debug!(jid, %expiration, "applying cached ephemeralExpiration to outgoing message");
            options.insert("ephemeralExpiration".into(), expiration);
        }
    }

    async fn options(&self, whatsapp: &dyn WhatsApp, jid: &str, cmd: &Value) -> Map<String, Value> {
        let mut options = object(cmd.get("options"));
        Self::sanitize_quoted(&mut options);
        self.apply_ephemeral(whatsapp, jid, &mut options).await;
        options
    }

    async fn send(
        &self,
        whatsapp: &dyn WhatsApp,
        jid: &str,
        content: &Value,
        options: &Map<String, Value>,
    ) -> Result<Option<proto::WebMessageInfo>> {
        let (content, modifiers) = content_from_json(content).map_err(from_error)?;
        let options =
            SendOptions::from_json(&Value::Object(options.clone())).map_err(from_error)?;
        whatsapp
            .send_message(jid, content, modifiers, options)
            .await
            .map_err(from_error)
    }

    /// Run one command. Returns `None` when no socket exists yet, as the
    /// TypeScript handler returns undefined.
    pub async fn handle(&self, cmd: &Value) -> Result<Option<Outcome>> {
        let Some(whatsapp) = self.socket.read().await.clone() else {
            tracing::error!("Socket not initialized");
            return Ok(None);
        };
        let whatsapp = whatsapp.as_ref();
        let jid = to_jid(cmd)?;
        let kind = cmd.get("type").and_then(Value::as_str).unwrap_or_default();
        let sent = |message: Option<proto::WebMessageInfo>| {
            Ok(Some(Outcome {
                to: Some(jid.clone()),
                message_id: message_id(&message),
                ..Default::default()
            }))
        };
        match kind {
            "send_text" => {
                let options = self.options(whatsapp, &jid, cmd).await;
                let content = match js::get(cmd, "message") {
                    Some(message) => message.clone(),
                    None => json!({"text": cmd.get("text").cloned().unwrap_or(Value::Null)}),
                };
                let message = self.send(whatsapp, &jid, &content, &options).await?;
                tracing::debug!(
                    jid,
                    has_options = js::truthy(cmd.get("options")),
                    has_quoted = options.contains_key("quoted"),
                    has_custom_message = js::truthy(cmd.get("message")),
                    "sent text message"
                );
                sent(message)
            }
            "send_media" => {
                let media = cmd.get("media").cloned().unwrap_or(Value::Null);
                if media.is_null() {
                    return Err(fail(
                        "Cannot read properties of undefined (reading 'data_base64')",
                    ));
                }
                let bytes = self.media_bytes(&media).await?;
                let mut message = object(cmd.get("message"));
                // Media text goes in the caption.
                let text = [message.get("text"), message.get("conversation")]
                    .into_iter()
                    .find(|v| js::truthy(*v))
                    .flatten()
                    .cloned();
                if let Some(text) = text {
                    message.insert("caption".into(), text);
                    message.remove("text");
                    message.remove("conversation");
                }
                if let Some(mimetype) = js::get(&media, "mimetype") {
                    message.insert("mimetype".into(), mimetype.clone());
                }
                message.insert(
                    "fileName".into(),
                    js::get(&media, "filename")
                        .cloned()
                        .unwrap_or_else(|| json!("file")),
                );
                let field = match media.get("type").and_then(Value::as_str) {
                    Some("image") => "image",
                    Some("video") => "video",
                    Some("audio") => "audio",
                    Some("sticker") => "sticker",
                    _ => "document",
                };
                message.insert(
                    field.into(),
                    json!({"type":"Buffer","data":STANDARD.encode(&bytes)}),
                );
                let options = self.options(whatsapp, &jid, cmd).await;
                let sent_message = self
                    .send(whatsapp, &jid, &Value::Object(message), &options)
                    .await?;
                tracing::debug!(
                    jid,
                    media_type = field,
                    has_options = js::truthy(cmd.get("options")),
                    has_quoted = options.contains_key("quoted"),
                    has_custom_message = js::truthy(cmd.get("message")),
                    "sent media message"
                );
                sent(sent_message)
            }
            "send_presence" => {
                let presence = match cmd.get("presence").and_then(Value::as_str) {
                    Some("composing") => Presence::Composing,
                    Some("recording") => Presence::Recording,
                    Some("paused") => Presence::Paused,
                    _ => {
                        return Err(fail(format!(
                            "Invalid presence: {}. Must be one of composing, recording, paused",
                            cmd.get("presence")
                                .map(js::string)
                                .unwrap_or_else(|| "undefined".into())
                        )));
                    }
                };
                // Best-effort: the update shows reliably only after subscribing.
                if let Err(err) = whatsapp.presence_subscribe(&jid).await {
                    tracing::warn!(%err, jid, "presenceSubscribe failed, sending presence update anyway");
                }
                whatsapp
                    .send_presence_update(presence, &jid)
                    .await
                    .map_err(from_error)?;
                tracing::debug!(jid, presence = presence.as_str(), "sent presence update");
                Ok(Some(Outcome {
                    to: Some(jid),
                    ..Default::default()
                }))
            }
            "send_reaction" => {
                let Some(Value::String(reaction)) = cmd.get("reaction") else {
                    return Err(fail(
                        "send_reaction requires a \"reaction\" string (use an empty string to remove a reaction)",
                    ));
                };
                let key = require_key(cmd, "send_reaction")?;
                let content =
                    json!({"react":{"text":reaction,"key":message_key(&key, &jid, false)}});
                let message = self.send(whatsapp, &jid, &content, &Map::new()).await?;
                tracing::debug!(jid, reaction, message_id = %key["id"], "sent reaction");
                sent(message)
            }
            "send_read_receipt" => {
                let keys = match js::get(cmd, "message_keys") {
                    Some(keys) => keys.clone(),
                    None => match js::get(cmd, "message_key") {
                        Some(key) => json!([key]),
                        None => json!([]),
                    },
                };
                let valid = keys.as_array().filter(|keys| {
                    !keys.is_empty() && keys.iter().all(|k| js::truthy(k.get("id")))
                });
                let Some(keys) = valid else {
                    return Err(fail(
                        "send_read_receipt requires \"message_keys\" (array) or \"message_key\" (single), each with an \"id\" field",
                    ));
                };
                let keys = keys
                    .iter()
                    .map(|k| proto::MessageKey::from_value(&message_key(k, &jid, false)))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(from_error)?;
                let count = keys.len();
                whatsapp.read_messages(keys).await.map_err(from_error)?;
                tracing::debug!(jid, count, "sent read receipt");
                Ok(Some(Outcome {
                    to: Some(jid),
                    ..Default::default()
                }))
            }
            "send_edit" => {
                let Some(Value::String(text)) = cmd.get("text") else {
                    return Err(fail("send_edit requires a \"text\" string"));
                };
                let key = require_key(cmd, "send_edit")?;
                // Only your own messages can be edited, so fromMe defaults to true.
                let content = json!({"text":text,"edit":message_key(&key, &jid, true)});
                let message = self.send(whatsapp, &jid, &content, &Map::new()).await?;
                tracing::debug!(jid, message_id = %key["id"], "edited message");
                sent(message)
            }
            "send_delete" => {
                let key = require_key(cmd, "send_delete")?;
                let content = json!({"delete":message_key(&key, &jid, true)});
                let message = self.send(whatsapp, &jid, &content, &Map::new()).await?;
                tracing::debug!(jid, message_id = %key["id"], "deleted message");
                sent(message)
            }
            "send_location" => {
                let location = cmd.get("location").cloned().unwrap_or(Value::Null);
                let (Some(latitude), Some(longitude)) = (
                    location.get("latitude").filter(|v| v.is_number()),
                    location.get("longitude").filter(|v| v.is_number()),
                ) else {
                    return Err(fail(
                        "send_location requires \"location\" with numeric \"latitude\" and \"longitude\"",
                    ));
                };
                let mut fields = json!({"degreesLatitude":latitude,"degreesLongitude":longitude});
                for (from, to) in [("name", "name"), ("address", "address")] {
                    if let Some(v) = location.get(from).filter(|v| !v.is_null()) {
                        fields[to] = v.clone();
                    }
                }
                let message = self
                    .send(whatsapp, &jid, &json!({ "location": fields }), &Map::new())
                    .await?;
                tracing::debug!(jid, %latitude, %longitude, "sent location");
                sent(message)
            }
            "send_contact" => {
                let contacts = match js::get(cmd, "contacts") {
                    Some(contacts) => contacts.clone(),
                    None => match js::get(cmd, "contact") {
                        Some(contact) => json!([contact]),
                        None => json!([]),
                    },
                };
                let valid = contacts.as_array().filter(|list| {
                    !list.is_empty() && list.iter().all(|c| js::truthy(c.get("vcard")))
                });
                let Some(list) = valid else {
                    return Err(fail(
                        "send_contact requires \"contacts\" (array) or \"contact\" (single), each with a \"vcard\" string",
                    ));
                };
                let entries: Vec<Value> = list
                    .iter()
                    .map(|c| {
                        let mut entry = json!({});
                        if let Some(v) = c.get("displayName").filter(|v| !v.is_null()) {
                            entry["displayName"] = v.clone();
                        }
                        entry["vcard"] = c["vcard"].clone();
                        entry
                    })
                    .collect();
                let mut contacts = json!({});
                if list.len() == 1
                    && let Some(v) = list[0].get("displayName").filter(|v| !v.is_null())
                {
                    contacts["displayName"] = v.clone();
                }
                contacts["contacts"] = json!(entries);
                let message = self
                    .send(
                        whatsapp,
                        &jid,
                        &json!({ "contacts": contacts }),
                        &Map::new(),
                    )
                    .await?;
                tracing::debug!(jid, count = list.len(), "sent contact");
                sent(message)
            }
            "send_poll" => {
                let poll = cmd.get("poll").cloned().unwrap_or(Value::Null);
                let values = poll.get("values").and_then(Value::as_array);
                if !poll.get("name").is_some_and(Value::is_string)
                    || values.is_none_or(|v| v.len() < 2)
                {
                    return Err(fail(
                        "send_poll requires \"poll\" with a \"name\" string and at least 2 \"values\"",
                    ));
                }
                let options = self.options(whatsapp, &jid, cmd).await;
                let mut fields = json!({"name":poll["name"],"values":poll["values"]});
                if let Some(count) = poll.get("selectableCount").filter(|v| !v.is_null()) {
                    fields["selectableCount"] = count.clone();
                }
                let message = self
                    .send(whatsapp, &jid, &json!({ "poll": fields }), &options)
                    .await?;
                tracing::debug!(jid, question = %poll["name"], option_count = values.map_or(0, Vec::len), has_options = js::truthy(cmd.get("options")), "sent poll");
                // The poll's secret exists only on the sent message; a caller
                // with a correlation_id can keep it to decrypt votes later.
                let secret = message
                    .as_ref()
                    .and_then(|m| m.message.as_ref())
                    .and_then(|m| m.message_context_info.as_ref())
                    .and_then(|c| c.message_secret.as_ref())
                    .map(|s| json!(STANDARD.encode(s)))
                    .unwrap_or(Value::Null);
                let mut extra = Map::new();
                extra.insert("message_secret".into(), secret);
                Ok(Some(Outcome {
                    to: Some(jid.clone()),
                    message_id: message_id(&message),
                    extra,
                }))
            }
            "vote_poll" => {
                let key = require_key(cmd, "vote_poll")?;
                let selected = match cmd.get("selected_options") {
                    Some(Value::Array(names)) if names.iter().all(Value::is_string) => names
                        .iter()
                        .map(|n| n.as_str().unwrap_or_default().to_owned())
                        .collect::<Vec<_>>(),
                    _ => {
                        return Err(fail(
                            "vote_poll requires \"selected_options\" (array of option names, empty to withdraw the vote)",
                        ));
                    }
                };
                let id = js::string(&key["id"]);
                let cached = self.polls.get(&id);
                if let Some(poll) = &cached
                    && let Some(unknown) = selected.iter().find(|n| !poll.options.contains(n))
                {
                    return Err(fail(format!(
                        "vote_poll: {unknown:?} is not an option of poll {id} ({})",
                        poll.options.join(", ")
                    )));
                }
                let secret = match js::get(cmd, "message_secret") {
                    Some(secret) => js::base64_lenient(&js::string(secret)),
                    None => cached.as_ref().map(|p| p.secret.clone()).ok_or_else(|| {
                        fail(format!(
                            "vote_poll: poll {id} was not seen by this connector; pass its \"message_secret\""
                        ))
                    })?,
                };
                // The key as events carried it, with the caller's fields on top.
                let mut poll_key = cached.map_or_else(|| message_key(&key, &jid, false), |p| p.key);
                for field in ["remoteJid", "fromMe", "participant"] {
                    if let Some(value) = key.get(field).filter(|v| !v.is_null()) {
                        poll_key[field] = value.clone();
                    }
                }
                let user = whatsapp
                    .user()
                    .ok_or_else(|| fail("vote_poll: not logged in"))?;
                // Our own poll's key has no addressing mode and no author
                // to read it from, so ask for the chat's.
                let own_unaddressed = js::truthy(poll_key.get("fromMe"))
                    && !poll_key.get("addressingMode").is_some_and(Value::is_string);
                let chat_lid = if !own_unaddressed {
                    None
                } else {
                    let chat =
                        js::get(&poll_key, "remoteJid").map_or_else(|| jid.clone(), js::string);
                    match whatsapp.lid_addressed(&chat).await {
                        Ok(lid) => lid,
                        Err(err) => {
                            tracing::warn!(%err, chat, "failed to read the chat's addressing mode");
                            None
                        }
                    }
                };
                let (creator, voter) =
                    polls::vote_identities(&poll_key, &user, chat_lid).map_err(fail)?;
                let creator = js::get(cmd, "poll_creator_jid").map_or(creator, js::string);
                let voter = js::get(cmd, "voter_jid").map_or(voter, js::string);
                let mut proto_key = proto::MessageKey {
                    remote_jid: Some(
                        js::get(&poll_key, "remoteJid").map_or_else(|| jid.clone(), js::string),
                    ),
                    from_me: Some(js::truthy(poll_key.get("fromMe"))),
                    id: Some(id),
                    ..Default::default()
                };
                proto_key.participant = js::get(&poll_key, "participant").map(js::string);
                let content = Content::PollVote(Box::new(PollVote {
                    poll_key: proto_key,
                    poll_creator_jid: creator.clone(),
                    voter_jid: voter.clone(),
                    message_secret: secret,
                    selected_options: selected.clone(),
                }));
                let message = whatsapp
                    .send_message(&jid, content, Modifiers::default(), Default::default())
                    .await
                    .map_err(from_error)?;
                tracing::debug!(jid, poll_id = %key["id"], ?selected, creator, voter, "sent poll vote");
                let mut extra = Map::new();
                extra.insert("poll_creator_jid".into(), json!(creator));
                extra.insert("voter_jid".into(), json!(voter));
                Ok(Some(Outcome {
                    to: Some(jid.clone()),
                    message_id: message_id(&message),
                    extra,
                }))
            }
            "raw" => {
                let body = cmd.get("body");
                let Some(body @ Value::Object(_)) = body else {
                    return Err(fail(
                        "raw requires a \"body\" object to pass directly to sock.sendMessage",
                    ));
                };
                let options = self.options(whatsapp, &jid, cmd).await;
                let message = self.send(whatsapp, &jid, body, &options).await?;
                tracing::debug!(jid, body_keys = ?body.as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()), "sent raw message");
                sent(message)
            }
            _ => Err(fail(format!(
                "Unknown command type: {}",
                cmd.get("type")
                    .map(js::string)
                    .unwrap_or_else(|| "undefined".into())
            ))),
        }
    }

    async fn user(&self) -> Option<User> {
        self.socket.read().await.as_ref()?.user()
    }

    /// Run a command and, when it has a `correlation_id`, publish a
    /// `command_ack`. The command's error is returned either way.
    pub async fn dispatch(&self, cmd: &Value) -> Result<()> {
        let result = self.handle(cmd).await;
        let correlation = js::get(cmd, "correlation_id").cloned();
        let user = self.user().await;
        let from = user.as_ref().map_or(Value::Null, |u| json!(u.id));
        let me_lid = user
            .as_ref()
            .and_then(|u| u.lid.clone())
            .map_or(Value::Null, |lid| json!(lid));
        let command_type = cmd.get("type").cloned().unwrap_or(Value::Null);
        match result {
            Ok(outcome) => {
                if let Some(correlation_id) = correlation {
                    let outcome = outcome.unwrap_or_default();
                    let mut payload = json!({
                        "correlation_id": correlation_id,
                        "command_type": command_type,
                        "to": outcome.to,
                        "from": from,
                        "me_lid": me_lid,
                        "ok": true,
                        "message_id": outcome.message_id,
                    });
                    payload
                        .as_object_mut()
                        .expect("ack object")
                        .extend(outcome.extra);
                    self.output
                        .send(&json!({"type":"command_ack","payload":payload}))
                        .await;
                }
                Ok(())
            }
            Err(error) => {
                if let Some(correlation_id) = correlation {
                    let to = match cmd.get("to") {
                        Some(Value::String(to)) if !to.is_empty() => json!(normalize_jid(to)),
                        _ => Value::Null,
                    };
                    let payload = json!({
                        "correlation_id": correlation_id,
                        "command_type": command_type,
                        "to": to,
                        "from": from,
                        "me_lid": me_lid,
                        "ok": false,
                        "error": error.0,
                    });
                    self.output
                        .send(&json!({"type":"command_ack","payload":payload}))
                        .await;
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phone_numbers_become_user_jids() {
        assert_eq!(normalize_jid("+1 234 567"), "1234567@s.whatsapp.net");
        assert_eq!(normalize_jid("123@g.us"), "123@g.us");
    }
}
