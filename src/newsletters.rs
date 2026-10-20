//! MEX (GraphQL over IQ) queries, newsletter responses and newsletter/MEX notifications.
use crate::{
    binary::{BinaryNode, NodeContent},
    jid::S_WHATSAPP_NET,
    proto,
};
use prost::Message as _;
use serde_json::{Value, json};

/// Persisted GraphQL query identifiers.
pub mod query_id {
    pub const CREATE: &str = "8823471724422422";
    pub const UPDATE_METADATA: &str = "24250201037901610";
    pub const METADATA: &str = "6563316087068696";
    pub const SUBSCRIBERS: &str = "9783111038412085";
    pub const FOLLOW: &str = "24404358912487870";
    pub const UNFOLLOW: &str = "9767147403369991";
    pub const MUTE: &str = "29766401636284406";
    pub const UNMUTE: &str = "9864994326891137";
    pub const ADMIN_COUNT: &str = "7130823597031706";
    pub const CHANGE_OWNER: &str = "7341777602580933";
    pub const DEMOTE: &str = "6551828931592903";
    pub const DELETE: &str = "30062808666639665";
    pub const REACHOUT_TIMELOCK: &str = "23983697327930364";
    pub const MESSAGE_CAPPING_INFO: &str = "24503548349331633";
}
/// Response keys under the GraphQL `data` object.
pub mod path {
    pub const CREATE: &str = "xwa2_newsletter_create";
    pub const SUBSCRIBERS: &str = "xwa2_newsletter_subscribers";
    pub const METADATA: &str = "xwa2_newsletter";
    pub const ADMIN_COUNT: &str = "xwa2_newsletter_admin";
    pub const MUTE: &str = "xwa2_newsletter_mute_v2";
    pub const UNMUTE: &str = "xwa2_newsletter_unmute_v2";
    pub const JOIN: &str = "xwa2_newsletter_join_v2";
    pub const LEAVE: &str = "xwa2_newsletter_leave_v2";
    pub const CHANGE_OWNER: &str = "xwa2_newsletter_change_owner";
    pub const DEMOTE: &str = "xwa2_newsletter_demote";
    pub const DELETE: &str = "xwa2_newsletter_delete_v2";
    pub const UPDATE: &str = "xwa2_newsletter_update";
    pub const REACHOUT_TIMELOCK: &str = "xwa2_fetch_account_reachout_timelock";
    pub const MESSAGE_CAPPING_INFO: &str = "xwa2_message_capping_info";
}
pub const ENFORCEMENT_DEFAULT: &str = "DEFAULT";
const ENFORCEMENT_TYPES: [&str; 18] = [
    "BIZ_COMMERCE_VIOLATION_ALCOHOL",
    "BIZ_COMMERCE_VIOLATION_ADULT",
    "BIZ_COMMERCE_VIOLATION_ANIMALS",
    "BIZ_COMMERCE_VIOLATION_BODY_PARTS_FLUIDS",
    "BIZ_COMMERCE_VIOLATION_DATING",
    "BIZ_COMMERCE_VIOLATION_DIGITAL_SERVICES_PRODUCTS",
    "BIZ_COMMERCE_VIOLATION_DRUGS",
    "BIZ_COMMERCE_VIOLATION_DRUGS_ONLY_OTC",
    "BIZ_COMMERCE_VIOLATION_GAMBLING",
    "BIZ_COMMERCE_VIOLATION_HEALTHCARE",
    "BIZ_COMMERCE_VIOLATION_REAL_FAKE_CURRENCY",
    "BIZ_COMMERCE_VIOLATION_SUPPLEMENTS",
    "BIZ_COMMERCE_VIOLATION_TOBACCO",
    "BIZ_COMMERCE_VIOLATION_VIOLENT_CONTENT",
    "BIZ_COMMERCE_VIOLATION_WEAPONS",
    "BIZ_QUALITY",
    ENFORCEMENT_DEFAULT,
    "WEB_COMPANION_ONLY",
];

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The server answered with GraphQL errors; `code` is the first error's code or 400.
    #[error("GraphQL server error: {message}")]
    Server {
        message: String,
        code: Box<Value>,
        data: Box<Value>,
    },
    #[error("Failed to {0}, unexpected response structure.")]
    Unexpected(String),
    /// Payloads upstream fails on with a JSON syntax or type error.
    #[error("malformed MEX payload")]
    Malformed,
}
impl Error {
    pub fn status_code(&self) -> u16 {
        match self {
            // Boom rejects codes that are not numeric or are below 400.
            Self::Server { code, .. } => {
                let code = parse_int(&js_string(code));
                if code >= 400.0 && code <= f64::from(u16::MAX) {
                    code as u16
                } else {
                    500
                }
            }
            Self::Unexpected(_) => 400,
            Self::Malformed => 500,
        }
    }
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Value::String(text) => !text.is_empty(),
        _ => true,
    }
}
// String() of the values these payloads carry; containers use their JSON text.
fn js_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}
// Property read on a JSON value: only objects have the keys used here.
fn get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_object()?.get(key)
}
// Property read that throws on null and undefined, as plain member access does.
fn member<'a>(value: Option<&'a Value>, key: &str) -> Result<Option<&'a Value>, Error> {
    match value {
        None | Some(Value::Null) => Err(Error::Malformed),
        Some(value) => Ok(get(value, key)),
    }
}
/// Decimal JS parseInt; NaN without leading digits.
fn parse_int(text: &str) -> f64 {
    let text = text.trim_start();
    let (sign, digits) = match text.strip_prefix('-') {
        Some(rest) => (-1.0, rest),
        None => (1.0, text.strip_prefix('+').unwrap_or(text)),
    };
    let end = digits
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(digits.len());
    if end == 0 {
        return f64::NAN;
    }
    sign * digits[..end].parse::<f64>().unwrap_or(f64::NAN)
}
fn parse_int_value(value: Option<&Value>) -> f64 {
    value.map_or(f64::NAN, |value| parse_int(&js_string(value)))
}
// Buffer.from(text, 'binary'): the low byte of every UTF-16 unit.
fn binary_bytes(text: &str) -> Vec<u8> {
    text.encode_utf16().map(|unit| unit as u8).collect()
}
// content.toString() of a node: text, UTF-8 bytes or the Array join of child objects.
fn content_string(content: &NodeContent) -> String {
    match content {
        NodeContent::Text(text) => text.clone(),
        NodeContent::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        NodeContent::Children(children) => vec!["[object Object]"; children.len()].join(","),
    }
}
// JS truthiness of node content: only missing content and the empty string are falsy.
fn present(node: Option<&BinaryNode>) -> Option<&NodeContent> {
    node?
        .content
        .as_ref()
        .filter(|content| !matches!(content, NodeContent::Text(text) if text.is_empty()))
}

/// The `w:mex` IQ carrying one persisted query; the socket assigns its id.
pub fn query(variables: &Value, query_id: &str) -> BinaryNode {
    let body = json!({ "variables": variables }).to_string();
    BinaryNode::new("iq")
        .attr("type", "get")
        .attr("to", S_WHATSAPP_NET)
        .attr("xmlns", "w:mex")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("query")
                .attr("query_id", query_id)
                .with_content(NodeContent::Bytes(body.into_bytes())),
        ]))
}
/// Extract `data[data_path]` (or `data` for an empty path) from a MEX result.
/// A JSON null is a successful response; only a missing value is unexpected.
pub fn response(result: &BinaryNode, data_path: &str) -> Result<Value, Error> {
    if let Some(content) = present(result.child("result")) {
        let data: Value =
            serde_json::from_str(&content_string(content)).map_err(|_| Error::Malformed)?;
        if data.is_null() {
            return Err(Error::Malformed);
        }
        match get(&data, "errors") {
            Some(Value::Array(errors)) if !errors.is_empty() => {
                let mut messages = vec![];
                for error in errors {
                    if error.is_null() {
                        return Err(Error::Malformed);
                    }
                    messages.push(
                        get(error, "message")
                            .filter(|message| truthy(message))
                            .map_or_else(|| "Unknown error".into(), js_string),
                    );
                }
                let code = get(&errors[0], "extensions")
                    .and_then(|extensions| get(extensions, "error_code"))
                    .filter(|code| truthy(code))
                    .cloned()
                    .unwrap_or(json!(400))
                    .into();
                return Err(Error::Server {
                    message: messages.join(", "),
                    code,
                    data: errors[0].clone().into(),
                });
            }
            Some(Value::String(errors)) if !errors.is_empty() => return Err(Error::Malformed),
            _ => {}
        }
        let payload = get(&data, "data");
        let value = if data_path.is_empty() {
            payload
        } else {
            payload.and_then(|payload| get(payload, data_path))
        };
        if let Some(value) = value {
            return Ok(value.clone());
        }
    }
    let action = data_path.strip_prefix("xwa2_").unwrap_or(data_path);
    Err(Error::Unexpected(action.replace('_', " ")))
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Picture {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "directPath", skip_serializing_if = "Option::is_none")]
    pub direct_path: Option<String>,
}
/// A created newsletter. Numeric fields are NaN when the server text is not a number.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Metadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub creation_time: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invite: Option<String>,
    pub subscribers: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification: Option<String>,
    pub picture: Picture,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mute_state: Option<String>,
}
/// Fields a newsletter update can change; unset fields are not sent.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Update {
    pub name: Option<String>,
    pub description: Option<String>,
    /// Base64 JPEG, or an empty string to remove the picture.
    pub picture: Option<String>,
}
impl Update {
    pub fn variables(&self, jid: &str) -> Value {
        let mut updates = serde_json::Map::new();
        for (key, value) in [
            ("name", &self.name),
            ("description", &self.description),
            ("picture", &self.picture),
        ] {
            if let Some(value) = value {
                updates.insert(key.into(), json!(value));
            }
        }
        updates.insert("settings".into(), Value::Null);
        json!({"newsletter_id": jid, "updates": updates})
    }
}
/// How a newsletter is addressed in a metadata query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Invite,
    Jid,
}
impl Key {
    pub fn variables(self, key: &str) -> Value {
        json!({
            "fetch_creation_time": true,
            "fetch_full_image": true,
            "fetch_viewer_metadata": true,
            "input": {"key": key, "type": if self == Self::Invite { "INVITE" } else { "JID" }}
        })
    }
}
pub fn create_variables(name: &str, description: Option<&str>) -> Value {
    json!({"input": {"name": name, "description": description}})
}
fn text(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(str::to_owned)
}
pub fn parse_create_response(response: &Value) -> Result<Metadata, Error> {
    let thread = member(Some(response), "thread_metadata")?;
    let viewer = member(Some(response), "viewer_metadata")?;
    let picture = member(thread, "picture")?;
    Ok(Metadata {
        id: text(get(response, "id")),
        name: text(member(member(thread, "name")?, "text")?),
        creation_time: parse_int_value(member(thread, "creation_time")?),
        description: text(member(member(thread, "description")?, "text")?),
        invite: text(member(thread, "invite")?),
        subscribers: parse_int_value(member(thread, "subscribers_count")?),
        verification: text(member(thread, "verification")?),
        picture: Picture {
            id: text(member(picture, "id")?),
            direct_path: text(member(picture, "direct_path")?),
        },
        mute_state: text(member(viewer, "mute")?),
    })
}
/// The server's metadata object, either directly or nested under `result`.
pub fn parse_metadata(result: &Value) -> Option<Value> {
    let object = result.as_object()?;
    if object.get("id").is_some_and(Value::is_string) {
        return Some(result.clone());
    }
    object
        .get("result")
        .filter(|inner| {
            inner
                .as_object()
                .is_some_and(|inner| inner.contains_key("id"))
        })
        .cloned()
}
pub fn react_message(jid: &str, server_id: &str, reaction: Option<&str>) -> BinaryNode {
    let reaction = reaction.filter(|code| !code.is_empty());
    let mut node = BinaryNode::new("message").attr("to", jid);
    if reaction.is_none() {
        node = node.attr("edit", "7");
    }
    let mut child = BinaryNode::new("reaction");
    if let Some(code) = reaction {
        child = child.attr("code", code);
    }
    node.attr("type", "reaction")
        .attr("server_id", server_id)
        .with_content(NodeContent::Children(vec![child]))
}
pub fn fetch_messages(jid: &str, count: u64, since: Option<u64>, after: Option<u64>) -> BinaryNode {
    let mut updates = BinaryNode::new("message_updates").attr("count", count.to_string());
    if let Some(since) = since {
        updates = updates.attr("since", since.to_string());
    }
    if let Some(after) = after.filter(|after| *after != 0) {
        updates = updates.attr("after", after.to_string());
    }
    BinaryNode::new("iq")
        .attr("type", "get")
        .attr("xmlns", "newsletter")
        .attr("to", jid)
        .with_content(NodeContent::Children(vec![updates]))
}
pub fn subscribe_updates(jid: &str) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("type", "set")
        .attr("xmlns", "newsletter")
        .attr("to", jid)
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("live_updates").with_content(NodeContent::Children(vec![])),
        ]))
}
pub fn live_updates_duration(result: &BinaryNode) -> Option<String> {
    result
        .child("live_updates")?
        .attrs
        .get("duration")
        .filter(|duration| !duration.is_empty())
        .cloned()
}

/// Outgoing-message restriction state of the account.
#[derive(Debug, Clone, PartialEq)]
pub struct ReachoutTimelock {
    pub is_active: bool,
    /// Unix milliseconds; NaN where upstream holds an invalid Date.
    pub time_enforcement_ends: Option<f64>,
    pub enforcement_type: String,
}
/// State from the account query, which keeps unknown enforcement types as sent.
pub fn parse_reachout_timelock(result: &Value) -> ReachoutTimelock {
    let ends = get(result, "time_enforcement_ends")
        .filter(|ends| truthy(ends) && ends.as_str() != Some("0"));
    ReachoutTimelock {
        is_active: get(result, "is_active").is_some_and(truthy),
        time_enforcement_ends: ends.map(|ends| parse_int(&js_string(ends)) * 1000.0),
        enforcement_type: get(result, "enforcement_type")
            .filter(|kind| !kind.is_null())
            .map_or_else(|| ENFORCEMENT_DEFAULT.into(), js_string),
    }
}

/// What a newsletter or MEX notification asks the socket to do, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// newsletter.reaction, always counted once.
    Reaction {
        id: Option<String>,
        server_id: Option<String>,
        code: Option<String>,
    },
    /// newsletter.view; the count is NaN for non-numeric content.
    View {
        id: Option<String>,
        server_id: Option<String>,
        count: f64,
    },
    /// newsletter-participants.update
    Participant {
        id: Option<String>,
        author: Option<String>,
        user: Option<String>,
        action: Option<String>,
        new_role: Option<String>,
    },
    /// newsletter-settings.update
    Settings { id: Option<String>, update: Value },
    /// A plaintext newsletter message for upsertMessage(.., 'append').
    Message(Box<proto::WebMessageInfo>),
    /// connection.update with the reach-out timelock state.
    Timelock(ReachoutTimelock),
    /// message-capping.update with the server payload.
    MessageCapping(Value),
    /// lid-mapping.update
    LidMapping { lid: String, pn: String },
    /// One storeLIDPNMappings call, made even without mappings.
    StoreMappings(Vec<(String, String)>),
}

/// Effects of a `newsletter` notification, one child at a time.
pub fn notification(node: &BinaryNode) -> Vec<Effect> {
    let from = node.attrs.get("from").cloned();
    let author = node.attrs.get("participant").cloned();
    let attr = |child: &BinaryNode, name: &str| child.attrs.get(name).cloned();
    let mut effects = vec![];
    for child in node.all_children() {
        match child.tag.as_str() {
            "reaction" => effects.push(Effect::Reaction {
                id: from.clone(),
                server_id: attr(child, "message_id"),
                code: child.child_string("reaction"),
            }),
            "view" => {
                let count = child.content.as_ref().map(content_string);
                effects.push(Effect::View {
                    id: from.clone(),
                    server_id: attr(child, "message_id"),
                    count: parse_int(count.as_deref().filter(|s| !s.is_empty()).unwrap_or("0")),
                })
            }
            "participant" => effects.push(Effect::Participant {
                id: from.clone(),
                author: author.clone(),
                user: attr(child, "jid"),
                action: attr(child, "action"),
                new_role: attr(child, "role"),
            }),
            "update" => {
                if let Some(settings) = child.child("settings") {
                    let mut update = serde_json::Map::new();
                    for name in ["name", "description"] {
                        if let Some(content) = present(settings.child(name)) {
                            update.insert(name.into(), json!(content_string(content)));
                        }
                    }
                    effects.push(Effect::Settings {
                        id: from.clone(),
                        update: Value::Object(update),
                    });
                }
            }
            "message" => {
                let Some(content) = present(child.child("plaintext")) else {
                    continue;
                };
                let bytes = match content {
                    NodeContent::Text(text) => binary_bytes(text),
                    NodeContent::Bytes(bytes) => bytes.clone(),
                    // Child objects become zero bytes, which protobufjs reads as
                    // skipped field-zero varints: an empty message when they pair up.
                    NodeContent::Children(children) if children.len() % 2 == 0 => vec![],
                    NodeContent::Children(_) => continue,
                };
                let Ok(message) = proto::Message::decode(bytes.as_slice()) else {
                    tracing::error!("failed to decode plaintext newsletter message");
                    continue;
                };
                let timestamp = attr(child, "t").map_or(f64::NAN, |t| crate::privacy::number(&t));
                effects.push(Effect::Message(Box::new(proto::WebMessageInfo {
                    key: Some(proto::MessageKey {
                        remote_jid: from.clone(),
                        id: attr(child, "message_id")
                            .filter(|id| !id.is_empty())
                            .or_else(|| attr(child, "server_id")),
                        from_me: Some(false),
                        ..Default::default()
                    }),
                    message: Some(message),
                    message_timestamp: Some(crate::notifications::long(timestamp)),
                    ..Default::default()
                })));
            }
            _ => tracing::warn!(tag = %child.tag, "unknown newsletter notification child"),
        }
    }
    effects
}

/// Effects of a `mex` notification. `now_ms` supplies the default timelock expiry.
pub fn mex_notification(node: &BinaryNode, now_ms: f64) -> Result<Vec<Effect>, Error> {
    let Some(update) = node.child("update") else {
        return legacy_notification(node);
    };
    let Some(operation) = update.attrs.get("op_name").filter(|name| !name.is_empty()) else {
        return legacy_notification(node);
    };
    let Some(Ok(response)) = update
        .content
        .as_ref()
        .map(|content| serde_json::from_str::<Value>(&content_string(content)))
    else {
        return Ok(vec![]);
    };
    if response.is_null() {
        return Err(Error::Malformed);
    }
    // Any truthy `errors.length` discards the notification.
    let failed = match get(&response, "errors") {
        Some(Value::Array(errors)) => !errors.is_empty(),
        Some(Value::String(errors)) => !errors.is_empty(),
        Some(errors) => get(errors, "length").is_some_and(truthy),
        None => false,
    };
    let Some(data) = get(&response, "data").filter(|data| !failed && truthy(data)) else {
        return Ok(vec![]);
    };
    Ok(match operation.as_str() {
        "NotificationUserReachoutTimelockUpdate" => {
            let Some(payload) =
                get(data, "xwa2_notify_account_reachout_timelock").filter(|p| truthy(p))
            else {
                return Ok(vec![]);
            };
            if !get(payload, "is_active").is_some_and(truthy) {
                tracing::info!("reachout timelock restriction lifted");
                return Ok(vec![Effect::Timelock(ReachoutTimelock {
                    is_active: false,
                    time_enforcement_ends: None,
                    enforcement_type: ENFORCEMENT_DEFAULT.into(),
                })]);
            }
            let ends = get(payload, "time_enforcement_ends")
                .filter(|ends| truthy(ends))
                .map_or(now_ms + 60_000.0, |ends| {
                    parse_int(&js_string(ends)) * 1000.0
                });
            let kind = get(payload, "enforcement_type")
                .and_then(Value::as_str)
                .filter(|kind| ENFORCEMENT_TYPES.contains(kind))
                .unwrap_or(ENFORCEMENT_DEFAULT);
            tracing::info!(
                enforcement_type = kind,
                time_enforcement_ends = ends,
                "reachout timelock restriction set"
            );
            vec![Effect::Timelock(ReachoutTimelock {
                is_active: true,
                time_enforcement_ends: Some(ends),
                enforcement_type: kind.into(),
            })]
        }
        "MessageCappingInfoNotification" => {
            get(data, "xwa2_notify_new_chat_messages_capping_info_update")
                .filter(|payload| truthy(payload))
                .map(|payload| {
                    tracing::info!(%payload, "received message capping update");
                    Effect::MessageCapping(payload.clone())
                })
                .into_iter()
                .collect()
        }
        "NotificationNewsletterUpdate"
        | "NotificationLinkedProfilesUpdates"
        | "NotificationNewsletterAdminPromote"
        | "NotificationNewsletterAdminDemote"
        | "NotificationNewsletterUserSettingChange"
        | "NotificationNewsletterJoin"
        | "NotificationNewsletterLeave"
        | "NotificationNewsletterStateChange"
        | "NotificationNewsletterAdminMetadataUpdate"
        | "NotificationNewsletterOwnerUpdate"
        | "NotificationNewsletterAdminInviteRevoke"
        | "NotificationNewsletterWamoSubStatusChange"
        | "NotificationNewsletterBlockUser"
        | "NotificationNewsletterPaidPartnership"
        | "NotificationNewsletterMilestone"
        | "NewsletterResponseStateUpdate" => legacy_notification(node)?,
        _ => vec![],
    })
}
// The older payload shape: JSON in a <mex> child, an <update> child or the first child.
fn legacy_notification(node: &BinaryNode) -> Result<Vec<Effect>, Error> {
    let mex = node.child("mex").filter(|mex| present(Some(mex)).is_some());
    let Some(payload) = mex.or_else(|| node.child("update").or(node.all_children().first())) else {
        return Ok(vec![]);
    };
    let bytes = match present(Some(payload)) {
        Some(NodeContent::Text(text)) => binary_bytes(text),
        Some(NodeContent::Bytes(bytes)) => bytes.clone(),
        _ => return Ok(vec![]),
    };
    let Ok(data) = serde_json::from_str::<Value>(&String::from_utf8_lossy(&bytes)) else {
        return Ok(vec![]);
    };
    let operation = get(&data, "operation")
        .filter(|operation| !operation.is_null())
        .cloned()
        .or_else(|| payload.attrs.get("op_name").map(|name| json!(name)));
    let updates = get(&data, "updates")
        .filter(|updates| truthy(updates))
        .cloned()
        .or_else(|| {
            get(&data, "data")
                .and_then(|data| get(data, "xwa2_notify_linked_profiles"))
                .filter(|profiles| truthy(profiles))
                .map(|profiles| json!([profiles]))
        });
    let (Some(updates), Some(operation)) = (updates, operation.filter(truthy)) else {
        return Ok(vec![]);
    };
    tracing::info!(%operation, "got mex newsletter notification");
    let operation = operation.as_str().unwrap_or_default();
    if !matches!(
        operation,
        "NotificationNewsletterUpdate"
            | "NotificationNewsletterAdminPromote"
            | "NotificationLinkedProfilesUpdates"
    ) {
        tracing::info!(operation, "unhandled mex newsletter notification");
        return Ok(vec![]);
    }
    // for..of accepts arrays and strings; string characters carry no fields.
    let updates = match updates {
        Value::Array(updates) => updates,
        Value::String(text) => text.encode_utf16().map(|_| json!("")).collect(),
        _ => return Err(Error::Malformed),
    };
    let field = |update: &Value, key: &str| get(update, key).filter(|v| truthy(v)).map(js_string);
    let mut effects = vec![];
    for update in &updates {
        match operation {
            "NotificationNewsletterUpdate" => {
                if update.is_null() {
                    return Err(Error::Malformed);
                }
                let settings = get(update, "settings").filter(|settings| match settings {
                    Value::Object(map) => !map.is_empty(),
                    Value::Array(list) => !list.is_empty(),
                    Value::String(text) => !text.is_empty(),
                    _ => false,
                });
                if let (Some(id), Some(settings)) = (field(update, "jid"), settings) {
                    effects.push(Effect::Settings {
                        id: Some(id),
                        update: settings.clone(),
                    });
                }
            }
            "NotificationNewsletterAdminPromote" => {
                if update.is_null() {
                    return Err(Error::Malformed);
                }
                if let (Some(id), Some(user)) = (field(update, "jid"), field(update, "user")) {
                    effects.push(Effect::Participant {
                        id: Some(id),
                        author: node.attrs.get("from").cloned(),
                        user: Some(user),
                        action: Some("promote".into()),
                        new_role: Some("ADMIN".into()),
                    });
                }
            }
            _ => {
                let lid = field(update, "jid");
                let mut mappings = vec![];
                let profiles = get(update, "added_profiles").and_then(Value::as_array);
                for profile in profiles.into_iter().flatten() {
                    let pn = match profile {
                        Value::String(pn) => Some(pn.clone()),
                        profile => ["pn", "jid"]
                            .into_iter()
                            .find_map(|key| get(profile, key).filter(|v| !v.is_null()))
                            .filter(|pn| truthy(pn))
                            .map(js_string),
                    };
                    if let (Some(lid), Some(pn)) = (&lid, pn.filter(|pn| !pn.is_empty())) {
                        effects.push(Effect::LidMapping {
                            lid: lid.clone(),
                            pn: pn.clone(),
                        });
                        mappings.push((lid.clone(), pn));
                    }
                }
                effects.push(Effect::StoreMappings(mappings));
            }
        }
    }
    Ok(effects)
}
