//! Socket events in the shape of upstream's event map, as JSON.
use super::super::{ConnectionState, Error, Event};
use super::projection::{self, Action, GROUP, META};
use crate::{
    app_state::SettingUpdate,
    messages::group_effects::{Author, GroupEvent},
    proto,
    proto_json::{self, JsonField},
};
use serde_json::{Value, json};

impl Event {
    /// The upstream event-map entries this event corresponds to, as
    /// `(event name, JSON payload)` pairs in emission order.
    ///
    /// Payloads follow `JSON.stringify` of upstream's values: bytes are
    /// BufferJSON objects, dates are ISO strings, undefined fields are absent
    /// and Boom errors keep only their enumerable fields. Raw stanzas
    /// (`Event::Node`) have no counterpart and produce no entry;
    /// `Event::CredentialsUpdated` carries no fields, so its `creds.update`
    /// payload is empty.
    pub fn upstream(&self) -> Result<Vec<(&'static str, Value)>, Error> {
        let mut entries = vec![];
        for action in projection::project(self.clone())? {
            match action {
                Action::Payload(name, mut value) => {
                    public_payload(name, &mut value);
                    entries.push((name, value));
                }
                Action::Chats(updates) => entries.push((
                    "chats.update",
                    json!(updates.iter().map(chat_update).collect::<Vec<_>>()),
                )),
                Action::Native(event) => entries.extend(native(&event)?),
            }
        }
        Ok(entries)
    }
}

fn native(event: &Event) -> Result<Vec<(&'static str, Value)>, Error> {
    let entry = match event {
        Event::Connection(ConnectionState::Connecting) => (
            "connection.update",
            json!({"connection":"connecting","receivedPendingNotifications":false}),
        ),
        Event::Connection(ConnectionState::Open) => {
            ("connection.update", json!({"connection":"open"}))
        }
        Event::Connection(ConnectionState::Closed { code }) => (
            "connection.update",
            json!({"connection":"close","lastDisconnect":{
                "error":boom(*code, Value::Null),
                "date":iso_date(now_ms()),
            }}),
        ),
        Event::Online(online) => ("connection.update", json!({ "isOnline": online })),
        Event::Qr(qr) => ("connection.update", json!({ "qr": qr })),
        Event::PairingComplete => ("connection.update", json!({"isNewLogin":true})),
        Event::PendingNotifications => (
            "connection.update",
            json!({"receivedPendingNotifications":true}),
        ),
        Event::ReachoutTimelockUpdated(state) => {
            let mut lock = json!({"isActive":state.is_active});
            if let Some(ends) = state.time_enforcement_ends {
                lock["timeEnforcementEnds"] = if ends.is_finite() {
                    json!(iso_date(ends as i64))
                } else {
                    Value::Null
                };
            }
            lock["enforcementType"] = json!(state.enforcement_type);
            ("connection.update", json!({ "reachoutTimeLock": lock }))
        }
        Event::CredentialsUpdated => ("creds.update", json!({})),
        Event::Node(_) => return Ok(vec![]),
        Event::PresenceUpdated(update) => ("presence.update", serialize(update)?),
        Event::Calls(calls) => (
            "call",
            json!(
                calls
                    .iter()
                    .map(|call| {
                        let mut value = serialize(call)?;
                        let fields = value.as_object_mut().expect("call object");
                        fields.remove("dateMs");
                        let date = call.date_ms.map_or(Value::Null, |ms| json!(iso_date(ms)));
                        let mut ordered = serde_json::Map::new();
                        for (name, field) in std::mem::take(fields) {
                            if name == "isVideo" || name == "status" {
                                ordered.entry("date").or_insert(date.clone());
                            }
                            ordered.insert(name, field);
                        }
                        ordered.entry("date").or_insert(date);
                        Ok(Value::Object(ordered))
                    })
                    .collect::<Result<Vec<_>, Error>>()?
            ),
        ),
        Event::MediaUpdated(updates) => (
            "messages.media-update",
            json!(
                updates
                    .iter()
                    .map(|update| {
                        let mut value = json!({"key":update.key.to_value()});
                        if let Some(media) = &update.media {
                            value["media"] = json!({
                                "ciphertext":proto_json::bytes_to_value(&media.ciphertext),
                                "iv":proto_json::bytes_to_value(&media.iv),
                            });
                        }
                        if let Some(error) = &update.error {
                            value["error"] = boom(error.status, json!(error.attrs));
                        }
                        value
                    })
                    .collect::<Vec<_>>()
            ),
        ),
        Event::LabelEdited(label) => {
            let mut value = json!({"name":label.name,"color":label.color,"deleted":label.deleted});
            if let Some(id) = &label.id {
                value["id"] = json!(id);
            }
            if let Some(id) = &label.predefined_id {
                value["predefinedId"] = json!(id);
            }
            ("labels.edit", value)
        }
        Event::LabelAssociationUpdated(label) => {
            let mut association =
                json!({"type":if label.is_chat {"label_jid"} else {"label_message"}});
            if let Some(id) = &label.chat_id {
                association["chatId"] = json!(id);
            }
            if let Some(id) = &label.label_id {
                association["labelId"] = json!(id);
            }
            if let Some(id) = &label.message_id {
                association["messageId"] = json!(id);
            }
            (
                "labels.association",
                json!({"type":if label.add {"add"} else {"remove"},"association":association}),
            )
        }
        Event::SettingUpdated(setting) => ("settings.update", setting_value(setting)),
        Event::LidMappingUpdated { lid, pn } => {
            let mut value = json!({ "pn": pn });
            if let Some(lid) = lid {
                value["lid"] = json!(lid);
            }
            ("lid-mapping.update", value)
        }
        Event::ChatLocked(lock) => {
            let mut value = json!({"locked":lock.locked});
            if let Some(id) = &lock.id {
                value["id"] = json!(id);
            }
            ("chats.lock", value)
        }
        Event::GroupsUpserted(groups) => (
            "groups.upsert",
            json!(
                groups
                    .iter()
                    .map(|group| {
                        let mut value = serialize(&group.metadata)?;
                        for (name, field) in [
                            ("author", &group.author),
                            ("authorPn", &group.author_pn),
                            ("authorUsername", &group.author_username),
                        ] {
                            if let Some(field) = field {
                                value[name] = json!(field);
                            }
                        }
                        Ok(value)
                    })
                    .collect::<Result<Vec<_>, Error>>()?
            ),
        ),
        Event::BlocklistUpdated { blocklist, add } => (
            "blocklist.update",
            json!({"blocklist":blocklist,"type":if *add {"add"} else {"remove"}}),
        ),
        Event::NewsletterReaction {
            id,
            server_id,
            code,
        } => {
            let mut reaction = json!({});
            if let Some(code) = code {
                reaction["code"] = json!(code);
            }
            reaction["count"] = json!(1);
            let mut value = json!({});
            optional(&mut value, "id", id);
            optional(&mut value, "server_id", server_id);
            value["reaction"] = reaction;
            ("newsletter.reaction", value)
        }
        Event::NewsletterView {
            id,
            server_id,
            count,
        } => {
            let mut value = json!({});
            optional(&mut value, "id", id);
            optional(&mut value, "server_id", server_id);
            value["count"] = if count.is_finite() {
                json!(count)
            } else {
                Value::Null
            };
            ("newsletter.view", value)
        }
        Event::NewsletterParticipantsUpdated {
            id,
            author,
            user,
            new_role,
            action,
        } => {
            let mut value = json!({});
            optional(&mut value, "id", id);
            optional(&mut value, "author", author);
            optional(&mut value, "user", user);
            optional(&mut value, "new_role", new_role);
            optional(&mut value, "action", action);
            ("newsletter-participants.update", value)
        }
        Event::NewsletterSettingsUpdated { id, update } => {
            let mut value = json!({});
            optional(&mut value, "id", id);
            value["update"] = update.clone();
            ("newsletter-settings.update", value)
        }
        Event::MessageCappingUpdated(value) => ("message-capping.update", value.clone()),
        Event::GroupEffects(effects) => {
            return Ok(effects.iter().map(group_event).collect());
        }
        Event::MessagingHistoryStatus(status) => ("messaging-history.status", serialize(status)?),
        // Every other variant is projected before it reaches this function.
        other => {
            return Err(Error::EventProjection(format!(
                "no upstream projection for {other:?}"
            )));
        }
    };
    Ok(vec![entry])
}

fn serialize<T: serde::Serialize>(value: &T) -> Result<Value, Error> {
    serde_json::to_value(value).map_err(|e| Error::EventProjection(e.to_string()))
}

fn optional(value: &mut Value, name: &str, field: &Option<String>) {
    if let Some(field) = field {
        value[name] = json!(field);
    }
}

/// Upstream messages carry the stanza category and retry count as fields.
fn public_message(message: &mut Value) {
    let Some(fields) = message.as_object_mut() else {
        return;
    };
    if let Some(metadata) = fields.remove(META) {
        if let Some(key) = fields.get_mut("key").and_then(Value::as_object_mut) {
            if key.get("isViewOnce") == Some(&Value::Bool(false)) {
                key.remove("isViewOnce");
            }
            if let Some(id) = key.remove("serverId") {
                key.insert("server_id".into(), id);
            }
        }
        if let Some(category) = metadata.get("category").filter(|v| !v.is_null()) {
            fields.insert("category".into(), category.clone());
        }
        if let Some(count) = metadata.get("retry_count").filter(|v| !v.is_null()) {
            fields.insert("retryCount".into(), count.clone());
        }
    }
}

fn public_payload(name: &str, value: &mut Value) {
    match name {
        "messages.upsert" | "messaging-history.set" => {
            if let Some(messages) = value.get_mut("messages").and_then(Value::as_array_mut) {
                messages.iter_mut().for_each(public_message);
            }
        }
        "groups.update" => {
            if let Some(groups) = value.as_array_mut() {
                for group in groups.iter_mut().filter_map(Value::as_object_mut) {
                    if group.remove(GROUP).is_some() {
                        // A stub without its parameter leaves the field undefined.
                        for name in ["subject", "desc", "inviteCode"] {
                            if group.get(name).is_some_and(Value::is_null) {
                                group.remove(name);
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn chat_update(update: &crate::messages::own::ChatUpdate) -> Value {
    let mut value = update.to_value();
    let fields = value.as_object_mut().expect("chat update object");
    // A source-assigned undefined name or description is dropped by JSON.
    for name in ["name", "description"] {
        if fields.get(name).is_some_and(Value::is_null) {
            fields.remove(name);
        }
    }
    value
}

fn author(value: &mut Value, author: &Author) {
    optional(value, "author", &author.jid);
    optional(value, "authorPn", &author.pn);
    optional(value, "authorUsername", &author.username);
}

fn group_event(event: &GroupEvent) -> (&'static str, Value) {
    match event {
        GroupEvent::Participants {
            id,
            author: by,
            action,
            participants,
        } => {
            let mut value = json!({ "id": id });
            author(&mut value, by);
            value["participants"] = json!(participants);
            value["action"] = json!(action.as_str());
            ("group-participants.update", value)
        }
        GroupEvent::Changed { id, author: by, .. } => {
            // Projected as groups.update; kept for completeness.
            let mut value = json!({ "id": id });
            author(&mut value, by);
            ("groups.update", json!([value]))
        }
        GroupEvent::JoinRequest {
            id,
            author: by,
            participant,
            participant_pn,
            action,
            method,
        } => {
            let mut value = json!({ "id": id });
            author(&mut value, by);
            if let Some(v) = participant {
                value["participant"] = v.clone();
            }
            if let Some(v) = participant_pn {
                value["participantPn"] = v.clone();
            }
            optional(&mut value, "action", action);
            optional(&mut value, "method", method);
            ("group.join-request", value)
        }
        GroupEvent::MemberTag {
            group_id,
            label,
            participant,
            participant_alt,
            message_timestamp,
        } => {
            let mut value = json!({ "groupId": group_id });
            optional(&mut value, "participant", participant);
            optional(&mut value, "participantAlt", participant_alt);
            value["label"] = json!(label);
            value["messageTimestamp"] = if message_timestamp.is_finite() {
                json!(*message_timestamp as i64)
            } else {
                Value::Null
            };
            ("group.member-tag.update", value)
        }
    }
}

fn setting_value(setting: &SettingUpdate) -> Value {
    let (name, value) = match setting {
        SettingUpdate::Locale(locale) => ("locale", json!(locale)),
        SettingUpdate::NotificationActivity(value) => ("notificationActivitySetting", json!(value)),
        SettingUpdate::TimeFormat(format) => {
            let mut value = json!({});
            if let Some(enabled) = format.is_twenty_four_hour_format_enabled {
                value["isTwentyFourHourFormatEnabled"] = json!(enabled);
            }
            ("timeFormat", value)
        }
        SettingUpdate::RelayAllCalls(relay) => {
            let mut value = json!({});
            if let Some(enabled) = relay.is_enabled {
                value["isEnabled"] = json!(enabled);
            }
            ("privacySettingRelayAllCalls", value)
        }
        SettingUpdate::DisableLinkPreviews(previews) => {
            let mut value = json!({});
            if let Some(disabled) = previews.is_previews_disabled {
                value["isPreviewsDisabled"] = json!(disabled);
            }
            ("disableLinkPreviews", value)
        }
        SettingUpdate::ChannelsPersonalisedRecommendation(recommendation) => {
            let mut value = json!({});
            if let Some(opted_out) = recommendation.is_user_opted_out {
                value["isUserOptedOut"] = json!(opted_out);
            }
            ("channelsPersonalisedRecommendation", value)
        }
        SettingUpdate::StatusPrivacy(privacy) => {
            let mut value = json!({});
            // The value is a decoded message, so JSON.stringify writes enum names.
            if let Some(mode) = privacy.mode {
                value["mode"] = proto::sync_action_value::status_privacy_action::StatusDistributionMode::try_from(mode)
                    .map_or_else(|_| json!(mode), |mode| json!(mode.as_str_name()));
            }
            if !privacy.user_jid.is_empty() {
                value["userJid"] = json!(privacy.user_jid);
            }
            ("statusPrivacy", value)
        }
    };
    json!({"setting":name,"value":value})
}

/// The enumerable fields of a Boom error, which is what `JSON.stringify` keeps.
fn boom(status: u16, data: Value) -> Value {
    let error = match status {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        428 => "Precondition Required",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    };
    json!({
        "data":data,
        "isBoom":true,
        "isServer":status >= 500,
        "output":{"statusCode":status,"payload":{"statusCode":status,"error":error,"message":if status == 500 {"An internal server error occurred"} else {""}},"headers":{}},
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// `Date.prototype.toJSON` for a millisecond timestamp.
fn iso_date(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rest = ms.rem_euclid(86_400_000);
    // Howard Hinnant's civil-from-days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let year = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else if year < 0 {
        format!("-{:06}", -year)
    } else {
        format!("+{year:06}")
    };
    format!(
        "{year}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3_600_000,
        rest / 60_000 % 60,
        rest / 1000 % 60,
        rest % 1000
    )
}
