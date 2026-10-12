use baileys_rs::{app_state::*, auth::Contact, proto};
use serde_json::{Value, json};
pub fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn key(k: &proto::MessageKey) -> Value {
    let mut v = json!({"fromMe":k.from_me.unwrap_or(false)});
    if let Some(s) = &k.remote_jid {
        v["remoteJid"] = json!(s)
    }
    if let Some(s) = &k.id {
        v["id"] = json!(s)
    }
    v
}
pub fn normalized(event: &ActionEvent, me: &Contact) -> Value {
    let (name, data) = match event {
        ActionEvent::Chat(c) => {
            let mut v = json!({"id":c.id});
            if let Some(x) = c.mute_end_time {
                v["muteEndTime"] = json!(x)
            }
            if let Some(x) = c.pinned {
                v["pinned"] = json!(x)
            }
            if let Some(x) = c.archived {
                v["archived"] = json!(x)
            }
            if c.clear_archived {
                v["archived"] = Value::Null
            }
            if let Some(x) = c.unread_count {
                v["unreadCount"] = json!(x)
            }
            if c.clear_unread_count {
                v["unreadCount"] = Value::Null
            }
            ("chats.update", json!([v]))
        }
        ActionEvent::Contacts(c) => ("contacts.upsert", json!(c)),
        ActionEvent::DeleteMessages(keys) => (
            "messages.delete",
            json!({"keys":keys.iter().map(key).collect::<Vec<_>>()}),
        ),
        ActionEvent::Stars(updates) => (
            "messages.update",
            json!(
                updates
                    .iter()
                    .map(|u| json!({"key":key(&u.key),"update":{"starred":u.starred}}))
                    .collect::<Vec<_>>()
            ),
        ),
        ActionEvent::DeleteChats(ids) => ("chats.delete", json!(ids)),
        ActionEvent::PushName(name) => {
            let mut me = me.clone();
            me.name = Some(name.clone());
            ("creds.update", json!({"me":me}))
        }
        ActionEvent::UnarchiveChats(value) => (
            "creds.update",
            json!({"accountSettings":{"unarchiveChats":value}}),
        ),
        ActionEvent::LabelEdit(l) => {
            let mut v = json!({"name":l.name,"color":l.color,"deleted":l.deleted});
            if let Some(id) = &l.id {
                v["id"] = json!(id)
            }
            if let Some(id) = &l.predefined_id {
                v["predefinedId"] = json!(id)
            }
            ("labels.edit", v)
        }
        ActionEvent::LabelAssociation(l) => {
            let mut a = json!({"type":if l.is_chat{"label_jid"}else{"label_message"}});
            if let Some(s) = &l.chat_id {
                a["chatId"] = json!(s)
            }
            if let Some(s) = &l.label_id {
                a["labelId"] = json!(s)
            }
            if let Some(s) = &l.message_id {
                a["messageId"] = json!(s)
            }
            (
                "labels.association",
                json!({"type":if l.add{"add"}else{"remove"},"association":a}),
            )
        }
        ActionEvent::LidMapping { lid, pn } => {
            let mut v = json!({"pn":pn});
            if let Some(lid) = lid {
                v["lid"] = json!(lid)
            }
            ("lid-mapping.update", v)
        }
        ActionEvent::Lock(c) => {
            let mut v = json!({"locked":c.locked});
            if let Some(id) = &c.id {
                v["id"] = json!(id)
            }
            ("chats.lock", v)
        }
        ActionEvent::Setting(s) => {
            let (setting, value) = match s {
                SettingUpdate::Locale(s) => ("locale", json!(s)),
                SettingUpdate::NotificationActivity(s) => ("notificationActivitySetting", json!(s)),
                SettingUpdate::TimeFormat(s) => {
                    let mut v = json!({});
                    if let Some(b) = s.is_twenty_four_hour_format_enabled {
                        v["isTwentyFourHourFormatEnabled"] = json!(b)
                    }
                    ("timeFormat", v)
                }
                SettingUpdate::RelayAllCalls(s) => {
                    let mut v = json!({});
                    if let Some(b) = s.is_enabled {
                        v["isEnabled"] = json!(b)
                    }
                    ("privacySettingRelayAllCalls", v)
                }
                SettingUpdate::DisableLinkPreviews(s) => {
                    let mut v = json!({});
                    if let Some(b) = s.is_previews_disabled {
                        v["isPreviewsDisabled"] = json!(b)
                    }
                    ("disableLinkPreviews", v)
                }
                SettingUpdate::ChannelsPersonalisedRecommendation(s) => {
                    let mut v = json!({});
                    if let Some(b) = s.is_user_opted_out {
                        v["isUserOptedOut"] = json!(b)
                    }
                    ("channelsPersonalisedRecommendation", v)
                }
                SettingUpdate::StatusPrivacy(s) => {
                    let mut v = json!({});
                    if !s.user_jid.is_empty() {
                        v["userJid"] = json!(s.user_jid);
                    }
                    if let Some(mode) = s.mode {
                        v["mode"] = proto::sync_action_value::status_privacy_action::StatusDistributionMode::try_from(mode).map(|mode|json!(mode.as_str_name())).unwrap_or_else(|_|json!(mode))
                    }
                    ("statusPrivacy", v)
                }
            };
            ("settings.update", json!({"setting":setting,"value":value}))
        }
    };
    let mut v = json!({"event":name,"data":data});
    if let ActionEvent::Chat(c) = event
        && let Some(condition) = &c.conditional
    {
        v["conditions"] = json!([
            condition.evaluate(None, None),
            condition.evaluate(None, Some(None)),
            condition.evaluate(Some(Some(0)), None),
            condition.evaluate(Some(Some(10)), None),
            condition.evaluate(Some(Some(50)), None),
            condition.evaluate(Some(Some(10)), Some(Some(100)))
        ]);
    }
    v
}
