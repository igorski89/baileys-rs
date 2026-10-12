use baileys_rs::{
    app_state::{self, ChatModification, LabelBody, LastMessages, QuickReply, StarredMessage},
    auth::MinimalMessage,
    proto::{self, sync_action_value as action},
};
use prost::Message;
use serde_json::Value;
fn text(v: &Value) -> Option<String> {
    v.as_str().map(str::to_owned)
}
fn key(v: &Value) -> proto::MessageKey {
    proto::MessageKey {
        remote_jid: text(&v["remoteJid"]),
        from_me: v["fromMe"].as_bool(),
        id: text(&v["id"]),
        participant: text(&v["participant"]),
    }
}
fn last(v: &Value) -> Option<LastMessages> {
    match v {
        Value::Array(list) => Some(LastMessages::List(
            list.iter()
                .map(|m| MinimalMessage {
                    key: Some(key(&m["key"])),
                    message_timestamp: m["messageTimestamp"].as_u64(),
                })
                .collect(),
        )),
        Value::Object(_) => Some(LastMessages::Range(action::SyncActionMessageRange {
            last_message_timestamp: v["lastMessageTimestamp"].as_i64(),
            last_system_message_timestamp: v["lastSystemMessageTimestamp"].as_i64(),
            messages: v["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|m| action::SyncActionMessage {
                    key: Some(key(&m["key"])),
                    timestamp: m["timestamp"].as_i64(),
                })
                .collect(),
        })),
        _ => None,
    }
}
// Typed modifications cannot express an empty or unknown JavaScript object.
fn modification(v: &Value) -> Option<ChatModification> {
    let object = v.as_object().unwrap();
    let last_messages = last(&v["lastMessages"]);
    // Branch order follows the source's `in` checks.
    Some(if object.contains_key("mute") {
        ChatModification::Mute(v["mute"].as_i64())
    } else if object.contains_key("archive") {
        ChatModification::Archive {
            archive: v["archive"].as_bool().unwrap(),
            last_messages,
        }
    } else if object.contains_key("markRead") {
        ChatModification::MarkRead {
            read: v["markRead"].as_bool().unwrap(),
            last_messages,
        }
    } else if let Some(d) = object.get("deleteForMe") {
        ChatModification::DeleteForMe {
            delete_media: d["deleteMedia"].as_bool().unwrap(),
            key: key(&d["key"]),
            timestamp: d["timestamp"].as_i64().unwrap(),
        }
    } else if object.contains_key("clear") {
        ChatModification::Clear { last_messages }
    } else if object.contains_key("pin") {
        ChatModification::Pin(v["pin"].as_bool().unwrap())
    } else if let Some(c) = object.get("contact") {
        ChatModification::Contact(c.as_object().map(|_| action::ContactAction {
            full_name: text(&c["fullName"]),
            first_name: text(&c["firstName"]),
            lid_jid: text(&c["lidJid"]),
            save_on_primary_addressbook: c["saveOnPrimaryAddressbook"].as_bool(),
            ..Default::default()
        }))
    } else if let Some(d) = object.get("disableLinkPreviews") {
        ChatModification::DisableLinkPreviews(action::PrivacySettingDisableLinkPreviewsAction {
            is_previews_disabled: d["isPreviewsDisabled"].as_bool(),
        })
    } else if let Some(s) = object.get("star") {
        ChatModification::Star {
            messages: s["messages"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| StarredMessage {
                    id: text(&m["id"]).unwrap(),
                    from_me: m["fromMe"].as_bool(),
                })
                .collect(),
            star: s["star"].as_bool().unwrap(),
        }
    } else if object.contains_key("delete") {
        ChatModification::Delete { last_messages }
    } else if let Some(name) = object.get("pushNameSetting") {
        ChatModification::PushNameSetting(text(name).unwrap())
    } else if let Some(q) = object.get("quickReply") {
        ChatModification::QuickReply(QuickReply {
            shortcut: text(&q["shortcut"]),
            message: text(&q["message"]),
            timestamp: text(&q["timestamp"]),
            deleted: q["deleted"].as_bool(),
        })
    } else if let Some(l) = object.get("addLabel") {
        ChatModification::AddLabel(LabelBody {
            id: text(&l["id"]).unwrap(),
            name: text(&l["name"]),
            color: l["color"].as_i64().map(|n| n as i32),
            predefined_id: l["predefinedId"].as_i64().map(|n| n as i32),
            deleted: l["deleted"].as_bool(),
        })
    } else if let Some(l) = object.get("addChatLabel") {
        ChatModification::AddChatLabel {
            label_id: text(&l["labelId"]).unwrap(),
        }
    } else if let Some(l) = object.get("removeChatLabel") {
        ChatModification::RemoveChatLabel {
            label_id: text(&l["labelId"]).unwrap(),
        }
    } else if let Some(l) = object.get("addMessageLabel") {
        ChatModification::AddMessageLabel {
            message_id: text(&l["messageId"]).unwrap(),
            label_id: text(&l["labelId"]).unwrap(),
        }
    } else {
        let l = object.get("removeMessageLabel")?;
        ChatModification::RemoveMessageLabel {
            message_id: text(&l["messageId"]).unwrap(),
            label_id: text(&l["labelId"]).unwrap(),
        }
    })
}
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
#[test]
fn chat_modifications_match_original_patches_and_errors() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/chat_modifications.json")).unwrap();
    let now = fixture["now"].as_i64().unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 347);
    let mut unsupported = 0;
    for case in cases {
        let Some(input) = modification(&case["mod"]) else {
            assert_eq!(case["error"]["message"], "not supported");
            unsupported += 1;
            continue;
        };
        let result = app_state::chat_modification_patch(input, case["jid"].as_str().unwrap(), now);
        match (&case["expected"], result) {
            (Value::Null, Err(app_state::Error::Modification(message))) => {
                assert_eq!(case["error"]["message"], message, "{case}");
                assert_eq!(case["error"]["status"], 400, "{case}");
            }
            (expected, Ok(patch)) if expected.is_object() => {
                assert_eq!(expected["type"], patch.name.as_str(), "{case}");
                assert_eq!(expected["index"], serde_json::json!(patch.index), "{case}");
                assert_eq!(expected["apiVersion"], patch.api_version, "{case}");
                assert_eq!(expected["operation"], patch.operation, "{case}");
                assert_eq!(
                    patch.sync_action,
                    proto::SyncActionValue::decode(
                        unhex(expected["action"].as_str().unwrap()).as_slice()
                    )
                    .unwrap(),
                    "{case}"
                );
            }
            (_, result) => panic!("unexpected result {result:?} for {case}"),
        }
    }
    assert_eq!(unsupported, 2);
}
