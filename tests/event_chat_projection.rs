use baileys_rs::{
    app_state::ChatCondition, event_buffer::Buffer, history::Chat, messages::own::ChatUpdate,
    proto, proto_json::JsonField,
};
use serde_json::{Value, json};
use std::time::Duration;
#[path = "support/json_numbers.rs"]
mod numbers;

#[test]
fn typed_chat_projections_retain_all_original_buffer_outputs() {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/event_buffer.json")).unwrap();
    let mut histories = 0;
    let mut updates = 0;
    for case in fixtures["cases"].as_array().unwrap() {
        for step in case["steps"].as_array().unwrap() {
            for batch in step["emitted"].as_array().unwrap() {
                if let Some(chats) = batch["messaging-history.set"]["chats"].as_array() {
                    for value in chats {
                        let chat = Chat::from_value(value).unwrap();
                        assert_eq!(
                            numbers::canonical(&chat.to_value()),
                            numbers::canonical(value)
                        );
                        histories += 1;
                    }
                }
                if let Some(chats) = batch["chats.update"].as_array() {
                    for value in chats {
                        let chat = ChatUpdate::from_value(value).unwrap();
                        assert_eq!(
                            numbers::canonical(&chat.to_value()),
                            numbers::canonical(value)
                        );
                        updates += 1;
                    }
                }
            }
        }
    }
    assert_eq!((histories, updates), (271, 79));
}

#[test]
fn history_absorbs_typed_conditions_and_runtime_values_without_protocol_wrapping() {
    let key = proto::MessageKey {
        remote_jid: Some("200@s.whatsapp.net".into()),
        id: Some("M".into()),
        from_me: Some(false),
        ..Default::default()
    };
    let message = proto::WebMessageInfo {
        key: Some(key),
        message_timestamp: Some(u64::MAX),
        message: Some(proto::Message {
            conversation: Some("full message body".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut buffer = Buffer::new();
    buffer.buffer(Duration::ZERO);
    let update = ChatUpdate {
        id: "200@s.whatsapp.net".into(),
        pinned: Some(Some(1_791_799_695_000.0)),
        unread_count: Some(-1),
        clear_archived: true,
        ephemeral_expiration: Some(None),
        conditional: Some(ChatCondition {
            id: "200@s.whatsapp.net".into(),
            message_range: None,
        }),
        ..Default::default()
    };
    buffer.emit_native_chat_updates(vec![update]).unwrap();
    let chat = Chat {
        conversation: proto::Conversation {
            id: "200@s.whatsapp.net".into(),
            messages: vec![proto::HistorySyncMsg {
                message: Some(message.clone()),
                msg_order_id: None,
            }],
            ..Default::default()
        },
        last_message_recv_timestamp: Some(5.0),
        runtime_fields: Default::default(),
    };
    buffer
        .emit(
            "messaging-history.set",
            json!({"chats":[chat.to_value()], "contacts":[], "messages":[]}),
        )
        .unwrap();
    let (_, batches) = buffer.flush();
    let value = &batches[0].events["messaging-history.set"]["chats"][0];
    let chat = Chat::from_value(value).unwrap();
    assert_eq!(chat.conversation.pinned, None);
    assert_eq!(chat.conversation.unread_count, None);
    assert_eq!(chat.conversation.archived, None);
    assert_eq!(chat.runtime_fields["pinned"], json!(1_791_799_695_000.0));
    assert_eq!(chat.runtime_fields["unreadCount"], json!(-1));
    assert_eq!(chat.runtime_fields["archived"], Value::Null);
    assert_eq!(chat.runtime_fields["ephemeralExpiration"], Value::Null);
    assert_eq!(
        chat.conversation.messages[0].message.as_ref(),
        Some(&message)
    );
    assert_eq!(chat.last_message_recv_timestamp, Some(5.0));
    assert_eq!(chat.to_value(), *value);
}

#[test]
fn update_projection_distinguishes_missing_null_and_runtime_extensions() {
    let value = json!({
        "id":"200@s.whatsapp.net", "name":null, "description":null,
        "unreadCount":null, "archived":null, "muteEndTime":null,
        "pinned":1_791_799_695_000.0, "ephemeralExpiration":null,
        "unreadExtension":{"count":-1},
        "messages":[{"message":{"key":{"id":"M"},"message":{"conversation":"body"}}, "futureWrapperField":true}]
    });
    let update = ChatUpdate::from_value(&value).unwrap();
    assert_eq!(update.name, Some(None));
    assert!(update.clear_unread_count && update.clear_archived);
    assert_eq!(update.mute_end_time, Some(None));
    assert_eq!(update.pinned, Some(Some(1_791_799_695_000.0)));
    assert_eq!(
        update.messages.as_ref().unwrap()[0]
            .message
            .as_ref()
            .unwrap()
            .conversation
            .as_deref(),
        Some("body")
    );
    assert_eq!(update.to_value(), value);
    let empty = ChatUpdate::from_value(&json!({"id":"200@s.whatsapp.net"})).unwrap();
    assert_eq!(empty.name, None);
    assert!(!empty.clear_unread_count);
    assert!(ChatUpdate::from_value(&json!({"id":null})).is_err());
    assert!(Chat::from_value(&json!({"id":42})).is_err());
    let fractional = json!({"id":"200@s.whatsapp.net", "unreadCount":1.25});
    assert_eq!(
        ChatUpdate::from_value(&fractional).unwrap().to_value(),
        fractional
    );
    assert_eq!(
        ChatUpdate::from_value(&json!({"id":"x", "unreadCount":2.0}))
            .unwrap()
            .unread_count,
        Some(2)
    );
}
