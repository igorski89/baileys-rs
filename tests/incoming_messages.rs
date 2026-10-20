use baileys_rs::{
    binary::{BinaryNode, NodeContent},
    messages::{
        self,
        incoming::{self, Plan},
        own,
    },
    proto,
};
use prost::Message as _;
use serde_json::{Value, json};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/incoming_messages.json")).unwrap()
}
fn hex(v: &Value) -> Vec<u8> {
    v.as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn wire(v: Vec<u8>) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    for (k, v) in v["attrs"].as_object().unwrap() {
        n = n.attr(k, v.as_str().unwrap());
    }
    if let Some(c) = v["content"].as_array() {
        n = n.with_content(NodeContent::Children(c.iter().map(node).collect()));
    }
    n
}
#[test]
fn cleaned_keys_classifiers_and_common_receive_effects_match_original() {
    for case in fixture()["cleanup"].as_array().unwrap() {
        let mut message = proto::WebMessageInfo::decode(hex(&case["input"]).as_slice()).unwrap();
        incoming::clean(&mut message, "100:7@s.whatsapp.net", "1000:7@lid").unwrap();
        assert_eq!(message.encode_to_vec(), hex(&case["output"]), "{case}");
        assert_eq!(incoming::is_real(&message), case["real"].as_bool().unwrap());
        assert_eq!(
            incoming::increments_unread(&message),
            case["unread"].as_bool().unwrap()
        );
        let chat = incoming::chat_id(message.key.as_ref().unwrap());
        if case.get("error").is_some() {
            assert!(chat.is_err());
            continue;
        }
        assert_eq!(chat.unwrap(), case["chat"].as_str().unwrap());
        let effects = own::effects_received(&message, true).unwrap();
        let mut events = vec![];
        if let Some(mutation) = effects.mutation {
            let update = match mutation {
                own::Mutation::Revoke {
                    key, original_key, ..
                } => {
                    json!({"key":wire(key.encode_to_vec()),"revoke":true,"originalKey":wire(original_key.encode_to_vec())})
                }
                own::Mutation::Edit {
                    key,
                    message,
                    timestamp,
                    ..
                } => {
                    json!({"key":wire(key.encode_to_vec()),"message":wire(message.encode_to_vec()),"timestamp":timestamp as i64})
                }
            };
            events.push(json!({"name":"messages.update","data":[update]}));
        }
        if let Some(r) = effects.reaction {
            events.push(json!({"name":"messages.reaction","data":[{"key":r.key.map(|k|wire(k.encode_to_vec())),"reaction":wire(r.reaction.encode_to_vec())}]}));
        }
        if let Some(chat) = effects.chat {
            let mut value = json!({"id":chat.id});
            if let Some(m) = chat.messages {
                value["messages"] = json!(
                    m.iter()
                        .map(|m| wire(m.encode_to_vec()))
                        .collect::<Vec<_>>()
                );
            }
            if let Some(t) = chat.conversation_timestamp {
                value["conversationTimestamp"] = json!(t as i64);
            }
            if let Some(c) = chat.unread_count {
                value["unreadCount"] = json!(c);
            }
            if let Some(v) = chat.archived {
                value["archived"] = json!(v);
            }
            if let Some(v) = chat.read_only {
                value["readOnly"] = json!(v);
            }
            if let Some(v) = chat.ephemeral_setting_timestamp {
                value["ephemeralSettingTimestamp"] = json!(v as i64);
            }
            if let Some(v) = chat.ephemeral_expiration {
                value["ephemeralExpiration"] = json!(v);
            }
            events.push(json!({"name":"chats.update","data":[value]}));
        }
        assert_eq!(json!(events), case["events"], "{case}");
    }
}
#[test]
fn incoming_receive_decisions_and_cleaned_upserts_match_original_handler() {
    for case in fixture()["policies"].as_array().unwrap() {
        let message = proto::WebMessageInfo::decode(hex(&case["input"]).as_slice()).unwrap();
        let mut envelope = messages::Envelope {
            message,
            key_metadata: messages::KeyMetadata {
                remote_jid_alt: case["alt"].as_str().map(str::to_owned),
                ..Default::default()
            },
            category: case["category"].as_str().map(str::to_owned),
            retry_count: None,
            author: case["author"].as_str().unwrap().into(),
            sender: String::new(),
        };
        let node = node(&case["node"]);
        let action = incoming::plan(
            &envelope,
            &node,
            case["active"].as_bool().unwrap(),
            1700000100,
        );
        let mut expected = vec![];
        match action {
            Plan::Drop { nack } => {
                assert_eq!(case["trace"], json!([{"kind":"ack","nack":nack}]), "{case}");
                continue;
            }
            Plan::Retry => {
                expected.push(json!({"kind":"retry"}));
                expected.push(json!({"kind":"ack","nack":500}));
            }
            Plan::Placeholder => {
                expected.push(json!({"kind":"placeholder"}));
                expected.push(json!({"kind":"ack","nack":null}));
            }
            Plan::Newsletter => expected.push(json!({"kind":"ack","nack":null})),
            Plan::Delivery {
                kind,
                participant,
                history,
            } => {
                let key = envelope.message.key.as_ref().unwrap();
                expected.push(json!({"kind":"receipt","jid":key.remote_jid,"participant":participant,"ids":[key.id],"type":kind.map(|t|t.as_str())}));
                if history {
                    expected.push(json!({"kind":"receipt","jid":key.remote_jid,"participant":null,"ids":[key.id],"type":"hist_sync"}));
                }
            }
        }
        incoming::clean(&mut envelope.message, "100:7@s.whatsapp.net", "1000:7@lid").unwrap();
        expected.push(json!({"kind":"upsert","type":if node.attrs["offline"].is_empty(){"notify"}else{"append"},"wire":wire(envelope.message.encode_to_vec())}));
        assert_eq!(json!(expected), case["trace"], "{case}");
    }
}
