use baileys_rs::{
    binary::{self, BinaryNode, NodeContent},
    messages::{
        generation::{Content, Disappearing, Modifiers},
        own::{self, Mutation},
    },
    proto, socket,
};
use prost::Message as _;
use serde_json::{Value, json};
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn wire(bytes: Vec<u8>) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn kw(k: &proto::MessageKey) -> String {
    wire(k.encode_to_vec())
}
fn key(v: &Value) -> proto::MessageKey {
    serde_json::from_value(v.clone()).unwrap()
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/send_message.json")).unwrap()
}
fn metadata_input(case: &Value) -> (Content, Modifiers) {
    let i = &case["input"];
    let content = match case["kind"].as_str().unwrap() {
        "delete" | "deleteEdit" => Content::Delete(key(&i["delete"])),
        "pin" => Content::Pin {
            key: key(&i["pin"]),
            kind: 1,
            duration_seconds: None,
        },
        "poll" | "pollEdit" => Content::Poll(Box::default()),
        "event" => Content::Event(Box::default()),
        "disappearing" | "groupDisappearing" => {
            Content::Disappearing(match &i["disappearingMessagesInChat"] {
                Value::Bool(true) => Disappearing::Enabled,
                Value::Bool(false) => Disappearing::Disabled,
                v => Disappearing::Seconds(v.as_u64().unwrap() as u32),
            })
        }
        _ => Content::text("dummy"),
    };
    (
        content,
        Modifiers {
            edit: i.get("edit").map(key),
            ..Default::default()
        },
    )
}
fn node(v: &Value) -> BinaryNode {
    let mut node = BinaryNode::new(v["tag"].as_str().unwrap());
    for (k, v) in v["attrs"].as_object().unwrap() {
        node = node.attr(k, v.as_str().unwrap());
    }
    if let Some(children) = v["content"].as_array() {
        node = node.with_content(NodeContent::Children(
            children.iter().map(node_from_json).collect(),
        ));
    }
    node
}
fn node_from_json(v: &Value) -> BinaryNode {
    node(v)
}
#[test]
fn send_metadata_matches_original_send_message_precedence() {
    for case in fixture()["cases"].as_array().unwrap() {
        if case["kind"] == "groupDisappearing" {
            continue;
        }
        let (input, modifiers) = metadata_input(case);
        let meta = socket::send_metadata(&input, &modifiers);
        let expected = &case["relays"][0];
        assert_eq!(
            serde_json::to_value(meta.additional_attributes).unwrap(),
            expected["attributes"],
            "{case}"
        );
        let expected: Vec<_> = expected["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| binary::encode(&node(v)).unwrap())
            .collect();
        assert_eq!(
            meta.additional_nodes
                .iter()
                .map(|n| binary::encode(n).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
    }
}
#[test]
fn own_message_effects_match_original_upsert_and_process_message() {
    for case in fixture()["cases"].as_array().unwrap() {
        if case["full"].is_null() || case["emit"] == false {
            continue;
        }
        let full =
            proto::WebMessageInfo::decode(hex(case["full"].as_str().unwrap()).as_slice()).unwrap();
        let effects = own::effects(&full, case["unarchive"].as_bool().unwrap()).unwrap();
        let mut actual = vec![
            json!({"name":"messages.upsert","messages":[wire(full.encode_to_vec())],"type":"append"}),
        ];
        if let Some(mutation) = effects.mutation {
            let update = match mutation {
                Mutation::Revoke { key, original_key } => {
                    json!({"key":kw(&key),"revoke":true,"stub":proto::web_message_info::StubType::Revoke as i32,"originalKey":kw(&original_key)})
                }
                Mutation::Edit {
                    key,
                    message,
                    timestamp,
                } => {
                    json!({"key":kw(&key),"message":wire(message.encode_to_vec()),"timestamp":timestamp})
                }
            };
            actual.push(json!({"name":"messages.update","updates":[update]}));
        }
        if let Some(reaction) = effects.reaction {
            actual.push(json!({"name":"messages.reaction","updates":[{"key":reaction.key.as_ref().map(kw),"reaction":wire(reaction.reaction.encode_to_vec())}]}));
        }
        if let Some(chat) = effects.chat {
            let mut c = json!({"id":chat.id});
            if let Some(messages) = chat.messages {
                c["messages"] = json!(
                    messages
                        .into_iter()
                        .map(|m| wire(m.encode_to_vec()))
                        .collect::<Vec<_>>()
                );
            }
            if let Some(timestamp) = chat.conversation_timestamp {
                c["conversationTimestamp"] = json!(timestamp);
            }
            if let Some(archived) = chat.archived {
                c["archived"] = json!(archived);
            }
            if let Some(read_only) = chat.read_only {
                c["readOnly"] = json!(read_only);
            }
            if let Some(timestamp) = chat.ephemeral_setting_timestamp {
                c["ephemeralSettingTimestamp"] = json!(timestamp);
            }
            if let Some(expiration) = chat.ephemeral_expiration {
                c["ephemeralExpiration"] = json!(expiration);
            }
            actual.push(json!({"name":"chats.update","updates":[c]}));
        }
        let mut actual = Value::Array(actual);
        let mut expected = case["events"].clone();
        normalize_numbers(&mut actual);
        normalize_numbers(&mut expected);
        assert_eq!(actual, expected, "{case}");
    }
}

fn normalize_numbers(value: &mut Value) {
    match value {
        Value::Number(n) => *value = json!(n.as_f64().unwrap()),
        Value::Array(values) => {
            for v in values {
                normalize_numbers(v);
            }
        }
        Value::Object(values) => {
            for v in values.values_mut() {
                normalize_numbers(v);
            }
        }
        _ => {}
    }
}
