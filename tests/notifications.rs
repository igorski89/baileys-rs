#[path = "support/json_numbers.rs"]
mod json_numbers;
use baileys_rs::{
    binary::{BinaryNode, NodeContent},
    notifications::{self, Effect},
    proto,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use serde_json::{Value, json};
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    n.content = match &v["content"] {
        Value::Array(children) => Some(NodeContent::Children(children.iter().map(node).collect())),
        Value::String(text) => Some(NodeContent::Text(text.clone())),
        Value::Object(buffer) => Some(NodeContent::Bytes(
            STANDARD.decode(buffer["data"].as_str().unwrap()).unwrap(),
        )),
        _ => None,
    };
    n
}
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
// JSON.stringify writes NaN as null.
fn number(value: f64) -> Value {
    if value.is_finite() {
        json!(value)
    } else {
        Value::Null
    }
}
fn events(effects: &[Effect]) -> Vec<Value> {
    let mut events = vec![];
    for effect in effects {
        match effect {
            Effect::GroupCreated(group) => {
                let mut chat = json!({"id": group.metadata.id});
                if let Some(subject) = &group.metadata.subject {
                    chat["name"] = json!(subject);
                }
                chat["conversationTimestamp"] = group.metadata.creation.map_or(Value::Null, number);
                events.push(json!({"name": "chats.upsert", "data": [chat]}));
                let mut metadata = serde_json::to_value(&group.metadata).unwrap();
                for (name, value) in [
                    ("author", &group.author),
                    ("authorPn", &group.author_pn),
                    ("authorUsername", &group.author_username),
                ] {
                    if let Some(value) = value {
                        metadata[name] = json!(value);
                    }
                }
                events.push(json!({"name": "groups.upsert", "data": [metadata]}));
            }
            Effect::Picture { id, removed } => events.push(json!({"name": "contacts.update", "data": [{
                "id": id, "imgUrl": if *removed { "removed" } else { "changed" }}]})),
            Effect::DisappearingMode { duration, timestamp } => events.push(json!({"name": "creds.update", "data": {
                "accountSettings": {"unarchiveChats": true, "defaultDisappearingMode": {
                    "ephemeralExpiration": number(*duration), "ephemeralSettingTimestamp": number(*timestamp)}}}})),
            Effect::Blocklist { jid, add } => events.push(json!({"name": "blocklist.update", "data": {
                "blocklist": [jid], "type": if *add { "add" } else { "remove" }}})),
        }
    }
    events
}
#[test]
fn group_picture_and_account_notifications_match_original_handler() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/notifications.json")).unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 510);
    for case in cases {
        let input = node(&case["node"]);
        let result = notifications::process(&input, fixture["me"].as_str().unwrap());
        assert_eq!(result.is_err(), case["error"].as_bool().unwrap(), "{case}");
        let Ok(result) = result else { continue };
        assert_eq!(
            json_numbers::canonical(&json!(events(&result.effects))),
            json_numbers::canonical(&case["events"]),
            "{case}"
        );
        let expected: Vec<_> = case["upserts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|upsert| {
                assert_eq!(upsert["kind"], "append");
                proto::WebMessageInfo::decode(unhex(upsert["wire"].as_str().unwrap()).as_slice())
                    .unwrap()
            })
            .collect();
        assert_eq!(
            result.message.into_iter().collect::<Vec<_>>(),
            expected,
            "{case}"
        );
    }
}
#[test]
fn device_list_notifications_match_original_cache_and_session_effects() {
    use baileys_rs::{
        devices::{self, Cache, Change},
        jid::Jid,
    };
    let fixture: Value = serde_json::from_str(include_str!("fixtures/notifications.json")).unwrap();
    let cases = fixture["deviceCases"].as_array().unwrap();
    assert_eq!(cases.len(), 123);
    for case in cases {
        let cache = Cache::new(Default::default());
        let seed = case["seed"].as_object().unwrap();
        cache
            .update(
                seed.values()
                    .flat_map(|entries| entries.as_array().unwrap())
                    .map(|entry| Jid {
                        user: entry["user"].as_str().unwrap().into(),
                        server: entry["server"].as_str().unwrap().into(),
                        device: Some(entry["device"].as_u64().unwrap() as u16),
                        domain_type: u8::from(entry["server"] == "lid"),
                    })
                    .collect(),
            )
            .unwrap();
        // The socket deletes removed sessions per user before updating that user's cache.
        let mut deleted = vec![];
        let mut failed = false;
        if let Some(list) = devices::parse_notification(&node(&case["node"])) {
            for (user, entries) in &list.users {
                if list.change == Change::Remove {
                    if case["failDelete"] == true {
                        failed = true;
                        break;
                    }
                    deleted.push(
                        entries
                            .iter()
                            .map(|(raw, _)| raw.clone())
                            .collect::<Vec<_>>(),
                    );
                }
                let jids: Vec<_> = entries.iter().map(|(_, jid)| jid.clone()).collect();
                cache.apply_change(user, list.change, &jids).unwrap();
            }
        }
        assert_eq!(failed, case["error"].as_bool().unwrap(), "{case}");
        assert_eq!(json!(deleted), case["deleted"], "{case}");
        let expected = case["cache"].as_object().unwrap();
        for user in seed
            .keys()
            .chain(expected.keys())
            .chain([&"400".to_string()])
        {
            let actual = cache.cached(user).unwrap().map(|entries| {
                entries
                    .iter()
                    .map(|d| json!({"user": d.user, "server": d.server, "device": d.device}))
                    .collect::<Vec<_>>()
            });
            // A device-less JID is stored with upstream's undefined device; natively zero.
            let wanted = expected.get(user).map(|entries| {
                entries
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| json!({"user": e["user"], "server": e["server"], "device": e["device"].as_u64()}))
                    .collect::<Vec<_>>()
            });
            assert_eq!(actual, wanted, "{user} {case}");
        }
    }
}
