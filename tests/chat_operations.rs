#[path = "support/json_numbers.rs"]
mod json_numbers;
use baileys_rs::{
    binary::{BinaryNode, NodeContent},
    chats::{self, BlockLookup, PictureType},
};
use base64::{Engine, engine::general_purpose::STANDARD};
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
struct Outcome {
    queries: Vec<BinaryNode>,
    sent: Vec<BinaryNode>,
    result: Value,
    error: Option<chats::Error>,
    events: Vec<Value>,
}
// Runs one operation as the socket composes these helpers, with the harness's canned I/O.
fn run(case: &Value, me: &str, lid: &str) -> Outcome {
    let args = case["args"].as_array().unwrap();
    let text = |i: usize| args.get(i).and_then(Value::as_str).unwrap_or_default();
    let response = case["opts"]["responses"]
        .get(0)
        .map(node)
        .unwrap_or_else(|| BinaryNode::new("iq"));
    let mut out = Outcome {
        queries: vec![],
        sent: vec![],
        result: Value::Null,
        error: None,
        events: vec![],
    };
    match case["op"].as_str().unwrap() {
        "updateDefaultDisappearingMode" => out.queries.push(
            chats::default_disappearing_mode_query(args[0].as_u64().unwrap() as u32),
        ),
        "getBotListV2" => {
            out.queries.push(chats::bot_list_query());
            out.result = json!(chats::extract_bot_list(&response));
        }
        op @ ("updateProfilePicture" | "removeProfilePicture") => {
            match chats::profile_picture_target(text(0), me) {
                Ok(target) => {
                    let image = (op == "updateProfilePicture").then(|| {
                        let dimensions = args.get(2).map_or("null".into(), Value::to_string);
                        format!("IMG:{}:{dimensions}", text(1)).into_bytes()
                    });
                    out.queries.push(chats::profile_picture_update_query(
                        target.as_deref(),
                        image,
                    ));
                }
                Err(error) => out.error = Some(error),
            }
        }
        "updateProfileStatus" => out.queries.push(chats::profile_status_query(text(0))),
        "fetchBlocklist" => {
            out.queries
                .push(baileys_rs::socket::startup::blocklist_query());
            out.result = json!(baileys_rs::socket::startup::extract_blocklist(&response));
        }
        "updateBlockStatus" => {
            let block = text(1) == "block";
            let built = chats::block_lookup(text(0)).and_then(|lookup| {
                let mapped = match &lookup {
                    // The source only looks up the phone number when blocking.
                    BlockLookup::PnForLid(lid) => {
                        block.then(|| case["opts"]["pn"][lid].as_str()).flatten()
                    }
                    BlockLookup::LidForPn(pn) => case["opts"]["lid"][pn].as_str(),
                };
                chats::block_status_query(text(0), block, &lookup, mapped)
            });
            match built {
                Ok(query) => out.queries.push(query),
                Err(error) => out.error = Some(error),
            }
        }
        "getBusinessProfile" => {
            out.queries.push(chats::business_profile_query(text(0)));
            out.result = json!(chats::extract_business_profile(&response));
        }
        "cleanDirtyBits" => {
            let timestamp = args.get(1).map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            });
            out.sent.push(chats::clean_dirty_bits_node(
                text(0),
                timestamp.as_deref(),
                "TAG-1",
            ));
        }
        "profilePictureUrl" => {
            let kind = if text(1) == "image" {
                PictureType::Image
            } else {
                PictureType::Preview
            };
            let normalized = baileys_rs::jid::normalized_user(text(0));
            let enabled = case["opts"]["pictureToken"].as_bool().unwrap_or(true);
            let token = if enabled && chats::profile_picture_uses_token(text(0), me, Some(lid)) {
                case["opts"]["token"][&normalized]
                    .as_array()
                    .map(|nodes| nodes.iter().map(node).collect())
                    .unwrap_or_default()
            } else {
                vec![]
            };
            out.queries
                .push(chats::profile_picture_url_query(text(0), kind, token));
            out.result = json!(chats::extract_profile_picture_url(&response));
        }
        "createCallLink" => {
            let start = args.get(1).and_then(|event| event["startTime"].as_u64());
            out.queries
                .push(chats::call_link_query(text(0) == "video", start, "TAG-1"));
            out.result = json!(chats::extract_call_link_token(&response));
        }
        "dirty" => {
            let last = case["opts"]["lastSync"].as_u64();
            if let Some(dirty) = chats::account_sync_dirty(&node(&args[0]), last) {
                if let Some(from) = dirty.clean_from {
                    out.sent.push(chats::clean_dirty_bits_node(
                        "account_sync",
                        Some(&from.to_string()),
                        "TAG-1",
                    ));
                }
                out.events.push(json!({"name": "creds.update", "data": {
                    "lastAccountSyncTimestamp": dirty.last_account_sync_timestamp}}));
            }
        }
        other => panic!("unhandled operation {other}"),
    }
    out
}
#[test]
fn chat_operations_match_original_stanzas_results_and_errors() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/chat_operations.json")).unwrap();
    let (me, lid) = (
        fixture["me"].as_str().unwrap(),
        fixture["lid"].as_str().unwrap(),
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 190);
    let mut compared = 0;
    for case in cases {
        // USync status/disappearing lookups and the push-name modification reuse
        // components with their own parity fixtures.
        if matches!(
            case["op"].as_str().unwrap(),
            "fetchStatus" | "fetchDisappearingDuration" | "updateProfileName"
        ) {
            continue;
        }
        compared += 1;
        let out = run(case, me, lid);
        match (&case["error"], &out.error) {
            (Value::Null, None) => (),
            (expected, Some(error)) => {
                assert_eq!(expected["message"], error.to_string(), "{case}");
                assert_eq!(expected["status"], error.status_code(), "{case}");
            }
            _ => panic!("error mismatch for {case}"),
        }
        let expected: Vec<_> = case["queries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|q| node(&q["node"]))
            .collect();
        assert_eq!(out.queries, expected, "{case}");
        let sent: Vec<_> = case["sent"].as_array().unwrap().iter().map(node).collect();
        assert_eq!(out.sent, sent, "{case}");
        assert_eq!(
            json_numbers::canonical(&out.result),
            json_numbers::canonical(&case["result"]),
            "{case}"
        );
        assert_eq!(
            json_numbers::canonical(&json!(out.events)),
            json_numbers::canonical(&case["events"]),
            "{case}"
        );
    }
    assert_eq!(compared, 182);
}
