//! Upstream parity for the exported helpers the socket itself does not use.
use baileys_rs::{
    binary::{self, BinaryNode, NodeContent},
    connection::{self, Browser},
    messages::{self, EventResponseUpdate, generation, updates},
    proto, version,
};
use prost::Message as _;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/utilities.json")).unwrap()
}
fn hex(value: &Value) -> Vec<u8> {
    let text = value.as_str().unwrap();
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn info(value: &Value) -> proto::WebMessageInfo {
    proto::WebMessageInfo::decode(hex(value).as_slice()).unwrap()
}
fn sequences<T: prost::Message + Default>(
    cases: &Value,
    apply: impl Fn(&mut proto::WebMessageInfo, T),
) {
    for case in cases.as_array().unwrap() {
        let mut message = info(&case["start"]);
        for step in case["steps"].as_array().unwrap() {
            apply(
                &mut message,
                T::decode(hex(&step["update"]).as_slice()).unwrap(),
            );
            assert_eq!(message, info(&step["expected"]), "{step}");
        }
    }
}
#[test]
fn message_update_helpers_match_upstream() {
    let fixture = fixture();
    for case in fixture["devices"].as_array().unwrap() {
        assert_eq!(
            updates::device_kind(case["id"].as_str().unwrap()),
            case["expected"],
            "{case}"
        );
    }
    sequences(&fixture["receipts"], updates::update_with_receipt);
    sequences(&fixture["reactions"], updates::update_with_reaction);
    sequences(&fixture["pollUpdates"], updates::update_with_poll_update);
    sequences(
        &fixture["responseSteps"],
        updates::update_with_event_response,
    );
}
#[test]
fn aggregations_match_upstream() {
    let fixture = fixture();
    for case in fixture["votes"].as_array().unwrap() {
        let votes = updates::aggregate_votes(&info(&case["message"]), case["me"].as_str());
        assert_eq!(
            serde_json::to_value(votes).unwrap(),
            case["expected"],
            "{case}"
        );
    }
    use proto::message::event_response_message::EventResponseType as Type;
    for case in fixture["responses"].as_array().unwrap() {
        // The port aggregates the decrypted responses; an entry without a key cannot be built natively.
        let responses: Vec<_> = case["responses"]
            .as_array()
            .unwrap()
            .iter()
            .map(|response| EventResponseUpdate {
                key: Default::default(),
                event_response_message_key: if response["key"].is_null() {
                    Default::default()
                } else {
                    proto::MessageKey::decode(hex(&response["key"]).as_slice()).unwrap()
                },
                sender_timestamp_ms: None,
                response: proto::message::EventResponseMessage {
                    response: response["response"]
                        .as_str()
                        .and_then(Type::from_str_name)
                        .map(|kind| kind as i32),
                    ..Default::default()
                },
            })
            .collect();
        let result = updates::aggregate_event_responses(&responses, case["me"].as_str());
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            case["expected"],
            "{case}"
        );
    }
}
#[test]
fn small_helpers_match_upstream() {
    let fixture = fixture();
    for case in fixture["extensions"].as_array().unwrap() {
        let message = proto::Message::decode(hex(&case["message"]).as_slice()).unwrap();
        // Upstream throws where the message has no MIME type; both outcomes are `None` here.
        assert_eq!(
            generation::media_extension(&message)
                .map(Value::from)
                .unwrap_or(Value::Null),
            case["expected"],
            "{case}"
        );
    }
    for case in fixture["platforms"].as_array().unwrap() {
        assert_eq!(
            connection::is_business_platform(case["platform"].as_str().unwrap()),
            case["expected"],
            "{case}"
        );
    }
    for case in fixture["versions"].as_array().unwrap() {
        let text = case["text"].as_str().unwrap();
        let parsed = match case["kind"].as_str().unwrap() {
            "baileys" => version::parse_baileys_version(text),
            _ => version::parse_wa_web_version(text),
        }
        .filter(|_| case["status"] == 200);
        assert_eq!(parsed.is_some(), case["expected"]["isLatest"], "{case}");
        assert_eq!(
            serde_json::to_value(parsed.unwrap_or(baileys_rs::defaults::WA_VERSION)).unwrap(),
            case["expected"]["version"],
            "{case}"
        );
    }
    let appropriate = Browser::appropriate("Chrome");
    assert_eq!(appropriate.1, "Chrome");
    if cfg!(target_os = "macos") {
        assert_eq!(appropriate.0, "Mac OS");
    }
    if cfg!(unix) {
        assert!(!appropriate.2.is_empty());
    }
}
fn tree(value: &Value) -> BinaryNode {
    let mut node = BinaryNode::new(value["tag"].as_str().unwrap());
    for pair in value["attrs"].as_array().unwrap() {
        node = node.attr(pair[0].as_str().unwrap(), pair[1].as_str().unwrap());
    }
    let content = &value["content"];
    node.content = if let Some(children) = content["children"].as_array() {
        Some(NodeContent::Children(children.iter().map(tree).collect()))
    } else if let Some(text) = content["text"].as_str() {
        Some(NodeContent::Text(text.into()))
    } else if content.is_null() {
        None
    } else {
        Some(NodeContent::Bytes(hex(&content["bytes"])))
    };
    node
}
#[test]
fn node_helpers_match_upstream() {
    let fixture = fixture();
    for case in fixture["strings"].as_array().unwrap() {
        assert_eq!(
            tree(&case["node"]).to_tree_string(),
            case["expected"],
            "{case}"
        );
    }
    for case in fixture["dictionaries"].as_array().unwrap() {
        let node = binary::decode(&hex(&case["node"])).unwrap();
        let dictionary = node.dictionary(case["tag"].as_str().unwrap());
        // JavaScript lists integer-like keys first; the table itself is the same.
        let expected: std::collections::HashMap<_, _> = case["expected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| (pair[0].as_str().unwrap(), pair[1].as_str().unwrap()))
            .collect();
        assert_eq!(dictionary.len(), expected.len(), "{case}");
        for (key, value) in &dictionary {
            assert_eq!(expected[key.as_str()], value, "{case}");
        }
    }
    for case in fixture["nodeMessages"].as_array().unwrap() {
        let node = binary::decode(&hex(&case["node"])).unwrap();
        let expected: Vec<_> = case["expected"]
            .as_array()
            .unwrap()
            .iter()
            .map(info)
            .collect();
        assert_eq!(node.messages().unwrap(), expected);
    }
    let broken = BinaryNode::new("result").with_content(NodeContent::Children(vec![
        BinaryNode::new("message").with_content(NodeContent::Bytes(vec![0xff])),
    ]));
    assert!(broken.messages().is_err());
    let _ = messages::key_author(None, None, "me");
}
async fn serve(
    status: &'static str,
    body: &'static str,
) -> (String, tokio::task::JoinHandle<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 4096];
        let read = stream.read(&mut request).await.unwrap();
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
        String::from_utf8_lossy(&request[..read]).to_lowercase()
    });
    (format!("http://{address}/"), server)
}
#[tokio::test]
async fn version_lookups_fetch_parse_and_fall_back() {
    let client = reqwest::Client::new();
    let (url, server) = serve("200 OK", "x \"client_revision\":1023456789 y").await;
    let latest = version::fetch_wa_web_version_from(&client, &url).await;
    assert_eq!(
        (latest.version, latest.is_latest),
        ([2, 3000, 1023456789], true)
    );
    let request = server.await.unwrap();
    assert!(request.contains("sec-fetch-site: none"), "{request}");
    assert!(request.contains("chrome/131.0.0.0"), "{request}");

    let (url, _server) = serve("500 Internal Server Error", "\"client_revision\":1").await;
    let failed = version::fetch_wa_web_version_from(&client, &url).await;
    assert_eq!(failed.version, baileys_rs::defaults::WA_VERSION);
    assert!(!failed.is_latest);
    assert_eq!(
        failed.error.as_deref(),
        Some("Failed to fetch sw.js: Internal Server Error")
    );

    let (url, _server) = serve(
        "200 OK",
        "1\n2\n3\n4\n5\n6\nconst version = [2, 3000, 77]\n",
    )
    .await;
    let latest = version::fetch_baileys_version_from(&client, &url).await;
    assert_eq!((latest.version, latest.is_latest), ([2, 3000, 77], true));

    let (url, _server) = serve("200 OK", "no version here").await;
    let failed = version::fetch_baileys_version_from(&client, &url).await;
    assert!(!failed.is_latest);
    assert_eq!(
        failed.error.as_deref(),
        Some("Could not parse version from Defaults/index.ts")
    );
    let refused = version::fetch_baileys_version_from(&client, "http://127.0.0.1:1/").await;
    assert!(!refused.is_latest && refused.error.is_some());
}
