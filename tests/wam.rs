#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::wam::{self, BinaryInfo, Error, EventInput, Value as Field};
use serde_json::Value;
use sha2::{Digest, Sha256};
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn field(value: &Value) -> Field {
    match value {
        Value::Null => Field::Null,
        Value::Bool(flag) => Field::Bool(*flag),
        Value::Number(number) => Field::Number(number.as_f64().unwrap()),
        Value::String(text) => Field::Text(text.clone()),
        // Long strings are recorded by their repeated character and length.
        repeated => Field::Text(
            repeated["repeat"]
                .as_str()
                .unwrap()
                .repeat(repeated["count"].as_u64().unwrap() as usize),
        ),
    }
}
fn fields(value: &Value) -> Vec<(String, Field)> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| (name.clone(), field(value)))
        .collect()
}
#[test]
fn wam_buffers_match_original_encoder() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/wam.json")).unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 41);
    for case in cases {
        let input = &case["input"];
        let mut info = BinaryInfo::default();
        if let Some(sequence) = input["sequence"].as_u64() {
            info.sequence = sequence as u16;
        }
        if let Some(version) = input["protocolVersion"].as_u64() {
            info.protocol_version = version as u8;
        }
        for event in input["events"].as_array().unwrap() {
            let (name, body) = event.as_object().unwrap().iter().next().unwrap();
            info.events.push(EventInput {
                name: name.clone(),
                props: fields(&body["props"]),
                globals: fields(&body["globals"]),
            });
        }
        // Non-finite numbers are named, since JSON cannot carry them.
        if let Some(special) = input["special"].as_str() {
            info.events[0].props[0].1 = Field::Number(match special {
                "NaN" => f64::NAN,
                "Infinity" => f64::INFINITY,
                _ => f64::NEG_INFINITY,
            });
        }
        let result = wam::encode(&info);
        match case["error"].as_str() {
            Some("missing") => assert_eq!(result, Err(Error::Missing), "{case}"),
            Some(_) => assert!(
                matches!(
                    result,
                    Err(Error::UnknownEvent(_) | Error::UnknownGlobal(_))
                ),
                "{case}"
            ),
            None => {
                let wire = result.unwrap();
                match case["wire"].as_str() {
                    Some(expected) => assert_eq!(hex(&wire), expected, "{case}"),
                    None => assert_eq!(
                        hex(&Sha256::digest(&wire)),
                        case["sha256"].as_str().unwrap(),
                        "{case}"
                    ),
                }
            }
        }
    }
}
#[test]
fn generated_event_and_global_tables_match_original_identifiers() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/wam.json")).unwrap();
    assert_eq!(wam::WEB_EVENTS.len() as u64, fixture["events"]);
    assert_eq!(wam::WEB_GLOBALS.len() as u64, fixture["globals"]);
    let numbered = |names: Vec<&str>| -> Vec<(String, Field)> {
        names
            .into_iter()
            .enumerate()
            .map(|(index, name)| (name.to_owned(), Field::Number(index as f64 + 2.0)))
            .collect()
    };
    let encode = |event: EventInput| {
        wam::encode(&BinaryInfo {
            events: vec![event],
            ..Default::default()
        })
        .unwrap()
    };
    let mut digest = Sha256::new();
    for event in wam::WEB_EVENTS {
        digest.update(encode(EventInput {
            name: event.name.into(),
            props: numbered(event.props.iter().map(|prop| prop.name).collect()),
            globals: vec![],
        }));
    }
    digest.update(encode(EventInput {
        name: "WamDroppedEvent".into(),
        props: vec![],
        globals: numbered(wam::WEB_GLOBALS.iter().map(|global| global.name).collect()),
    }));
    assert_eq!(hex(&digest.finalize()), fixture["tables"].as_str().unwrap());
}
#[tokio::test]
async fn wam_buffer_is_sent_as_a_stats_query() {
    use baileys_rs::{
        auth::SqliteAuthStore,
        binary::BinaryNode,
        crypto::KeyPair,
        socket::{Config, Socket},
    };
    use socket_peer::{Peer, ROOT_PRIVATE};
    use std::time::Duration;
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            url: format!("ws://{}", listener.local_addr().unwrap()),
            initialize_session: false,
            fire_init_queries: false,
            announce_presence_on_connect: false,
            noise: baileys_rs::noise::Config {
                certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
                ..Default::default()
            },
            ..Default::default()
        };
        let buffer = wam::encode(&BinaryInfo {
            sequence: 9,
            events: vec![EventInput {
                name: "WamDroppedEvent".into(),
                props: vec![("droppedEventCode".into(), Field::Number(5.0))],
                globals: vec![],
            }],
            ..Default::default()
        })
        .unwrap();
        let sent = buffer.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let request = peer.read().await;
            assert_eq!(request.tag, "iq");
            assert_eq!(request.attrs["to"], "@s.whatsapp.net");
            assert_eq!(request.attrs["xmlns"], "w:stats");
            assert!(!request.attrs.contains_key("type"));
            let add = request.child("add").unwrap();
            let time: u64 = add.attrs["t"].parse().unwrap();
            assert!(time > 1_700_000_000);
            assert_eq!(request.child_buffer("add").unwrap(), sent);
            peer.result(&request).await;
            peer.closed().await;
        });
        let store = SqliteAuthStore::in_memory().unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(baileys_rs::auth::Contact {
            id: "100:7@s.whatsapp.net".into(),
            ..Default::default()
        });
        store.save_credentials(&creds).unwrap();
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let result = socket.send_wam_buffer(buffer).await.unwrap();
        assert_eq!(result.attrs["type"], "result");
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
