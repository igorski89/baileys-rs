#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, KeyMutation, KeyStore, KeyValue, SqliteAuthStore, TcToken},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    signal::mapping::{Mapping, Store},
    socket::{
        Config, Event, Socket,
        presence::{self, IgnoreJid, Presence},
    },
};
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{net::TcpListener, time::timeout};
fn fixture() -> Value {
    fn numbers(value: &mut Value) {
        match value {
            Value::Number(number) => *value = json!(number.as_f64().unwrap()),
            Value::Array(values) => values.iter_mut().for_each(numbers),
            Value::Object(values) => values.values_mut().for_each(numbers),
            _ => (),
        }
    }
    let mut value = serde_json::from_str(include_str!("fixtures/presence.json")).unwrap();
    numbers(&mut value);
    value
}
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    if let Some(c) = v["content"].as_array() {
        n.content = Some(NodeContent::Children(c.iter().map(node).collect()));
    }
    n
}
fn value(n: &BinaryNode) -> Value {
    let mut v = json!({"tag":n.tag,"attrs":n.attrs});
    if let Some(NodeContent::Children(c)) = &n.content {
        v["content"] = json!(c.iter().map(value).collect::<Vec<_>>());
    }
    v
}
#[test]
fn presence_nodes_events_and_ignore_rules_match_original() {
    for case in fixture()["cases"].as_array().unwrap() {
        if case["kind"] == "send" {
            let me: Contact = serde_json::from_value(case["input"]["me"].clone()).unwrap();
            let kind: Presence = serde_json::from_value(case["input"]["type"].clone()).unwrap();
            let result = presence::update_node(&me, kind, case["input"]["to"].as_str());
            if case["expected"]["error"] == true {
                assert!(result.is_err(), "{case}");
                continue;
            }
            let mut nodes = vec![];
            let mut events = vec![];
            if let Some(n) = result.unwrap() {
                if kind.is_global() {
                    events.push(json!({"name":"connection.update","data":{"isOnline":kind==Presence::Available}}));
                    if kind == Presence::Available {
                        nodes.push(json!({"tag":"unified_session"}));
                    }
                }
                nodes.push(value(&n));
            }
            assert_eq!(
                json!({"nodes":nodes,"events":events}),
                case["expected"],
                "{case}"
            );
        } else {
            let ignored = case["ignored"].as_bool().unwrap();
            let result = presence::decode(&node(&case["input"]), &IgnoreJid::new(move |_| ignored));
            if case["expected"]["error"] == true {
                assert!(result.is_err());
                continue;
            }
            let events = result
                .unwrap()
                .into_iter()
                .map(|data| json!({"name":"presence.update","data":data}))
                .collect::<Vec<_>>();
            assert_eq!(
                json!({"nodes":[],"events":events}),
                case["expected"],
                "{case}"
            );
        }
    }
}
#[test]
fn subscription_normalization_and_user_gates_match_original() {
    for case in fixture()["subscriptions"].as_array().unwrap() {
        let to = case["input"]["to"].as_str().unwrap();
        let target = presence::subscription_target(to);
        assert_eq!(
            json!(target.iter().collect::<Vec<_>>()),
            case["expected"]["lookups"]
        );
        let content = if target.is_some() {
            case["input"]["content"]
                .as_array()
                .map(|c| c.iter().map(node).collect())
                .unwrap_or_default()
        } else {
            vec![]
        };
        assert_eq!(
            json!([value(&presence::subscription_node(to, "test-id", content))]),
            case["expected"]["nodes"]
        );
    }
}

async fn setup(online: bool, name: Option<&str>) -> (TcpListener, Config, SqliteAuthStore) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        fire_init_queries: false,
        mark_online_on_connect: online,
        query_timeout: Duration::from_secs(3),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100:2@s.whatsapp.net".into(),
        lid: Some("100@lid".into()),
        name: name.map(str::to_owned),
        ..Default::default()
    });
    store.save_credentials(&creds).unwrap();
    (listener, config, store)
}
#[tokio::test]
async fn startup_and_explicit_presence_use_saved_identity_and_online_events() {
    let future = async {
        for (announce, online) in [(true, true), (true, false), (false, true)] {
            let (listener, mut config, store) = setup(online, Some("@A@@B@")).await;
            config.announce_presence_on_connect = announce;
            let server = tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                    .await;
                if announce {
                    if online {
                        assert_eq!(peer.read().await.tag, "ib");
                    }
                    let n = peer.read().await;
                    assert_eq!(n.tag, "presence");
                    assert_eq!(n.attrs["name"], "AB");
                    assert_eq!(
                        n.attrs["type"],
                        if online { "available" } else { "unavailable" }
                    );
                }
                assert_eq!(peer.read().await.tag, "ib");
                let n = peer.read().await;
                assert_eq!(n.attrs["type"], "unavailable");
                assert_eq!(peer.read().await.tag, "ib");
                assert_eq!(peer.read().await.attrs["type"], "available");
                for (to, kind, from) in [
                    ("200:3@lid", Presence::Recording, "100@lid"),
                    (
                        "200@hosted.lid",
                        Presence::Composing,
                        "100:2@s.whatsapp.net",
                    ),
                    (
                        "200@s.whatsapp.net",
                        Presence::Paused,
                        "100:2@s.whatsapp.net",
                    ),
                ] {
                    let n = peer.read().await;
                    assert_eq!(
                        n,
                        presence::update_node(
                            &Contact {
                                id: "100:2@s.whatsapp.net".into(),
                                lid: Some("100@lid".into()),
                                ..Default::default()
                            },
                            kind,
                            Some(to)
                        )
                        .unwrap()
                        .unwrap()
                    );
                    assert_eq!(n.attrs["from"], from);
                }
                peer.closed().await;
            });
            let (socket, mut events) = Socket::connect(config, store).await.unwrap();
            socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
            socket
                .send_presence_update(Presence::Unavailable, None)
                .await
                .unwrap();
            socket
                .send_presence_update(Presence::Available, None)
                .await
                .unwrap();
            for (to, kind) in [
                ("200:3@lid", Presence::Recording),
                ("200@hosted.lid", Presence::Composing),
                ("200@s.whatsapp.net", Presence::Paused),
            ] {
                socket.send_presence_update(kind, Some(to)).await.unwrap();
            }
            assert!(
                socket
                    .send_presence_update(Presence::Composing, None)
                    .await
                    .is_err()
            );
            let actual = std::iter::from_fn(|| events.try_recv().ok())
                .filter_map(|e| {
                    if let Event::Online(v) = e {
                        Some(v)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                if announce {
                    vec![online, false, true]
                } else {
                    vec![false, true]
                }
            );
            socket.close().await.unwrap();
            server.await.unwrap();
        }
    };
    timeout(Duration::from_secs(12), future).await.unwrap();
}
#[tokio::test]
async fn incoming_presence_bypasses_buffers_and_subscription_uses_normalized_tokens() {
    let future = async {
        let (listener, mut config, store) = setup(false, None).await;
        config.should_ignore_jid = IgnoreJid::new(|jid| jid == "group@g.us");
        Store::new(store.clone())
            .store_mappings(&[Mapping {
                pn: "200@s.whatsapp.net".into(),
                lid: "900@lid".into(),
            }])
            .unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for (jid, timestamp) in [
            ("900@lid", now),
            ("expired@s.whatsapp.net", 0),
            ("group@g.us", now),
            ("200@hosted.lid", now),
        ] {
            store
                .set(&[KeyMutation::put(
                    jid,
                    KeyValue::Tctoken(TcToken {
                        token: vec![1, 2, 3],
                        timestamp: Some(timestamp.into()),
                        sender_timestamp: Some(9),
                        extra: Default::default(),
                    }),
                )])
                .unwrap();
        }
        let receive = fixture()["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["kind"] == "receive" && c["ignored"] == false)
            .cloned()
            .collect::<Vec<_>>();
        let expected = receive
            .iter()
            .filter(|c| c["input"]["attrs"]["from"] != "group@g.us")
            .flat_map(|c| {
                c["expected"]["events"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
            })
            .map(|e| e["data"].clone())
            .collect::<Vec<_>>();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                .await;
            assert_eq!(peer.read().await.tag, "ib");
            let mut ids = std::collections::BTreeSet::new();
            for (to, token) in [
                ("200:3@c.us", true),
                ("900:5@lid", true),
                ("expired@s.whatsapp.net", false),
                ("group@g.us", false),
                ("200@hosted.lid", false),
                ("channel@newsletter", false),
            ] {
                let n = peer.read().await;
                assert_eq!(n.tag, "presence");
                assert_eq!(n.attrs["to"], to.replace("@c.us", "@s.whatsapp.net"));
                assert_eq!(n.attrs["type"], "subscribe");
                assert!(ids.insert(n.attrs["id"].clone()));
                if token {
                    assert_eq!(n.child("tctoken").unwrap().attrs["t"], now.to_string());
                    assert_eq!(
                        n.child("tctoken").unwrap().content,
                        Some(NodeContent::Bytes(vec![1, 2, 3]))
                    );
                } else {
                    assert!(n.content.is_none());
                }
            }
            for c in receive {
                peer.send(node(&c["input"])).await;
            }
            let probe = peer.read().await;
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket.buffer_events();
        socket
            .send_presence_update(Presence::Available, None)
            .await
            .unwrap(); // no saved name: no wire/event
        for to in [
            "200:3@c.us",
            "900:5@lid",
            "expired@s.whatsapp.net",
            "group@g.us",
            "200@hosted.lid",
            "channel@newsletter",
        ] {
            socket.presence_subscribe(to).await.unwrap();
        }
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .attr("xmlns", "probe"),
                None,
            )
            .await
            .unwrap();
        let actual = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|e| {
                if let Event::PresenceUpdated(v) = e {
                    Some(json!(v))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        let expired = store
            .get(
                baileys_rs::auth::KeyCategory::Tctoken,
                &["expired@s.whatsapp.net".into()],
            )
            .unwrap();
        let KeyValue::Tctoken(entry) = &expired["expired@s.whatsapp.net"] else {
            panic!()
        };
        assert!(entry.token.is_empty());
        assert_eq!(entry.sender_timestamp, Some(9));
        socket.close().await.unwrap();
        server.await.unwrap();
    };
    timeout(Duration::from_secs(12), future).await.unwrap();
}
