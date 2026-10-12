#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    receipts,
    socket::{
        Config, Event, Socket,
        dispatch::{self, Admission},
        presence::IgnoreJid,
    },
};
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, time::timeout};
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    n
}
fn value(n: &BinaryNode) -> Value {
    json!({"tag":n.tag,"attrs":n.attrs})
}
#[test]
fn admission_receipt_recipient_filters_and_nacks_match_original() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/dispatch.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let n = node(&case["node"]);
        let me: Contact = serde_json::from_value(case["me"].clone()).unwrap();
        let ignored = case["ignored"].as_bool().unwrap();
        let trace = Arc::new(Mutex::new(Vec::<Value>::new()));
        let t = trace.clone();
        let filter = IgnoreJid::new(move |jid| {
            t.lock().unwrap().push(json!(["filter", jid]));
            ignored
        });
        match dispatch::admission(&n, Some(&me), &filter) {
            Admission::Ignored => trace.lock().unwrap().push(json!([
                "ack",
                value(&receipts::acknowledgement(
                    &n,
                    (n.tag == "message").then_some(500),
                    Some(&me.id)
                ))
            ])),
            Admission::Offline => trace
                .lock()
                .unwrap()
                .push(json!(["offline", n.tag, value(&n)])),
            Admission::Online => {
                let mut t = trace.lock().unwrap();
                t.push(json!(["online", format!("handling {}", n.tag)]));
                t.push(json!(["exec", value(&n)]));
            }
        }
        assert_eq!(json!(*trace.lock().unwrap()), case["expected"], "{case}");
    }
}
async fn setup() -> (TcpListener, Config, SqliteAuthStore) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        fire_init_queries: false,
        announce_presence_on_connect: false,
        query_timeout: Duration::from_secs(3),
        should_ignore_jid: IgnoreJid::new(|jid| jid == "blocked@s.whatsapp.net"),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut c = store.load_credentials().unwrap();
    c.me = Some(Contact {
        id: "100:2@s.whatsapp.net".into(),
        lid: Some("100@lid".into()),
        ..Default::default()
    });
    store.save_credentials(&c).unwrap();
    (listener, config, store)
}
#[tokio::test]
async fn ignored_nodes_ack_before_processing_and_do_not_enter_buffers_or_offline_queue() {
    let future = async {
        let (listener, config, store) = setup().await;
        let (done, ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                .await;
            assert_eq!(peer.read().await.tag, "ib");
            for (tag, from, participant, recipient) in [
                ("message", "blocked@s.whatsapp.net", None, None),
                (
                    "receipt",
                    "100:7@s.whatsapp.net",
                    None,
                    Some("blocked@s.whatsapp.net"),
                ),
                (
                    "receipt",
                    "group@g.us",
                    Some("blocked@s.whatsapp.net"),
                    None,
                ),
                ("notification", "blocked@s.whatsapp.net", None, None),
                ("call", "blocked@s.whatsapp.net", None, None),
            ] {
                let mut n = BinaryNode::new(tag)
                    .attr("from", from)
                    .attr("id", tag)
                    .attr("offline", "1");
                if let Some(s) = participant {
                    n = n.attr("participant", s);
                }
                if let Some(s) = recipient {
                    n = n.attr("recipient", s);
                }
                if tag == "notification" {
                    n = n
                        .attr("type", "server_sync")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("collection").attr("name", "regular"),
                        ]));
                }
                peer.send(n.clone()).await;
                let ack = peer.read().await;
                // A group receipt filters the group, not its participant, and processes normally.
                assert_eq!(
                    ack,
                    receipts::acknowledgement(
                        &n,
                        (tag == "message").then_some(500),
                        Some("100:2@s.whatsapp.net")
                    )
                );
            }
            done.send(()).unwrap();
            let probe = peer.read().await;
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        assert!(!socket.is_buffering_events());
        ready.await.unwrap();
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .attr("xmlns", "probe"),
                None,
            )
            .await
            .unwrap();
        let raw=std::iter::from_fn(||events.try_recv().ok()).filter(|e|matches!(e,Event::Node(n) if ["message","receipt","notification","call"].contains(&n.tag.as_str()))).count();
        assert_eq!(raw, 5);
        assert!(!socket.is_buffering_events());
        socket.close().await.unwrap();
        server.await.unwrap();
    };
    timeout(Duration::from_secs(12), future).await.unwrap();
}
#[tokio::test]
async fn offline_receipts_wait_for_prior_notification_but_online_receipts_continue() {
    let future = async {
        let (listener, config, store) = setup().await;
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        let (done, complete) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                .await;
            assert_eq!(peer.read().await.tag, "ib");
            let notification = BinaryNode::new("notification")
                .attr("from", "@s.whatsapp.net")
                .attr("id", "N")
                .attr("type", "server_sync")
                .attr("offline", "1")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("collection").attr("name", "regular"),
                ]));
            peer.send(notification.clone()).await;
            let query = peer.read().await;
            assert_eq!(query.attrs["xmlns"], "w:sync:app:state");
            for (id, offline) in [("OFFLINE", true), ("ONLINE", false)] {
                let mut n = BinaryNode::new("receipt")
                    .attr("from", "200@s.whatsapp.net")
                    .attr("id", id)
                    .attr("t", "1700000000");
                if offline {
                    n = n.attr("offline", "1");
                }
                peer.send(n).await;
            }
            let ack = peer.read().await;
            assert_eq!(ack.tag, "ack");
            assert_eq!(ack.attrs["id"], "ONLINE");
            entered.send(()).unwrap();
            wait.await.unwrap();
            peer.send(
                BinaryNode::new("iq")
                    .attr("type", "error")
                    .attr("id", query.attrs["id"].clone())
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error").attr("code", "500"),
                    ])),
            )
            .await;
            assert_eq!(peer.read().await.attrs["id"], "N");
            assert_eq!(peer.read().await.attrs["id"], "OFFLINE");
            done.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        ready.await.unwrap();
        release.send(()).unwrap();
        complete.await.unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
    };
    timeout(Duration::from_secs(12), future).await.unwrap();
}
#[tokio::test]
async fn presence_online_state_switches_delivery_receipt_activity() {
    use baileys_rs::{proto, socket::presence::Presence};
    use prost::Message;
    let future = async {
        let (listener, config, store) = setup().await;
        let mut creds = store.load_credentials().unwrap();
        creds.me.as_mut().unwrap().name = Some("Alice".into());
        store.save_credentials(&creds).unwrap();
        let (done, ready) = tokio::sync::oneshot::channel();
        let receipt_done = Arc::new(tokio::sync::Notify::new());
        let server_receipt_done = receipt_done.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                .await;
            assert_eq!(peer.read().await.tag, "ib");
            for (online, id) in [(true, "ACTIVE"), (false, "INACTIVE")] {
                if online {
                    assert_eq!(peer.read().await.tag, "ib");
                }
                assert_eq!(
                    peer.read().await.attrs["type"],
                    if online { "available" } else { "unavailable" }
                );
                let message = proto::Message {
                    conversation: Some("hello".into()),
                    ..Default::default()
                };
                peer.send(
                    BinaryNode::new("message")
                        .attr("from", "200@s.whatsapp.net")
                        .attr("id", id)
                        .attr("t", "1700000000")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("plaintext")
                                .with_content(NodeContent::Bytes(message.encode_to_vec())),
                        ])),
                )
                .await;
                let receipt = peer.read().await;
                assert_eq!(receipt.tag, "receipt");
                assert_eq!(receipt.attrs["id"], id);
                assert_eq!(
                    receipt.attrs.get("type").map(String::as_str),
                    if online { None } else { Some("inactive") }
                );
                server_receipt_done.notify_one();
                let barrier = peer.read().await;
                peer.result(&barrier).await;
            }
            done.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        for kind in [Presence::Available, Presence::Unavailable] {
            socket.send_presence_update(kind, None).await.unwrap();
            receipt_done.notified().await;
            socket
                .query(
                    BinaryNode::new("iq")
                        .attr("type", "get")
                        .attr("xmlns", "barrier"),
                    None,
                )
                .await
                .unwrap();
        }
        ready.await.unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
    };
    timeout(Duration::from_secs(12), future).await.unwrap();
}
