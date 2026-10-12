#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    calls::{self, Cache, OfferCache},
    crypto::KeyPair,
    messages::own::UpsertKind,
    receipts,
    socket::{Config, Event, Socket},
};
use futures_util::future::BoxFuture;
use prost::Message;
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Notify, broadcast},
    time::timeout,
};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/calls.json")).unwrap()
}
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    if let Some(c) = v["content"].as_array() {
        n.content = Some(NodeContent::Children(c.iter().map(node).collect()));
    }
    n
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn numbers(mut value: Value) -> Value {
    fn convert(v: &mut Value) {
        match v {
            Value::Number(n) => *v = json!(n.as_f64().unwrap()),
            Value::Array(vs) => vs.iter_mut().for_each(convert),
            Value::Object(vs) => vs.values_mut().for_each(convert),
            _ => (),
        }
    }
    convert(&mut value);
    value
}
async fn setup() -> (TcpListener, Config, SqliteAuthStore) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        fire_init_queries: false,
        announce_presence_on_connect: false,
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
        id: "100@s.whatsapp.net".into(),
        lid: Some("100@lid".into()),
        ..Default::default()
    });
    store.save_credentials(&creds).unwrap();
    (listener, config, store)
}
async fn call_event(events: &mut broadcast::Receiver<Event>) -> Vec<calls::Event> {
    loop {
        if let Event::Calls(calls) = events.recv().await.unwrap() {
            return calls;
        }
    }
}
#[tokio::test]
async fn automatic_and_manual_calls_match_original_events_messages_and_acks() {
    let future = async {
        let f = fixture();
        let case = f["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| {
                c["ops"].as_array().unwrap().len() == 4
                    && c["ops"][0]["node"]["attrs"]["from"] == "group@g.us"
                    && c["expected"]["calls"][0][0]["isVideo"] == true
            })
            .unwrap()
            .clone();
        for automatic in [true, false] {
            let (listener, mut config, store) = setup().await;
            config.process_calls = automatic;
            let release = Arc::new(Notify::new());
            let next = release.clone();
            let acknowledged = Arc::new(Notify::new());
            let server_acknowledged = acknowledged.clone();
            let ops = case["ops"].as_array().unwrap().clone();
            let server = tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                    .await;
                assert_eq!(peer.read().await.tag, "ib");
                for op in ops {
                    let n = node(&op["node"]);
                    peer.send(n.clone()).await;
                    assert_eq!(
                        peer.read().await,
                        receipts::acknowledgement(&n, None, Some("100@s.whatsapp.net"))
                    );
                    server_acknowledged.notify_one();
                    next.notified().await;
                }
                peer.closed().await;
            });
            let (socket, mut events) = Socket::connect(config, store).await.unwrap();
            socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
            let mut messages = case["expected"]["messages"].as_array().unwrap().iter();
            for (op, expected) in case["ops"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case["expected"]["calls"].as_array().unwrap())
            {
                if !automatic {
                    loop {
                        if let Event::Node(n) = events.recv().await.unwrap()
                            && n.tag == "call"
                        {
                            assert_eq!(n, node(&op["node"]));
                            socket.handle_call(&n).await.unwrap();
                            break;
                        }
                    }
                }
                let actual = call_event(&mut events).await;
                assert_eq!(numbers(json!(actual)), numbers(expected.clone()));
                if actual[0].status == calls::Status::Timeout
                    || (actual[0].status == calls::Status::Offer
                        && actual[0].is_group == Some(true))
                {
                    let expected = messages.next().unwrap();
                    loop {
                        if let Event::MessagesUpserted(upsert) = events.recv().await.unwrap() {
                            assert_eq!(upsert.messages.len(), 1);
                            assert_eq!(
                                hex(&upsert.messages[0].encode_to_vec()),
                                expected["wire"].as_str().unwrap()
                            );
                            assert_eq!(
                                match upsert.kind {
                                    UpsertKind::Append => "append",
                                    UpsertKind::Notify => "notify",
                                },
                                expected["kind"].as_str().unwrap()
                            );
                            break;
                        }
                    }
                }
                acknowledged.notified().await;
                release.notify_one();
            }
            assert!(messages.next().is_none());
            socket.close().await.unwrap();
            server.await.unwrap();
        }
    };
    timeout(Duration::from_secs(15), future).await.unwrap();
}
#[tokio::test]
async fn external_offer_cache_survives_socket_close_and_is_reused_on_reconnect() {
    let future = async {
        let cache = Cache::default();
        let f = fixture();
        let case = f["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| {
                c["ops"].as_array().unwrap().len() == 4
                    && c["ops"][0]["node"]["attrs"]["from"] == "group@g.us"
                    && c["expected"]["calls"][0][0]["isVideo"] == true
            })
            .unwrap();
        for index in [0, 2] {
            let (listener, mut config, store) = setup().await;
            config.call_offer_cache = Some(cache.clone());
            let n = node(&case["ops"][index]["node"]);
            let (done, ready) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                    .await;
                assert_eq!(peer.read().await.tag, "ib");
                peer.send(n.clone()).await;
                assert_eq!(peer.read().await, receipts::acknowledgement(&n, None, None));
                done.send(()).unwrap();
                peer.closed().await;
            });
            let (socket, mut events) = Socket::connect(config, store).await.unwrap();
            assert_eq!(
                numbers(json!(call_event(&mut events).await)),
                numbers(case["expected"]["calls"][index].clone())
            );
            ready.await.unwrap();
            socket.close().await.unwrap();
            server.await.unwrap();
            assert_eq!(cache.get("CALL").await.unwrap().is_some(), index == 0);
        }
    };
    timeout(Duration::from_secs(12), future).await.unwrap();
}
struct FailingCache;
impl OfferCache for FailingCache {
    fn get<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<calls::Event>, calls::Error>> {
        Box::pin(async { Err(calls::Error::Cache("cache get failure".into())) })
    }
    fn set<'a>(&'a self, _: &'a str, _: calls::Event) -> BoxFuture<'a, Result<(), calls::Error>> {
        Box::pin(async { Ok(()) })
    }
    fn delete<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<(), calls::Error>> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::test]
async fn malformed_calls_and_cache_errors_ack_and_leave_the_offline_queue_running() {
    let future = async {
        let (listener, mut config, store) = setup().await;
        config.call_offer_cache = Some(Cache::new(FailingCache));
        let (done, ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "100@lid"))
                .await;
            assert_eq!(peer.read().await.tag, "ib");
            for n in [
                BinaryNode::new("call")
                    .attr("from", "200@s.whatsapp.net")
                    .attr("id", "EMPTY")
                    .attr("offline", "1"),
                BinaryNode::new("call")
                    .attr("from", "200@s.whatsapp.net")
                    .attr("id", "FAIL")
                    .attr("offline", "1")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("offer").attr("call-id", "CALL"),
                    ])),
                BinaryNode::new("receipt")
                    .attr("from", "200@s.whatsapp.net")
                    .attr("id", "RECEIPT")
                    .attr("offline", "1"),
            ] {
                peer.send(n.clone()).await;
                assert_eq!(peer.read().await, receipts::acknowledgement(&n, None, None));
            }
            done.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        ready.await.unwrap();
        assert!(
            !std::iter::from_fn(|| events.try_recv().ok()).any(|e| matches!(e, Event::Calls(_)))
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    };
    timeout(Duration::from_secs(12), future).await.unwrap();
}
