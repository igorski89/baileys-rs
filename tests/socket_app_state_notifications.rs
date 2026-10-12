#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    app_state::CollectionRequest,
    auth::{Contact, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    proto,
    socket::{Config, Event, Socket},
};
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
async fn setup() -> (TcpListener, Config, SqliteAuthStore) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        fire_init_queries: false,
        announce_presence_on_connect: false,
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        query_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100@s.whatsapp.net".into(),
        ..Default::default()
    });
    store.save_credentials(&creds).unwrap();
    (listener, config, store)
}
fn reply(request: &BinaryNode, name: &str, patch: Option<Vec<u8>>) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("id", &request.attrs["id"])
        .attr("type", "result")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("sync").with_content(NodeContent::Children(vec![
                BinaryNode::new("collection")
                    .attr("name", name)
                    .attr("has_more_patches", "false")
                    .with_content(NodeContent::Children(
                        patch
                            .map(|p| {
                                vec![BinaryNode::new("patch").with_content(NodeContent::Bytes(p))]
                            })
                            .unwrap_or_default(),
                    )),
            ])),
        ]))
}
fn notify(id: &str, children: Vec<BinaryNode>) -> BinaryNode {
    BinaryNode::new("notification")
        .attr("id", id)
        .attr("from", "s.whatsapp.net")
        .attr("type", "server_sync")
        .with_content(NodeContent::Children(children))
}
fn collection() -> BinaryNode {
    BinaryNode::new("collection")
        .attr("name", "regular")
        .attr("version", "999")
}
#[tokio::test]
async fn manual_dispatch_and_ack_trace_match_original_handler() {
    timeout(Duration::from_secs(20), async {
        let cases: Value =
            serde_json::from_str(include_str!("fixtures/app_state_notifications.json")).unwrap();
        for case in cases
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["ackFail"] == false)
        {
            let node: BinaryNode = serde_json::from_value(case["node"].clone()).unwrap();
            let expected = case["expected"]["trace"].as_array().unwrap().clone();
            let fail = case["fail"].as_bool().unwrap();
            let (listener, mut config, store) = setup().await;
            config.process_app_state_notifications = false;
            let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let logs = observed.clone();
            let server = tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                for operation in &expected {
                    let request = peer.read().await;
                    if operation["operation"] == "sync" {
                        let names: Vec<String> =
                            serde_json::from_value(operation["names"].clone()).unwrap();
                        let mut comparable = request.clone();
                        comparable.attrs.shift_remove("id");
                        assert_eq!(
                            comparable,
                            baileys_rs::app_state::collection_query(
                                &names
                                    .iter()
                                    .map(|name| CollectionRequest {
                                        name: name.clone(),
                                        version: 0,
                                        force_snapshot: false
                                    })
                                    .collect::<Vec<_>>()
                            )
                        );
                        logs.lock().unwrap().push(operation.clone());
                        if fail {
                            peer.send(
                                BinaryNode::new("iq")
                                    .attr("id", &request.attrs["id"])
                                    .attr("type", "error")
                                    .with_content(NodeContent::Children(vec![
                                        BinaryNode::new("error")
                                            .attr("code", "503")
                                            .attr("text", "synthetic sync error"),
                                    ])),
                            )
                            .await;
                        } else {
                            peer.send(reply(&request, &names[0], None)).await;
                        }
                    } else {
                        let ack: BinaryNode =
                            serde_json::from_value(operation["node"].clone()).unwrap();
                        assert_eq!(request, ack);
                        logs.lock()
                            .unwrap()
                            .push(json!({"operation":"ack","node":request}));
                    }
                }
                peer.closed().await;
            });
            let (socket, mut events) = Socket::connect(config, store).await.unwrap();
            let result = socket.handle_app_state_notification(&node).await;
            assert_eq!(
                result.is_err(),
                case["expected"]["error"].is_string(),
                "{}",
                case
            );
            if let Ok(handled) = result {
                assert!(handled);
            }
            // The peer completes its trace before accepting close; no message
            // upsert is generated by server_sync notifications.
            assert!(
                !std::iter::from_fn(|| events.try_recv().ok())
                    .any(|e| matches!(e, Event::MessagesUpserted(_)))
            );
            socket.close().await.unwrap();
            server.await.unwrap();
            assert_eq!(json!(*observed.lock().unwrap()), case["expected"]["trace"]);
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn automatic_notifications_serialize_resync_apply_actions_ack_errors_and_keep_queries_live() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup().await;
        store
            .set(&[KeyMutation::put(
                "AQID",
                KeyValue::AppStateSyncKey(proto::message::AppStateSyncKeyData {
                    key_data: Some(vec![7; 32]),
                    ..Default::default()
                }),
            )])
            .unwrap();
        let fixtures: Value =
            serde_json::from_str(include_str!("fixtures/app_state_codec.json")).unwrap();
        let patches: Vec<_> = fixtures["batches"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["variant"] == "good" && c["validate"] == true && c["minimum"].is_null())
            .unwrap()["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| unhex(p.as_str().unwrap()))
            .collect();
        let (waiting, ready) = tokio::sync::oneshot::channel();
        let (finished, done) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(notify(
                "N1",
                vec![
                    collection(),
                    BinaryNode::new("collection").attr("name", "ignored"),
                ],
            ))
            .await;
            peer.send(notify("N2", vec![collection()])).await;
            let first = peer.read().await;
            assert_eq!(
                first.child("sync").unwrap().children("collection").count(),
                1
            );
            assert_eq!(
                first
                    .child("sync")
                    .unwrap()
                    .child("collection")
                    .unwrap()
                    .attrs["version"],
                "0"
            );
            waiting.send(()).unwrap();
            let probe = peer.read().await;
            assert_eq!(probe.attrs["xmlns"], "probe");
            peer.result(&probe).await;
            peer.send(reply(&first, "regular", Some(patches[0].clone())))
                .await;
            let pending = [peer.read().await, peer.read().await];
            let ack = pending.iter().find(|n| n.tag == "ack").unwrap();
            assert_eq!(ack.tag, "ack");
            assert_eq!(ack.attrs["id"], "N1");
            let second = pending.iter().find(|n| n.tag == "iq").unwrap();
            assert_eq!(
                second
                    .child("sync")
                    .unwrap()
                    .child("collection")
                    .unwrap()
                    .attrs["version"],
                "1"
            );
            peer.send(reply(second, "regular", Some(patches[1].clone())))
                .await;
            assert_eq!(peer.read().await.attrs["id"], "N2");
            peer.send(notify("N3", vec![collection()])).await;
            let failed = peer.read().await;
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &failed.attrs["id"])
                    .attr("type", "error")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error").attr("code", "503"),
                    ])),
            )
            .await;
            let ack = peer.read().await;
            assert_eq!(ack.tag, "ack");
            assert_eq!(ack.attrs["id"], "N3");
            assert!(!ack.attrs.contains_key("error"));
            peer.send(notify("N4", vec![])).await;
            assert_eq!(peer.read().await.attrs["id"], "N4");
            finished.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
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
        done.await.unwrap();
        socket.flush_events().unwrap();
        let mut chats = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let Event::ChatsUpdated(updates) = event {
                chats.extend(updates)
            }
        }
        assert_eq!(
            chats.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            ["0", "1"]
        );
        assert!(chats.iter().all(|c| c.mute_end_time == Some(Some(0.0))));
        let states = store
            .get(KeyCategory::AppStateSyncVersion, &["regular".into()])
            .unwrap();
        assert!(
            matches!(states.get("regular"),Some(KeyValue::AppStateSyncVersion(s))if s.version==2)
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn disabled_automatic_dispatch_and_closed_ack_failure_preserve_manual_results() {
    timeout(Duration::from_secs(8), async {
        let (listener, mut config, store) = setup().await;
        config.process_app_state_notifications = false;
        let node = notify("MANUAL", vec![]);
        let sent = node.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(sent).await;
            let probe = peer.read().await;
            assert_eq!(probe.tag, "iq");
            peer.result(&probe).await;
            let ack = peer.read().await;
            assert_eq!(ack.tag, "ack");
            assert_eq!(ack.attrs["id"], "MANUAL");
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        loop {
            if let Event::Node(n) = events.recv().await.unwrap()
                && n.attrs.get("id").is_some_and(|id| id == "MANUAL")
            {
                break;
            }
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
        assert!(socket.handle_app_state_notification(&node).await.unwrap());
        socket.close().await.unwrap();
        server.await.unwrap();
        assert!(socket.handle_app_state_notification(&node).await.unwrap());
        assert!(
            !socket
                .handle_app_state_notification(
                    &BinaryNode::new("notification").attr("type", "picture")
                )
                .await
                .unwrap()
        );
        assert!(
            socket
                .handle_app_state_notification(&notify("FAIL", vec![collection()]))
                .await
                .is_err()
        );
        assert!(
            socket
                .handle_app_state_notification(&notify(
                    "MISSING",
                    vec![BinaryNode::new("collection")]
                ))
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
}
