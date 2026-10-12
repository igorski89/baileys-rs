#[path = "support/socket_peer.rs"]
mod socket_peer;
// Shared parsing helpers also contain receipt-only fixtures.
#[allow(dead_code)]
#[path = "support/receipt_helpers.rs"]
mod support;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    jid, messages, proto, relay,
    signal::direct,
    socket::{Config, Event, Socket},
};
use prost::Message as _;
use socket_peer::{Peer, ROOT_PRIVATE};
use support::{hex, node};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
fn store(temp: &tempfile::TempDir) -> SqliteAuthStore {
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    let mut creds: Credentials = serde_json::from_value(fixture["empty"].clone()).unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/connection.json")).unwrap();
    let node =
        baileys_rs::binary::decode(&hex(fixture["pair"][0]["stanza"].as_str().unwrap())).unwrap();
    baileys_rs::connection::configure_successful_pairing(&node, &creds)
        .unwrap()
        .apply(&mut creds);
    store.save_credentials(&creds).unwrap();
    store
}
async fn listener() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        fire_init_queries: false,
        announce_presence_on_connect: false,
        url: format!("ws://{}", listener.local_addr().unwrap()),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        initialize_session: false,
        query_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    (listener, config)
}
#[tokio::test]
async fn incoming_receipts_automatically_emit_status_and_user_updates_then_ack() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let creds = store.load_credentials().unwrap();
        let user = jid::Jid::parse(creds.me_id().unwrap()).unwrap().user;
        let routing = creds.routing_info.clone();
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/incoming_receipts.json")).unwrap();
        let cases: Vec<_> = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["node"]["attrs"]["type"] != "retry")
            .cloned()
            .collect();
        let expected: Vec<_> = cases
            .iter()
            .flat_map(|c| c["events"].as_array().unwrap().clone())
            .collect();
        let (listener, config) = listener().await;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.payload.device, Some(7));
            assert_eq!(peer.routing, routing);
            peer.send(BinaryNode::new("success").attr("lid", "900:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            for case in cases {
                let mut incoming = node(&case["node"]);
                if incoming.attrs["from"].starts_with("100:") {
                    incoming
                        .attrs
                        .insert("from".into(), format!("{user}:2@s.whatsapp.net"));
                }
                peer.send(incoming.clone()).await;
                assert_eq!(
                    peer.read().await,
                    baileys_rs::receipts::acknowledgement(&incoming, None, None)
                );
            }
            done_tx.send(()).unwrap();
            let probe = peer.read().await;
            assert!(probe.child("probe").is_some());
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        for expected in expected {
            loop {
                match events.recv().await.unwrap() {
                    Event::MessagesUpdated(updates) => {
                        assert_eq!(expected["name"], "messages.update");
                        let data = expected["data"].as_array().unwrap();
                        assert_eq!(updates.len(), data.len());
                        for (actual, expected) in updates.iter().zip(data) {
                            assert_eq!(actual.key.id.as_deref(), expected["key"]["id"].as_str());
                            assert_eq!(
                                actual.key.remote_jid.as_deref(),
                                expected["key"]["remoteJid"].as_str()
                            );
                            assert_eq!(
                                actual.status as i64,
                                expected["update"]["status"].as_i64().unwrap()
                            );
                            assert_eq!(
                                actual.message_timestamp,
                                expected["update"]["messageTimestamp"].as_f64().unwrap()
                            );
                        }
                        break;
                    }
                    Event::MessageReceiptsUpdated(updates) => {
                        assert_eq!(expected["name"], "message-receipt.update");
                        assert_eq!(updates.len(), expected["data"].as_array().unwrap().len());
                        break;
                    }
                    _ => {}
                }
            }
        }
        done_rx.await.unwrap();
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![BinaryNode::new("probe")])),
                None,
            )
            .await
            .unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
fn encrypted_message(repo: &direct::Repository, me: &str, enc: &BinaryNode) -> proto::Message {
    let kind = if enc.attrs["type"] == "pkmsg" {
        direct::MessageType::PreKey
    } else {
        direct::MessageType::Signal
    };
    let Some(NodeContent::Bytes(ciphertext)) = &enc.content else {
        panic!()
    };
    let bytes = repo.decrypt(me, kind, ciphertext).unwrap();
    proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap()
}
#[tokio::test]
async fn automatic_retry_receipts_use_cached_and_async_messages_and_preserve_double_limits() {
    use std::sync::{Arc, Mutex};
    timeout(Duration::from_secs(12), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let creds = store.load_credentials().unwrap();
        let me = creds.me_id().unwrap().to_owned();
        let routing = creds.routing_info.clone();
        let bob_store = SqliteAuthStore::in_memory().unwrap();
        let bob = direct::Repository::new(bob_store.clone());
        let mut bob_creds = bob_store.load_credentials().unwrap();
        bob_creds.account = Some(Default::default());
        let (listener, mut config) = listener().await;
        let slot = Arc::new(Mutex::new(None::<Socket>));
        let lookup_slot = slot.clone();
        let lookups = Arc::new(Mutex::new(vec![]));
        let observed = lookups.clone();
        config.get_message = relay::MessageLookup::new(move |key| {
            let socket = lookup_slot.lock().unwrap().as_ref().unwrap().clone();
            let observed = observed.clone();
            async move {
                let id = key.id.clone().unwrap();
                observed.lock().unwrap().push(id.clone());
                socket
                    .query(
                        BinaryNode::new("iq").attr("type", "get").with_content(
                            NodeContent::Children(vec![
                                BinaryNode::new("lookup").attr("message", &id),
                            ]),
                        ),
                        None,
                    )
                    .await
                    .map_err(|e| relay::Error::Patch(e.to_string()))?;
                Ok(Some(proto::Message {
                    conversation: Some(format!("lookup {id}")),
                    ..Default::default()
                }))
            }
        });
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.payload.device, Some(7));
            assert_eq!(peer.routing, routing);
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let ready = peer.read().await;
            assert!(ready.child("ready").is_some());
            peer.result(&ready).await;
            for (attempt, count) in [2u64, 3, 4, 5].into_iter().enumerate() {
                let key = KeyPair::generate().unwrap();
                let id = attempt as u32 + 1;
                bob_store
                    .set(&[KeyMutation::put(
                        id.to_string(),
                        KeyValue::PreKey(key.clone()),
                    )])
                    .unwrap();
                let incoming = BinaryNode::new("message")
                    .attr("id", "A")
                    .attr("from", "800@lid")
                    .attr("participant", "800:1@lid")
                    .attr("t", "1700000000");
                let mut receipt = baileys_rs::retry::request::receipt(
                    &incoming,
                    "A",
                    count,
                    &bob_creds,
                    Some((id, &key)),
                )
                .unwrap();
                let from = receipt.attrs.shift_remove("to").unwrap();
                receipt.attrs.insert("from".into(), from);
                let Some(NodeContent::Children(children)) = &mut receipt.content else {
                    panic!()
                };
                children.insert(
                    0,
                    BinaryNode::new("list").with_content(NodeContent::Children(vec![
                        BinaryNode::new("item").attr("id", "B"),
                    ])),
                );
                let ack = baileys_rs::receipts::acknowledgement(&receipt, None, None);
                peer.send(receipt).await;
                if attempt < 3 {
                    for id in if attempt == 0 {
                        vec!["B"]
                    } else {
                        vec!["A", "B"]
                    } {
                        let query = peer.read().await;
                        assert_eq!(query.child("lookup").unwrap().attrs["message"], id);
                        peer.result(&query).await;
                    }
                }
                for id in match attempt {
                    0 | 1 => vec!["A", "B"],
                    2 => vec!["B"],
                    _ => vec![],
                } {
                    let sent = peer.read().await;
                    assert_eq!(sent.attrs["id"], id);
                    assert_eq!(sent.attrs["to"], "800:1@lid");
                    assert_eq!(sent.child("enc").unwrap().attrs["count"], count.to_string());
                    let payload = encrypted_message(&bob, &me, sent.child("enc").unwrap());
                    let expected = if attempt == 0 && id == "A" {
                        "cached A".to_owned()
                    } else {
                        format!("lookup {id}")
                    };
                    assert_eq!(payload.conversation.as_deref(), Some(expected.as_str()));
                    let participants = sent.child("participants").unwrap().all_children();
                    assert_eq!(
                        encrypted_message(&bob, &me, participants[0].child("enc").unwrap())
                            .conversation
                            .as_deref(),
                        Some(expected.as_str())
                    );
                }
                assert_eq!(peer.read().await, ack);
            }
            done_tx.send(()).unwrap();
            let probe = peer.read().await;
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store).await.unwrap();
        *slot.lock().unwrap() = Some(socket.clone());
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket
            .message_retry_manager()
            .unwrap()
            .add_recent_message(
                "800@lid",
                "A",
                proto::Message {
                    conversation: Some("cached A".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![BinaryNode::new("ready")])),
                None,
            )
            .await
            .unwrap();
        done_rx.await.unwrap();
        assert_eq!(
            socket
                .message_retry_manager()
                .unwrap()
                .statistics()
                .unwrap()
                .successful_retries,
            6
        );
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![BinaryNode::new("probe")])),
                None,
            )
            .await
            .unwrap();
        assert_eq!(*lookups.lock().unwrap(), ["B", "A", "B", "A", "B"]);
        assert!(
            socket
                .message_retry_manager()
                .unwrap()
                .get_recent_message("800@lid", "A")
                .unwrap()
                .is_none()
        );
        socket.close().await.unwrap();
        slot.lock().unwrap().take();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
