#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Credentials, SqliteAuthStore},
    binary::{self, BinaryNode, NodeContent},
    crypto::KeyPair,
    socket::{Config, ConnectionState, Error, Event, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use tokio::{
    net::TcpListener,
    sync::broadcast,
    time::{Duration, timeout},
};

fn credentials() -> Credentials {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    serde_json::from_value(fixture["empty"].clone()).unwrap()
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn store(temp: &tempfile::TempDir, registered: bool) -> SqliteAuthStore {
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    let mut creds = credentials();
    if registered {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/connection.json")).unwrap();
        let node = binary::decode(&hex(fixture["pair"][0]["stanza"].as_str().unwrap())).unwrap();
        baileys_rs::connection::configure_successful_pairing(&node, &creds)
            .unwrap()
            .apply(&mut creds);
    }
    store.save_credentials(&creds).unwrap();
    store
}
async fn listener() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        connect_timeout: Duration::from_secs(2),
        query_timeout: Duration::from_secs(2),
        initialize_session: false,
        ..Default::default()
    };
    (listener, config)
}
async fn event(
    events: &mut broadcast::Receiver<Event>,
    predicate: impl Fn(&Event) -> bool,
) -> Event {
    timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.unwrap();
            if predicate(&event) {
                return event;
            }
        }
    })
    .await
    .unwrap()
}
fn query(id: &str) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("type", "get")
        .attr("id", id)
        .attr("xmlns", "test")
}

#[tokio::test]
async fn encrypted_incoming_message_decrypts_on_blocking_executor() {
    use baileys_rs::auth::{KeyCategory, KeyMutation, KeyStore, KeyValue};
    use baileys_rs::{messages, proto, signal::direct};
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let recipient = store(&temp, true);
        recipient
            .set(&[KeyMutation::put(
                "200",
                KeyValue::DeviceList(vec!["1".into()]),
            )])
            .unwrap();
        let creds = recipient.load_credentials().unwrap();
        let sender_store = SqliteAuthStore::in_memory().unwrap();
        sender_store
            .save_credentials(&Credentials::generate().unwrap())
            .unwrap();
        let sender = direct::Repository::new(sender_store);
        sender
            .inject_session(
                creds.me_id().unwrap(),
                &direct::Bundle {
                    registration_id: creds.registration_id.into(),
                    identity: baileys_rs::crypto::signal_public_key(
                        &creds.signed_identity_key.public,
                    )
                    .unwrap()
                    .to_vec(),
                    signed: direct::PublicPreKey {
                        id: creds.signed_pre_key.key_id,
                        public: baileys_rs::crypto::signal_public_key(
                            &creds.signed_pre_key.key_pair.public,
                        )
                        .unwrap()
                        .to_vec(),
                    },
                    signature: creds.signed_pre_key.signature.clone(),
                    pre_key: None,
                },
            )
            .unwrap();
        let content = proto::Message {
            conversation: Some("native message over Noise".into()),
            ..Default::default()
        };
        let encrypted = sender
            .encrypt(creds.me_id().unwrap(), &messages::encode(&content).unwrap())
            .unwrap();
        let stanza = BinaryNode::new("message")
            .attr("id", "MSG1")
            .attr("from", "200:1@s.whatsapp.net")
            .attr("t", "1700000000")
            .with_content(NodeContent::Children(vec![
                BinaryNode::new("enc")
                    .attr("type", "pkmsg")
                    .with_content(NodeContent::Bytes(encrypted.ciphertext)),
            ]));
        let second_content = proto::Message {
            conversation: Some("message after envelope migration".into()),
            ..Default::default()
        };
        let second_encrypted = sender
            .encrypt(
                creds.me_id().unwrap(),
                &messages::encode(&second_content).unwrap(),
            )
            .unwrap();
        let second_stanza = BinaryNode::new("message")
            .attr("id", "MSG2")
            .attr("from", "200:1@s.whatsapp.net")
            .attr("t", "1700000001")
            .attr("sender_lid", "900@lid")
            .with_content(NodeContent::Children(vec![
                BinaryNode::new("enc")
                    .attr("type", "pkmsg")
                    .with_content(NodeContent::Bytes(second_encrypted.ciphertext)),
            ]));
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            peer.send(stanza).await;
            peer.send(second_stanza).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, recipient.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let Event::Node(node) = event(
            &mut events,
            |e| matches!(e, Event::Node(n) if n.tag == "message"),
        )
        .await
        else {
            unreachable!()
        };
        let decrypted = socket.decrypt_message(node).await.unwrap();
        assert!(decrypted.envelope.message.message_stub_type.is_none());
        assert_eq!(decrypted.envelope.message.message, Some(content));
        assert!(
            recipient
                .get(KeyCategory::Session, &["200.1".into()])
                .unwrap()
                .contains_key("200.1")
        );
        let Event::Node(node) = event(
            &mut events,
            |e| matches!(e, Event::Node(n) if n.tag == "message"),
        )
        .await
        else {
            unreachable!()
        };
        let decrypted = socket.decrypt_message(node).await.unwrap();
        assert!(decrypted.mapping_errors.is_empty());
        assert!(decrypted.envelope.message.message_stub_type.is_none());
        assert_eq!(decrypted.envelope.message.message, Some(second_content));
        assert!(
            recipient
                .get(KeyCategory::Session, &["200.1".into()])
                .unwrap()
                .is_empty()
        );
        assert!(
            recipient
                .get(KeyCategory::Session, &["900_1.1".into()])
                .unwrap()
                .contains_key("900_1.1")
        );
        assert!(
            direct::Repository::new(recipient)
                .validate_session("200:1@s.whatsapp.net")
                .unwrap()
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn own_session_migration_failure_keeps_logged_in_socket_open() {
    use baileys_rs::auth::{KeyCategory, KeyMutation, KeyStore, KeyValue};
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        let record = serde_json::json!({"version":"invalid"});
        store.set(&[KeyMutation::put("123456789.7", KeyValue::Session(record.clone()))]).unwrap();
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let request = peer.read().await;
            peer.result(&request).await;
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket.query(query("after-migration-failure"), None).await.unwrap();
        assert!(matches!(store.get(KeyCategory::Session, &["123456789.7".into()]).unwrap().get("123456789.7"), Some(KeyValue::Session(value)) if value == &record));
        assert!(store.get(KeyCategory::Session, &["987654_1.7".into()]).unwrap().is_empty());
        assert_eq!(baileys_rs::signal::mapping::Store::new(store).get_lid_for_pn("123456789:7@s.whatsapp.net").unwrap().as_deref(), Some("987654:7@lid"));
        socket.close().await.unwrap();
        server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn device_discovery_cache_and_session_assertions_use_lid_addresses() {
    use baileys_rs::auth::{KeyCategory, KeyStore, KeyValue};
    fn device_response(request: &BinaryNode, user: &str, lid: Option<&str>) -> BinaryNode {
        let mut protocols = vec![
            BinaryNode::new("devices").with_content(NodeContent::Children(vec![
                BinaryNode::new("device-list").with_content(NodeContent::Children(vec![
                    BinaryNode::new("device").attr("id", "0"),
                    BinaryNode::new("device")
                        .attr("id", "1")
                        .attr("key-index", "9"),
                ])),
            ])),
        ];
        if let Some(lid) = lid {
            protocols.push(BinaryNode::new("lid").attr("val", lid));
        }
        BinaryNode::new("iq")
            .attr("id", &request.attrs["id"])
            .attr("type", "result")
            .with_content(NodeContent::Children(vec![
                BinaryNode::new("usync").with_content(NodeContent::Children(vec![
                    BinaryNode::new("list").with_content(NodeContent::Children(vec![
                        BinaryNode::new("user")
                            .attr("jid", user)
                            .with_content(NodeContent::Children(protocols)),
                    ])),
                ])),
            ]))
    }
    fn bundle_response(request: &BinaryNode, template: &BinaryNode, jid: &str) -> BinaryNode {
        let mut reply = template.clone();
        reply.attrs.insert("id".into(), request.attrs["id"].clone());
        let Some(NodeContent::Children(list)) = &mut reply.content else {
            unreachable!()
        };
        let Some(NodeContent::Children(users)) = &mut list[0].content else {
            unreachable!()
        };
        users[0].attrs.insert("jid".into(), jid.into());
        reply
    }
    fn assert_key(request: &BinaryNode, jid: &str, force: bool) {
        let users = request.child("key").unwrap().all_children();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].attrs["jid"], jid);
        assert_eq!(
            users[0].attrs.get("reason").map(String::as_str),
            force.then_some("identity")
        );
    }
    timeout(Duration::from_secs(12), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        let fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/usync.json")).unwrap();
        let template = binary::decode(&hex(fixture["signal"]["bundles"].as_str().unwrap())).unwrap();
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let request = peer.read().await;
            assert_eq!(request.child("usync").unwrap().attrs["context"], "message");
            assert_eq!(request.child("usync").unwrap().child("list").unwrap().all_children()[0].attrs["jid"], "200@s.whatsapp.net");
            peer.send(device_response(&request, "200@s.whatsapp.net", Some("800@lid"))).await;
            let request = peer.read().await;
            assert_key(&request, "800@lid", true);
            peer.send(bundle_response(&request, &template, "800@lid")).await;
            let request = peer.read().await;
            assert_key(&request, "800:1@lid", false);
            peer.send(bundle_response(&request, &template, "800:1@lid")).await;
            let request = peer.read().await;
            assert_key(&request, "800:1@lid", true);
            peer.send(bundle_response(&request, &template, "800:1@lid")).await;
            let request = peer.read().await;
            assert_eq!(request.child("usync").unwrap().attrs["context"], "message");
            peer.send(device_response(&request, "200@s.whatsapp.net", Some("800@lid"))).await;
            let request = peer.read().await;
            assert_key(&request, "800@lid", true);
            peer.send(BinaryNode::new("iq").attr("type", "error").attr("id", &request.attrs["id"]).with_content(NodeContent::Children(vec![BinaryNode::new("error").attr("code", "403")]))).await;
            let request = peer.read().await;
            assert_eq!(request.child("usync").unwrap().attrs["context"], "background");
            peer.send(device_response(&request, "300@s.whatsapp.net", Some("700@lid"))).await;
            let request = peer.read().await;
            assert_key(&request, "700:1@lid", true);
            peer.send(bundle_response(&request, &template, "700:1@lid")).await;
            let request = peer.read().await;
            let users = request.child("key").unwrap().all_children();
            assert_eq!(users.iter().map(|u| u.attrs["jid"].as_str()).collect::<Vec<_>>(), ["700:1@lid", "800@lid"]);
            assert!(users.iter().all(|u| u.attrs.get("reason").is_some_and(|s| s == "identity")));
            peer.send(bundle_response(&request, &template, "700:1@lid")).await;
            let request = peer.read().await;
            assert_eq!(request.child("usync").unwrap().attrs["context"], "background");
            peer.send(device_response(&request, "400@s.whatsapp.net", None)).await;
            let request = peer.read().await;
            assert!(request.child("key").unwrap().all_children().is_empty());
            peer.result(&request).await;
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let devices = socket.get_usync_devices(&["100:7@s.whatsapp.net".into(), "200@s.whatsapp.net".into()], true, false).await.unwrap();
        assert_eq!(devices.iter().map(|d| d.jid.as_str()).collect::<Vec<_>>(), ["100:7@s.whatsapp.net", "200@s.whatsapp.net", "200:1@s.whatsapp.net"]);
        let cached = socket.get_usync_devices(&["200@s.whatsapp.net".into()], true, true).await.unwrap();
        assert_eq!(cached[0].device, 0); // Cached source filtering does not change with ignoreZero.
        assert!(!socket.assert_sessions(&["200@s.whatsapp.net".into(), "200@s.whatsapp.net".into()], false).await.unwrap());
        assert!(socket.assert_sessions(&["200:1@s.whatsapp.net".into()], false).await.unwrap());
        assert!(!socket.assert_sessions(&["200:1@s.whatsapp.net".into()], false).await.unwrap());
        assert!(socket.assert_sessions(&["800:1@lid".into(), "800:1@lid".into()], true).await.unwrap());
        socket.invalidate_user_devices(&["200@s.whatsapp.net".into()]).unwrap();
        let refreshed = socket.get_usync_devices(&["200@s.whatsapp.net".into()], true, true).await.unwrap();
        assert_eq!(refreshed.iter().map(|d| d.device).collect::<Vec<_>>(), [1]);
        assert!(matches!(store.get(KeyCategory::DeviceList, &["200".into()]).unwrap().get("200"), Some(KeyValue::DeviceList(devices)) if devices == &["1"]));
        assert!(socket.assert_sessions(&["300:1@s.whatsapp.net".into()], true).await.unwrap());
        assert!(!socket.assert_sessions(&["300:1@s.whatsapp.net".into()], false).await.unwrap());
        assert!(socket.assert_sessions(&["200@s.whatsapp.net".into(), "700:1@lid".into(), "200@s.whatsapp.net".into()], true).await.unwrap());
        assert!(socket.assert_sessions(&["400@s.whatsapp.net".into()], true).await.unwrap());
        assert!(!socket.assert_sessions(&[], false).await.unwrap());
        assert!(socket.get_usync_devices(&["bad".into()], true, false).await.is_err());
        socket.close().await.unwrap();
        server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn encrypted_login_queries_errors_and_shutdown() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        use baileys_rs::auth::KeyMutation;
        let direct_fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/direct.json")).unwrap();
        let original_session = direct_fixture["initial"]["alice"].clone();
        store.set(&[KeyMutation::put("123456789.7", KeyValue::Session(original_session.clone()))]).unwrap();
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.payload.username, Some(123456789));
            assert_eq!(peer.payload.device, Some(7));
            assert!(peer.payload.device_pairing_data.is_none());
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let a = peer.read().await;
            let b = peer.read().await;
            peer.send_many(&[
                BinaryNode::new("iq")
                    .attr("id", &b.attrs["id"])
                    .attr("type", "result"),
                BinaryNode::new("iq")
                    .attr("id", &a.attrs["id"])
                    .attr("type", "result"),
            ])
            .await;
            let bad = peer.read().await;
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &bad.attrs["id"])
                    .attr("type", "error")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error")
                            .attr("code", "403")
                            .attr("text", "denied"),
                    ])),
            )
            .await;
            let pending = peer.read().await;
            assert_eq!(pending.attrs["id"], "pending");
            peer.send(BinaryNode::new("stream:error").attr("code", "515"))
                .await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        assert_eq!(socket.state(), ConnectionState::Open);
        let (a, b) = tokio::join!(
            socket.query(query("a"), None),
            socket.query(query("b"), None)
        );
        assert_eq!(a.unwrap().attrs["id"], "a");
        assert_eq!(b.unwrap().attrs["id"], "b");
        assert_eq!(
            socket
                .query(query("bad"), None)
                .await
                .unwrap_err()
                .status_code(),
            Some(403)
        );
        assert!(matches!(
            socket.query(query("pending"), None).await,
            Err(Error::Closed(515))
        ));
        event(&mut events, |e| {
            matches!(e, Event::Connection(ConnectionState::Closed { code: 515 }))
        })
        .await;
        assert_eq!(
            store
                .load_credentials()
                .unwrap()
                .me
                .as_ref()
                .unwrap()
                .lid
                .as_deref(),
            Some("987654:7@lid")
        );
        let mappings = baileys_rs::signal::mapping::Store::new(store.clone());
        assert_eq!(mappings.get_lid_for_pn("123456789:7@s.whatsapp.net").unwrap().as_deref(), Some("987654:7@lid"));
        use baileys_rs::auth::{KeyStore,KeyCategory,KeyValue};
        let keys = store.get(KeyCategory::DeviceList, &["123456789".into()]).unwrap();
        assert!(matches!(keys.get("123456789"), Some(KeyValue::DeviceList(devices)) if devices == &["7"]));
        assert!(store.get(KeyCategory::Session, &["123456789.7".into()]).unwrap().is_empty());
        assert!(matches!(store.get(KeyCategory::Session, &["987654_1.7".into()]).unwrap().get("987654_1.7"), Some(KeyValue::Session(value)) if value == &original_session));
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn prekeys_are_uploaded_and_acknowledged_before_open() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        let (listener, mut config) = listener().await;
        config.initialize_session = true;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            let count = peer.read().await;
            assert!(count.child("count").is_some());
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &count.attrs["id"])
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("count").attr("value", "1"),
                    ])),
            )
            .await;
            let upload = peer.read().await;
            assert_eq!(upload.child("list").unwrap().all_children().len(), 5);
            assert_eq!(upload.child_buffer("type"), Some([5].as_slice()));
            peer.result(&upload).await;
            let active = peer.read().await;
            assert!(active.child("active").is_some());
            peer.result(&active).await;
            let digest = peer.read().await;
            assert!(digest.child("digest").is_some());
            peer.send(
                BinaryNode::new("iq")
                    .attr("type", "result")
                    .attr("id", &digest.attrs["id"])
                    .with_content(NodeContent::Children(vec![BinaryNode::new("digest")])),
            )
            .await;
            assert!(peer.read().await.child("unified_session").is_some());
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(3)).await.unwrap();
        let creds = store.load_credentials().unwrap();
        assert_eq!(creds.next_pre_key_id, 6);
        assert_eq!(creds.first_unuploaded_pre_key_id, 6);
        socket.close().await.unwrap();
        assert_eq!(socket.state(), ConnectionState::Closed { code: 428 });
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn query_timeout_cancellation_duplicate_and_capacity_cleanup() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        let (listener, mut config) = listener().await;
        config.max_pending_queries = 1;
        let (seen_tx, mut seen) = tokio::sync::mpsc::channel(8);
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            for _ in 0..4 {
                let node = peer.read().await;
                seen_tx.send(node.attrs["id"].clone()).await.unwrap();
                if node.attrs["id"] == "done" {
                    peer.result(&node).await;
                }
            }
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store).await.unwrap();
        assert!(matches!(
            socket
                .query(query("timeout"), Some(Duration::from_millis(35)))
                .await,
            Err(Error::Timeout)
        ));
        assert_eq!(seen.recv().await.unwrap(), "timeout");
        let clone = socket.clone();
        let cancelled = tokio::spawn(async move { clone.query(query("cancel"), None).await });
        assert_eq!(seen.recv().await.unwrap(), "cancel");
        assert!(matches!(
            socket.query(query("cancel"), None).await,
            Err(Error::PendingLimit)
        ));
        assert!(matches!(
            socket.query(query("full"), None).await,
            Err(Error::PendingLimit)
        ));
        cancelled.abort();
        let _ = cancelled.await;
        // Cancellation and new commands use different channels; allow maintenance to observe the closed receiver.
        tokio::time::sleep(Duration::from_millis(40)).await;
        let clone = socket.clone();
        let cancelled = tokio::spawn(async move { clone.query(query("cancel"), None).await });
        assert_eq!(seen.recv().await.unwrap(), "cancel");
        cancelled.abort();
        let _ = cancelled.await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(socket.query(query("done"), None).await.is_ok());
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn qr_pairing_persistence_and_reconnect_routing() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, false);
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert!(peer.payload.device_pairing_data.is_some());
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", "qr")
                    .attr("type", "set")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("pair-device").with_content(NodeContent::Children(vec![
                            BinaryNode::new("ref")
                                .with_content(NodeContent::Bytes(b"ref".to_vec())),
                        ])),
                    ])),
            )
            .await;
            assert_eq!(peer.read().await.attrs["type"], "result");
            let fixture: serde_json::Value =
                serde_json::from_str(include_str!("fixtures/connection.json")).unwrap();
            peer.send(
                binary::decode(&hex(fixture["pair"][0]["stanza"].as_str().unwrap())).unwrap(),
            )
            .await;
            let reply = peer.read().await;
            assert_eq!(reply.attrs["type"], "result");
            assert!(reply.child("pair-device-sign").is_some());
            assert!(peer.read().await.child("unified_session").is_some());
            peer.send_many(&[
                BinaryNode::new("ib").with_content(NodeContent::Children(vec![
                    BinaryNode::new("edge_routing").with_content(NodeContent::Children(vec![
                        BinaryNode::new("routing_info")
                            .with_content(NodeContent::Bytes(b"route".to_vec())),
                    ])),
                ])),
                BinaryNode::new("stream:error").attr("code", "515"),
            ])
            .await;
            peer.closed().await;
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.routing, Some(b"route".to_vec()));
            assert_eq!(peer.payload.username, Some(123456789));
            assert!(peer.payload.device_pairing_data.is_none());
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            peer.closed().await;
        });
        let (_socket, mut events) = Socket::connect(config.clone(), store.clone())
            .await
            .unwrap();
        let qr = event(&mut events, |e| matches!(e, Event::Qr(_))).await;
        let Event::Qr(qr) = qr else { unreachable!() };
        assert!(qr.contains("#ref,"));
        assert!(!format!("{:?}", Event::Qr(qr)).contains("ref,"));
        event(&mut events, |e| matches!(e, Event::PairingComplete)).await;
        event(&mut events, |e| {
            matches!(e, Event::Connection(ConnectionState::Closed { code: 515 }))
        })
        .await;
        let creds = store.load_credentials().unwrap();
        assert!(creds.account.is_some());
        assert_eq!(creds.routing_info, Some(b"route".to_vec()));
        let (socket, _) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn qr_rotation_and_expiry_are_terminal() {
    timeout(Duration::from_secs(5), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, false);
        let (listener, mut config) = listener().await;
        config.qr_timeout = Some(Duration::from_millis(35));
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", "qr")
                    .attr("type", "set")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("pair-device").with_content(NodeContent::Children(
                            ["first", "second"]
                                .map(|r| {
                                    BinaryNode::new("ref")
                                        .with_content(NodeContent::Bytes(r.as_bytes().to_vec()))
                                })
                                .to_vec(),
                        )),
                    ])),
            )
            .await;
            peer.read().await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        for reference in ["first,", "second,"] {
            let Event::Qr(qr) = event(&mut events, |e| matches!(e, Event::Qr(_))).await else {
                unreachable!()
            };
            assert!(qr.contains(&format!("#{reference}")));
        }
        event(&mut events, |e| {
            matches!(e, Event::Connection(ConnectionState::Closed { code: 408 }))
        })
        .await;
        assert!(matches!(
            socket.wait_for_open(Duration::from_secs(1)).await,
            Err(Error::Closed(408))
        ));
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_last_handle_stops_actor_and_keepalive_pings() {
    timeout(Duration::from_secs(5), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        let (listener, mut config) = listener().await;
        config.keep_alive_interval = Duration::from_millis(35);
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            let ping = peer.read().await;
            assert!(ping.child("ping").is_some());
            assert_eq!(ping.attrs["xmlns"], "w:p");
            peer.result(&ping).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        event(&mut events, |e| matches!(e,Event::Node(n) if n.tag=="iq")).await;
        drop(socket);
        event(&mut events, |e| {
            matches!(e, Event::Connection(ConnectionState::Closed { code: 428 }))
        })
        .await;
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn phone_pairing_persists_derived_secret_after_finish_ack() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, false);
        let (listener, config) = listener().await;
        let (secret_tx, secret_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            let request = peer.read().await;
            let reg = request.child("link_code_companion_reg").unwrap();
            assert_eq!(reg.attrs["stage"], "companion_hello");
            let wrapped = reg
                .child_buffer("link_code_pairing_wrapped_companion_ephemeral_pub")
                .unwrap();
            let key = baileys_rs::crypto::derive_pairing_code_key("ABCDEFGH", &wrapped[..32]);
            let companion =
                baileys_rs::crypto::aes_decrypt_ctr(&wrapped[48..], &key, &wrapped[32..48])
                    .unwrap();
            assert_eq!(companion, credentials().pairing_ephemeral_key_pair.public);
            let fixture: serde_json::Value =
                serde_json::from_str(include_str!("fixtures/connection.json")).unwrap();
            let mut notification =
                binary::decode(&hex(fixture["phone"]["notification"].as_str().unwrap())).unwrap();
            notification
                .attrs
                .insert("id".into(), "phone-notify".into());
            notification
                .attrs
                .insert("type".into(), "link_code_companion_reg".into());
            notification
                .attrs
                .insert("from".into(), "@s.whatsapp.net".into());
            peer.send(notification).await;
            let finish = peer.read().await;
            let reg = finish.child("link_code_companion_reg").unwrap();
            assert_eq!(reg.attrs["stage"], "companion_finish");
            let bytes = reg
                .child_buffer("link_code_pairing_wrapped_key_bundle")
                .unwrap();
            // The fixture's primary ephemeral/identity key is synthetic private [42;32].
            let primary = KeyPair::from_private([42; 32]);
            let shared = primary.shared_key(&companion).unwrap();
            let expanded = baileys_rs::crypto::hkdf(
                &shared,
                32,
                Some(&bytes[..32]),
                b"link_code_pairing_key_bundle_encryption_key",
            )
            .unwrap();
            let plain =
                baileys_rs::crypto::aes_decrypt_gcm(&bytes[44..], &expanded, &bytes[32..44], b"")
                    .unwrap();
            assert_eq!(&plain[..32], credentials().signed_identity_key.public);
            assert_eq!(&plain[32..64], primary.public);
            let identity_shared = primary.shared_key(&plain[..32]).unwrap();
            let adv = baileys_rs::crypto::hkdf(
                &[shared.as_slice(), identity_shared.as_slice(), &plain[64..]].concat(),
                32,
                None,
                b"adv_secret",
            )
            .unwrap();
            let ack = peer.read().await;
            assert_eq!(ack.tag, "ack");
            assert_eq!(ack.attrs["id"], "phone-notify");
            assert_eq!(ack.attrs["type"], "link_code_companion_reg");
            peer.result(&finish).await;
            secret_tx.send(adv).unwrap();
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        assert_eq!(
            socket
                .request_pairing_code("123456789", Some("ABCDEFGH".into()))
                .await
                .unwrap(),
            "ABCDEFGH"
        );
        let secret = secret_rx.await.unwrap();
        loop {
            event(&mut events, |e| matches!(e, Event::CredentialsUpdated)).await;
            if store.load_credentials().unwrap().registered {
                break;
            }
        }
        use base64::Engine;
        assert_eq!(
            store.load_credentials().unwrap().adv_secret_key,
            base64::engine::general_purpose::STANDARD.encode(secret)
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn prekey_retry_reuses_keys_and_missing_digest_uploads_again() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        let (listener, mut config) = listener().await;
        config.initialize_session = true;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("t", "1700000000"))
                .await;
            let count = peer.read().await;
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &count.attrs["id"])
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("count").attr("value", "1"),
                    ])),
            )
            .await;
            let upload = peer.read().await;
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &upload.attrs["id"])
                    .attr("type", "error")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error").attr("code", "500"),
                    ])),
            )
            .await;
            let retry = peer.read().await;
            assert_eq!(retry.content, upload.content);
            peer.result(&retry).await;
            let active = peer.read().await;
            assert!(active.child("active").is_some());
            peer.result(&active).await;
            let digest = peer.read().await;
            peer.result(&digest).await;
            let fallback = peer.read().await;
            assert_eq!(fallback.child("list").unwrap().all_children().len(), 5);
            assert_eq!(
                fallback.child("list").unwrap().all_children()[0].child_buffer("id"),
                Some([0, 0, 6].as_slice())
            );
            peer.result(&fallback).await;
            let unified = peer.read().await;
            let id: i128 = unified.child("unified_session").unwrap().attrs["id"]
                .parse()
                .unwrap();
            let expected =
                (1700000000000_i128 + 3 * 24 * 60 * 60 * 1000) % (7 * 24 * 60 * 60 * 1000);
            assert!((id - expected).abs() < 3000);
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(5)).await.unwrap();
        let creds = store.load_credentials().unwrap();
        assert_eq!(creds.next_pre_key_id, 11);
        assert_eq!(creds.first_unuploaded_pre_key_id, 11);
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejects_untrusted_server_before_client_finish() {
    timeout(Duration::from_secs(5), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, false);
        let (listener, mut config) = listener().await;
        config.noise.certificate_public_key = KeyPair::from_private([99; 32]).public;
        let server = tokio::spawn(async move {
            Peer::accept(listener.accept().await.unwrap().0).await;
        });
        assert!(matches!(
            Socket::connect(config, store).await,
            Err(Error::Noise(baileys_rs::noise::Error::Certificate(_)))
        ));
        // The test peer expects client_finish; dropping the rejected connection ends its task.
        assert!(server.await.unwrap_err().is_panic());
    })
    .await
    .unwrap();
}

#[test]
fn callback_names_match_upstream_router() {
    let node = BinaryNode::new("notification")
        .attr("type", "encrypt")
        .with_content(NodeContent::Children(vec![BinaryNode::new("count")]));
    assert_eq!(
        baileys_rs::socket::callback_names(&node),
        [
            "CB:notification,type:encrypt,count",
            "CB:notification,type:encrypt",
            "CB:notification,type",
            "CB:notification,,count",
            "CB:notification"
        ]
    );
}

#[tokio::test]
async fn usync_contact_lid_and_session_establishment_use_encrypted_queries() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp, true);
        let (listener, config) = listener().await;
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/usync.json")).unwrap();
        let response = binary::decode(&hex(fixture["response"].as_str().unwrap())).unwrap();
        let bundles = binary::decode(&hex(fixture["signal"]["bundles"].as_str().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let contact = peer.read().await;
            let usync = contact.child("usync").unwrap();
            assert_eq!(contact.attrs["xmlns"], "usync");
            assert_ne!(contact.attrs["id"], usync.attrs["sid"]);
            assert!(usync.child("query").unwrap().child("contact").is_some());
            let mut result = response.clone();
            result
                .attrs
                .insert("id".into(), contact.attrs["id"].clone());
            peer.send(result).await;
            let lid = peer.read().await;
            assert_eq!(lid.child("usync").unwrap().attrs["context"], "background");
            assert_eq!(
                lid.child("usync")
                    .unwrap()
                    .child("list")
                    .unwrap()
                    .all_children()
                    .len(),
                1
            );
            let mut result = response;
            result.attrs.insert("id".into(), lid.attrs["id"].clone());
            peer.send(result).await;
            let query = peer.read().await;
            assert_eq!(query.attrs["xmlns"], "encrypt");
            assert_eq!(
                query.child("key").unwrap().all_children()[0].attrs["jid"],
                "200:1@s.whatsapp.net"
            );
            let mut result = bundles;
            result.attrs.insert("id".into(), query.attrs["id"].clone());
            peer.send(result).await;
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        assert_eq!(
            socket
                .on_whatsapp(&["+100".into(), "900@lid".into()])
                .await
                .unwrap(),
            [("100@s.whatsapp.net".into(), true)]
        );
        let mappings = socket
            .fetch_lid_mappings(&["100@s.whatsapp.net".into(), "900@lid".into()])
            .await
            .unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].lid, "900@lid");
        let repository = baileys_rs::signal::direct::Repository::new(store);
        assert_eq!(
            socket
                .establish_sessions(&["200:1@s.whatsapp.net".into()], repository.clone())
                .await
                .unwrap(),
            1
        );
        assert!(repository.validate_session("200:1@s.whatsapp.net").unwrap());
        assert_eq!(
            repository
                .session_info("200:1@s.whatsapp.net")
                .unwrap()
                .unwrap()
                .1,
            11
        );
        assert_eq!(socket.establish_sessions(&[], repository).await.unwrap(), 0);
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
