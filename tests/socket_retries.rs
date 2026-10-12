#[path = "support/socket_peer.rs"]
mod socket_peer;
// This shared fixture module also includes receipt-only helpers.
#[allow(dead_code)]
#[path = "support/receipt_helpers.rs"]
mod support;
use baileys_rs::{
    auth::*,
    binary::{self, BinaryNode},
    crypto::{self, KeyPair},
    jid, messages, proto,
    signal::direct,
    socket::{Config, Socket},
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
fn receiver() -> (direct::Repository, direct::Bundle) {
    let store = SqliteAuthStore::in_memory().unwrap();
    let c = store.load_credentials().unwrap();
    let bundle = direct::Bundle {
        registration_id: c.registration_id.into(),
        identity: crypto::signal_public_key(&c.signed_identity_key.public)
            .unwrap()
            .to_vec(),
        signed: direct::PublicPreKey {
            id: c.signed_pre_key.key_id,
            public: crypto::signal_public_key(&c.signed_pre_key.key_pair.public)
                .unwrap()
                .to_vec(),
        },
        signature: c.signed_pre_key.signature.clone(),
        pre_key: None,
    };
    (direct::Repository::new(store), bundle)
}
#[tokio::test]
async fn socket_retry_counts_limits_prekeys_recreation_and_fallback_phone_requests() {
    timeout(Duration::from_secs(12), async {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/retry_requests.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let temp = tempfile::tempdir().unwrap();
            let store = store(&temp);
            let mut creds = store.load_credentials().unwrap();
            let me = creds.me_id().unwrap().to_owned();
            let own = jid::Jid::parse(&me).unwrap().normalized();
            let routing = creds.routing_info.clone();
            creds.next_pre_key_id = 17;
            creds.first_unuploaded_pre_key_id = 1;
            store.save_credentials(&creds).unwrap();
            let mutations: Vec<_> = (1..=16)
                .map(|id| {
                    KeyMutation::put(id.to_string(), KeyValue::PreKey(creds.noise_key.clone()))
                })
                .collect();
            store.set(&mutations).unwrap();
            let (phone, bundle) = receiver();
            let sending = direct::Repository::new(store.clone());
            sending
                .lid_mapping()
                .store_mappings(&[baileys_rs::signal::mapping::Mapping {
                    pn: own.clone(),
                    lid: "987654@lid".into(),
                }])
                .unwrap();
            sending.inject_session("987654@lid", &bundle).unwrap();
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
                enable_recent_message_cache: case["options"]["manager"].as_bool().unwrap(),
                max_msg_retry_count: 3,
                enable_auto_session_recreation: case["options"]["auto"].as_bool().unwrap(),
                query_timeout: Duration::from_secs(2),
                ..Default::default()
            };
            let server_case = case.clone();
            let fallback = !config.enable_recent_message_cache;
            let server = tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                assert_eq!(peer.payload.device, Some(7));
                assert_eq!(peer.routing, routing);
                peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                    .await;
                assert!(peer.read().await.child("unified_session").is_some());
                if fallback {
                    let pdo = peer.read().await;
                    assert_eq!(pdo.attrs["category"], "peer");
                    assert_eq!(pdo.attrs["push_priority"], "high_force");
                    assert_eq!(pdo.child("meta").unwrap().attrs["appdata"], "default");
                    let enc = pdo.child("enc").unwrap();
                    let kind = if enc.attrs["type"] == "pkmsg" {
                        direct::MessageType::PreKey
                    } else {
                        direct::MessageType::Signal
                    };
                    let Some(binary::NodeContent::Bytes(ciphertext)) = &enc.content else {
                        panic!("missing cipher bytes")
                    };
                    let bytes = phone.decrypt(&me, kind, ciphertext).unwrap();
                    let message = proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap();
                    let dsm = message.device_sent_message.unwrap();
                    assert_eq!(dsm.destination_jid.as_deref(), Some(own.as_str()));
                    let request = dsm
                        .message
                        .unwrap()
                        .protocol_message
                        .unwrap()
                        .peer_data_operation_request_message
                        .unwrap();
                    assert_eq!(
                        request.peer_data_operation_request_type,
                        Some(
                            proto::message::PeerDataOperationRequestType::PlaceholderMessageResend
                                as i32
                        )
                    );
                    assert_eq!(
                        request.placeholder_message_resend_request[0]
                            .message_key
                            .as_ref()
                            .unwrap()
                            .id
                            .as_deref(),
                        Some("RETRY")
                    );
                }
                for step in server_case["steps"].as_array().unwrap() {
                    if let Some(wire) = step["wire"].as_str() {
                        assert_eq!(binary::encode(&peer.read().await).unwrap(), hex(wire));
                    }
                }
                peer.closed().await;
            });
            let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
            socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
            let incoming = node(&case["node"]);
            for (attempt, step) in case["steps"].as_array().unwrap().iter().enumerate() {
                socket
                    .send_retry_request(&incoming, attempt == 4)
                    .await
                    .unwrap();
                let c = store.load_credentials().unwrap();
                assert_eq!(
                    c.first_unuploaded_pre_key_id,
                    step["first"].as_u64().unwrap() as u32
                );
                assert_eq!(c.next_pre_key_id, 17);
            }
            if let Some(manager) = socket.message_retry_manager() {
                let stats = manager.statistics().unwrap();
                assert_eq!(stats.total_retries, 4);
                assert_eq!(stats.failed_retries, 1);
                assert_eq!(
                    stats.session_recreations,
                    if case["options"]["auto"] == true {
                        2
                    } else {
                        0
                    }
                );
                manager.cancel_pending_phone_request("RETRY").unwrap();
            }
            let mut updates = 0;
            while let Ok(event) = events.try_recv() {
                if matches!(event, baileys_rs::socket::Event::CredentialsUpdated) {
                    updates += 1;
                }
            }
            assert!(updates >= 3);
            socket.close().await.unwrap();
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn scheduled_resend_and_resolved_placeholder_and_failed_key_preparation() {
    timeout(Duration::from_secs(12), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let mut creds = store.load_credentials().unwrap();
        let me = creds.me_id().unwrap().to_owned();
        let own = jid::Jid::parse(&me).unwrap().normalized();
        let routing = creds.routing_info.clone();
        creds.next_pre_key_id = 2;
        creds.first_unuploaded_pre_key_id = 1;
        store.save_credentials(&creds).unwrap();
        let (phone, bundle) = receiver();
        let (remote, remote_bundle) = receiver();
        let sending = direct::Repository::new(store.clone());
        sending
            .lid_mapping()
            .store_mappings(&[baileys_rs::signal::mapping::Mapping {
                pn: own.clone(),
                lid: "987654@lid".into(),
            }])
            .unwrap();
        sending.inject_session("987654@lid", &bundle).unwrap();
        sending
            .inject_session("200:1@s.whatsapp.net", &remote_bundle)
            .unwrap();
        drop(remote);
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
        let (delivered_tx, delivered_rx) = tokio::sync::oneshot::channel();
        let (fresh_tx, fresh_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.payload.device, Some(7));
            assert_eq!(peer.routing, routing);
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let probe = peer.read().await;
            assert!(probe.child("probe").is_some());
            peer.result(&probe).await;
            let first = peer.read().await;
            assert_eq!(first.attrs["type"], "retry");
            assert_eq!(first.child("retry").unwrap().attrs["count"], "1");
            assert!(first.child("keys").is_none());
            let second = peer.read().await;
            assert_eq!(second.child("retry").unwrap().attrs["count"], "2");
            assert!(second.child("keys").is_some());
            let pdo = peer.read().await;
            assert_eq!(pdo.attrs["category"], "peer");
            let enc = pdo.child("enc").unwrap();
            let Some(binary::NodeContent::Bytes(ciphertext)) = &enc.content else {
                panic!()
            };
            let bytes = phone
                .decrypt(&me, direct::MessageType::PreKey, ciphertext)
                .unwrap();
            let payload = proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap();
            let dsm = payload.device_sent_message.unwrap();
            assert_eq!(dsm.destination_jid.as_deref(), Some(own.as_str()));
            let request = dsm
                .message
                .unwrap()
                .protocol_message
                .unwrap()
                .peer_data_operation_request_message
                .unwrap();
            assert_eq!(
                request.placeholder_message_resend_request[0]
                    .message_key
                    .as_ref()
                    .unwrap()
                    .id
                    .as_deref(),
                Some("SCHEDULED")
            );
            delivered_tx.send(()).unwrap();
            let fresh = peer.read().await;
            assert_eq!(fresh.attrs["id"], "FRESH");
            let bundle = baileys_rs::signal::wire::extract_retry_bundle(&fresh).unwrap();
            assert_eq!(bundle.pre_key.as_ref().unwrap().id, 2);
            phone.inject_session(&me, &bundle).unwrap();
            fresh_tx
                .send(phone.encrypt(&me, b"generated retry key").unwrap())
                .unwrap();
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut incoming = BinaryNode::new("message")
            .attr("from", "200:1@s.whatsapp.net")
            .attr("id", "WAIT")
            .attr("t", "1700000000")
            .attr("sender_lid", "800@lid");
        let mut data =
            messages::decode_node(&incoming, creds.me_id().unwrap(), "987654:7@lid").unwrap();
        data.message.push_name = Some("Original metadata".into());
        let key = data.message.key.clone().unwrap();
        let worker = socket.clone();
        let worker_key = key.clone();
        let preserved = data.clone();
        let request = tokio::spawn(async move {
            worker
                .request_placeholder_resend(&worker_key, Some(preserved))
                .await
                .unwrap()
        });
        timeout(Duration::from_secs(2), async {
            while !socket.has_pending_placeholder_resend("WAIT").unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            socket
                .request_placeholder_resend(&key, None)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            socket.resolve_placeholder_resend("WAIT").unwrap(),
            Some(data)
        );
        assert_eq!(request.await.unwrap().as_deref(), Some("RESOLVED"));
        incoming.attrs.insert("id".into(), "BAD".into());
        let before = serde_json::to_value(store.load_credentials().unwrap()).unwrap();
        assert!(matches!(
            socket
                .send_retry_request(&incoming, true)
                .await
                .unwrap_err(),
            baileys_rs::socket::Error::Auth(Error::InvalidCredentials("missing retry prekey"))
        ));
        socket
            .message_retry_manager()
            .unwrap()
            .cancel_pending_phone_request("BAD")
            .unwrap();
        assert_eq!(
            serde_json::to_value(store.load_credentials().unwrap()).unwrap(),
            before
        );
        socket
            .query(
                BinaryNode::new("iq").attr("type", "get").with_content(
                    binary::NodeContent::Children(vec![BinaryNode::new("probe")]),
                ),
                None,
            )
            .await
            .unwrap();
        store
            .set(&[KeyMutation::put(
                "1",
                KeyValue::PreKey(creds.noise_key.clone()),
            )])
            .unwrap();
        incoming.attrs.insert("id".into(), "SCHEDULED".into());
        socket.send_retry_request(&incoming, false).await.unwrap();
        assert!(sending.validate_session("200:1@s.whatsapp.net").unwrap());
        socket.send_retry_request(&incoming, false).await.unwrap();
        assert!(!sending.validate_session("200:1@s.whatsapp.net").unwrap());
        assert_eq!(
            store
                .load_credentials()
                .unwrap()
                .first_unuploaded_pre_key_id,
            2
        );
        timeout(Duration::from_secs(7), delivered_rx)
            .await
            .unwrap()
            .unwrap();
        let stats = socket
            .message_retry_manager()
            .unwrap()
            .statistics()
            .unwrap();
        assert_eq!(stats.phone_requests, 1);
        assert_eq!(stats.session_recreations, 1);
        incoming.attrs.insert("id".into(), "FRESH".into());
        socket.send_retry_request(&incoming, true).await.unwrap();
        socket
            .message_retry_manager()
            .unwrap()
            .cancel_pending_phone_request("FRESH")
            .unwrap();
        let credentials = store.load_credentials().unwrap();
        assert_eq!(credentials.first_unuploaded_pre_key_id, 3);
        assert_eq!(credentials.next_pre_key_id, 3);
        assert!(
            store
                .get(KeyCategory::PreKey, &["2".into()])
                .unwrap()
                .contains_key("2")
        );
        let reply = fresh_rx.await.unwrap();
        assert_eq!(
            sending
                .decrypt("987654@lid", reply.kind, &reply.ciphertext)
                .unwrap(),
            b"generated retry key"
        );
        assert!(
            store
                .get(KeyCategory::PreKey, &["2".into()])
                .unwrap()
                .is_empty()
        );
        assert!(socket.has_pending_placeholder_resend("SCHEDULED").unwrap());
        socket.close().await.unwrap();
        assert!(!socket.has_pending_placeholder_resend("SCHEDULED").unwrap());
        server.await.unwrap();
    })
    .await
    .unwrap();
}
