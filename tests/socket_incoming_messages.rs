#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
#[allow(dead_code)]
#[path = "support/receipt_helpers.rs"]
mod support;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    messages, proto,
    signal::direct,
    socket::{Config, Event, Socket},
};
use prost::Message as _;
use socket_peer::{Peer, ROOT_PRIVATE};
use support::hex;
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
fn stanza(id: &str, from: &str, content: proto::Message) -> BinaryNode {
    BinaryNode::new("message")
        .attr("id", id)
        .attr("from", from)
        .attr("t", "1700000000")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("plaintext").with_content(NodeContent::Bytes(content.encode_to_vec())),
        ]))
}
async fn upsert(events: &mut tokio::sync::broadcast::Receiver<Event>) -> messages::own::Upsert {
    timeout(Duration::from_secs(3), async {
        loop {
            if let Event::MessagesUpserted(value) = events.recv().await.unwrap() {
                return value;
            }
        }
    })
    .await
    .unwrap()
}
fn sender_for(creds: &Credentials, pre_key: Option<direct::PublicPreKey>) -> direct::Repository {
    let sender_auth = SqliteAuthStore::in_memory().unwrap();
    sender_auth
        .save_credentials(&Credentials::generate().unwrap())
        .unwrap();
    let sender = direct::Repository::new(sender_auth);
    sender
        .inject_session(
            creds.me_id().unwrap(),
            &direct::Bundle {
                registration_id: creds.registration_id.into(),
                identity: baileys_rs::crypto::signal_public_key(&creds.signed_identity_key.public)
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
                pre_key,
            },
        )
        .unwrap();
    sender
}
#[tokio::test]
async fn automatic_signal_decryption_orders_receipts_keys_upserts_and_chat_effects() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let creds = auth.load_credentials().unwrap();
        auth.set(&[KeyMutation::put(
            "200",
            KeyValue::DeviceList(vec!["1".into()]),
        )])
        .unwrap();
        let sender = sender_for(&creds, None);
        let text = proto::Message {
            conversation: Some("received over Signal".into()),
            ..Default::default()
        };
        let reaction = proto::Message {
            reaction_message: Some(proto::message::ReactionMessage {
                key: Some(proto::MessageKey {
                    remote_jid: Some(creds.me_id().unwrap().into()),
                    from_me: Some(false),
                    id: Some("TARGET".into()),
                    ..Default::default()
                }),
                text: Some("👍".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut nodes = vec![];
        for (id, content) in [("FIRST", &text), ("SECOND", &reaction)] {
            let encrypted = sender
                .encrypt(creds.me_id().unwrap(), &messages::encode(content).unwrap())
                .unwrap();
            let mut node = BinaryNode::new("message")
                .attr("id", id)
                .attr("from", "200:1@s.whatsapp.net")
                .attr("t", "1700000000")
                .attr("notify", "Alice")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("enc")
                        .attr(
                            "type",
                            if encrypted.kind == direct::MessageType::PreKey {
                                "pkmsg"
                            } else {
                                "msg"
                            },
                        )
                        .with_content(NodeContent::Bytes(encrypted.ciphertext)),
                ]));
            if id == "SECOND" {
                node = node.attr("sender_lid", "2000@lid").attr("offline", "1");
            }
            nodes.push(node);
        }
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            peer.send_many(&nodes).await;
            for id in ["FIRST", "SECOND"] {
                let receipt = peer.read().await;
                assert_eq!(receipt.tag, "receipt");
                assert_eq!(receipt.attrs["id"], id);
                assert_eq!(receipt.attrs["type"], "inactive");
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let first = upsert(&mut events).await;
        assert_eq!(first.kind, messages::own::UpsertKind::Notify);
        assert_eq!(first.messages[0].message.as_ref(), Some(&text));
        assert_eq!(
            first.messages[0]
                .key
                .as_ref()
                .unwrap()
                .remote_jid
                .as_deref(),
            Some("200@s.whatsapp.net")
        );
        let mut contacts = false;
        let mut unread = false;
        let second = loop {
            match events.recv().await.unwrap() {
                Event::ContactsUpdated(values) => {
                    assert_eq!(values[0].notify.as_deref(), Some("Alice"));
                    contacts = true;
                }
                Event::ChatsUpdated(values) => {
                    assert_eq!(values[0].unread_count, Some(1));
                    unread = true;
                }
                Event::MessagesUpserted(value) => break value,
                _ => {}
            }
        };
        assert!(contacts && unread);
        assert_eq!(second.kind, messages::own::UpsertKind::Append);
        assert_eq!(
            second.metadata[0].key_metadata.remote_jid_alt.as_deref(),
            Some("2000@lid")
        );
        let values = loop {
            if let Event::MessageReactions(v) = events.recv().await.unwrap() {
                break v;
            }
        };
        let key = values[0].key.as_ref().unwrap();
        assert_eq!(key.from_me, Some(true));
        assert_eq!(key.remote_jid.as_deref(), Some("200@s.whatsapp.net"));
        let recent = socket
            .message_retry_manager()
            .unwrap()
            .get_recent_message("200:1@s.whatsapp.net", "SECOND")
            .unwrap()
            .unwrap();
        assert_eq!(
            recent
                .message
                .reaction_message
                .unwrap()
                .key
                .unwrap()
                .from_me,
            Some(true)
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn automatic_failures_nacks_retry_and_newsletter_processing_keep_queries_responsive() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let (listener, mut config) = listener().await;
        config.retry_request_delay = Duration::from_millis(150);
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            let secret = BinaryNode::new("message")
                .attr("id", "SECRET")
                .attr("from", "200@s.whatsapp.net")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("enc").attr("type", "msmsg"),
                ]));
            peer.send(secret).await;
            assert_eq!(peer.read().await.attrs["error"], "495");
            peer.send(
                BinaryNode::new("message")
                    .attr("id", "MALFORMED")
                    .attr("from", "bad")
                    .attr("t", "1"),
            )
            .await;
            assert_eq!(peer.read().await.attrs["error"], "500");
            let failure = BinaryNode::new("message")
                .attr("id", "RETRY")
                .attr("from", "200:1@s.whatsapp.net")
                .attr("t", "1700000000")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("enc")
                        .attr("type", "msg")
                        .with_content(NodeContent::Bytes(vec![0])),
                ]));
            peer.send(failure).await;
            let mut ack = false;
            let mut query = false;
            let mut retry = false;
            while !ack || !query || !retry {
                let n = peer.read().await;
                if n.tag == "iq" {
                    assert!(n.child("probe").is_some());
                    peer.result(&n).await;
                    query = true;
                } else if n.tag == "receipt" {
                    assert_eq!(n.attrs["type"], "retry");
                    retry = true;
                } else {
                    assert_eq!(n.tag, "ack");
                    assert_eq!(n.attrs["error"], "500");
                    ack = true;
                }
            }
            let news = stanza(
                "NEWS",
                "channel@newsletter",
                proto::Message {
                    conversation: Some("newsletter".into()),
                    ..Default::default()
                },
            )
            .attr("offline", "1")
            .attr("server_id", "123");
            peer.send(news).await;
            let ack = peer.read().await;
            assert_eq!(ack.tag, "ack");
            assert_eq!(ack.attrs["id"], "NEWS");
            assert!(!ack.attrs.contains_key("error"));
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if let Event::Node(n) = events.recv().await.unwrap()
                    && n.attrs.get("id").is_some_and(|id| id == "RETRY")
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![BinaryNode::new("probe")])),
                None,
            )
            .await
            .unwrap();
        let failed = upsert(&mut events).await;
        assert_eq!(
            failed.messages[0].message_stub_type,
            Some(proto::web_message_info::StubType::Ciphertext as i32)
        );
        let news = upsert(&mut events).await;
        assert_eq!(news.kind, messages::own::UpsertKind::Append);
        assert_eq!(
            news.metadata[0].key_metadata.server_id.as_deref(),
            Some("123")
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn manual_dispatch_and_self_pushname_updates_use_persistent_actor_credentials() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let (listener, mut config) = listener().await;
        config.process_messages = false;
        let me = auth.load_credentials().unwrap().me_id().unwrap().to_owned();
        let from = baileys_rs::jid::encode(
            &baileys_rs::jid::Jid::parse(&me).unwrap().user,
            "s.whatsapp.net",
            Some(2),
            None,
        );
        let node = stanza(
            "OWN",
            &from,
            proto::Message {
                conversation: Some("own device".into()),
                ..Default::default()
            },
        )
        .attr("recipient", "200@s.whatsapp.net")
        .attr("sender_lid", "2000@lid")
        .attr("notify", "New Name");
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            peer.send(node).await;
            let receipt = peer.read().await;
            assert_eq!(receipt.tag, "receipt");
            assert_eq!(receipt.attrs["type"], "sender");
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let node = loop {
            if let Event::Node(node) = events.recv().await.unwrap()
                && node.tag == "message"
            {
                break node;
            }
        };
        assert!(
            timeout(Duration::from_millis(20), upsert(&mut events))
                .await
                .is_err()
        );
        socket.handle_message(&node).await.unwrap();
        let received = upsert(&mut events).await;
        assert_eq!(
            received.messages[0].key.as_ref().unwrap().from_me,
            Some(true)
        );
        assert_eq!(
            auth.load_credentials()
                .unwrap()
                .me
                .as_ref()
                .unwrap()
                .name
                .as_deref(),
            Some("New Name")
        );
        assert_eq!(
            direct::Repository::new(auth.clone())
                .lid_mapping()
                .get_lid_for_pn("200@s.whatsapp.net")
                .unwrap()
                .as_deref(),
            Some("2000@lid")
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn consumed_prekey_errors_send_parsing_nack_without_retry_or_upsert() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let creds = auth.load_credentials().unwrap();
        let sender = sender_for(
            &creds,
            Some(direct::PublicPreKey {
                id: 9999,
                public: baileys_rs::crypto::signal_public_key(
                    &KeyPair::from_private([33; 32]).public,
                )
                .unwrap()
                .to_vec(),
            }),
        );
        let encrypted = sender
            .encrypt(
                creds.me_id().unwrap(),
                &messages::encode(&proto::Message {
                    conversation: Some("missing prekey".into()),
                    ..Default::default()
                })
                .unwrap(),
            )
            .unwrap();
        let node = BinaryNode::new("message")
            .attr("id", "MISSING")
            .attr("from", "200:1@s.whatsapp.net")
            .attr("t", "1700000000")
            .with_content(NodeContent::Children(vec![
                BinaryNode::new("enc")
                    .attr("type", "pkmsg")
                    .with_content(NodeContent::Bytes(encrypted.ciphertext)),
            ]));
        let (listener, config) = listener().await;
        let (done, ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            peer.send(node).await;
            let ack = peer.read().await;
            assert_eq!(ack.tag, "ack");
            assert_eq!(ack.attrs["error"], "487");
            done.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        ready.await.unwrap();
        assert!(
            timeout(Duration::from_millis(20), upsert(&mut events))
                .await
                .is_err()
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
