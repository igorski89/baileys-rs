#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, Credentials, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::{self, KeyPair},
    messages::own::UpsertKind,
    proto::web_message_info::StubType,
    receipts,
    signal::direct,
    socket::{Config, Event, IdentityChange, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
async fn setup(temp: &tempfile::TempDir) -> (TcpListener, Config, SqliteAuthStore) {
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
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100:7@s.whatsapp.net".into(),
        lid: Some("900:7@lid".into()),
        ..Default::default()
    });
    creds.account_settings.unarchive_chats = true;
    store.save_credentials(&creds).unwrap();
    (listener, config, store)
}
fn notification(id: &str, from: &str, kind: &str, children: Vec<BinaryNode>) -> BinaryNode {
    BinaryNode::new("notification")
        .attr("id", id)
        .attr("from", from)
        .attr("type", kind)
        .attr("t", "1700000000")
        .with_content(NodeContent::Children(children))
}
fn nodes() -> Vec<BinaryNode> {
    let group = BinaryNode::new("group")
        .attr("id", "group")
        .attr("subject", "Created")
        .attr("creation", "1699999000")
        .attr("creator", "300@lid")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("participant").attr("jid", "300@lid"),
        ]));
    vec![
        notification(
            "CREATE",
            "group@g.us",
            "w:gp2",
            vec![BinaryNode::new("create").with_content(NodeContent::Children(vec![group]))],
        )
        .attr("participant", "300@lid")
        .attr("participant_pn", "300@s.whatsapp.net"),
        notification(
            "SUBJECT",
            "group@g.us",
            "w:gp2",
            vec![BinaryNode::new("subject").attr("subject", "Renamed")],
        )
        .attr("participant", "300@lid")
        .attr("offline", "1"),
        notification(
            "PICTURE",
            "200:3@s.whatsapp.net",
            "picture",
            vec![BinaryNode::new("set").attr("id", "PIC")],
        ),
        notification(
            "BLOCK",
            "100@s.whatsapp.net",
            "account_sync",
            vec![
                BinaryNode::new("blocklist").with_content(NodeContent::Children(vec![
                    BinaryNode::new("item")
                        .attr("jid", "200@s.whatsapp.net")
                        .attr("action", "block"),
                    BinaryNode::new("item")
                        .attr("jid", "201@lid")
                        .attr("action", "unblock"),
                ])),
            ],
        ),
        notification(
            "MODE",
            "100@s.whatsapp.net",
            "account_sync",
            vec![
                BinaryNode::new("disappearing_mode")
                    .attr("duration", "86400")
                    .attr("t", "1700000006"),
            ],
        ),
        // The source dereferences a missing child; the ACK is still sent.
        notification("BROKEN", "100@s.whatsapp.net", "account_sync", vec![]),
    ]
}
#[tokio::test]
async fn notifications_publish_effects_stub_messages_credentials_and_acks() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let (listener, config, store) = setup(&temp).await;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            // The offline subject change waits in the shared queue but is still acknowledged.
            let mut acked = vec![];
            for node in nodes() {
                peer.send(node.clone()).await;
                let ack = peer.read().await;
                assert_eq!(ack.tag, "ack");
                assert_eq!(ack.attrs["class"], "notification");
                assert_eq!(ack, receipts::acknowledgement(&node, None, None));
                acked.push(ack.attrs["id"].clone());
            }
            assert_eq!(
                acked,
                ["CREATE", "SUBJECT", "PICTURE", "BLOCK", "MODE", "BROKEN"]
            );
            done_tx.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let (mut chats, mut groups, mut stubs, mut pictures, mut blocks, mut mode) =
            (vec![], vec![], vec![], vec![], vec![], false);
        while !(chats.len() == 1
            && groups.len() == 1
            && stubs.len() == 2
            && pictures.len() == 1
            && blocks.len() == 2
            && mode)
        {
            match events.recv().await.unwrap() {
                Event::ChatsUpserted(values) => chats.extend(values),
                Event::GroupsUpserted(values) => groups.extend(values),
                Event::MessagesUpserted(upsert) => {
                    assert_eq!(upsert.kind, UpsertKind::Append);
                    stubs.extend(upsert.messages);
                }
                Event::ContactsUpdated(values) => pictures.extend(
                    values
                        .into_iter()
                        .filter(|contact| contact.img_url.is_some()),
                ),
                Event::BlocklistUpdated { blocklist, add } => blocks.push((blocklist, add)),
                Event::CredentialsUpdated => {
                    mode = store
                        .load_credentials()
                        .unwrap()
                        .account_settings
                        .default_disappearing_mode
                        .is_some()
                }
                _ => {}
            }
        }
        assert_eq!(chats[0].conversation.id, "group@g.us");
        assert_eq!(chats[0].conversation.name.as_deref(), Some("Created"));
        assert_eq!(
            chats[0].conversation.conversation_timestamp,
            Some(1699999000)
        );
        assert_eq!(groups[0].metadata.subject.as_deref(), Some("Created"));
        assert_eq!(groups[0].author.as_deref(), Some("300@lid"));
        assert_eq!(groups[0].author_pn.as_deref(), Some("300@s.whatsapp.net"));
        stubs.sort_by_key(|m| m.message_stub_type);
        assert_eq!(
            stubs[0].message_stub_type,
            Some(StubType::GroupCreate as i32)
        );
        assert_eq!(stubs[0].message_stub_parameters, ["Created"]);
        let key = stubs[0].key.as_ref().unwrap();
        assert_eq!(key.remote_jid.as_deref(), Some("group@g.us"));
        assert_eq!(key.id.as_deref(), Some("CREATE"));
        assert_eq!(key.participant.as_deref(), Some("300@lid"));
        assert_eq!(key.from_me, Some(false));
        assert_eq!(stubs[0].message_timestamp, Some(1700000000));
        assert_eq!(
            stubs[1].message_stub_type,
            Some(StubType::GroupChangeSubject as i32)
        );
        assert_eq!(stubs[1].message_stub_parameters, ["Renamed"]);
        assert_eq!(pictures[0].id, "200@s.whatsapp.net");
        assert_eq!(pictures[0].img_url, Some(Some("changed".into())));
        assert_eq!(
            blocks,
            [
                (vec![Some("200@s.whatsapp.net".to_string())], true),
                (vec![Some("201@lid".to_string())], false)
            ]
        );
        let settings = store.load_credentials().unwrap().account_settings.clone();
        assert!(settings.unarchive_chats);
        let saved = settings.default_disappearing_mode.unwrap();
        assert_eq!(saved.ephemeral_expiration, Some(86400));
        assert_eq!(saved.ephemeral_setting_timestamp, Some(1700000006));
        done_rx.await.unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn disabled_notifications_stay_raw_until_dispatched_manually() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let (listener, mut config, store) = setup(&temp).await;
        config.process_notifications = false;
        let node = nodes().remove(2);
        let sent = node.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            peer.send(sent.clone()).await;
            assert_eq!(
                peer.read().await,
                receipts::acknowledgement(&sent, None, None)
            );
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let raw = loop {
            match events.recv().await.unwrap() {
                Event::Node(raw) if raw.tag == "notification" => break raw,
                Event::ContactsUpdated(_) => panic!("disabled notification was processed"),
                _ => {}
            }
        };
        assert_eq!(raw, node);
        assert!(socket.handle_notification(&raw).await.unwrap());
        assert!(
            !socket
                .handle_notification(&BinaryNode::new("notification").attr("type", "unknown"))
                .await
                .unwrap()
        );
        loop {
            if let Event::ContactsUpdated(values) = events.recv().await.unwrap() {
                assert_eq!(values[0].img_url, Some(Some("changed".into())));
                break;
            }
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn removed_devices_delete_their_sessions_before_acknowledgement() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let (listener, config, store) = setup(&temp).await;
        let ids = ["200.1".to_string(), "200.2".to_string()];
        store
            .set(&[
                KeyMutation::put("200.1", KeyValue::Session(serde_json::json!({}))),
                KeyMutation::put("200.2", KeyValue::Session(serde_json::json!({}))),
            ])
            .unwrap();
        let node = notification(
            "DEVICES",
            "200@s.whatsapp.net",
            "devices",
            vec![
                BinaryNode::new("remove")
                    .attr("device_hash", "HASH")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("device").attr("jid", "200:1@s.whatsapp.net"),
                    ])),
            ],
        );
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            peer.send(node.clone()).await;
            assert_eq!(
                peer.read().await,
                receipts::acknowledgement(&node, None, None)
            );
            done_tx.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        done_rx.await.unwrap();
        let remaining = store.get(KeyCategory::Session, &ids).unwrap();
        assert_eq!(remaining.keys().collect::<Vec<_>>(), ["200.2"]);
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
fn encrypt(id: &str, from: &str, children: Vec<BinaryNode>) -> BinaryNode {
    notification(id, from, "encrypt", children)
}
#[tokio::test]
async fn low_prekey_notifications_upload_keys_before_acknowledgement() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let (listener, config, store) = setup(&temp).await;
        let before = store.load_credentials().unwrap().next_pre_key_id;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let count = |value: &str| vec![BinaryNode::new("count").attr("value", value)];
            // Enough keys, and an unparseable count, upload nothing.
            for (id, value) in [("ENOUGH", "5"), ("NAN", "many")] {
                let node = encrypt(id, "@s.whatsapp.net", count(value));
                peer.send(node.clone()).await;
                assert_eq!(
                    peer.read().await,
                    receipts::acknowledgement(&node, None, None)
                );
            }
            // The source dereferences a missing count; the ACK still follows.
            let node = encrypt("MISSING", "@s.whatsapp.net", vec![]);
            peer.send(node.clone()).await;
            assert_eq!(
                peer.read().await,
                receipts::acknowledgement(&node, None, None)
            );
            // A low count uploads five keys; a failed upload is retried after backoff.
            let node = encrypt("LOW", "@s.whatsapp.net", count("4"));
            peer.send(node.clone()).await;
            let upload = peer.read().await;
            assert_eq!(upload.tag, "iq");
            assert_eq!(upload.attrs["xmlns"], "encrypt");
            assert_eq!(upload.attrs["type"], "set");
            assert_eq!(upload.child("list").unwrap().all_children().len(), 5);
            assert!(upload.child("skey").is_some());
            peer.result(&upload).await;
            assert_eq!(
                peer.read().await,
                receipts::acknowledgement(&node, None, None)
            );
            done_tx.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        done_rx.await.unwrap();
        let creds = store.load_credentials().unwrap();
        assert_eq!(creds.next_pre_key_id, before + 5);
        assert_eq!(creds.first_unuploaded_pre_key_id, before + 5);
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn identity_changes_follow_original_skip_rules_and_refresh_open_sessions() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let (listener, mut config, store) = setup(&temp).await;
        config.process_notifications = false;
        let peer_creds = Credentials::generate().unwrap();
        direct::Repository::new(store.clone())
            .inject_session(
                "200@lid",
                &direct::Bundle {
                    registration_id: peer_creds.registration_id.into(),
                    identity: crypto::signal_public_key(&peer_creds.signed_identity_key.public)
                        .unwrap()
                        .to_vec(),
                    signed: direct::PublicPreKey {
                        id: peer_creds.signed_pre_key.key_id,
                        public: crypto::signal_public_key(
                            &peer_creds.signed_pre_key.key_pair.public,
                        )
                        .unwrap()
                        .to_vec(),
                    },
                    signature: peer_creds.signed_pre_key.signature.clone(),
                    pre_key: None,
                },
            )
            .unwrap();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "900:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            // The forced refresh requests the changed contact's key bundle.
            let mut query = peer.read().await;
            if query.child("usync").is_some() {
                // Background PN alias lookup; an empty result leaves the address unchanged.
                peer.result(&query).await;
                query = peer.read().await;
            }
            assert_eq!(query.attrs["xmlns"], "encrypt");
            let user = &query.child("key").unwrap().all_children()[0];
            assert_eq!(user.attrs["jid"], "200@lid");
            assert_eq!(user.attrs["reason"], "identity");
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &query.attrs["id"])
                    .attr("type", "error")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error")
                            .attr("code", "500")
                            .attr("text", "unavailable"),
                    ])),
            )
            .await;
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let identity = || vec![BinaryNode::new("identity")];
        let mut missing_from = encrypt("I0", "", identity());
        missing_from.attrs.shift_remove("from");
        for (node, expected) in [
            (missing_from, IdentityChange::InvalidNotification),
            (
                encrypt("I1", "", identity()),
                IdentityChange::InvalidNotification,
            ),
            (
                encrypt("I2", "200@s.whatsapp.net", vec![]),
                IdentityChange::NoIdentityNode,
            ),
            (
                encrypt("I3", "200:4@s.whatsapp.net", identity()),
                IdentityChange::SkippedCompanionDevice(4),
            ),
            (
                encrypt("I4", "100@s.whatsapp.net", identity()),
                IdentityChange::SkippedSelfPrimary,
            ),
            (
                encrypt("I5", "900@lid", identity()),
                IdentityChange::SkippedSelfPrimary,
            ),
            (
                encrypt("I6", "300@s.whatsapp.net", identity()),
                IdentityChange::SkippedNoSession,
            ),
            (
                encrypt("I7", "300@s.whatsapp.net", identity()),
                IdentityChange::Debounced,
            ),
            (
                encrypt("I8", "200@lid", identity()).attr("offline", "1"),
                IdentityChange::SkippedOffline,
            ),
            (
                encrypt("I9", "200@lid", identity()),
                IdentityChange::Debounced,
            ),
        ] {
            assert_eq!(
                socket.handle_identity_change(&node).await.unwrap(),
                expected,
                "{node:?}"
            );
        }
        // The debounce entry lasts five seconds, as upstream's NodeCache TTL.
        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(5001)).await;
        tokio::time::resume();
        assert_eq!(
            socket
                .handle_identity_change(&encrypt("I10", "200@lid", identity()))
                .await
                .unwrap(),
            IdentityChange::SessionRefreshFailed
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn privacy_token_notifications_store_tokens_under_the_sender_lid() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let (listener, config, store) = setup(&temp).await;
        let node = notification(
            "TOKEN",
            "200@s.whatsapp.net",
            "privacy_token",
            vec![
                BinaryNode::new("tokens").with_content(NodeContent::Children(vec![
                    BinaryNode::new("token")
                        .attr("type", "trusted_contact")
                        .attr("t", "1700000000")
                        .with_content(NodeContent::Bytes(vec![1, 2, 3])),
                ])),
            ],
        )
        .attr("sender_lid", "2000:3@lid");
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            peer.send(node.clone()).await;
            assert_eq!(
                peer.read().await,
                receipts::acknowledgement(&node, None, None)
            );
            done_tx.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        done_rx.await.unwrap();
        let stored = store
            .get(KeyCategory::Tctoken, &["2000@lid".to_string()])
            .unwrap();
        let Some(KeyValue::Tctoken(entry)) = stored.get("2000@lid") else {
            panic!("token was not stored under the sender LID")
        };
        assert_eq!(entry.token, [1, 2, 3]);
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn errored_message_acks_mark_messages_failed_and_recover_missing_tokens() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let (listener, config, store) = setup(&temp).await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let ack = |id: &str| {
                BinaryNode::new("ack")
                    .attr("id", id)
                    .attr("class", "message")
                    .attr("from", "200@lid")
            };
            peer.send(ack("FINE")).await;
            peer.send(ack("OTHER").attr("class", "receipt").attr("error", "500"))
                .await;
            peer.send(ack("INVALID").attr("error", "479")).await;
            // Two restricted acks for one chat start a single token issuance.
            peer.send(ack("RESTRICTED").attr("error", "463")).await;
            peer.send(ack("AGAIN").attr("error", "463")).await;
            let issue = peer.read().await;
            assert_eq!(issue.attrs["xmlns"], "privacy");
            let token = &issue.child("tokens").unwrap().all_children()[0];
            assert_eq!(token.attrs["jid"], "200@lid");
            assert_eq!(token.attrs["type"], "trusted_contact");
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &issue.attrs["id"])
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("tokens").with_content(NodeContent::Children(vec![
                            BinaryNode::new("token")
                                .attr("jid", "200@lid")
                                .attr("type", "trusted_contact")
                                .attr("t", "1700000000")
                                .with_content(NodeContent::Bytes(vec![9, 9])),
                        ])),
                    ])),
            )
            .await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut updates = vec![];
        while updates.len() < 3 {
            if let Event::MessageUpdates(values) = events.recv().await.unwrap() {
                updates.extend(values);
            }
        }
        updates.sort_by_key(|u| u.key.id.clone());
        let summary: Vec<_> = updates
            .iter()
            .map(|u| {
                assert_eq!(u.key.from_me, Some(true));
                assert_eq!(u.key.remote_jid.as_deref(), Some("200@lid"));
                assert_eq!(u.info.status, Some(0));
                (
                    u.key.id.clone().unwrap(),
                    u.info.message_stub_parameters.clone(),
                )
            })
            .collect();
        let restricted = vec![
            "463".to_string(),
            "Your account has been restricted".to_string(),
        ];
        assert_eq!(
            summary,
            [
                ("AGAIN".to_string(), restricted.clone()),
                ("INVALID".to_string(), vec!["479".to_string()]),
                ("RESTRICTED".to_string(), restricted),
            ]
        );
        // The recovered token is stored for the chat once the issuance reply arrives.
        loop {
            let stored = store
                .get(KeyCategory::Tctoken, &["200@lid".to_string()])
                .unwrap();
            if let Some(KeyValue::Tctoken(entry)) = stored.get("200@lid") {
                assert_eq!(entry.token, [9, 9]);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
