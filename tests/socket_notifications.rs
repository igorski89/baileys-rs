#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    messages::own::UpsertKind,
    proto::web_message_info::StubType,
    receipts,
    socket::{Config, Event, Socket},
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
