#[path = "support/json_numbers.rs"]
mod json_numbers;
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::{self, KeyPair},
    messages, proto,
    signal::direct,
    socket::{Config, Event, SendOptions, Socket},
};
use messages::{
    generation::{self, Content, GenerationHooks, Modifiers},
    own,
};
use prost::Message as _;
use socket_peer::{Peer, ROOT_PRIVATE};
use std::sync::{Arc, Mutex};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
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
fn receiver() -> (SqliteAuthStore, direct::Repository, direct::Bundle) {
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
    (store.clone(), direct::Repository::new(store), bundle)
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
        connect_timeout: Duration::from_secs(2),
        query_timeout: Duration::from_secs(2),
        initialize_session: false,
        ..Default::default()
    };
    (listener, config)
}
struct Hooks(Arc<Mutex<Option<Socket>>>);
impl generation::ContentHooks for Hooks {
    fn now_ms(&self) -> Result<i64, generation::Error> {
        Ok(1_700_000_000_123)
    }
    fn message_secret(&self) -> Result<Vec<u8>, generation::Error> {
        Ok(vec![7; 32])
    }
    fn link_preview(
        &self,
        url: String,
    ) -> generation::HookFuture<'_, Option<generation::LinkPreview>> {
        let socket = self.0.lock().unwrap().clone().unwrap();
        Box::pin(async move {
            socket
                .query(
                    BinaryNode::new("iq")
                        .attr("type", "get")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("preview").attr("url", &url),
                        ])),
                    None,
                )
                .await
                .map_err(|e| generation::Error::Hook(e.to_string()))?;
            Ok(Some(generation::LinkPreview {
                matched_text: Some(url),
                title: Some("mock preview".into()),
                ..Default::default()
            }))
        })
    }
}
fn plaintext(repo: &direct::Repository, me: &str, enc: &BinaryNode) -> proto::Message {
    let kind = if enc.attrs["type"] == "pkmsg" {
        direct::MessageType::PreKey
    } else {
        direct::MessageType::Signal
    };
    let Some(NodeContent::Bytes(bytes)) = &enc.content else {
        panic!("missing ciphertext")
    };
    let bytes = repo.decrypt(me, kind, bytes).unwrap();
    proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap()
}
async fn next_own(events: &mut tokio::sync::broadcast::Receiver<Event>) -> Event {
    loop {
        let e = events.recv().await.unwrap();
        if matches!(
            e,
            Event::MessagesUpserted(_)
                | Event::MessageUpdates(_)
                | Event::MessageReactions(_)
                | Event::ChatsUpdated(_)
        ) {
            return e;
        }
    }
}
#[tokio::test]
async fn send_generates_encrypts_metadata_and_own_events_and_keeps_actor_responsive() {
    timeout(Duration::from_secs(12), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let mut creds = store.load_credentials().unwrap();
        creds.account_settings.unarchive_chats = true;
        store.save_credentials(&creds).unwrap();
        let me = creds.me_id().unwrap().to_owned();
        let routing = creds.routing_info.clone();
        let (_bob_store, bob, bundle) = receiver();
        direct::Repository::new(store.clone())
            .inject_session("800:1@lid", &bundle)
            .unwrap();
        let (_, _, own_bundle) = receiver();
        direct::Repository::new(store.clone())
            .inject_session("987654:2@lid", &own_bundle)
            .unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        store
            .set(&[KeyMutation::put(
                "800:1@lid",
                KeyValue::Tctoken(TcToken {
                    token: vec![1],
                    timestamp: Some(now.into()),
                    sender_timestamp: Some(now),
                    ..Default::default()
                }),
            )])
            .unwrap();
        let target = proto::MessageKey {
            remote_jid: Some("group@g.us".into()),
            id: Some("TARGET".into()),
            from_me: Some(false),
            ..Default::default()
        };
        let mut own_target = target.clone();
        own_target.from_me = Some(true);
        let cases = vec![
            (
                Content::text("visit https://example.com/path"),
                Modifiers::default(),
                None,
                None,
            ),
            (
                Content::Reaction(Box::new(proto::message::ReactionMessage {
                    key: Some(own_target.clone()),
                    text: Some("💚".into()),
                    ..Default::default()
                })),
                Modifiers::default(),
                None,
                None,
            ),
            (
                Content::Delete(target.clone()),
                Modifiers::default(),
                Some("8"),
                None,
            ),
            (
                Content::text("edited"),
                Modifiers {
                    edit: Some(own_target.clone()),
                    ..Default::default()
                },
                Some("1"),
                None,
            ),
            (
                Content::Pin {
                    key: own_target,
                    kind: 1,
                    duration_seconds: None,
                },
                Modifiers::default(),
                Some("2"),
                None,
            ),
            (
                Content::Poll(Box::new(generation::Poll {
                    name: "Poll".into(),
                    values: vec!["A".into()],
                    selectable_count: Some(1),
                    message_secret: Some(vec![8; 32]),
                    ..Default::default()
                })),
                Modifiers::default(),
                None,
                Some("polltype"),
            ),
            (
                Content::Event(Box::new(generation::Event {
                    name: "Event".into(),
                    start_time: 1_700_000_000,
                    message_secret: Some(vec![9; 32]),
                    ..Default::default()
                })),
                Modifiers::default(),
                None,
                Some("event_type"),
            ),
            (
                Content::Disappearing(generation::Disappearing::Seconds(60)),
                Modifiers::default(),
                None,
                None,
            ),
        ];
        let (listener, mut config) = listener().await;
        let slot = Arc::new(Mutex::new(None));
        config.generation_hooks = GenerationHooks::new(Hooks(slot.clone()));
        let expected: Vec<_> = cases
            .iter()
            .map(|(_, _, edit, meta)| (*edit, *meta))
            .collect();
        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.payload.device, Some(7));
            assert_eq!(peer.routing, routing);
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let preview = peer.read().await;
            assert_eq!(
                preview.child("preview").unwrap().attrs["url"],
                "https://example.com/path"
            );
            peer.result(&preview).await;
            let discovery = peer.read().await;
            assert!(discovery.child("usync").is_some());
            let list = BinaryNode::new("device-list").with_content(NodeContent::Children(vec![
                BinaryNode::new("device")
                    .attr("id", "2")
                    .attr("key-index", "1"),
            ]));
            let devices =
                BinaryNode::new("devices").with_content(NodeContent::Children(vec![list]));
            let user = BinaryNode::new("user")
                .attr("jid", "987654@lid")
                .with_content(NodeContent::Children(vec![devices]));
            let users = BinaryNode::new("list").with_content(NodeContent::Children(vec![user]));
            let usync = BinaryNode::new("usync").with_content(NodeContent::Children(vec![users]));
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &discovery.attrs["id"])
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![usync])),
            )
            .await;
            for (i, (edit, meta)) in expected.into_iter().enumerate() {
                let node = peer.read().await;
                assert_eq!(node.tag, "message", "unexpected stanza {node:?}");
                assert_eq!(node.attrs["id"], format!("SEND{i}"));
                assert_eq!(node.attrs.get("edit").map(String::as_str), edit);
                let metas: Vec<_> = node
                    .all_children()
                    .iter()
                    .filter(|n| n.tag == "meta")
                    .collect();
                if let Some(key) = meta {
                    assert!(
                        metas
                            .iter()
                            .any(|n| n.attrs.get(key).is_some_and(|v| v == "creation"))
                    );
                } else {
                    assert!(
                        !metas.iter().any(|n| n.attrs.contains_key("polltype")
                            || n.attrs.contains_key("event_type"))
                    );
                }
                let enc = node
                    .child("participants")
                    .unwrap()
                    .all_children()
                    .iter()
                    .find(|n| n.attrs["jid"] == "800:1@lid")
                    .unwrap()
                    .child("enc")
                    .unwrap();
                sent_tx.send(plaintext(&bob, &me, enc)).unwrap();
            }
            for (tag, expiration) in [("ephemeral", Some("604800")), ("not_ephemeral", None)] {
                let iq = peer.read().await;
                let child = iq.child(tag).unwrap();
                assert_eq!(
                    child.attrs.get("expiration").map(String::as_str),
                    expiration
                );
                peer.result(&iq).await;
            }
            let probe = peer.read().await;
            assert!(probe.child("probe").is_some());
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        slot.lock().unwrap().replace(socket.clone());
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket
            .apply_server_props(&indexmap::IndexMap::from([(
                "10518".into(),
                "false".into(),
            )]))
            .unwrap();
        for (i, (input, modifiers, _, _)) in cases.into_iter().enumerate() {
            let full = socket
                .send_message(
                    "800:1@lid",
                    &input,
                    &modifiers,
                    SendOptions {
                        message_id: Some(format!("SEND{i}")),
                        timestamp_seconds: Some(1_700_000_000),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(sent_rx.recv().await.unwrap(), full.message.clone().unwrap());
            let effects = own::effects(&full, true).unwrap();
            let count = 1
                + usize::from(effects.mutation.is_some())
                + usize::from(effects.reaction.is_some())
                + usize::from(effects.chat.is_some());
            let mut upsert_seen = false;
            let mut mutation_seen = false;
            let mut reaction_seen = false;
            let mut chat_seen = false;
            for _ in 0..count {
                match next_own(&mut events).await {
                    Event::MessagesUpserted(upsert) => {
                        assert!(!upsert_seen);
                        upsert_seen = true;
                        assert_eq!(upsert.messages.as_slice(), std::slice::from_ref(&full));
                        assert_eq!(upsert.kind, own::UpsertKind::Append);
                    }
                    Event::MessageUpdates(updates) => {
                        assert!(!mutation_seen);
                        mutation_seen = true;
                        assert_eq!(updates.len(), 1);
                        let update = &updates[0];
                        match effects.mutation.as_ref().unwrap() {
                            own::Mutation::Revoke {
                                key, original_key, ..
                            } => {
                                assert_eq!(&update.key, key);
                                assert_eq!(update.info.key.as_ref(), Some(original_key));
                                assert_eq!(
                                    update.info.message_stub_type,
                                    Some(proto::web_message_info::StubType::Revoke as i32)
                                );
                                assert_eq!(
                                    update.update_value()["message"],
                                    serde_json::Value::Null
                                );
                            }
                            own::Mutation::Edit {
                                key,
                                message,
                                timestamp,
                                ..
                            } => {
                                assert_eq!(&update.key, key);
                                assert_eq!(update.info.message.as_ref(), Some(message.as_ref()));
                                assert_eq!(
                                    update.update_value()["messageTimestamp"].as_f64(),
                                    Some(*timestamp)
                                );
                            }
                        }
                    }
                    Event::MessageReactions(updates) => {
                        assert!(!reaction_seen);
                        reaction_seen = true;
                        assert_eq!(
                            updates.as_slice(),
                            std::slice::from_ref(effects.reaction.as_ref().unwrap())
                        );
                    }
                    Event::ChatsUpdated(updates) => {
                        assert!(!chat_seen);
                        chat_seen = true;
                        assert_eq!(
                            updates.as_slice(),
                            std::slice::from_ref(effects.chat.as_ref().unwrap())
                        );
                    }
                    _ => panic!("unexpected own event"),
                }
            }
            assert!(upsert_seen);
            assert_eq!(mutation_seen, effects.mutation.is_some());
            assert_eq!(reaction_seen, effects.reaction.is_some());
            assert_eq!(chat_seen, effects.chat.is_some());
            assert!(
                socket
                    .message_retry_manager()
                    .unwrap()
                    .get_recent_message("800:1@lid", &format!("SEND{i}"))
                    .unwrap()
                    .is_some()
            );
        }
        for value in [
            generation::Disappearing::Enabled,
            generation::Disappearing::Disabled,
        ] {
            assert!(
                socket
                    .send_message(
                        "group@g.us",
                        &Content::Disappearing(value),
                        &Modifiers::default(),
                        SendOptions::default()
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        let error = socket
            .send_message(
                "800:1@lid",
                &Content::Contacts(Box::default()),
                &Modifiers::default(),
                SendOptions::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.status_code(), Some(400));
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![BinaryNode::new("probe")])),
                None,
            )
            .await
            .unwrap();
        while let Ok(e) = events.try_recv() {
            assert!(!matches!(e, Event::MessagesUpserted(_)));
        }
        socket.close().await.unwrap();
        assert!(
            socket
                .send_message(
                    "800:1@lid",
                    &Content::text("closed"),
                    &Modifiers::default(),
                    SendOptions {
                        message_id: Some("FAIL".into()),
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        while let Ok(e) = events.try_recv() {
            assert!(!matches!(e, Event::MessagesUpserted(_)));
        }
        slot.lock().unwrap().take();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
struct FixedHooks(u8);
impl generation::ContentHooks for FixedHooks {
    fn message_secret(&self) -> Result<Vec<u8>, generation::Error> {
        Ok(vec![self.0; 32])
    }
}
#[tokio::test]
async fn newsletter_send_supports_generated_ids_hook_override_and_disabled_own_events() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let (listener, mut config) = listener().await;
        config.emit_own_events = false;
        config.generation_hooks = GenerationHooks::new(FixedHooks(1));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.payload.device, Some(7));
            assert!(peer.routing.is_none());
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            for _ in 0..2 {
                let node = peer.read().await;
                assert_eq!(node.tag, "message");
                assert_eq!(node.attrs["to"], "channel@newsletter");
                let bytes = node.child_buffer("plaintext").unwrap();
                tx.send((
                    node.attrs["id"].clone(),
                    proto::Message::decode(bytes).unwrap(),
                ))
                .unwrap();
            }
            let probe = peer.read().await;
            assert!(probe.child("probe").is_some());
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut ids = vec![];
        for (override_hook, byte) in [(false, 1), (true, 2)] {
            let options = SendOptions {
                message_id: override_hook.then(String::new),
                timestamp_seconds: Some(0),
                hooks: override_hook.then(|| GenerationHooks::new(FixedHooks(2))),
                ..Default::default()
            };
            let full = socket
                .send_message(
                    "channel@newsletter",
                    &Content::text("newsletter"),
                    &Modifiers::default(),
                    options,
                )
                .await
                .unwrap()
                .unwrap();
            let (id, plain) = rx.recv().await.unwrap();
            assert_eq!(full.key.as_ref().unwrap().id.as_deref(), Some(id.as_str()));
            assert!(id.starts_with("3EB0"));
            assert_eq!(id.len(), 22);
            ids.push(id);
            assert_eq!(full.message.as_ref(), Some(&plain));
            assert_eq!(full.message_timestamp, Some(0));
            assert_eq!(
                plain.message_context_info.unwrap().message_secret,
                Some(vec![byte; 32])
            );
        }
        assert_ne!(ids[0], ids[1]);
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![BinaryNode::new("probe")])),
                None,
            )
            .await
            .unwrap();
        while let Ok(event) = events.try_recv() {
            assert!(!matches!(
                event,
                Event::MessagesUpserted(_)
                    | Event::MessageUpdates(_)
                    | Event::MessageReactions(_)
                    | Event::ChatsUpdated(_)
            ));
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[derive(Clone, Default)]
struct MediaUpload {
    paths: Arc<Mutex<Vec<std::path::PathBuf>>>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}
impl baileys_rs::media::MediaUploader for MediaUpload {
    fn upload_media<'a>(
        &'a self,
        path: std::path::PathBuf,
        _kind: baileys_rs::media::MediaType,
        _hash: String,
        _options: baileys_rs::media::UploadOptions,
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<baileys_rs::media::UploadResponse, baileys_rs::media::Error>,
    > {
        Box::pin(async move {
            assert_eq!(std::fs::read(&path).unwrap(), (0..17u8).collect::<Vec<_>>());
            self.paths.lock().unwrap().push(path);
            if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(baileys_rs::media::Error::UploadFailed);
            }
            Ok(baileys_rs::media::UploadResponse {
                url: Some("https://media.example/file".into()),
                direct_path: Some("/media/file".into()),
                ..Default::default()
            })
        })
    }
}
#[tokio::test]
async fn send_media_prepares_newsletter_images_video_notes_and_products_before_relay() {
    timeout(Duration::from_secs(8), async {
        use baileys_rs::media;
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let (listener, mut config) = listener().await;
        config.emit_own_events = false;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            for _ in 0..3 {
                let node = peer.read().await;
                assert_eq!(node.tag, "message");
                assert_eq!(node.attrs["to"], "channel@newsletter");
                tx.send(proto::Message::decode(node.child_buffer("plaintext").unwrap()).unwrap())
                    .unwrap();
            }
            let probe = peer.read().await;
            assert!(probe.child("probe").is_some());
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let uploader = MediaUpload::default();
        let input = media::Input::bytes((0..17u8).collect::<Vec<_>>());
        let contents = [
            Content::Media(Box::new(media::Request {
                input: input.clone(),
                payload: media::Payload::Image(Box::new(proto::message::ImageMessage {
                    caption: Some("image caption".into()),
                    ..Default::default()
                })),
                video_note: false,
            })),
            Content::VideoNote(input.clone()),
            Content::Product {
                image: input,
                template: Box::new(proto::message::ProductMessage {
                    body: Some("product body".into()),
                    ..Default::default()
                }),
            },
        ];
        for (i, content) in contents.iter().enumerate() {
            let full = socket
                .send_message(
                    "channel@newsletter",
                    content,
                    &Modifiers {
                        mentions: vec!["300@lid".into()],
                        ..Default::default()
                    },
                    SendOptions {
                        message_id: Some(format!("MEDIA{i}")),
                        content: generation::ContentOptions {
                            media: media::PrepareOptions {
                                uploader: Some(media::UploadFunction::new(uploader.clone())),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap();
            let wire = rx.recv().await.unwrap();
            assert_eq!(full.message.as_ref(), Some(&wire));
            match i {
                0 => {
                    let p = wire.image_message.unwrap();
                    assert_eq!(p.caption.as_deref(), Some("image caption"));
                    assert!(p.media_key.is_none());
                    assert_eq!(p.file_length, Some(17));
                    assert_eq!(p.context_info.unwrap().mentioned_jid, ["300@lid"]);
                }
                1 => {
                    assert!(wire.video_message.is_none());
                    assert_eq!(wire.ptv_message.unwrap().file_length, Some(17));
                }
                _ => {
                    let p = wire.product_message.unwrap();
                    assert_eq!(p.body.as_deref(), Some("product body"));
                    assert_eq!(
                        p.product.unwrap().product_image.unwrap().file_length,
                        Some(17)
                    );
                }
            }
        }
        uploader
            .fail
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let error = socket
            .send_message(
                "channel@newsletter",
                &contents[0],
                &Modifiers::default(),
                SendOptions {
                    content: generation::ContentOptions {
                        media: media::PrepareOptions {
                            uploader: Some(media::UploadFunction::new(uploader.clone())),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.status_code(), Some(500));
        assert!(uploader.paths.lock().unwrap().iter().all(|p| !p.exists()));
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

#[tokio::test]
async fn own_publication_matches_complete_original_buffered_event_sequences() {
    use baileys_rs::proto_json::JsonField;
    use serde_json::{Value, json};
    fn wire(bytes: Vec<u8>) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
    fn event_value(event: Event) -> Option<Value> {
        Some(match event {
            Event::MessagesUpserted(upsert) => {
                json!({"name":"messages.upsert","type":if upsert.kind==own::UpsertKind::Append{"append"}else{"notify"},"messages":upsert.messages.iter().map(|m|wire(m.encode_to_vec())).collect::<Vec<_>>()})
            }
            Event::ChatsUpdated(chats) => {
                json!({"name":"chats.update","updates":chats.iter().map(|c|{
                let mut value=c.to_value();
                if let Some(messages)=&c.messages {value["messages"]=json!(messages.iter().map(|m|wire(m.encode_to_vec())).collect::<Vec<_>>());}value
            }).collect::<Vec<_>>()})
            }
            Event::MessageUpdates(updates) => {
                json!({"name":"messages.update","updates":updates.iter().map(|update|{
                let value=update.update_value();let key=wire(update.key.encode_to_vec());
                if value.get("message").is_some_and(Value::is_null) {
                    json!({"key":key,"revoke":true,"stub":update.info.message_stub_type,"originalKey":update.info.key.as_ref().map(|k|wire(k.encode_to_vec()))})
                }else{json!({"key":key,"message":wire(update.info.message.as_ref().unwrap().encode_to_vec()),"timestamp":value["messageTimestamp"]})}
            }).collect::<Vec<_>>()})
            }
            Event::MessageReactions(updates) => {
                json!({"name":"messages.reaction","updates":updates.iter().map(|u|json!({"key":u.key.as_ref().map(|k|wire(k.encode_to_vec())),"reaction":wire(u.reaction.encode_to_vec())})).collect::<Vec<_>>()})
            }
            _ => return None,
        })
    }
    timeout(Duration::from_secs(10), async {
        let fixture: Value =
            serde_json::from_str(include_str!("fixtures/send_message.json")).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut count = 0;
        for case in fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["emit"] == true && c["full"].is_string())
        {
            let mut creds = auth.load_credentials().unwrap();
            creds.me.as_mut().unwrap().id = "100:7@s.whatsapp.net".into();
            creds.account_settings.unarchive_chats = case["unarchive"].as_bool().unwrap();
            auth.save_credentials(&creds).unwrap();
            let message =
                proto::WebMessageInfo::decode(hex(case["full"].as_str().unwrap()).as_slice())
                    .unwrap();
            let envelope = messages::Envelope {
                message,
                key_metadata: Default::default(),
                category: None,
                retry_count: None,
                author: "100@s.whatsapp.net".into(),
                sender: "100@s.whatsapp.net".into(),
                protobuf: false,
            };
            // The outer scope holds each synthetic case together while the
            // automatic publication scope executes, then source flushes it.
            socket.buffer_events();
            socket
                .upsert_message(&envelope, own::UpsertKind::Append)
                .await
                .unwrap();
            socket.flush_events().unwrap();
            let mut actual = vec![];
            while let Ok(event) = events.try_recv() {
                if let Some(value) = event_value(event) {
                    actual.push(value);
                }
            }
            assert_eq!(
                json_numbers::canonical(&json!(actual)),
                json_numbers::canonical(&case["bufferedEvents"]),
                "{}",
                case["kind"]
            );
            count += 1;
        }
        assert!(count >= 30);
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
struct PreviewHooks;
impl generation::ContentHooks for PreviewHooks {
    fn link_preview(
        &self,
        url: String,
    ) -> generation::HookFuture<'_, Option<generation::LinkPreview>> {
        Box::pin(async move {
            Ok(Some(generation::LinkPreview {
                matched_text: Some(url),
                title: Some("Hook title".into()),
                ..Default::default()
            }))
        })
    }
}
#[tokio::test]
async fn built_in_link_preview_is_attempted_when_enabled_and_never_blocks_sending() {
    timeout(Duration::from_secs(10), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let (listener, mut config) = listener().await;
        // The preview host resolves to a local port that drops every connection.
        let pages = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = || {
            reqwest::Client::builder()
                .resolve("preview.example.com", pages.local_addr().unwrap())
                .build()
                .unwrap()
        };
        config.media_http = Some(baileys_rs::media::HttpClient::with_clients(
            client(),
            client(),
        ));
        config.link_previews.enabled = true;
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = attempts.clone();
        let page_server = tokio::spawn(async move {
            loop {
                let (stream, _) = pages.accept().await.unwrap();
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(stream);
            }
        });
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            for _ in 0..3 {
                let node = peer.read().await;
                let bytes = node.child_buffer("plaintext").unwrap();
                tx.send(proto::Message::decode(bytes).unwrap()).unwrap();
            }
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let text = "See https://preview.example.com/page for details";
        let send = |text: &'static str, hooks: Option<GenerationHooks>| {
            let socket = socket.clone();
            async move {
                socket
                    .send_message(
                        "channel@newsletter",
                        &Content::text(text),
                        &Modifiers::default(),
                        SendOptions {
                            hooks,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .unwrap();
            }
        };
        // The page cannot be fetched: the text is sent without a preview.
        send(text, None).await;
        let sent = rx.recv().await.unwrap().extended_text_message.unwrap();
        assert_eq!(sent.text.as_deref(), Some(text));
        assert_eq!(sent.title, None);
        assert_eq!(sent.matched_text, None);
        assert!(attempts.load(std::sync::atomic::Ordering::SeqCst) >= 1);
        // A hook that supplies a preview is used instead of fetching.
        let before = attempts.load(std::sync::atomic::Ordering::SeqCst);
        send(text, Some(GenerationHooks::new(PreviewHooks))).await;
        let sent = rx.recv().await.unwrap().extended_text_message.unwrap();
        assert_eq!(sent.title.as_deref(), Some("Hook title"));
        assert_eq!(
            sent.matched_text.as_deref(),
            Some("https://preview.example.com/page")
        );
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), before);
        // Text without a URL fetches nothing.
        send("no link here", None).await;
        assert_eq!(
            rx.recv()
                .await
                .unwrap()
                .extended_text_message
                .unwrap()
                .title,
            None
        );
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), before);
        socket.close().await.unwrap();
        server.await.unwrap();
        page_server.abort();
    })
    .await
    .unwrap();
}
