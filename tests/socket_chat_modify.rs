#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    app_state::{self, ChatModification, KeyProvider},
    auth::{Contact, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    proto,
    socket::{Config, Event, Socket},
};
use prost::Message as _;
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
async fn setup(key: bool) -> (TcpListener, Config, SqliteAuthStore) {
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
        id: "100:7@s.whatsapp.net".into(),
        ..Default::default()
    });
    if key {
        creds.my_app_state_key_id = Some("AQID".into());
        store
            .set(&[KeyMutation::put(
                "AQID",
                KeyValue::AppStateSyncKey(proto::message::AppStateSyncKeyData {
                    key_data: Some(vec![7; 32]),
                    ..Default::default()
                }),
            )])
            .unwrap();
    }
    store.save_credentials(&creds).unwrap();
    (listener, config, store)
}
fn collection(request: &BinaryNode) -> &BinaryNode {
    assert_eq!(request.attrs["xmlns"], "w:sync:app:state");
    assert_eq!(request.attrs["type"], "set");
    let collections: Vec<_> = request
        .child("sync")
        .unwrap()
        .children("collection")
        .collect();
    assert_eq!(collections.len(), 1);
    collections[0]
}
// A resync reply with no snapshot or patches leaves the stored version unchanged.
fn unchanged(request: &BinaryNode, name: &str) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("id", &request.attrs["id"])
        .attr("type", "result")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("sync").with_content(NodeContent::Children(vec![
                BinaryNode::new("collection").attr("name", name),
            ])),
        ]))
}
#[tokio::test]
async fn chat_modifications_resync_upload_persist_and_publish_own_effects() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup(true).await;
        let server_store = store.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let provider = KeyProvider::from_store(server_store);
            let mut state = app_state::new_lthash_state();
            for (name, index, version) in [
                ("regular_low", "pin_v1", 0u64),
                ("regular_low", "archive", 1),
                ("regular_high", "mute", 0),
            ] {
                // Each patch first resynchronises its collection from the stored version.
                let resync = peer.read().await;
                let requested = collection(&resync);
                assert_eq!(requested.attrs["name"], name);
                assert_eq!(requested.attrs["version"], version.to_string());
                assert_eq!(
                    requested.attrs["return_snapshot"],
                    (version == 0).to_string()
                );
                assert!(requested.child("patch").is_none());
                peer.send(unchanged(&resync, name)).await;
                let upload = peer.read().await;
                let sent = collection(&upload);
                assert_eq!(sent.attrs["name"], name);
                assert_eq!(sent.attrs["version"], version.to_string());
                assert_eq!(sent.attrs["return_snapshot"], "false");
                let Some(NodeContent::Bytes(bytes)) = &sent.child("patch").unwrap().content else {
                    panic!("patch must be binary")
                };
                let mut patch = proto::SyncdPatch::decode(bytes.as_slice()).unwrap();
                assert_eq!(patch.mutations.len(), 1);
                assert_eq!(
                    patch.key_id.as_ref().unwrap().id.as_deref(),
                    Some([1u8, 2, 3].as_slice())
                );
                // The uploaded patch verifies against the previous state with all MACs checked.
                patch.version = Some(proto::SyncdVersion {
                    version: Some(version + 1),
                });
                if version == 0 {
                    state = app_state::new_lthash_state();
                }
                let decoded =
                    app_state::decode_patches(&mut [patch], name, &state, &provider, None, true)
                        .await
                        .unwrap();
                state = decoded.state;
                assert_eq!(state.version, version + 1);
                let mutation = decoded.mutations.values().next().unwrap();
                assert_eq!(mutation.index[0], index);
                assert_eq!(mutation.index[1], "200@s.whatsapp.net");
                peer.result(&upload).await;
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let jid = "200@s.whatsapp.net";
        socket
            .chat_modify(ChatModification::Pin(true), jid)
            .await
            .unwrap();
        socket
            .chat_modify(
                ChatModification::Archive {
                    archive: true,
                    last_messages: None,
                },
                jid,
            )
            .await
            .unwrap();
        socket
            .chat_modify(ChatModification::Mute(Some(1_900_000_000_000)), jid)
            .await
            .unwrap();
        // Own effects are published from the uploaded patch itself.
        let (mut pinned, mut archived, mut muted) = (false, false, false);
        while !(pinned && archived && muted) {
            if let Event::ChatsUpdated(updates) = events.recv().await.unwrap() {
                for update in updates {
                    assert_eq!(update.id, jid);
                    pinned |= matches!(update.pinned, Some(Some(_)));
                    archived |= update.archived == Some(true);
                    muted |= update.mute_end_time == Some(Some(1_900_000_000_000.0));
                }
            }
        }
        let versions = store
            .get(
                KeyCategory::AppStateSyncVersion,
                &["regular_low".to_string(), "regular_high".to_string()],
            )
            .unwrap();
        for (name, version) in [("regular_low", 2), ("regular_high", 1)] {
            let Some(KeyValue::AppStateSyncVersion(state)) = versions.get(name) else {
                panic!("missing {name} version")
            };
            assert_eq!(state.version, version);
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn chat_modifications_require_an_app_state_key_and_valid_input() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup(false).await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let missing_key = socket
            .chat_modify(ChatModification::Pin(true), "200@s.whatsapp.net")
            .await
            .unwrap_err();
        assert_eq!(missing_key.status_code(), Some(400));
        assert_eq!(missing_key.to_string(), "App state key not present!");
        let invalid = socket
            .star("200@s.whatsapp.net", vec![], true)
            .await
            .unwrap_err();
        assert_eq!(invalid.status_code(), Some(400));
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
