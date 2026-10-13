#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::BinaryNode,
    crypto::{self, KeyPair},
    socket::{Config, Error, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
fn config(listener: &TcpListener) -> Config {
    Config {
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
    }
}
fn store() -> SqliteAuthStore {
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100:7@s.whatsapp.net".into(),
        ..Default::default()
    });
    store.save_credentials(&creds).unwrap();
    store
}
fn error(request: &BinaryNode) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("type", "error")
        .attr("id", request.attrs["id"].clone())
        .with_content(baileys_rs::binary::NodeContent::Children(vec![
            BinaryNode::new("error")
                .attr("code", "406")
                .attr("text", "not-acceptable"),
        ]))
}
#[tokio::test]
async fn signed_pre_key_rotation_uploads_next_key_and_persists_it_only_on_success() {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let store = store();
        let before = store.load_credentials().unwrap();
        let identity = before.signed_identity_key.public;
        let first_id = before.signed_pre_key.key_id;
        let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            // The first rotation is rejected; the second is accepted.
            let rejected = peer.read().await;
            assert!(rejected.child("rotate").is_some());
            peer.send(error(&rejected)).await;
            let request = peer.read().await;
            assert_eq!(request.tag, "iq");
            assert_eq!(request.attrs["to"], "@s.whatsapp.net");
            assert_eq!(request.attrs["type"], "set");
            assert_eq!(request.attrs["xmlns"], "encrypt");
            assert_eq!(request.all_children().len(), 1);
            let rotate = request.child("rotate").unwrap();
            assert_eq!(rotate.all_children().len(), 1);
            let skey = rotate.child("skey").unwrap();
            let tags: Vec<_> = skey.all_children().iter().map(|n| n.tag.as_str()).collect();
            assert_eq!(tags, ["id", "value", "signature"]);
            let id = skey.child_buffer("id").unwrap().to_vec();
            let value = skey.child_buffer("value").unwrap().to_vec();
            let signature = skey.child_buffer("signature").unwrap().to_vec();
            peer.result(&request).await;
            let _ = sent_tx.send((id, value, signature));
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let failed = socket.rotate_signed_pre_key().await.unwrap_err();
        assert_eq!(failed.status_code(), Some(406));
        let unchanged = store.load_credentials().unwrap().signed_pre_key.clone();
        assert_eq!(unchanged.key_id, first_id);
        assert_eq!(
            unchanged.key_pair.public,
            before.signed_pre_key.key_pair.public
        );
        socket.rotate_signed_pre_key().await.unwrap();
        let (id, value, signature) = sent_rx.await.unwrap();
        let next = first_id + 1;
        assert_eq!(id, next.to_be_bytes()[1..]);
        assert_eq!(signature.len(), 64);
        assert!(crypto::verify_signature(
            &identity,
            &crypto::signal_public_key(&value).unwrap(),
            &signature
        ));
        let after = store.load_credentials().unwrap();
        assert_eq!(after.signed_pre_key.key_id, next);
        assert_eq!(after.signed_pre_key.key_pair.public.to_vec(), value);
        assert_eq!(after.signed_pre_key.signature, signature);
        assert_ne!(
            after.signed_pre_key.key_pair.public,
            before.signed_pre_key.key_pair.public
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn key_bundle_digest_check_reuploads_pre_keys_when_the_server_has_no_bundle() {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let digest = |request: &BinaryNode| {
                assert_eq!(request.attrs["to"], "@s.whatsapp.net");
                assert_eq!(request.attrs["type"], "get");
                assert_eq!(request.attrs["xmlns"], "encrypt");
                assert!(request.child("digest").is_some());
            };
            // A bundle is present.
            let request = peer.read().await;
            digest(&request);
            peer.send(
                BinaryNode::new("iq")
                    .attr("type", "result")
                    .attr("id", request.attrs["id"].clone())
                    .with_content(baileys_rs::binary::NodeContent::Children(vec![
                        BinaryNode::new("digest"),
                    ])),
            )
            .await;
            // No bundle: pre-keys are uploaded again.
            let request = peer.read().await;
            digest(&request);
            peer.result(&request).await;
            let upload = peer.read().await;
            assert_eq!(upload.attrs["type"], "set");
            assert_eq!(upload.attrs["xmlns"], "encrypt");
            assert_eq!(upload.child("list").unwrap().all_children().len(), 5);
            assert!(upload.child("skey").is_some());
            peer.result(&upload).await;
            // A failed query surfaces its own error without an upload.
            let request = peer.read().await;
            digest(&request);
            peer.send(error(&request)).await;
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket.digest_key_bundle().await.unwrap();
        assert!(matches!(
            socket.digest_key_bundle().await,
            Err(Error::DigestMissing)
        ));
        assert_eq!(
            socket.digest_key_bundle().await.unwrap_err().status_code(),
            Some(406)
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn pre_key_upload_waiters_time_out_while_the_upload_itself_continues() {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(&listener);
        config.pre_key_upload_timeout = Duration::from_millis(300);
        let store = store();
        let before = store.load_credentials().unwrap().next_pre_key_id;
        let (timed_out_tx, timed_out_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            // The first upload is answered only after its caller has given up.
            let slow = peer.read().await;
            assert_eq!(slow.child("list").unwrap().all_children().len(), 5);
            timed_out_rx.await.unwrap();
            peer.result(&slow).await;
            let next = peer.read().await;
            assert_eq!(next.child("list").unwrap().all_children().len(), 5);
            peer.result(&next).await;
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let started = std::time::Instant::now();
        let error = socket.upload_pre_keys(5).await.unwrap_err();
        assert!(matches!(error, Error::PreKeyUploadTimeout), "{error:?}");
        assert_eq!(error.status_code(), Some(408));
        assert_eq!(error.to_string(), "Pre-key upload timeout");
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(started.elapsed() < Duration::from_secs(2));
        timed_out_tx.send(()).unwrap();
        // The abandoned upload still completes once the server answers.
        while store
            .load_credentials()
            .unwrap()
            .first_unuploaded_pre_key_id
            != before + 5
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // A later upload starts afresh and succeeds.
        socket.upload_pre_keys(5).await.unwrap();
        let after = store.load_credentials().unwrap();
        assert_eq!(after.next_pre_key_id, before + 10);
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn expired_privacy_tokens_are_pruned_once_when_coming_online() {
    use baileys_rs::{
        auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, TcToken},
        privacy::Repository,
        socket::presence::Presence,
    };
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let store = store();
        let mut creds = store.load_credentials().unwrap();
        creds.me.as_mut().unwrap().name = Some("Me".into());
        store.save_credentials(&creds).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let token = |timestamp: u64| TcToken {
            token: vec![1],
            timestamp: Some(timestamp.into()),
            sender_timestamp: None,
            extra: Default::default(),
        };
        let seed = |jid: &str, timestamp: u64| {
            store
                .set(&[KeyMutation::put(jid, KeyValue::Tctoken(token(timestamp)))])
                .unwrap();
            Repository::new(store.clone()).mark_issued(jid, 0).unwrap();
        };
        seed("dead@lid", 0);
        seed("fresh@lid", now);
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            loop {
                let node = peer.read().await;
                if node.tag == "iq" && node.child("probe").is_some() {
                    peer.result(&node).await;
                    break;
                }
            }
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let held = |jid: &str| {
            store
                .get(KeyCategory::Tctoken, &[jid.to_owned()])
                .unwrap()
                .contains_key(jid)
        };
        assert!(held("dead@lid") && held("fresh@lid"));
        socket
            .send_presence_update(Presence::Available, None)
            .await
            .unwrap();
        while held("dead@lid") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(held("fresh@lid"));
        assert_eq!(
            Repository::new(store.clone()).read_index().unwrap(),
            ["fresh@lid"]
        );
        // Coming online again within a day does not prune again.
        seed("dead@lid", 0);
        socket
            .send_presence_update(Presence::Available, None)
            .await
            .unwrap();
        socket
            .query(
                BinaryNode::new("iq").attr("type", "get").with_content(
                    baileys_rs::binary::NodeContent::Children(vec![BinaryNode::new("probe")]),
                ),
                None,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(held("dead@lid"));
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
