#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    app_state::SyncOptions,
    auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    proto,
    socket::{Config, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
fn patches() -> Vec<Vec<u8>> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/app_state_codec.json")).unwrap();
    fixture["batches"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["variant"] == "good" && c["validate"] == true && c["minimum"].is_null())
        .unwrap()["input"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            p.as_str()
                .unwrap()
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
                .collect()
        })
        .collect()
}
async fn setup() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        query_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    (listener, config)
}
fn verify_request(request: &BinaryNode, version: u64, force: bool) {
    assert_eq!(request.attrs["xmlns"], "w:sync:app:state");
    let collections = request
        .child("sync")
        .unwrap()
        .children("collection")
        .collect::<Vec<_>>();
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0].attrs["name"], "regular");
    assert_eq!(collections[0].attrs["version"], version.to_string());
    assert_eq!(collections[0].attrs["return_snapshot"], force.to_string());
}
fn response(request: &BinaryNode, patch: Vec<u8>, more: bool) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("id", &request.attrs["id"])
        .attr("type", "result")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("sync").with_content(NodeContent::Children(vec![
                BinaryNode::new("collection")
                    .attr("name", "regular")
                    .attr("has_more_patches", more.to_string())
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("patch").with_content(NodeContent::Bytes(patch)),
                    ])),
            ])),
        ]))
}
fn put_key(store: &SqliteAuthStore) {
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
fn options() -> SyncOptions {
    SyncOptions {
        verify_snapshot: true,
        verify_patch: true,
        ..Default::default()
    }
}
#[tokio::test]
async fn pagination_commits_once_and_keeps_encrypted_queries_responsive() {
    timeout(Duration::from_secs(8),async {
        let dir=tempfile::tempdir().unwrap();let store=SqliteAuthStore::open(dir.path()).unwrap();put_key(&store);
        let (listener,config)=setup().await;let (waiting,ready)=tokio::sync::oneshot::channel();let patches=patches();
        let server=tokio::spawn(async move{
            let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;
            let first=peer.read().await;verify_request(&first,0,true);peer.send(response(&first,patches[0].clone(),true)).await;
            let second=peer.read().await;verify_request(&second,1,false);waiting.send(()).unwrap();
            let probe=peer.read().await;assert_eq!(probe.attrs["xmlns"],"probe");peer.result(&probe).await;
            peer.send(response(&second,patches[1].clone(),true)).await;
            let third=peer.read().await;verify_request(&third,2,false);peer.send(response(&third,patches[2].clone(),false)).await;
            peer.closed().await;
        });
        let (socket,_events)=Socket::connect(config,store.clone()).await.unwrap();
        let client=socket.clone();let task=tokio::spawn(async move{client.sync_app_state(&["regular".into()],&options()).await});
        ready.await.unwrap();assert!(store.get(KeyCategory::AppStateSyncVersion,&["regular".into()]).unwrap().is_empty());
        socket.query(BinaryNode::new("iq").attr("xmlns","probe").attr("type","get"),None).await.unwrap();
        let result=task.await.unwrap().unwrap();assert_eq!(result.states["regular"].version,3);assert_eq!(result.mutations.len(),3);assert!(result.blocked.is_empty());
        socket.close().await.unwrap();server.await.unwrap();
        let resumed=SqliteAuthStore::open(dir.path()).unwrap();let values=resumed.get(KeyCategory::AppStateSyncVersion,&["regular".into()]).unwrap();
        assert!(matches!(values.get("regular"),Some(KeyValue::AppStateSyncVersion(state)) if state.version==3 && state.hash==result.states["regular"].hash));
    }).await.unwrap();
}
#[tokio::test]
async fn missing_keys_park_then_explicit_key_retry_resumes_saved_state() {
    timeout(Duration::from_secs(8), async {
        let store = SqliteAuthStore::in_memory().unwrap();
        let (listener, config) = setup().await;
        let patch = patches().remove(0);
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert!(peer.payload.device_pairing_data.is_some());
            assert!(peer.routing.is_none());
            for _ in 0..3 {
                let request = peer.read().await;
                verify_request(&request, 0, true);
                peer.send(response(&request, patch.clone(), false)).await;
            }
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        let blocked = socket
            .sync_app_state(&["regular".into()], &options())
            .await
            .unwrap();
        assert_eq!(blocked.blocked, ["regular"]);
        assert_eq!(blocked.attempts["regular"], 2);
        assert_eq!(socket.blocked_app_state_collections().await, ["regular"]);
        assert!(
            store
                .get(KeyCategory::AppStateSyncVersion, &["regular".into()])
                .unwrap()
                .is_empty()
        );
        put_key(&store);
        let retry = socket.retry_blocked_app_state(&options()).await.unwrap();
        assert!(retry.blocked.is_empty());
        assert_eq!(retry.states["regular"].version, 1);
        assert_eq!(retry.mutations.len(), 1);
        assert!(
            socket
                .retry_blocked_app_state(&options())
                .await
                .unwrap()
                .mutations
                .is_empty()
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
