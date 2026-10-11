#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::SqliteAuthStore,
    binary::{self, BinaryNode, NodeContent},
    crypto::KeyPair,
    media::MediaConnection,
    socket::{Config, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};

async fn setup() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        query_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    (listener, config)
}
fn reply(request: &BinaryNode, ttl: &str, auth: &str, hosts: &[&str]) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("id", &request.attrs["id"])
        .attr("type", "result")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("media_conn")
                .attr("ttl", ttl)
                .attr("auth", auth)
                .with_content(NodeContent::Children(
                    hosts
                        .iter()
                        .map(|host| {
                            BinaryNode::new("host")
                                .attr("hostname", *host)
                                .attr("maxContentLengthBytes", "1048576")
                        })
                        .collect(),
                )),
        ]))
}
fn check_query(mut node: BinaryNode) {
    assert!(node.attrs.shift_remove("id").is_some());
    assert_eq!(
        binary::encode(&node).unwrap(),
        binary::encode(&MediaConnection::query()).unwrap()
    );
}
#[tokio::test]
async fn media_connection_queries_coalesce_refresh_expire_and_update_host() {
    timeout(Duration::from_secs(8), async {
        let (listener, config) = setup().await;
        let (done, finish) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert!(peer.payload.device_pairing_data.is_some());
            assert!(peer.routing.is_none());
            let request = peer.read().await;
            check_query(request.clone());
            tokio::time::sleep(Duration::from_millis(30)).await;
            peer.send(reply(
                &request,
                "60",
                "initial",
                &["first.example", "second.example"],
            ))
            .await;
            let request = peer.read().await;
            check_query(request.clone());
            peer.send(reply(&request, "0", "forced", &["changed.example"]))
                .await;
            let request = peer.read().await;
            check_query(request.clone());
            peer.send(reply(&request, "60", "renewed", &[])).await;
            let probe = peer.read().await;
            assert_eq!(probe.attrs["xmlns"], "probe");
            peer.result(&probe).await;
            peer.closed().await;
            finish.await.unwrap();
        });
        let (socket, _events) = Socket::connect(config, SqliteAuthStore::in_memory().unwrap())
            .await
            .unwrap();
        assert_eq!(socket.media_host().await, "mmg.whatsapp.net");
        let (a, b) = tokio::join!(
            socket.refresh_media_connection(false),
            socket.refresh_media_connection(false)
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(
            socket.refresh_media_connection(false).await.unwrap().auth,
            "initial"
        );
        assert_eq!(socket.media_host().await, "first.example");
        assert_eq!(
            socket.refresh_media_connection(true).await.unwrap().auth,
            "forced"
        );
        assert_eq!(socket.media_host().await, "changed.example");
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(
            socket.refresh_media_connection(false).await.unwrap().auth,
            "renewed"
        );
        assert_eq!(socket.media_host().await, "changed.example");
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("xmlns", "probe")
                    .attr("type", "get"),
                None,
            )
            .await
            .unwrap();
        socket.close().await.unwrap();
        assert!(socket.refresh_media_connection(false).await.is_err());
        done.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn media_connection_rejected_query_is_cached_until_connection_ends() {
    timeout(Duration::from_secs(5), async {
        let (listener, config) = setup().await;
        let (done, finish) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            let request = peer.read().await;
            check_query(request.clone());
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &request.attrs["id"])
                    .attr("type", "error")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error")
                            .attr("code", "503")
                            .attr("text", "unavailable"),
                    ])),
            )
            .await;
            finish.await.unwrap();
        });
        let (socket, _events) = Socket::connect(config, SqliteAuthStore::in_memory().unwrap())
            .await
            .unwrap();
        assert_eq!(
            socket
                .refresh_media_connection(false)
                .await
                .unwrap_err()
                .status_code(),
            Some(503)
        );
        assert_eq!(
            socket
                .refresh_media_connection(true)
                .await
                .unwrap_err()
                .status_code(),
            Some(503)
        );
        assert_eq!(socket.media_host().await, "mmg.whatsapp.net");
        assert_eq!(
            socket
                .upload_media(
                    std::path::Path::new("/synthetic/missing"),
                    baileys_rs::media::MediaType::Image,
                    "hash",
                    &Default::default()
                )
                .await
                .unwrap_err()
                .status_code(),
            Some(503)
        );
        socket.close().await.unwrap();
        done.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
