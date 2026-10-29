//! `logout` sends upstream's `remove-companion-device` request and, on an open
//! connection, waits for the server's answer before closing with 401.
#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::*,
    binary::BinaryNode,
    crypto::KeyPair,
    socket::{Config, ConnectionState, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};

async fn setup() -> (TcpListener, Config, SqliteAuthStore) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        fire_init_queries: false,
        announce_presence_on_connect: false,
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        query_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100:5@s.whatsapp.net".into(),
        ..Default::default()
    });
    store.save_credentials(&creds).unwrap();
    (listener, config, store)
}

async fn open(peer: &mut Peer) {
    peer.send(BinaryNode::new("success")).await;
    assert!(peer.read().await.child("unified_session").is_some());
}

fn assert_removal(node: &BinaryNode) {
    assert_eq!(node.tag, "iq", "{node:?}");
    assert_eq!(node.attrs["xmlns"], "md");
    assert_eq!(node.attrs["type"], "set");
    let remove = node.child("remove-companion-device").unwrap();
    assert_eq!(remove.attrs["jid"], "100:5@s.whatsapp.net");
    assert_eq!(remove.attrs["reason"], "user_initiated");
}

#[tokio::test]
async fn logout_waits_for_the_removal_answer_then_closes_with_401() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            open(&mut peer).await;
            let removal = peer.read().await;
            assert_removal(&removal);
            // The close must wait for this answer.
            tokio::time::sleep(Duration::from_millis(300)).await;
            peer.result(&removal).await;
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let started = std::time::Instant::now();
        socket.logout().await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(matches!(
            socket.state(),
            ConnectionState::Closed { code: 401, .. }
        ));
        server.await.unwrap();
    })
    .await
    .unwrap();
}

/// The server may end the stream with 401 instead of answering.
#[tokio::test]
async fn logout_succeeds_when_the_server_closes_with_401() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            open(&mut peer).await;
            assert_removal(&peer.read().await);
            peer.send(BinaryNode::new("stream:error").attr("code", "401"))
                .await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket.logout().await.unwrap();
        assert!(matches!(
            socket.state(),
            ConnectionState::Closed { code: 401, .. }
        ));
        server.await.unwrap();
    })
    .await
    .unwrap();
}
