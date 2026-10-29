//! A view-once message reaches a companion as `<unavailable type="view_once"/>`
//! without content. Upstream upserts it anyway, with `key.isViewOnce` and no
//! stub, and acks it without asking the phone for the content.
#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    socket::{Config, Event, Socket},
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

fn view_once(id: &str, kind: &str) -> BinaryNode {
    BinaryNode::new("message")
        .attr("id", id)
        .attr("from", "200@s.whatsapp.net")
        .attr("type", "media")
        .attr("t", "1700000000")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("unavailable").attr("type", kind),
        ]))
}

#[tokio::test]
async fn an_unavailable_view_once_message_is_upserted_without_content_or_stub() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(view_once("VIEWONCE", "view_once")).await;
            let receipt = peer.read().await;
            assert_eq!(receipt.tag, "receipt");
            assert_eq!(receipt.attrs["id"], "VIEWONCE");
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        let upsert = loop {
            if let Event::MessagesUpserted(upsert) = events.recv().await.unwrap() {
                break upsert;
            }
        };
        let message = &upsert.messages[0];
        assert_eq!(
            message.key.as_ref().unwrap().id.as_deref(),
            Some("VIEWONCE")
        );
        assert!(message.message.is_none());
        assert!(message.message_stub_type.is_none());
        assert!(upsert.metadata[0].key_metadata.is_view_once);
        assert!(upsert.metadata[0].key_metadata.unavailable_type.is_none());
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

/// The fanout variant is acked and dropped, as upstream, with no event and no
/// placeholder resend: the phone does not share view-once content.
#[tokio::test]
async fn a_view_once_fanout_is_acked_without_an_event_or_resend() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(view_once("FANOUT", "view_once_unavailable_fanout"))
                .await;
            // The probe is answered only once the message is acked.
            let (mut acked, mut probe) = (false, None);
            while !acked || probe.is_none() {
                let node = peer.read().await;
                match node.tag.as_str() {
                    "ack" => {
                        assert_eq!(node.attrs["id"], "FANOUT");
                        acked = true;
                    }
                    "iq" if node.attrs["xmlns"] == "probe" => probe = Some(node),
                    other => panic!("unexpected {other}: {node:?}"),
                }
            }
            peer.result(&probe.unwrap()).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .attr("xmlns", "probe"),
                None,
            )
            .await
            .unwrap();
        while let Ok(event) = events.try_recv() {
            assert!(!matches!(event, Event::MessagesUpserted(_)), "{event:?}");
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

/// Opt-in, not upstream: with `report_view_once_fanout` the fanout variant is
/// upserted like the `view_once` placeholder, its type in the key metadata.
/// The ack is unchanged: no receipt, no resend.
#[tokio::test]
async fn a_reported_view_once_fanout_is_upserted_after_the_same_ack() {
    timeout(Duration::from_secs(10), async {
        let (listener, mut config, store) = setup().await;
        config.report_view_once_fanout = true;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(view_once("FANOUT", "view_once_unavailable_fanout"))
                .await;
            let ack = peer.read().await;
            assert_eq!(ack.tag, "ack", "{ack:?}");
            assert_eq!(ack.attrs["id"], "FANOUT");
            assert!(!ack.attrs.contains_key("error"));
            let probe = peer.read().await;
            assert_eq!(probe.attrs["xmlns"], "probe", "{probe:?}");
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        let upsert = loop {
            if let Event::MessagesUpserted(upsert) = events.recv().await.unwrap() {
                break upsert;
            }
        };
        let message = &upsert.messages[0];
        assert_eq!(message.key.as_ref().unwrap().id.as_deref(), Some("FANOUT"));
        assert!(message.message.is_none());
        assert!(message.message_stub_type.is_none());
        assert!(message.message_stub_parameters.is_empty());
        let metadata = &upsert.metadata[0].key_metadata;
        assert!(metadata.is_view_once);
        assert_eq!(
            metadata.unavailable_type.as_deref(),
            Some("view_once_unavailable_fanout")
        );
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .attr("xmlns", "probe"),
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
