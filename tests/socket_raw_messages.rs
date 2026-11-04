//! `Config::emit_raw_messages` (not upstream) emits a received message's protobuf
//! as it was encoded, with the fields this schema drops.
#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    proto,
    socket::{Config, Event, Socket},
};
use prost::Message;
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

/// A text message followed by field 2000, which the schema does not know.
fn bytes_with_unknown_field() -> Vec<u8> {
    let mut bytes = proto::Message {
        conversation: Some("hello".into()),
        ..Default::default()
    }
    .encode_to_vec();
    bytes.extend([0x82, 0x7d, 0x01, 0x07]);
    bytes
}

async fn run(emit: bool) -> Vec<Event> {
    let (listener, mut config, store) = setup().await;
    config.emit_raw_messages = emit;
    let server = tokio::spawn(async move {
        let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
        peer.send(
            BinaryNode::new("message")
                .attr("id", "RAW")
                .attr("from", "200@s.whatsapp.net")
                .attr("t", "1700000000")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("plaintext")
                        .with_content(NodeContent::Bytes(bytes_with_unknown_field())),
                ])),
        )
        .await;
        peer.closed().await;
    });
    let (socket, mut events) = Socket::connect(config, store).await.unwrap();
    let mut seen = Vec::new();
    loop {
        let event = events.recv().await.unwrap();
        let done = matches!(event, Event::MessagesUpserted(_));
        seen.push(event);
        if done {
            break;
        }
    }
    socket.close().await.unwrap();
    server.await.unwrap();
    seen
}

#[tokio::test]
async fn the_raw_message_keeps_fields_the_schema_drops() {
    timeout(Duration::from_secs(10), async {
        let seen = run(true).await;
        let raw = seen
            .iter()
            .find_map(|event| match event {
                Event::RawMessage(raw) => Some(raw),
                _ => None,
            })
            .expect("raw message event");
        assert_eq!(raw.key.id.as_deref(), Some("RAW"));
        assert_eq!(raw.bytes.as_slice(), bytes_with_unknown_field().as_slice());
        // The decoded message loses the field; the raw bytes are how to see it.
        assert_ne!(
            proto::Message::decode(raw.bytes.as_slice())
                .unwrap()
                .encode_to_vec(),
            *raw.bytes
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn raw_messages_are_off_by_default() {
    timeout(Duration::from_secs(10), async {
        let seen = run(false).await;
        assert!(
            !seen
                .iter()
                .any(|event| matches!(event, Event::RawMessage(_)))
        );
    })
    .await
    .unwrap();
}
