#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    history, messages, proto,
    socket::{Config, Event, Socket},
};
use prost::Message as _;
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::io::Write;
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn wire(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
fn store(old: bool) -> SqliteAuthStore {
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100@s.whatsapp.net".into(),
        ..Default::default()
    });
    if old {
        creds.processed_history_messages.push(MinimalMessage {
            key: Some(proto::MessageKey {
                id: Some("OLD".into()),
                ..Default::default()
            }),
            message_timestamp: Some(9),
        });
    }
    store.save_credentials(&creds).unwrap();
    store
}
#[tokio::test]
async fn history_protocol_policy_self_guards_records_failures_and_metadata_match_source() {
    timeout(Duration::from_secs(30),async{
        let cases:Value=serde_json::from_str(include_str!("fixtures/history.json")).unwrap();
        for case in cases["protocolCases"].as_array().unwrap(){
            let store=store(case["old"].as_bool().unwrap());let(listener,mut config)=setup().await;let enabled=case["enabled"].as_bool().unwrap();config.history_policy=history::Policy::new(move |_|enabled);
            let payload=unhex(case["payload"].as_str().unwrap());let fail=case["fail"].as_bool().unwrap();config.history_downloader=Some(history::Downloader::new(move |_|{let payload=payload.clone();async move{if fail{Err(history::Error::Input("download failed"))}else{Ok(proto::HistorySync::decode(payload.as_slice()).unwrap())}}}));
            let server=tokio::spawn(async move{let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;peer.closed().await;});
            let(socket,mut events)=Socket::connect(config,store.clone()).await.unwrap();while events.try_recv().is_ok(){}
            let message=proto::WebMessageInfo::decode(unhex(case["message"].as_str().unwrap()).as_slice()).unwrap();let envelope=messages::Envelope{message,key_metadata:Default::default(),category:None,retry_count:None,author:"100@s.whatsapp.net".into(),sender:"100@s.whatsapp.net".into()};
            socket.buffer_events();let result=socket.upsert_message(&envelope,messages::own::UpsertKind::Notify).await;assert_eq!(result.is_err(),case["expected"]["error"].is_string(),"{}",case);
            socket.flush_events().unwrap();let mut observed=vec![];while let Ok(event)=events.try_recv(){match event {
                Event::CredentialsUpdated=>{let creds=store.load_credentials().unwrap();observed.push(json!({"name":"creds.update","records":creds.processed_history_messages.iter().map(|m|json!({"key":wire(&m.key.as_ref().unwrap().encode_to_vec()),"timestamp":m.message_timestamp.unwrap().to_string()})).collect::<Vec<_>>()}));},
                Event::MessagingHistorySet(set)=>{let mut value=json!({"name":"messaging-history.set","chunkOrder":set.chunk_order,"session":set.peer_data_request_session_id,"syncType":set.data.sync_type});if let Some(latest)=set.is_latest{value["isLatest"]=json!(latest)}assert_eq!(set.data.chats.len(),1);assert_eq!(set.data.contacts.len(),1);observed.push(value);},_=>{}
            }}assert_eq!(json!(observed),case["expected"]["bufferedEvents"],"{}",case);
            let ids:Vec<_>=case["expected"]["keys"].as_object().unwrap().keys().cloned().collect();let tokens=store.get(KeyCategory::Tctoken,&ids).unwrap();let mut observed=json!({});for(id,value)in tokens{let KeyValue::Tctoken(token)=value else{panic!()};let mut value=serde_json::to_value(&token).unwrap();value["token"]=json!(wire(&token.token));observed[id]=value;}assert_eq!(observed,case["expected"]["keys"]);
            if !case["expected"]["maps"].as_array().unwrap().is_empty(){assert_eq!(baileys_rs::signal::mapping::Store::new(store.clone()).get_lid_for_pn("200@s.whatsapp.net").unwrap().as_deref(),Some("201@lid"));}
            socket.close().await.unwrap();server.await.unwrap();
        }
    }).await.unwrap();
}
#[tokio::test]
async fn actual_history_notifications_receipt_inline_inflate_and_preserve_latest_across_reconnect()
{
    timeout(Duration::from_secs(10), async {
        let dir = tempfile::tempdir().unwrap();
        let persistent = SqliteAuthStore::open(dir.path()).unwrap();
        let creds = store(false).load_credentials().unwrap();
        persistent.save_credentials(&creds).unwrap();
        let history = proto::HistorySync {
            sync_type: 3,
            conversations: vec![proto::Conversation {
                id: "200@s.whatsapp.net".into(),
                lid_jid: Some("201@lid".into()),
                messages: vec![proto::HistorySyncMsg {
                    message: Some(proto::WebMessageInfo {
                        key: Some(proto::MessageKey {
                            remote_jid: Some("200@s.whatsapp.net".into()),
                            from_me: Some(false),
                            id: Some("old-message".into()),
                            ..Default::default()
                        }),
                        message_timestamp: Some(15),
                        ..Default::default()
                    }),
                    msg_order_id: Some(99),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(&history.encode_to_vec()).unwrap();
        let bytes = zlib.finish().unwrap();
        for latest in [true, false] {
            let (listener, config) = setup().await;
            let bytes = bytes.clone();
            let server = tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                let message = proto::Message {
                    protocol_message: Some(Box::new(proto::message::ProtocolMessage {
                        r#type: Some(
                            proto::message::protocol_message::Type::HistorySyncNotification as i32,
                        ),
                        history_sync_notification: Some(proto::message::HistorySyncNotification {
                            sync_type: Some(3),
                            initial_hist_bootstrap_inline_payload: Some(bytes),
                            chunk_order: Some(0),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                };
                peer.send(
                    BinaryNode::new("message")
                        .attr("id", "HISTORY")
                        .attr("from", "100@s.whatsapp.net")
                        .attr("t", "1700000000")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("plaintext")
                                .with_content(NodeContent::Bytes(message.encode_to_vec())),
                        ])),
                )
                .await;
                let mut history_receipt = false;
                let mut delivery_receipt = false;
                loop {
                    let node = peer.read().await;
                    if node.tag == "receipt"
                        && node.attrs.get("type").is_some_and(|v| v == "hist_sync")
                    {
                        history_receipt = true;
                    }
                    if node.tag == "receipt"
                        && node.attrs.get("type").is_some_and(|t| t == "sender")
                    {
                        delivery_receipt = true;
                    }
                    if node.tag == "iq" {
                        assert_eq!(node.attrs["xmlns"], "probe");
                        assert!(history_receipt && delivery_receipt);
                        peer.result(&node).await;
                        break;
                    }
                }
                peer.closed().await;
            });
            let (socket, mut events) = Socket::connect(config, persistent.clone()).await.unwrap();
            loop {
                if let Event::MessagingHistorySet(set) = events.recv().await.unwrap() {
                    assert_eq!(set.is_latest, Some(latest));
                    assert_eq!(set.data.messages.len(), 1);
                    assert_eq!(set.data.chats[0].last_message_recv_timestamp, Some(15.0));
                    assert!(
                        set.data.chats[0].conversation.messages[0]
                            .msg_order_id
                            .is_none()
                    );
                    break;
                }
            }
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
            assert_eq!(
                SqliteAuthStore::open(dir.path())
                    .unwrap()
                    .load_credentials()
                    .unwrap()
                    .processed_history_messages
                    .len(),
                if latest { 1 } else { 2 }
            );
        }
    })
    .await
    .unwrap();
}
