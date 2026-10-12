use baileys_rs::proto_json::JsonField;
#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
#[allow(dead_code)]
#[path = "support/receipt_helpers.rs"]
mod support;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    messages, proto, relay,
    signal::direct,
    socket::{Config, Event, Socket},
};
use prost::Message as _;
use socket_peer::{Peer, ROOT_PRIVATE};
use support::hex;
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
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
async fn listener() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        initialize_session: false,
        query_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    (listener, config)
}
fn stanza(id: &str, from: &str, content: proto::Message) -> BinaryNode {
    BinaryNode::new("message")
        .attr("id", id)
        .attr("from", from)
        .attr("t", "1700000000")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("plaintext").with_content(NodeContent::Bytes(content.encode_to_vec())),
        ]))
}
async fn upsert(events: &mut tokio::sync::broadcast::Receiver<Event>) -> messages::own::Upsert {
    timeout(Duration::from_secs(3), async {
        loop {
            if let Event::MessagesUpserted(value) = events.recv().await.unwrap() {
                return value;
            }
        }
    })
    .await
    .unwrap()
}
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/encrypted_messages.json")).unwrap()
}
fn bytes(value: &Value) -> Vec<u8> {
    hex(value.as_str().unwrap())
}
fn wire(bytes: Vec<u8>) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[derive(Default)]
struct LookupState {
    original: Option<proto::Message>,
    error: bool,
    keys: Vec<proto::MessageKey>,
    socket: Option<Socket>,
    probe: bool,
}
fn response_updates(updates: Vec<messages::own::Update>) -> Vec<messages::EventResponseUpdate> {
    updates
        .into_iter()
        .flat_map(|update| {
            let value = update.update_value();
            value["eventResponses"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|response| messages::EventResponseUpdate {
                    key: update.key.clone(),
                    event_response_message_key: proto::MessageKey::from_value(
                        &response["eventResponseMessageKey"],
                    )
                    .unwrap(),
                    sender_timestamp_ms: response
                        .get("senderTimestampMs")
                        .filter(|v| !v.is_null())
                        .map(|v| i64::from_value(v).unwrap()),
                    response: proto::message::EventResponseMessage::from_value(
                        &response["response"],
                    )
                    .unwrap(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}
#[tokio::test]
async fn event_response_lookup_mapping_failures_and_poll_policy_match_original_processor() {
    timeout(Duration::from_secs(15),async{
        let fixture=fixture();let mut cases=fixture["processing"].as_array().unwrap().clone();cases.sort_by_key(|c|c["variant"]!="unmapped");
        let temp=tempfile::tempdir().unwrap();let auth=store(&temp);let mut credentials=auth.load_credentials().unwrap();credentials.me.as_mut().unwrap().id="100:7@s.whatsapp.net".into();credentials.account_settings.unarchive_chats=true;auth.save_credentials(&credentials).unwrap();
        let state=Arc::new(Mutex::new(LookupState::default()));let callback=state.clone();let(listener,mut config)=listener().await;config.process_messages=false;
        config.get_message=relay::MessageLookup::new(move|key|{let callback=callback.clone();async move{
            let(original,error,probe,socket)={let mut state=callback.lock().unwrap();state.keys.push(key);(state.original.clone(),state.error,state.probe,state.socket.clone())};
            if error{return Err(relay::Error::Recipient);}
            if probe {let socket=socket.unwrap();socket.query(BinaryNode::new("iq").attr("type","get").with_content(NodeContent::Children(vec![BinaryNode::new("event_lookup_probe")])),None).await.map_err(|e|relay::Error::Patch(e.to_string()))?;}
            Ok(original)
        }});
        let count=cases.len()+1;let server=tokio::spawn(async move{
            let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;peer.send(BinaryNode::new("success")).await;assert_eq!(peer.read().await.tag,"ib");let mut received=0;
            loop {let node=peer.read().await;if node.child("finish").is_some(){peer.result(&node).await;assert_eq!(received,count);break;}
if node.tag=="receipt"{received+=1;}else{assert!(node.child("event_lookup_probe").is_some());peer.result(&node).await;}}peer.closed().await;
        });
        let(socket,mut events)=Socket::connect(config,auth.clone()).await.unwrap();socket.wait_for_open(Duration::from_secs(2)).await.unwrap();state.lock().unwrap().socket=Some(socket.clone());let mut mapped=false;
        for case in cases {
            if case["variant"]!="unmapped"&&!mapped {direct::Repository::new(auth.clone()).lid_mapping().store_mappings(&[baileys_rs::signal::mapping::Mapping{pn:"200@s.whatsapp.net".into(),lid:"2000@lid".into()}]).unwrap();mapped=true;}
            let input=proto::WebMessageInfo::decode(bytes(&case["input"]).as_slice()).unwrap();
            {let mut state=state.lock().unwrap();state.keys.clear();state.original=case["original"].as_str().map(|_|proto::Message::decode(bytes(&case["original"]).as_slice()).unwrap());state.error=case["variant"]=="lookupError";state.probe=case["variant"]=="good";}
            let key=input.key.as_ref().unwrap();let mut node=stanza("RESPONSE",if key.from_me==Some(true){"100:2@s.whatsapp.net"}else{key.remote_jid.as_deref().unwrap()},input.message.clone().unwrap());
            if key.from_me==Some(true){node=node.attr("recipient",key.remote_jid.as_deref().unwrap());}else if let Some(p)=key.participant.as_deref().filter(|s|!s.is_empty()){node=node.attr("participant",p);}
            if let Some(alt)=case["metadata"]["participantAlt"].as_str(){node=node.attr("participant_lid",alt);}
            socket.buffer_events();let result=socket.handle_message(&node).await;assert_eq!(result.is_err(),case.get("error").is_some(),"{case}");socket.flush_events().unwrap();
            let mut actual=vec![];while let Ok(event)=events.try_recv(){match event {
                Event::MessageUpdates(batch)=>{let updates=response_updates(batch);actual.push(json!({"name":"messages.update","data":updates.into_iter().map(|v|json!({"key":wire(v.key.encode_to_vec()),"responses":[{"key":wire(v.event_response_message_key.encode_to_vec()),"timestamp":v.sender_timestamp_ms.map(|v|v.to_string()),"response":wire(v.response.encode_to_vec())}]})).collect::<Vec<_>>()}));},
                Event::ChatsUpdated(updates)=>{let data:Vec<_>=updates.into_iter().map(|c|{let mut v=json!({"id":c.id});if let Some(m)=c.messages{v["messages"]=json!(m.into_iter().map(|m|wire(m.encode_to_vec())).collect::<Vec<_>>());}
if let Some(t)=c.conversation_timestamp{v["conversationTimestamp"]=json!(t as i64);}
if let Some(n)=c.unread_count{v["unreadCount"]=json!(n);}
if let Some(a)=c.archived{v["archived"]=json!(a);}
if let Some(r)=c.read_only{v["readOnly"]=json!(r);}
if let Some(t)=c.ephemeral_setting_timestamp{v["ephemeralSettingTimestamp"]=json!(t as i64);}
if let Some(e)=c.ephemeral_expiration{v["ephemeralExpiration"]=json!(e);}v}).collect();actual.push(json!({"name":"chats.update","data":data}));},_=>{}
            }}
            assert_eq!(json!(actual),case["bufferedEvents"],"{case}");let keys=state.lock().unwrap().keys.iter().map(|k|wire(k.encode_to_vec())).collect::<Vec<_>>();assert_eq!(json!(keys),case["lookups"],"{case}");
        }
        {
            let mut state=state.lock().unwrap();state.keys.clear();state.error=true;state.probe=false;
        }
        socket.buffer_events();socket.handle_message(&stanza("NO_KEY","300@s.whatsapp.net",proto::Message{enc_event_response_message:Some(Default::default()),..Default::default()})).await.unwrap();
        assert!(state.lock().unwrap().keys.is_empty());
        socket.flush_events().unwrap();let mut chat=false;
        while let Ok(event)=events.try_recv(){match event{Event::MessageUpdates(_)=>panic!("malformed response emitted update"),Event::ChatsUpdated(_)=>chat=true,_=>{}}}
        assert!(chat);
        socket.query(BinaryNode::new("iq").attr("type","get").with_content(NodeContent::Children(vec![BinaryNode::new("finish")])),None).await.unwrap();state.lock().unwrap().socket=None;socket.close().await.unwrap();server.await.unwrap();
    }).await.unwrap();
}
#[tokio::test]
async fn outgoing_own_event_responses_use_async_lookup_after_successful_relay() {
    timeout(Duration::from_secs(8), async {
        let fixture = fixture();
        let case = fixture["processing"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| {
                c["variant"] == "good"
                    && c["target"]["remoteJid"] == "200@s.whatsapp.net"
                    && proto::WebMessageInfo::decode(bytes(&c["input"]).as_slice())
                        .unwrap()
                        .key
                        .unwrap()
                        .from_me
                        == Some(true)
            })
            .unwrap();
        let content = proto::WebMessageInfo::decode(bytes(&case["input"]).as_slice())
            .unwrap()
            .message
            .unwrap();
        let original = proto::Message::decode(bytes(&case["original"]).as_slice()).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let mut credentials = auth.load_credentials().unwrap();
        credentials.me.as_mut().unwrap().id = "100:7@s.whatsapp.net".into();
        auth.save_credentials(&credentials).unwrap();
        let binding = Arc::new(Mutex::new(None::<Socket>));
        let callback = binding.clone();
        let (listener, mut config) = listener().await;
        config.get_message = relay::MessageLookup::new(move |key| {
            let callback = callback.clone();
            let original = original.clone();
            async move {
                assert_eq!(key.id.as_deref(), Some("EVENT"));
                let socket = callback.lock().unwrap().as_ref().unwrap().clone();
                socket
                    .query(
                        BinaryNode::new("iq").attr("type", "get").with_content(
                            NodeContent::Children(vec![BinaryNode::new("own_lookup_probe")]),
                        ),
                        None,
                    )
                    .await
                    .map_err(|e| relay::Error::Patch(e.to_string()))?;
                Ok(Some(original))
            }
        });
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            let sent = peer.read().await;
            assert_eq!(sent.tag, "message");
            assert_eq!(sent.attrs["id"], "OWN_RESPONSE");
            assert!(
                proto::Message::decode(sent.child_buffer("plaintext").unwrap())
                    .unwrap()
                    .enc_event_response_message
                    .is_some()
            );
            let probe = peer.read().await;
            assert!(probe.child("own_lookup_probe").is_some());
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        *binding.lock().unwrap() = Some(socket.clone());
        socket
            .send_message(
                "channel@newsletter",
                &messages::generation::Content::PreparedMedia(Box::new(content)),
                &messages::generation::Modifiers::default(),
                baileys_rs::socket::SendOptions {
                    message_id: Some("OWN_RESPONSE".into()),
                    timestamp_seconds: Some(1700000000),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let own = upsert(&mut events).await;
        assert_eq!(own.kind, messages::own::UpsertKind::Append);
        let response = loop {
            if let Event::MessageUpdates(updates) = events.recv().await.unwrap() {
                break response_updates(updates);
            }
        };
        assert_eq!(response[0].key.id.as_deref(), Some("EVENT"));
        assert_eq!(
            response[0].event_response_message_key.id.as_deref(),
            Some("OWN_RESPONSE")
        );
        assert_eq!(response[0].sender_timestamp_ms, Some(0));
        assert_eq!(response[0].response.response, Some(1));
        assert_eq!(response[0].response.extra_guest_count, Some(2));
        *binding.lock().unwrap() = None;
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
