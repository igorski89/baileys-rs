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
    messages, proto,
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
        fire_init_queries: false,
        announce_presence_on_connect: false,
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
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/phone_protocol.json")).unwrap()
}
fn bytes(v: &Value) -> Vec<u8> {
    hex(v.as_str().unwrap())
}
fn wire(v: Vec<u8>) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
fn cached(v: &Value) -> messages::Envelope {
    messages::Envelope {
        message: proto::WebMessageInfo {
            key: Some(serde_json::from_value(v["key"].clone()).unwrap()),
            message_timestamp: v["messageTimestamp"].as_u64(),
            push_name: v["pushName"].as_str().map(str::to_owned),
            participant: v["participant"].as_str().map(str::to_owned),
            verified_biz_name: v["verifiedBizName"].as_str().map(str::to_owned),
            message_stub_type: Some(2),
            message_stub_parameters: vec!["discard cached stub".into()],
            broadcast: Some(true),
            ..Default::default()
        },
        key_metadata: messages::KeyMetadata {
            remote_jid_alt: v["key"]["remoteJidAlt"].as_str().map(str::to_owned),
            participant_alt: v["key"]["participantAlt"].as_str().map(str::to_owned),
            ..Default::default()
        },
        category: None,
        retry_count: None,
        author: "3000@lid".into(),
        sender: "2000@lid".into(),
    }
}
#[tokio::test]
async fn phone_keys_pdo_restoration_and_lid_sync_match_original_processor() {
    timeout(Duration::from_secs(15),async{
        for case in fixture()["cases"].as_array().unwrap(){
            let temp=tempfile::tempdir().unwrap();let auth=store(&temp);let mut creds=auth.load_credentials().unwrap();creds.me.as_mut().unwrap().id="100:7@s.whatsapp.net".into();auth.save_credentials(&creds).unwrap();
            let(listener,mut config)=listener().await;config.process_messages=false;
            let server=tokio::spawn(async move{let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;peer.send(BinaryNode::new("success")).await;assert_eq!(peer.read().await.tag,"ib");let receipt=peer.read().await;assert_eq!(receipt.tag,"receipt");peer.closed().await;});
            let(socket,mut events)=Socket::connect(config,auth.clone()).await.unwrap();socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
            while events.try_recv().is_ok() {}
            let mut pending=vec![];
            for(id,value)in case["cached"].as_object().unwrap(){let data=cached(value);let key=data.message.key.clone().unwrap();let copy=socket.clone();pending.push(tokio::spawn(async move{copy.request_placeholder_resend(&key,Some(data)).await}));
                timeout(Duration::from_secs(1),async{while !socket.has_pending_placeholder_resend(id).unwrap(){tokio::task::yield_now().await;}}).await.unwrap();}
            let input=proto::WebMessageInfo::decode(bytes(&case["input"]).as_slice()).unwrap();let node=stanza("PHONE",if case["fromMe"]==true{"100:2@s.whatsapp.net"}else{"300@s.whatsapp.net"},input.message.unwrap());socket.buffer_events();let result=socket.handle_message(&node).await;assert_eq!(result.is_err(),case.get("error").is_some(),"{case}");socket.flush_events().unwrap();
            let mut actual=vec![];while let Ok(event)=events.try_recv(){match event{
                Event::CredentialsUpdated=>actual.push(json!({"name":"creds.update","data":{"myAppStateKeyId":auth.load_credentials().unwrap().my_app_state_key_id.clone()}})),
                Event::MessagesUpserted(upsert)=>{let messages:Vec<_>=upsert.messages.iter().enumerate().filter(|(_,m)|m.key.as_ref().and_then(|k|k.id.as_deref())!=Some("PHONE")).map(|(i,m)|{if let Some(metadata)=upsert.metadata_for(i){assert_eq!(metadata.key_metadata.remote_jid_alt.as_deref(),Some("200@s.whatsapp.net"));}wire(m.encode_to_vec())}).collect();if !messages.is_empty(){assert_eq!(upsert.kind,messages::own::UpsertKind::Notify);actual.push(json!({"name":"messages.upsert","messages":messages,"requestId":upsert.request_id,"type":"notify"}));}},_=>{}
            }}
            assert_eq!(json!(actual),case["bufferedEvents"],"{case}");
            for(id,expected)in case["keys"].as_object().unwrap(){let values=auth.get(KeyCategory::AppStateSyncKey,std::slice::from_ref(id)).unwrap();let actual=values.get(id).map(|v|match v{KeyValue::AppStateSyncKey(data)=>wire(data.encode_to_vec()),_=>panic!()});assert_eq!(json!(actual),*expected,"{case}");}
            let repository=direct::Repository::new(auth.clone());for pair in case["maps"].as_array().unwrap(){// Later duplicate PN entries overwrite earlier pairs.
                let pn=pair["pn"].as_str().unwrap();let expected=case["maps"].as_array().unwrap().iter().rev().find(|p|p["pn"]==pn).unwrap()["lid"].as_str().unwrap();assert_eq!(repository.lid_mapping().get_lid_for_pn(pn).unwrap().as_deref(),Some(expected),"{case}");}
            for id in case["deleted"].as_array().unwrap(){assert!(!socket.has_pending_placeholder_resend(id.as_str().unwrap()).unwrap(),"{case}");}
            for request in pending{request.abort();let _=request.await;}
            socket.close().await.unwrap();server.await.unwrap();
            if case["kind"]=="share"&&case["variant"]=="malformed"&&case["fromMe"]==true{let reopened=SqliteAuthStore::open(temp.path()).unwrap();assert!(reopened.get(KeyCategory::AppStateSyncKey,&["+/8=".into(),"AQI=".into()]).unwrap().is_empty());assert!(reopened.load_credentials().unwrap().my_app_state_key_id.is_none());}
        }
    }).await.unwrap();
}
#[tokio::test]
async fn lid_sync_moves_existing_device_sessions_and_key_shares_survive_reopen() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let auth = store(&temp);
        let mut creds = auth.load_credentials().unwrap();
        creds.me.as_mut().unwrap().id = "100:7@s.whatsapp.net".into();
        auth.save_credentials(&creds).unwrap();
        let peer_creds = Credentials::generate().unwrap();
        let repository = direct::Repository::new(auth.clone());
        repository
            .inject_session(
                "200:1@s.whatsapp.net",
                &direct::Bundle {
                    registration_id: peer_creds.registration_id.into(),
                    identity: baileys_rs::crypto::signal_public_key(
                        &peer_creds.signed_identity_key.public,
                    )
                    .unwrap()
                    .to_vec(),
                    signed: direct::PublicPreKey {
                        id: peer_creds.signed_pre_key.key_id,
                        public: baileys_rs::crypto::signal_public_key(
                            &peer_creds.signed_pre_key.key_pair.public,
                        )
                        .unwrap()
                        .to_vec(),
                    },
                    signature: peer_creds.signed_pre_key.signature.clone(),
                    pre_key: None,
                },
            )
            .unwrap();
        auth.set(&[KeyMutation::put(
            "200",
            KeyValue::DeviceList(vec!["1".into()]),
        )])
        .unwrap();
        let before = repository
            .session_info("200:1@s.whatsapp.net")
            .unwrap()
            .unwrap();
        let (listener, mut config) = listener().await;
        config.process_messages = false;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert_eq!(peer.read().await.tag, "ib");
            for _ in 0..2 {
                assert_eq!(peer.read().await.tag, "receipt");
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        for (kind, variant) in [("lid", "normal"), ("share", "normal")] {
            let fixture = fixture();
            let case = fixture["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["fromMe"] == true && c["kind"] == kind && c["variant"] == variant)
                .unwrap();
            let message = proto::WebMessageInfo::decode(bytes(&case["input"]).as_slice()).unwrap();
            socket
                .handle_message(&stanza(
                    "PHONE",
                    "100:2@s.whatsapp.net",
                    message.message.unwrap(),
                ))
                .await
                .unwrap();
            let _ = upsert(&mut events).await;
        }
        assert_eq!(
            repository.session_info("2000:1@lid").unwrap().unwrap(),
            before
        );
        assert!(
            !auth
                .get(KeyCategory::Session, &["200.1".into()])
                .unwrap()
                .contains_key("200.1")
        );
        socket.close().await.unwrap();
        server.await.unwrap();
        drop(repository);
        drop(auth);
        drop(socket);
        let reopened = SqliteAuthStore::open(temp.path()).unwrap();
        assert_eq!(
            reopened
                .load_credentials()
                .unwrap()
                .my_app_state_key_id
                .as_deref(),
            Some("AQI=")
        );
        assert!(matches!(
            reopened
                .get(KeyCategory::AppStateSyncKey, &["AQI=".into()])
                .unwrap()
                .get("AQI="),
            Some(KeyValue::AppStateSyncKey(_))
        ));
    })
    .await
    .unwrap();
}
