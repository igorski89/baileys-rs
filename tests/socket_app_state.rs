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
#[path = "support/app_state_actions.rs"]
mod actions;
#[path = "support/json_numbers.rs"]
mod json_numbers;
use baileys_rs::{
    app_state::{ActionEvent, ChatMutation, MutationMap},
    socket::Event,
};
use prost::Message as _;
#[tokio::test]
async fn parsed_actions_publish_source_events_and_persist_credentials_and_mappings() {
    timeout(Duration::from_secs(12),async {
        let cases:serde_json::Value=serde_json::from_str(include_str!("fixtures/app_state_actions.json")).unwrap();
        let store=SqliteAuthStore::in_memory().unwrap();let mut creds=store.load_credentials().unwrap();
        let me:baileys_rs::auth::Contact=serde_json::from_value(cases[0]["me"].clone()).unwrap();creds.me=Some(me.clone());creds.account_settings.default_disappearing_mode=Some(baileys_rs::auth::DisappearingMode{ephemeral_expiration:Some(1800),..Default::default()});store.save_credentials(&creds).unwrap();
        let (listener,config)=setup().await;let server=tokio::spawn(async move{let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;peer.closed().await;});
        let (socket,mut events)=Socket::connect(config,store.clone()).await.unwrap();while events.try_recv().is_ok(){}
        let normal=cases.as_array().unwrap().iter().filter(|c|c["mode"]=="normal" && c["index"][0]=="other");
        let initial=cases.as_array().unwrap().iter().filter(|c|c["mode"]=="initialUnarchive" && c["index"][0]=="other" && (c["events"][0]["data"][0]["archived"]==true || c["events"][0]["data"][0].get("unreadCount").is_some_and(serde_json::Value::is_null)));
        for case in normal.chain(initial) {
            let value=proto::SyncActionValue::decode(actions::unhex(case["action"].as_str().unwrap()).as_slice()).unwrap();
            if value.push_name_setting.as_ref().and_then(|n|n.name.as_deref())==Some("Old"){continue;}
            let mutation=ChatMutation{index:case["index"].clone(),sync_action:proto::SyncActionData{value:Some(value.clone()),..Default::default()}};
            let mut mutations=MutationMap::new();mutations.insert("test".into(),mutation);socket.apply_app_state_mutations(&mutations,case["mode"]!="normal").await.unwrap();
            let mut observed=Vec::new();
            while let Ok(event)=events.try_recv(){
                let action=match event {
                    Event::ChatsUpdated(mut c)=>ActionEvent::Chat(c.remove(0)),Event::MessagesDeleted(k)=>ActionEvent::DeleteMessages(k),Event::ContactsUpserted(c)=>ActionEvent::Contacts(c),Event::MessagesStarred(s)=>ActionEvent::Stars(s),Event::ChatsDeleted(c)=>ActionEvent::DeleteChats(c),Event::LabelEdited(l)=>ActionEvent::LabelEdit(l),Event::LabelAssociationUpdated(l)=>ActionEvent::LabelAssociation(l),Event::SettingUpdated(s)=>ActionEvent::Setting(s),Event::LidMappingUpdated{lid,pn}=>ActionEvent::LidMapping{lid,pn},Event::ChatLocked(c)=>ActionEvent::Lock(c),
                    Event::CredentialsUpdated=>{
                        let creds=store.load_credentials().unwrap();
                        let data=if value.push_name_setting.is_some(){serde_json::json!({"me":creds.me})}else{assert!(creds.account_settings.default_disappearing_mode.is_none());serde_json::json!({"accountSettings":{"unarchiveChats":creds.account_settings.unarchive_chats}})};
                        observed.push(serde_json::json!({"event":"creds.update","data":data}));continue;
                    }, _=>continue,
                };observed.push(actions::normalized(&action,&me));
            }
            assert_eq!(json_numbers::canonical(&serde_json::json!(observed)),json_numbers::canonical(&case["events"]),"{}",case["action"]);
            if value.contact_action.as_ref().and_then(|c|c.full_name.as_deref())==Some("Full") {
                assert_eq!(baileys_rs::signal::direct::Repository::new(store.clone()).lid_mapping().get_lid_for_pn("200@s.whatsapp.net").unwrap().as_deref(),Some("201@lid"));
            }
        }
        assert_eq!(store.load_credentials().unwrap().me.as_ref().unwrap().name.as_deref(),Some("New"));
        socket.close().await.unwrap();server.await.unwrap();
    }).await.unwrap();
}
#[tokio::test]
async fn phone_key_share_automatically_resyncs_parked_collections_and_emits_actions() {
    timeout(Duration::from_secs(8), async {
        let store = SqliteAuthStore::in_memory().unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(baileys_rs::auth::Contact {
            id: "100@s.whatsapp.net".into(),
            ..Default::default()
        });
        store.save_credentials(&creds).unwrap();
        let (listener, config) = setup().await;
        let patch = patches().remove(0);
        let (send_key, key_ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            for _ in 0..2 {
                let request = peer.read().await;
                verify_request(&request, 0, true);
                peer.send(response(&request, patch.clone(), false)).await;
            }
            key_ready.await.unwrap();
            let message = proto::Message {
                protocol_message: Some(Box::new(proto::message::ProtocolMessage {
                    r#type: Some(
                        proto::message::protocol_message::Type::AppStateSyncKeyShare as i32,
                    ),
                    app_state_sync_key_share: Some(proto::message::AppStateSyncKeyShare {
                        keys: vec![proto::message::AppStateSyncKey {
                            key_id: Some(proto::message::AppStateSyncKeyId {
                                key_id: Some(vec![1, 2, 3]),
                            }),
                            key_data: Some(proto::message::AppStateSyncKeyData {
                                key_data: Some(vec![7; 32]),
                                ..Default::default()
                            }),
                        }],
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            };
            peer.send(
                BinaryNode::new("message")
                    .attr("id", "shared-key")
                    .attr("from", "100@s.whatsapp.net")
                    .attr("t", "1700000000")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("plaintext")
                            .with_content(NodeContent::Bytes(message.encode_to_vec())),
                    ])),
            )
            .await;
            loop {
                let request = peer.read().await;
                if request.tag == "iq" {
                    verify_request(&request, 0, true);
                    peer.send(response(&request, patch.clone(), false)).await;
                    break;
                }
                assert!(request.tag == "ack" || request.tag == "receipt");
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        let blocked = socket
            .resync_app_state(&["regular".into()], false, &options())
            .await
            .unwrap();
        assert_eq!(blocked.blocked, ["regular"]);
        send_key.send(()).unwrap();
        loop {
            if let Event::ChatsUpdated(chats) = events.recv().await.unwrap()
                && chats
                    .iter()
                    .any(|c| c.id == "0" && c.mute_end_time == Some(Some(0.0)))
            {
                break;
            }
        }
        assert!(socket.blocked_app_state_collections().await.is_empty());
        let values = store
            .get(KeyCategory::AppStateSyncVersion, &["regular".into()])
            .unwrap();
        assert!(
            matches!(values.get("regular"),Some(KeyValue::AppStateSyncVersion(s))if s.version==1)
        );
        assert_eq!(
            store
                .load_credentials()
                .unwrap()
                .my_app_state_key_id
                .as_deref(),
            Some("AQID")
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
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

#[tokio::test]
async fn socket_buffer_consolidates_real_publishers_and_cancelled_scopes_flush() {
    use baileys_rs::{
        messages::{self, own},
        proto_json::JsonField,
    };
    use serde_json::json;
    timeout(Duration::from_secs(10), async {
        let store = SqliteAuthStore::in_memory().unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(baileys_rs::auth::Contact {
            id: "100@s.whatsapp.net".into(),
            ..Default::default()
        });
        store.save_credentials(&creds).unwrap();
        let (listener, config) = setup().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        while events.try_recv().is_ok() {}
        socket.buffer_events();
        let envelope = messages::Envelope {
            message: proto::WebMessageInfo {
                key: Some(proto::MessageKey {
                    id: Some("M".into()),
                    remote_jid: Some("200@s.whatsapp.net".into()),
                    from_me: Some(false),
                    participant: None,
                }),
                message: Some(proto::Message {
                    conversation: Some("buffered body".into()),
                    ..Default::default()
                }),
                message_timestamp: Some(10),
                ..Default::default()
            },
            key_metadata: messages::KeyMetadata {
                remote_jid_alt: Some("201@lid".into()),
                addressing_mode: "pn".into(),
                ..Default::default()
            },
            category: None,
            retry_count: None,
            author: "200@s.whatsapp.net".into(),
            sender: "200@s.whatsapp.net".into(),
        };
        socket
            .upsert_message(&envelope, own::UpsertKind::Notify)
            .await
            .unwrap();
        let mut second = envelope.clone();
        second.message.message_timestamp = Some(20);
        socket
            .upsert_message(&second, own::UpsertKind::Notify)
            .await
            .unwrap();
        assert!(socket.is_buffering_events());
        while let Ok(event) = events.try_recv() {
            assert!(matches!(event, Event::Connection(_)));
        }
        assert!(socket.flush_events().unwrap());
        let mut message_seen = false;
        let mut chat_seen = false;
        while let Ok(event) = events.try_recv() {
            match event {
                Event::MessagesUpserted(v) => {
                    assert_eq!(v.messages.len(), 1);
                    assert_eq!(v.messages[0].message_timestamp, Some(10));
                    assert_eq!(
                        v.messages[0]
                            .message
                            .as_ref()
                            .unwrap()
                            .conversation
                            .as_deref(),
                        Some("buffered body")
                    );
                    assert_eq!(
                        v.metadata_for(0)
                            .unwrap()
                            .key_metadata
                            .remote_jid_alt
                            .as_deref(),
                        Some("201@lid")
                    );
                    message_seen = true;
                }
                Event::ChatsUpdated(v) => {
                    assert_eq!(v.len(), 1);
                    assert_eq!(v[0].to_value()["unreadCount"], json!(2));
                    chat_seen = true;
                }
                _ => (),
            }
        }
        assert!(message_seen && chat_seen);
        let mut operation =
            Box::pin(socket.with_buffered_events(std::future::pending::<
                Result<(), baileys_rs::socket::Error>,
            >()));
        assert!(
            timeout(Duration::from_millis(1), &mut operation)
                .await
                .is_err()
        );
        drop(operation);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!socket.is_buffering_events());
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn automatic_message_and_sync_scopes_flush_success_failure_and_cancellation() {
    use baileys_rs::{
        messages::{self, own},
        relay,
    };
    timeout(Duration::from_secs(10), async {
        let store=SqliteAuthStore::in_memory().unwrap();
        let mut credentials=store.load_credentials().unwrap();
        credentials.me=Some(baileys_rs::auth::Contact {id:"100@s.whatsapp.net".into(),..Default::default()});
        store.save_credentials(&credentials).unwrap();
        let (lookup_entered,lookup_seen)=tokio::sync::oneshot::channel();
        let lookup_entered=std::sync::Arc::new(std::sync::Mutex::new(Some(lookup_entered)));
        let (listener,mut config)=setup().await;
        config.get_message=relay::MessageLookup::new(move |_| {
            let entered=lookup_entered.clone();
            async move {
                if let Some(entered)=entered.lock().unwrap().take() {let _=entered.send(());}
                std::future::pending::<Result<Option<proto::Message>,relay::Error>>().await
            }
        });
        let (sync_entered,sync_seen)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;
            let request=peer.read().await;
            assert_eq!(request.attrs["xmlns"],"w:sync:app:state");
            sync_entered.send(()).unwrap();
            peer.closed().await;
        });
        let (socket,mut events)=Socket::connect(config,store).await.unwrap();
        let mut envelope=messages::Envelope {
            message:proto::WebMessageInfo {
                key:Some(proto::MessageKey {remote_jid:Some("200@s.whatsapp.net".into()),id:Some("AUTO".into()),from_me:Some(false),participant:None}),
                message:Some(proto::Message {conversation:Some("automatic scope".into()),..Default::default()}),
                message_timestamp:Some(10),..Default::default()
            },
            key_metadata:Default::default(),category:None,retry_count:None,author:"200@s.whatsapp.net".into(),sender:"200@s.whatsapp.net".into(),
        };
        socket.upsert_message(&envelope,own::UpsertKind::Notify).await.unwrap();
        let finished=tokio::time::Instant::now();
        assert!(socket.is_buffering_events());
        while let Ok(event)=events.try_recv(){assert!(matches!(event,Event::Connection(_)));}
        let mut chat=false;let mut message=false;
        while !(chat&&message){match events.recv().await.unwrap(){Event::ChatsUpdated(_)=>chat=true,Event::MessagesUpserted(_)=>message=true,_=>()}}
        assert!(finished.elapsed()>=Duration::from_millis(90));
        assert!(!socket.is_buffering_events());
        envelope.message.key.as_mut().unwrap().id=Some("CANCEL".into());
        envelope.message.message=Some(proto::Message {enc_event_response_message:Some(proto::message::EncEventResponseMessage {event_creation_message_key:Some(proto::MessageKey {id:Some("EVENT".into()),..Default::default()}),..Default::default()}),..Default::default()});
        let mut operation=Box::pin(socket.upsert_message(&envelope,own::UpsertKind::Notify));
        tokio::select!{result=&mut operation=>panic!("lookup unexpectedly completed {result:?}"), result=lookup_seen=>result.unwrap()}
        assert!(socket.is_buffering_events());drop(operation);
        let cancelled=loop{if let Event::MessagesUpserted(upsert)=events.recv().await.unwrap(){break upsert;}};
        assert_eq!(cancelled.messages[0].key.as_ref().unwrap().id.as_deref(),Some("CANCEL"));
        assert!(!socket.is_buffering_events());
        envelope.message.key=None;
        assert!(socket.upsert_message(&envelope,own::UpsertKind::Notify).await.is_err());
        assert!(socket.is_buffering_events());
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!socket.is_buffering_events());
        let collections=["regular".into()];let options=SyncOptions::default();
        let mut operation=Box::pin(socket.resync_app_state(&collections,false,&options));
        tokio::select!{result=&mut operation=>panic!("sync unexpectedly completed: success={}",result.is_ok()),result=sync_seen=>result.unwrap()}
        assert!(socket.is_buffering_events());drop(operation);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(!socket.is_buffering_events());
        socket.close().await.unwrap();server.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn connection_close_clears_parked_app_state_collections() {
    timeout(Duration::from_secs(10), async {
        let store = SqliteAuthStore::in_memory().unwrap();
        let (listener, config) = setup().await;
        let patch = patches().remove(0);
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            for _ in 0..2 {
                let request = peer.read().await;
                peer.send(response(&request, patch.clone(), false)).await;
            }
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store).await.unwrap();
        let result = socket
            .resync_app_state(&["regular".into()], false, &options())
            .await
            .unwrap();
        assert_eq!(result.blocked, ["regular"]);
        assert_eq!(socket.blocked_app_state_collections().await, ["regular"]);
        socket.close().await.unwrap();
        assert!(socket.blocked_app_state_collections().await.is_empty());
        server.await.unwrap();
    })
    .await
    .unwrap();
}
