#[path = "support/socket_peer.rs"]
#[allow(dead_code)]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    history, messages, proto,
    socket::{Config, Event, Socket, initial_sync::Phase},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
fn store(counter: u32) -> SqliteAuthStore {
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100@s.whatsapp.net".into(),
        ..Default::default()
    });
    creds.account_sync_counter = counter;
    store.save_credentials(&creds).unwrap();
    store
}
async fn setup() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        fire_init_queries: false,
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        keep_alive_interval: Duration::from_secs(3600),
        query_timeout: Duration::from_secs(2),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        ..Default::default()
    };
    (listener, config)
}
fn envelope(sync_type: Option<i32>) -> messages::Envelope {
    messages::Envelope {
        message: proto::WebMessageInfo {
            key: Some(proto::MessageKey {
                remote_jid: Some("100@s.whatsapp.net".into()),
                id: Some("HISTORY".into()),
                from_me: Some(true),
                participant: None,
            }),
            message: Some(match sync_type {
                Some(sync_type) => proto::Message {
                    protocol_message: Some(Box::new(proto::message::ProtocolMessage {
                        r#type: Some(5),
                        history_sync_notification: Some(proto::message::HistorySyncNotification {
                            sync_type: Some(sync_type),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                None => proto::Message {
                    conversation: Some("held until initial sync".into()),
                    ..Default::default()
                },
            }),
            message_timestamp: Some(10),
            ..Default::default()
        },
        key_metadata: Default::default(),
        category: None,
        retry_count: None,
        author: "100@s.whatsapp.net".into(),
        sender: "100@s.whatsapp.net".into(),
    }
}
async fn pending(events: &mut tokio::sync::broadcast::Receiver<Event>) {
    loop {
        if matches!(events.recv().await.unwrap(), Event::PendingNotifications) {
            break;
        }
    }
}
async fn counter_updated(
    events: &mut tokio::sync::broadcast::Receiver<Event>,
    store: &SqliteAuthStore,
) {
    loop {
        if matches!(events.recv().await.unwrap(), Event::CredentialsUpdated)
            && store.load_credentials().unwrap().account_sync_counter == 1
        {
            break;
        }
    }
}
#[tokio::test]
async fn pending_notifications_wait_skip_reconnect_timeout_and_persist_account_counter() {
    timeout(Duration::from_secs(60), async {
        for mode in ["disabled", "reconnect", "timeout", "skip"] {
            let auth = store(u32::from(mode == "reconnect"));
            let (listener, mut config) = setup().await;
            config.history_policy = history::Policy::new(move |n| {
                mode != "disabled" && (mode != "skip" || n.sync_type == Some(3))
            });
            let server = tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                peer.send(
                    BinaryNode::new("ib")
                        .with_content(NodeContent::Children(vec![BinaryNode::new("offline")])),
                )
                .await;
                peer.closed().await;
            });
            let (socket, mut events) = Socket::connect(config, auth.clone()).await.unwrap();
            pending(&mut events).await;
            if mode == "disabled" || mode == "reconnect" {
                assert_eq!(socket.initial_sync_phase(), Phase::Online);
                timeout(Duration::from_secs(1), async {
                    while socket.is_buffering_events() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            } else if mode == "skip" {
                assert_eq!(socket.initial_sync_phase(), Phase::AwaitingInitialSync);
                socket
                    .upsert_message(&envelope(Some(2)), messages::own::UpsertKind::Notify)
                    .await
                    .unwrap();
                assert_eq!(socket.initial_sync_phase(), Phase::Online);
                assert!(!socket.is_buffering_events());
            } else {
                assert_eq!(socket.initial_sync_phase(), Phase::AwaitingInitialSync);
                socket
                    .upsert_message(&envelope(None), messages::own::UpsertKind::Notify)
                    .await
                    .unwrap();
                assert!(socket.is_buffering_events());
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(19)).await;
                tokio::task::yield_now().await;
                assert_eq!(socket.initial_sync_phase(), Phase::AwaitingInitialSync);
                assert_eq!(auth.load_credentials().unwrap().account_sync_counter, 0);
                tokio::time::advance(Duration::from_millis(1100)).await;
                tokio::task::yield_now().await;
                tokio::time::resume();
                counter_updated(&mut events, &auth).await;
                assert_eq!(socket.initial_sync_phase(), Phase::Online);
                assert!(!socket.is_buffering_events());
            }
            assert_eq!(
                auth.load_credentials().unwrap().account_sync_counter,
                u32::from(mode == "reconnect" || mode == "timeout")
            );
            socket.close().await.unwrap();
            assert_eq!(socket.initial_sync_phase(), Phase::Connecting);
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn history_and_five_collection_startup_run_concurrently_and_other_task_survives_errors() {
    timeout(Duration::from_secs(15),async {
        for mode in ["success","history_error","sync_error"] {
            let auth=store(0);let(listener,mut config)=setup().await;
            let (download_entered,download_seen)=oneshot::channel();let entered=Arc::new(Mutex::new(Some(download_entered)));
            let (download_release,download_wait)=oneshot::channel::<bool>();let wait=Arc::new(Mutex::new(Some(download_wait)));
            config.history_downloader=Some(history::Downloader::new(move |_| {let entered=entered.clone();let wait=wait.clone();async move {
                let received=wait.lock().unwrap().take().unwrap();entered.lock().unwrap().take().unwrap().send(()).unwrap();
                if !received.await.unwrap(){return Err(history::Error::Input("synthetic history error"));}
                Ok(proto::HistorySync {sync_type:0,..Default::default()})
            }}));
            let (query_entered,query_seen)=oneshot::channel();let (query_release,query_wait)=oneshot::channel();
            let server=tokio::spawn(async move {
                let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;
                peer.send(BinaryNode::new("ib").with_content(NodeContent::Children(vec![BinaryNode::new("offline")]))).await;
                let request=peer.read().await;assert_eq!(request.attrs["xmlns"],"w:sync:app:state");
                let collections=request.child("sync").unwrap().all_children();
                assert_eq!(collections.iter().map(|c|c.attrs["name"].as_str()).collect::<Vec<_>>(),["critical_block","critical_unblock_low","regular_high","regular_low","regular"]);
                assert!(collections.iter().all(|c|c.attrs["version"]=="0"&&c.attrs["return_snapshot"]=="true"));
                query_entered.send(()).unwrap();query_wait.await.unwrap();
                let response=if mode=="sync_error" {BinaryNode::new("iq").attr("type","error").attr("id",request.attrs["id"].clone()).with_content(NodeContent::Children(vec![BinaryNode::new("error").attr("code","500")]))}
                else {BinaryNode::new("iq").attr("type","result").attr("id",request.attrs["id"].clone()).with_content(NodeContent::Children(vec![BinaryNode::new("sync").with_content(NodeContent::Children(collections.iter().map(|c|BinaryNode::new("collection").attr("name",c.attrs["name"].clone()).attr("version","0")).collect()))]))};
                peer.send(response).await;
                if mode=="sync_error" {
                    let retry=peer.read().await;let collections=retry.child("sync").unwrap().all_children();
                    assert_eq!(collections.len(),5);
                    peer.send(BinaryNode::new("iq").attr("type","result").attr("id",retry.attrs["id"].clone()).with_content(NodeContent::Children(vec![BinaryNode::new("sync").with_content(NodeContent::Children(collections.iter().map(|c|BinaryNode::new("collection").attr("name",c.attrs["name"].clone()).attr("version","0")).collect()))]))).await;
                }
                peer.closed().await;
            });
            let(socket,mut events)=Socket::connect(config,auth.clone()).await.unwrap();pending(&mut events).await;
            let history=envelope(Some(0));let mut operation=Box::pin(socket.upsert_message(&history,messages::own::UpsertKind::Notify));
            tokio::select!{result=&mut operation=>panic!("startup completed too early {result:?}"),_=async {query_seen.await.unwrap();download_seen.await.unwrap();}=>()}
            assert_eq!(socket.initial_sync_phase(),Phase::Syncing);assert!(socket.is_buffering_events());
            if mode=="history_error" {
                download_release.send(false).unwrap();assert!(operation.await.is_err());
                assert_eq!(socket.initial_sync_phase(),Phase::Syncing);query_release.send(()).unwrap();
                counter_updated(&mut events,&auth).await;assert_eq!(socket.initial_sync_phase(),Phase::Online);
            }else if mode=="sync_error" {
                query_release.send(()).unwrap();assert!(operation.await.is_err());assert_eq!(socket.initial_sync_phase(),Phase::Syncing);
                download_release.send(true).unwrap();
                timeout(Duration::from_secs(2),async{loop{socket.flush_events().unwrap();if std::iter::from_fn(||events.try_recv().ok()).any(|e|matches!(e,Event::MessagingHistorySet(_))){break;}tokio::task::yield_now().await;}}).await.unwrap();
                assert_eq!(auth.load_credentials().unwrap().account_sync_counter,0);
                let mut share=envelope(None);share.message.message=Some(proto::Message {protocol_message:Some(Box::new(proto::message::ProtocolMessage {r#type:Some(proto::message::protocol_message::Type::AppStateSyncKeyShare as i32),app_state_sync_key_share:Some(Default::default()),..Default::default()})),..Default::default()});
                socket.upsert_message(&share,messages::own::UpsertKind::Notify).await.unwrap();
                assert_eq!(socket.initial_sync_phase(),Phase::Online);assert_eq!(auth.load_credentials().unwrap().account_sync_counter,1);
            }else {
                query_release.send(()).unwrap();
                tokio::select!{result=&mut operation=>panic!("download completed too early {result:?}"),_=counter_updated(&mut events,&auth)=>()}
                assert_eq!(socket.initial_sync_phase(),Phase::Online);assert!(!socket.is_buffering_events());
                download_release.send(true).unwrap();operation.await.unwrap();
                loop{if let Event::MessagingHistorySet(set)=events.recv().await.unwrap(){assert_eq!(set.is_latest,Some(true));break;}}
            }
            socket.close().await.unwrap();server.await.unwrap();
        }
    }).await.unwrap();
}
