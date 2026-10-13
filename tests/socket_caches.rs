#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::BinaryNode,
    cache::{Cache, CacheStore},
    crypto::KeyPair,
    messages::Envelope,
    proto,
    socket::{Config, Socket},
};
use futures_util::future::BoxFuture;
use socket_peer::{Peer, ROOT_PRIVATE};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, time::timeout};
// A store that suspends on every call and records what it was asked.
struct Recording<T> {
    entries: Mutex<HashMap<String, T>>,
    log: Mutex<Vec<String>>,
}
impl<T> Default for Recording<T> {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            log: Default::default(),
        }
    }
}
impl<T: Clone + Send> CacheStore<T> for Recording<T> {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Option<T>> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            self.entries.lock().unwrap().get(key).cloned()
        })
    }
    fn set<'a>(&'a self, key: &'a str, value: T) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            self.log.lock().unwrap().push(format!("set {key}"));
            self.entries.lock().unwrap().insert(key.into(), value);
        })
    }
    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            self.log.lock().unwrap().push(format!("del {key}"));
            self.entries.lock().unwrap().remove(key);
        })
    }
    fn flush_all(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.log.lock().unwrap().push("flush".into());
            self.entries.lock().unwrap().clear();
        })
    }
}
#[tokio::test]
async fn caller_supplied_retry_and_placeholder_stores_are_used_and_never_flushed() {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let counts = Arc::new(Recording::<u64>::default());
        let placeholders = Arc::new(Recording::<Option<Envelope>>::default());
        counts.entries.lock().unwrap().insert("KEPT".into(), 3);
        let config = Config {
            url: format!("ws://{}", listener.local_addr().unwrap()),
            initialize_session: false,
            fire_init_queries: false,
            announce_presence_on_connect: false,
            // Without the recent-message cache the counters live in the store.
            enable_recent_message_cache: false,
            max_msg_retry_count: 1,
            msg_retry_counter_cache: Some(Cache::from_arc(counts.clone())),
            placeholder_resend_cache: Some(Cache::from_arc(placeholders.clone())),
            noise: baileys_rs::noise::Config {
                certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = SqliteAuthStore::in_memory().unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(Contact {
            id: "100:7@s.whatsapp.net".into(),
            ..Default::default()
        });
        store.save_credentials(&creds).unwrap();
        let (done, mut finish) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            // Device lookups for the phone request are answered with nothing.
            loop {
                tokio::select! {
                    request = peer.read() => peer.result(&request).await,
                    _ = &mut finish => break,
                }
            }
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let incoming = BinaryNode::new("message")
            .attr("from", "200@s.whatsapp.net")
            .attr("id", "RETRIED")
            .attr("t", "1700000000");
        // Without the recent-message cache the phone is asked for the message, which
        // fails here for want of a session; the count has been stored by then.
        assert!(socket.send_retry_request(&incoming, false).await.is_err());
        assert_eq!(counts.entries.lock().unwrap()["RETRIED:undefined"], 1);
        // Over the limit the counter is dropped and nothing is sent.
        socket.send_retry_request(&incoming, false).await.unwrap();
        assert_eq!(
            *counts.log.lock().unwrap(),
            ["set RETRIED:undefined", "del RETRIED:undefined"]
        );
        // A placeholder resolved while the request waits is not sent to the phone.
        let key = proto::MessageKey {
            remote_jid: Some("200@s.whatsapp.net".into()),
            id: Some("PLACEHOLDER".into()),
            from_me: Some(false),
            participant: None,
        };
        let worker = socket.clone();
        let worker_key = key.clone();
        let request =
            tokio::spawn(async move { worker.request_placeholder_resend(&worker_key, None).await });
        while !placeholders
            .entries
            .lock()
            .unwrap()
            .contains_key("PLACEHOLDER")
        {
            tokio::task::yield_now().await;
        }
        assert!(
            socket
                .has_pending_placeholder_resend("PLACEHOLDER")
                .await
                .unwrap()
        );
        assert!(
            socket
                .request_placeholder_resend(&key, None)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            socket
                .resolve_placeholder_resend("PLACEHOLDER")
                .await
                .unwrap(),
            None
        );
        assert_eq!(request.await.unwrap().unwrap().as_deref(), Some("RESOLVED"));
        assert_eq!(
            *placeholders.log.lock().unwrap(),
            ["set RETRIED", "set PLACEHOLDER", "del PLACEHOLDER"]
        );
        done.send(()).unwrap();
        server.await.unwrap();
        // The server has gone, so the close may already have happened.
        socket.close().await.ok();
        // The socket leaves a caller's store as it is.
        assert_eq!(counts.entries.lock().unwrap()["KEPT"], 3);
        assert!(!counts.log.lock().unwrap().contains(&"flush".to_string()));
        assert!(
            !placeholders
                .log
                .lock()
                .unwrap()
                .contains(&"flush".to_string())
        );
    })
    .await
    .unwrap();
}
