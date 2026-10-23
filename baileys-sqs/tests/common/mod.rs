#![allow(dead_code, unused_imports)]
use baileys_rs::{
    media::{Input, Payload},
    messages::generation::{Content, Modifiers},
    proto,
    socket::{Event, SendOptions, presence::Presence},
};
use baileys_sqs::{
    app::WhatsAppLoop,
    commands::Commands,
    config::{AwsService, SessionDb, Settings},
    ephemeral::EphemeralCache,
    http,
    output::Output,
    poll,
    polls::PollCache,
    whatsapp::{BoxFuture, Result, User, WhatsApp},
};
use rusticmq::{
    CreateQueue, QueueManager,
    sqs::{ServerConfig, SqsService},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct Queues {
    pub base: String,
    pub sqs: aws_sdk_sqs::Client,
}

impl Queues {
    pub async fn start(names: &[(&str, bool)]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut manager = QueueManager::new();
        for (name, fifo) in names {
            manager
                .create_queue(
                    CreateQueue {
                        fifo: *fifo,
                        ..CreateQueue::named(*name)
                    },
                    0,
                )
                .unwrap();
        }
        let service = SqsService::new(
            manager,
            ServerConfig {
                base_url: base.clone(),
                ..Default::default()
            },
        );
        tokio::spawn(async move { axum::serve(listener, service.router()).await.unwrap() });
        let config = AwsService {
            region: "elasticmq".into(),
            endpoint_url: Some(base.clone()),
            access_key_id: Some("local".into()),
            secret_access_key: Some("local".into()),
        }
        .sdk_config()
        .await;
        Self {
            sqs: aws_sdk_sqs::Client::new(&config),
            base,
        }
    }

    /// The ElasticMQ-style URL the TypeScript compose files use.
    pub fn url(&self, name: &str) -> String {
        format!("{}/queue/{name}", self.base)
    }

    pub async fn send(&self, name: &str, body: &str) {
        self.sqs
            .send_message()
            .queue_url(self.url(name))
            .message_body(body)
            .send()
            .await
            .unwrap();
    }

    /// Everything currently in the queue, deleted as it is read.
    pub async fn drain(&self, name: &str, at_least: usize) -> Vec<Value> {
        let mut out = vec![];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let received = self
                .sqs
                .receive_message()
                .queue_url(self.url(name))
                .max_number_of_messages(10)
                .wait_time_seconds(if out.len() >= at_least { 0 } else { 1 })
                .send()
                .await
                .unwrap();
            let messages = received.messages.unwrap_or_default();
            if messages.is_empty()
                && (out.len() >= at_least || tokio::time::Instant::now() > deadline)
            {
                return out;
            }
            for message in messages {
                out.push(serde_json::from_str(message.body().unwrap()).unwrap());
                self.sqs
                    .delete_message()
                    .queue_url(self.url(name))
                    .receipt_handle(message.receipt_handle().unwrap())
                    .send()
                    .await
                    .unwrap();
            }
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Call {
    Send {
        jid: String,
        content: Content,
        modifiers: Modifiers,
        options: SendOptions,
    },
    Subscribe(String),
    Presence(Presence, String),
    Read(Vec<proto::MessageKey>),
    Resend(proto::MessageKey),
}

#[derive(Default)]
pub struct Fake {
    pub calls: Mutex<Vec<Call>>,
}

impl WhatsApp for Fake {
    fn user(&self) -> Option<User> {
        Some(User {
            id: "100:7@s.whatsapp.net".into(),
            lid: Some("900@lid".into()),
        })
    }
    fn send_message<'a>(
        &'a self,
        jid: &'a str,
        content: Content,
        modifiers: Modifiers,
        options: SendOptions,
    ) -> BoxFuture<'a, Result<Option<proto::WebMessageInfo>>> {
        let poll = matches!(content, Content::Poll(_));
        self.calls.lock().unwrap().push(Call::Send {
            jid: jid.into(),
            content,
            modifiers,
            options,
        });
        Box::pin(async move {
            Ok(Some(proto::WebMessageInfo {
                key: Some(proto::MessageKey {
                    id: Some("SENT".into()),
                    ..Default::default()
                }),
                message: poll.then(|| proto::Message {
                    message_context_info: Some(proto::MessageContextInfo {
                        message_secret: Some(vec![1, 2, 3]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        })
    }
    fn presence_subscribe<'a>(&'a self, jid: &'a str) -> BoxFuture<'a, Result<()>> {
        self.calls.lock().unwrap().push(Call::Subscribe(jid.into()));
        Box::pin(async { Err("subscribe refused".into()) })
    }
    fn send_presence_update<'a>(
        &'a self,
        presence: Presence,
        jid: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::Presence(presence, jid.into()));
        Box::pin(async { Ok(()) })
    }
    fn read_messages<'a>(&'a self, keys: Vec<proto::MessageKey>) -> BoxFuture<'a, Result<()>> {
        self.calls.lock().unwrap().push(Call::Read(keys));
        Box::pin(async { Ok(()) })
    }
    fn request_resend<'a>(
        &'a self,
        key: proto::MessageKey,
    ) -> BoxFuture<'a, Result<Option<String>>> {
        let id = (key.id.as_deref() != Some("PENDING"))
            .then(|| format!("REQ-{}", key.id.clone().unwrap_or_default()));
        self.calls.lock().unwrap().push(Call::Resend(key));
        Box::pin(async move { Ok(id) })
    }
    fn alternate_jid<'a>(&'a self, jid: &'a str) -> BoxFuture<'a, Result<Option<String>>> {
        let alternate = match jid {
            "15551234@s.whatsapp.net" => Some("777@lid".to_owned()),
            "777@lid" => Some("15551234@s.whatsapp.net".to_owned()),
            _ => None,
        };
        Box::pin(async move { Ok(alternate) })
    }
    fn lid_addressed<'a>(&'a self, jid: &'a str) -> BoxFuture<'a, Result<Option<bool>>> {
        let lid = jid.ends_with("@g.us") || jid.ends_with("@lid");
        Box::pin(async move { Ok(Some(lid)) })
    }
    fn download_media<'a>(
        &'a self,
        _: &'a proto::WebMessageInfo,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async { Ok(vec![0xff, 0xd8, 0xff]) })
    }
}

pub fn commands(
    queues: &Queues,
    output: &str,
    fake: Option<Arc<Fake>>,
    dir: &tempfile::TempDir,
) -> Commands {
    Commands {
        socket: Arc::new(tokio::sync::RwLock::new(
            fake.map(|f| f as Arc<dyn WhatsApp>),
        )),
        ephemeral: EphemeralCache::open(&SessionDb::File(dir.path().join("auth.sqlite3"))).unwrap(),
        polls: PollCache::open(&SessionDb::File(dir.path().join("auth.sqlite3"))).unwrap(),
        output: Output::new(queues.sqs.clone(), queues.url(output)),
        http: reqwest::Client::new(),
    }
}
