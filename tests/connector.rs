//! The connector against an in-process RusticMQ, with WhatsApp faked.
use baileys_rs::{
    media::{Input, Payload},
    messages::generation::{Content, Modifiers},
    proto,
    socket::{Event, SendOptions, presence::Presence},
};
use baileys_sqs::{
    app::WhatsAppLoop,
    commands::Commands,
    config::{AwsService, Settings},
    ephemeral::EphemeralCache,
    http,
    output::Output,
    poll,
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

struct Queues {
    base: String,
    sqs: aws_sdk_sqs::Client,
}

impl Queues {
    async fn start(names: &[(&str, bool)]) -> Self {
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
    fn url(&self, name: &str) -> String {
        format!("{}/queue/{name}", self.base)
    }

    async fn send(&self, name: &str, body: &str) {
        self.sqs
            .send_message()
            .queue_url(self.url(name))
            .message_body(body)
            .send()
            .await
            .unwrap();
    }

    /// Everything currently in the queue, deleted as it is read.
    async fn drain(&self, name: &str, at_least: usize) -> Vec<Value> {
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
enum Call {
    Send {
        jid: String,
        content: Content,
        modifiers: Modifiers,
        options: SendOptions,
    },
    Subscribe(String),
    Presence(Presence, String),
    Read(Vec<proto::MessageKey>),
}

#[derive(Default)]
struct Fake {
    calls: Mutex<Vec<Call>>,
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
    fn download_media<'a>(
        &'a self,
        _: &'a proto::WebMessageInfo,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async { Ok(vec![0xff, 0xd8, 0xff]) })
    }
}

fn commands(
    queues: &Queues,
    output: &str,
    fake: Option<Arc<Fake>>,
    dir: &tempfile::TempDir,
) -> Commands {
    Commands {
        socket: Arc::new(tokio::sync::RwLock::new(
            fake.map(|f| f as Arc<dyn WhatsApp>),
        )),
        ephemeral: EphemeralCache::open(dir.path()).unwrap(),
        output: Output::new(queues.sqs.clone(), queues.url(output)),
        http: reqwest::Client::new(),
    }
}

#[tokio::test]
async fn input_commands_run_in_order_acknowledge_and_never_block_the_queue() {
    let queues = Queues::start(&[("input", false), ("output", false)]).await;
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let commands = commands(&queues, "output", Some(fake.clone()), &dir);
    commands
        .ephemeral
        .update(Some("15551234@s.whatsapp.net"), Some(Some(86400.0)));
    for body in [
        json!({"type":"send_text","to":"+1 555 1234","text":"hi","correlation_id":"c1"}).to_string(),
        "not json".to_string(),
        json!({"type":"teleport","to":"1555","correlation_id":"c2"}).to_string(),
        json!({"type":"send_presence","to":"15551234","presence":"composing"}).to_string(),
        json!({"type":"send_poll","to":"15551234","poll":{"name":"Q","values":["a","b"]},"correlation_id":"c3"}).to_string(),
    ] {
        queues.send("input", &body).await;
    }
    let poller = tokio::spawn(poll::poll_loop(
        queues.sqs.clone(),
        queues.url("input"),
        commands,
    ));
    let acks = queues.drain("output", 3).await;
    assert_eq!(
        acks[0],
        json!({"type":"command_ack","payload":{"correlation_id":"c1","command_type":"send_text","to":"15551234@s.whatsapp.net","from":"100:7@s.whatsapp.net","me_lid":"900@lid","ok":true,"message_id":"SENT"}})
    );
    assert_eq!(
        acks[1],
        json!({"type":"command_ack","payload":{"correlation_id":"c2","command_type":"teleport","to":"1555@s.whatsapp.net","from":"100:7@s.whatsapp.net","me_lid":"900@lid","ok":false,"error":"Unknown command type: teleport"}})
    );
    assert_eq!(acks[2]["payload"]["message_secret"], "AQID");
    assert_eq!(acks.len(), 3);
    // Failed and malformed commands were deleted too.
    assert!(queues.drain("input", 0).await.is_empty());
    poller.abort();
    let calls = fake.calls.lock().unwrap();
    let Call::Send {
        jid,
        content: Content::Text(text),
        options,
        ..
    } = &calls[0]
    else {
        panic!("{calls:?}")
    };
    assert_eq!(jid, "15551234@s.whatsapp.net");
    assert_eq!(text.text, "hi");
    // The cached disappearing-messages duration is applied.
    assert_eq!(options.ephemeral_expiration, Some(86400));
    assert!(matches!(&calls[1], Call::Subscribe(j) if j == "15551234@s.whatsapp.net"));
    assert!(
        matches!(&calls[2], Call::Presence(Presence::Composing, j) if j == "15551234@s.whatsapp.net")
    );
    assert!(
        matches!(&calls[3], Call::Send { content: Content::Poll(p), .. } if p.values == ["a", "b"])
    );
}

#[tokio::test]
async fn media_reactions_receipts_and_raw_bodies_become_library_calls() {
    let queues = Queues::start(&[("output", false)]).await;
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let commands = commands(&queues, "output", Some(fake.clone()), &dir);
    for cmd in [
        json!({"type":"send_media","to":"1555","media":{"type":"image","data_base64":"/9j/","mimetype":"image/jpeg"},"message":{"text":"caption"}}),
        json!({"type":"send_reaction","to":"1555","reaction":"👍","message_key":{"id":"M1"}}),
        json!({"type":"send_read_receipt","to":"1555","message_keys":[{"id":"M1"},{"id":"M2","fromMe":true}]}),
        json!({"type":"send_edit","to":"1555","text":"fixed","message_key":{"id":"M3"}}),
        json!({"type":"raw","to":"1555","body":{"text":"once","viewOnce":true},"options":{"quoted":{"key":{"id":"Q"}}}}),
    ] {
        commands.dispatch(&cmd).await.unwrap();
    }
    let error = commands
        .dispatch(&json!({"type":"send_reaction","to":"1555","reaction":"x"}))
        .await
        .unwrap_err();
    assert_eq!(
        error.0,
        "send_reaction requires \"message_key\" with at least an \"id\" field"
    );
    let calls = fake.calls.lock().unwrap();
    let Call::Send {
        content: Content::Media(request),
        ..
    } = &calls[0]
    else {
        panic!()
    };
    assert!(matches!(&request.input, Input::Bytes(b) if b.as_ref() == [0xff, 0xd8, 0xff]));
    let Payload::Image(image) = &request.payload else {
        panic!()
    };
    assert_eq!(image.caption.as_deref(), Some("caption"));
    assert_eq!(image.mimetype.as_deref(), Some("image/jpeg"));
    let Call::Send {
        content: Content::Reaction(reaction),
        ..
    } = &calls[1]
    else {
        panic!()
    };
    let key = reaction.key.as_ref().unwrap();
    assert_eq!(key.remote_jid.as_deref(), Some("1555@s.whatsapp.net"));
    assert_eq!(key.from_me, Some(false));
    let Call::Read(keys) = &calls[2] else {
        panic!()
    };
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[1].from_me, Some(true));
    let Call::Send { modifiers, .. } = &calls[3] else {
        panic!()
    };
    // Edits default to the caller's own message.
    assert_eq!(modifiers.edit.as_ref().unwrap().from_me, Some(true));
    let Call::Send {
        modifiers, options, ..
    } = &calls[4]
    else {
        panic!()
    };
    assert!(modifiers.view_once);
    // A quoted message without content is stripped before sending.
    assert!(options.quoted.is_none());
}

#[tokio::test]
async fn fifo_output_groups_by_chat_and_drops_redelivered_events() {
    let queues = Queues::start(&[("output.fifo", true)]).await;
    let output = Output::new(queues.sqs.clone(), queues.url("output.fifo"));
    let event = |id: &str, status: u32| json!({"id":id,"type":"baileys_event","event":"messages.update","meta":{},"payload":[{"key":{"remoteJid":"1@s.whatsapp.net","id":"M"},"update":{"status":status}}]});
    output.send(&event("a", 3)).await;
    output.send(&event("b", 3)).await;
    output.send(&event("c", 4)).await;
    let received = queues.drain("output.fifo", 2).await;
    assert_eq!(received.len(), 2);
    assert_eq!(received[0]["id"], "a");
    assert_eq!(received[1]["id"], "c");
}

#[tokio::test]
async fn http_endpoint_authenticates_validates_and_dispatches() {
    let queues = Queues::start(&[("output", false)]).await;
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let serve = |commands: Commands| async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = http::router(http::Endpoint {
            commands,
            auth_token: Some("secret".into()),
        });
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        base
    };
    let base = serve(commands(&queues, "output", Some(fake.clone()), &dir)).await;
    let client = reqwest::Client::new();
    let call = |method: reqwest::Method, path: &str, token: Option<&str>, body: Option<&str>| {
        let mut request = client.request(method, format!("{base}{path}"));
        if let Some(token) = token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(body) = body {
            request = request.body(body.to_owned());
        }
        async move {
            let response = request.send().await.unwrap();
            let status = response.status().as_u16();
            (status, response.json::<Value>().await.unwrap())
        }
    };
    use reqwest::Method;
    assert_eq!(
        call(Method::GET, "/health", None, None).await,
        (200, json!({"ok":true,"connected":true}))
    );
    assert_eq!(call(Method::GET, "/commands", None, None).await.0, 404);
    assert_eq!(
        call(Method::POST, "/commands", None, Some("{}")).await,
        (401, json!({"ok":false,"error":"Unauthorized"}))
    );
    assert_eq!(
        call(Method::POST, "/commands", Some("wrong"), Some("{}"))
            .await
            .0,
        401
    );
    assert_eq!(
        call(Method::POST, "/commands", Some("secret"), Some("{nope")).await,
        (400, json!({"ok":false,"error":"Invalid JSON body"}))
    );
    assert_eq!(
        call(
            Method::POST,
            "/commands",
            Some("secret"),
            Some(r#"{"type":"send_text"}"#)
        )
        .await,
        (
            400,
            json!({"ok":false,"error":"Command must include \"type\" and \"to\""})
        )
    );
    assert_eq!(
        call(
            Method::POST,
            "/commands",
            Some("secret"),
            Some(r#"{"type":"send_presence","to":"1","presence":"away"}"#)
        )
        .await,
        (
            400,
            json!({"ok":false,"error":"Invalid presence: away. Must be one of composing, recording, paused"})
        )
    );
    assert_eq!(
        call(
            Method::POST,
            "/commands",
            Some("secret"),
            Some(r#"{"type":"send_text","to":"1","text":"hello"}"#)
        )
        .await,
        (200, json!({"ok":true}))
    );
    assert_eq!(fake.calls.lock().unwrap().len(), 1);
    let base = serve(commands(&queues, "output", None, &dir)).await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/commands"))
        .header("Authorization", "Bearer secret")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 503);
    let health: Value = client
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health, json!({"ok":true,"connected":false}));
}

#[tokio::test]
async fn events_are_forwarded_with_metadata_media_and_ephemeral_learning() {
    let queues = Queues::start(&[("output", false)]).await;
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let commands = commands(&queues, "output", Some(fake.clone()), &dir);
    let mut settings = Settings::from_env();
    settings.session_dir = dir.path().into();
    let app = WhatsAppLoop {
        settings,
        store: baileys_rs::auth::SqliteAuthStore::in_memory().unwrap(),
        version: [2, 3000, 1],
        commands: commands.clone(),
        storage: None,
    };
    let key = proto::MessageKey {
        remote_jid: Some("1@s.whatsapp.net".into()),
        from_me: Some(false),
        id: Some("IMG".into()),
        participant: None,
    };
    let image = proto::WebMessageInfo {
        key: Some(key.clone()),
        message_timestamp: Some(1_700_000_000),
        message: Some(proto::Message {
            image_message: Some(Box::new(proto::message::ImageMessage {
                mimetype: Some("image/jpeg".into()),
                media_key: Some(vec![1, 2, 3]),
                context_info: Some(Box::new(proto::ContextInfo {
                    expiration: Some(604800),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    app.forward(
        fake.as_ref(),
        &Event::MessagesUpserted(baileys_rs::messages::own::Upsert {
            messages: vec![image],
            kind: baileys_rs::messages::own::UpsertKind::Notify,
            request_id: None,
            metadata: vec![],
            metadata_indices: vec![],
            runtime_fields: vec![],
        }),
    )
    .await;
    app.forward(fake.as_ref(), &Event::Online(true)).await;
    app.forward(
        fake.as_ref(),
        &Event::MessagesUpdated(vec![baileys_rs::receipts::MessageUpdate {
            key,
            status: proto::web_message_info::Status::Read,
            message_timestamp: 1_700_000_001.0,
        }]),
    )
    .await;
    let events = queues.drain("output", 2).await;
    assert_eq!(events.len(), 2, "connection.update is not forwarded");
    let upsert = &events[0];
    assert_eq!(upsert["type"], "baileys_event");
    assert_eq!(upsert["event"], "messages.upsert");
    assert!(
        upsert["id"]
            .as_str()
            .unwrap()
            .starts_with("messages.upsert-")
    );
    assert_eq!(
        upsert["meta"],
        json!({"messages":[{"message_id":"IMG","from":"1@s.whatsapp.net","direction":"incoming","has_media":true,"timestamp":"1700000000"}],"type":"notify"})
    );
    assert_eq!(
        upsert["payload"]["messages"][0]["_media"],
        json!({"type":"image","mimetype":"image/jpeg","data_base64":"/9j/","storage":"base64"})
    );
    // Bytes in the message are base64, as the TypeScript connector sends them.
    assert_eq!(
        upsert["payload"]["messages"][0]["message"]["imageMessage"]["mediaKey"],
        "AQID"
    );
    assert_eq!(
        events[1]["meta"],
        json!({"updates":[{"message_id":"IMG","status":4}]})
    );
    // The message's context info revealed the chat's setting.
    assert_eq!(
        commands.ephemeral.get("1@s.whatsapp.net"),
        Some(json!(604800))
    );
}

#[test]
fn rusticmq_reads_the_elasticmq_configuration_used_by_compose() {
    let config = rusticmq::config::AppConfig::load(
        Some(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/elasticmq.conf"
        ))),
        0,
    )
    .unwrap();
    let names: Vec<_> = config.queues.iter().map(|q| q.name.as_str()).collect();
    assert!(
        names.contains(&"input-queue") && names.contains(&"output-queue"),
        "{names:?}"
    );
}

#[tokio::test]
async fn slow_event_handling_does_not_drop_events() {
    let (sender, receiver) = tokio::sync::broadcast::channel(4);
    let mut queue = baileys_sqs::app::queue(receiver);
    tokio::task::yield_now().await;
    let mut received = 0;
    for i in 0..64 {
        sender.send(Event::Online(i % 2 == 0)).unwrap();
        sender
            .send(Event::Node(baileys_rs::binary::BinaryNode::new("iq")))
            .unwrap();
        // Let the pump drain the broadcast slots, as the socket's emitter does
        // between stanzas; the consumer below has not read anything yet.
        tokio::task::yield_now().await;
    }
    drop(sender);
    while let Some(event) = queue.recv().await {
        assert!(matches!(event, Event::Online(_)));
        received += 1;
    }
    assert_eq!(received, 64);
}
