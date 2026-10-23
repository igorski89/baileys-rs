//! Prometheus metrics, in their own test binary: the recorder is global, so
//! no other test may add to the counts checked here.
use baileys_rs::{
    messages::{
        incoming::{NO_MESSAGE, StubUpdate},
        own::{Upsert, UpsertKind},
    },
    proto,
    socket::Event,
};
use baileys_sqs::{
    app::{WhatsAppLoop, open_session},
    config::{SessionDb, Settings},
    missing::MissingContent,
    output::Output,
    telemetry,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

mod common;
use common::*;

fn upsert(message: proto::WebMessageInfo, kind: UpsertKind, request_id: Option<&str>) -> Event {
    Event::MessagesUpserted(Upsert {
        messages: vec![message],
        kind,
        request_id: request_id.map(Into::into),
        metadata: vec![],
        metadata_indices: vec![],
        runtime_fields: vec![],
    })
}

fn key(id: &str, from_me: bool) -> proto::MessageKey {
    proto::MessageKey {
        remote_jid: Some("1@s.whatsapp.net".into()),
        from_me: Some(from_me),
        id: Some(id.into()),
        participant: None,
    }
}

/// The value of the series whose line starts with `series` followed by a space.
fn value(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

#[tokio::test]
async fn the_connector_counts_what_it_does_and_serves_it() {
    let handle = telemetry::install();
    let queues = Queues::start(&[("output", false)]).await;
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(Fake::default());
    let commands = commands(&queues, "output", Some(fake.clone()), &dir);
    let mut settings = Settings::from_env();
    settings.session_db = SessionDb::File(dir.path().join("auth.sqlite3"));
    let app = WhatsAppLoop {
        settings,
        store: baileys_rs::auth::SqliteAuthStore::in_memory().unwrap(),
        version: [2, 3000, 1],
        commands: commands.clone(),
        storage: None,
        missing: MissingContent::with_wait(Duration::from_millis(20)),
    };

    // Commands: one sent, one unknown, one resend for two keys of which one
    // is already pending.
    commands
        .dispatch(&json!({"type":"send_text","to":"1555","text":"hi"}))
        .await
        .unwrap();
    commands
        .dispatch(&json!({"type":"teleport","to":"1555"}))
        .await
        .unwrap_err();
    commands
        .dispatch(&json!({"type":"request_resend","to":"1555","message_keys":[{"id":"A"},{"id":"PENDING"}]}))
        .await
        .unwrap();

    // Messages: an incoming image, our own text, and a stub whose content
    // the phone is asked for and then sends.
    let image = proto::WebMessageInfo {
        key: Some(key("IMG", false)),
        message: Some(proto::Message {
            image_message: Some(Box::new(proto::message::ImageMessage {
                mimetype: Some("image/jpeg".into()),
                ..Default::default()
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    app.forward(fake.as_ref(), &upsert(image, UpsertKind::Notify, None))
        .await;
    let text = proto::WebMessageInfo {
        key: Some(key("OWN", true)),
        message: Some(proto::Message {
            conversation: Some("hi".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    app.forward(
        fake.as_ref(),
        &upsert(text.clone(), UpsertKind::Append, None),
    )
    .await;
    for id in ["STUB", "LOST"] {
        let stub = proto::WebMessageInfo {
            key: Some(key(id, false)),
            message_stub_type: Some(proto::web_message_info::StubType::Ciphertext as i32),
            message_stub_parameters: vec![NO_MESSAGE.into()],
            ..Default::default()
        };
        app.forward(fake.as_ref(), &upsert(stub, UpsertKind::Notify, None))
            .await;
        app.forward(
            fake.as_ref(),
            &Event::MessageStubsUpdated(vec![StubUpdate {
                key: key(id, false),
                parameters: vec![NO_MESSAGE.into(), format!("REQ-{id}")],
            }]),
        )
        .await;
    }
    let mut answer = text;
    answer.key = Some(key("STUB", false));
    app.forward(
        fake.as_ref(),
        &upsert(answer, UpsertKind::Notify, Some("REQ-STUB")),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(60)).await;

    // A publish to a queue that does not exist.
    Output::new(queues.sqs.clone(), queues.url("missing"))
        .send(&json!({"type":"connection","payload":{"status":"connected"}}))
        .await;

    telemetry::connection_state("open");

    // A session import, and the key counts of the resulting database.
    let source = baileys_rs::auth::SqliteAuthStore::in_memory().unwrap();
    source.load_credentials().unwrap();
    let folder = dir.path().join("session");
    source.export_baileys_folder(&folder).unwrap();
    let db = SessionDb::File(folder.join("auth.sqlite3"));
    open_session(&db).unwrap();
    assert_eq!(
        telemetry::store_counts(&folder.join("auth.sqlite3")).unwrap(),
        (0, 0)
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    telemetry::serve(handle, "127.0.0.1", port).await.unwrap();
    let response = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await
        .unwrap();
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    let text = response.text().await.unwrap();
    let expect = |series: &str, expected: f64| {
        assert_eq!(value(&text, series), Some(expected), "{series}\n{text}");
    };
    expect(
        r#"baileys_commands_total{type="send_text",result="ok"}"#,
        1.0,
    );
    expect(
        r#"baileys_commands_total{type="unknown",result="error"}"#,
        1.0,
    );
    expect(
        r#"baileys_commands_total{type="request_resend",result="ok"}"#,
        1.0,
    );
    expect(
        r#"baileys_command_duration_seconds_count{type="send_text"}"#,
        1.0,
    );
    expect(
        r#"baileys_messages_total{direction="in",type="imageMessage"}"#,
        1.0,
    );
    expect(
        r#"baileys_messages_total{direction="out",type="conversation"}"#,
        1.0,
    );
    expect(r#"baileys_messages_total{direction="in",type="stub"}"#, 2.0);
    expect(
        r#"baileys_messages_total{direction="in",type="conversation"}"#,
        1.0,
    );
    expect(
        r#"baileys_messages_without_content_total{reason="Message absent from node"}"#,
        2.0,
    );
    expect("baileys_missing_content_unrecovered_total", 1.0);
    // One by request_resend (the other key was pending), two automatic.
    expect(
        r#"baileys_placeholder_resends_total{result="requested"}"#,
        3.0,
    );
    expect(
        r#"baileys_placeholder_resends_total{result="answered"}"#,
        1.0,
    );
    expect(r#"baileys_media_total{type="image",result="ok"}"#, 1.0);
    expect(r#"baileys_media_bytes_count{type="image"}"#, 1.0);
    expect(r#"baileys_media_bytes_sum{type="image"}"#, 3.0);
    expect(
        r#"baileys_media_duration_seconds_count{type="image",stage="download"}"#,
        1.0,
    );
    expect(r#"baileys_publish_errors_total{event="connection"}"#, 1.0);
    assert!(
        value(
            &text,
            r#"baileys_events_published_total{event="messages.upsert"}"#
        ) >= Some(4.0)
    );
    assert!(value(&text, "baileys_publish_duration_seconds_count") >= Some(5.0));
    expect(r#"baileys_connection_state{state="open"}"#, 1.0);
    expect(r#"baileys_connection_state{state="closed"}"#, 0.0);
    expect(r#"baileys_session_import_total{result="ok"}"#, 1.0);
    assert!(value(&text, "baileys_last_message_received_timestamp_seconds") > Some(1.7e9));
    assert!(text.contains("# HELP baileys_pairing_code_requests_total"));
    expect("baileys_pairing_code_requests_total", 0.0);
}
