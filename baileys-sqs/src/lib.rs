//! Rust port of baileys-sqs: WhatsApp over AWS SQS, on baileys-rs.
//!
//! Commands arrive on INPUT_QUEUE (or the optional HTTP endpoint) and are
//! sent through WhatsApp; every WhatsApp event, connection change, QR code
//! and command acknowledgement is published on OUTPUT_QUEUE.
pub mod app;
pub mod commands;
pub mod config;
pub mod ephemeral;
pub mod http;
pub mod js;
pub mod media;
pub mod output;
pub mod poll;
pub mod whatsapp;

use std::sync::Arc;

/// The connector's main: WhatsApp, the HTTP endpoint when `PORT` is set, and
/// the input poll loop, which runs until the process ends.
pub async fn run(settings: config::Settings) -> Result<(), Box<dyn std::error::Error>> {
    let (Some(input_queue), Some(output_queue)) =
        (settings.input_queue.clone(), settings.output_queue.clone())
    else {
        eprintln!("❌ ERROR: INPUT_QUEUE and OUTPUT_QUEUE environment variables are required");
        std::process::exit(1);
    };
    let sqs = aws_sdk_sqs::Client::new(&settings.sqs.sdk_config().await);
    let http = reqwest::Client::new();
    if settings.session_db == config::SessionDb::Memory {
        tracing::warn!(
            "SESSION_DIR is unset or :memory:, so the session is kept in memory only and every start links a new device"
        );
    }
    let store = app::open_session(&settings.session_db)?;
    let commands = commands::Commands {
        socket: Arc::default(),
        ephemeral: ephemeral::EphemeralCache::open(&settings.session_db)?,
        output: output::Output::new(sqs.clone(), output_queue),
        http: http.clone(),
    };
    let storage = match settings.s3.clone() {
        Some(s3) => Some(media::S3Storage::new(s3).await),
        None => None,
    };
    let version = app::whatsapp_version(&settings, &http).await;
    tokio::spawn(
        app::WhatsAppLoop {
            settings: settings.clone(),
            store,
            version,
            commands: commands.clone(),
            storage,
        }
        .run(),
    );
    match settings.http_port {
        Some(port) => {
            http::start(
                http::Endpoint {
                    commands: commands.clone(),
                    auth_token: settings.auth_token.clone(),
                },
                &settings.http_host,
                port,
            )
            .await?
        }
        None => {
            tracing::info!("PORT not set - HTTP input endpoint disabled (SQS input queue only)")
        }
    }
    poll::poll_loop(sqs, input_queue, commands).await;
    Ok(())
}
