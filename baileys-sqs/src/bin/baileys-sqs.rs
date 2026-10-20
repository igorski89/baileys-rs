//! The connector: `cargo run --release` with the variables in `.env.example`.
use baileys_sqs::config::Settings;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // baileys-rs's TLS uses ring and the AWS SDK's aws-lc-rs; with both
    // linked, rustls has no process default and the WhatsApp connection
    // would panic.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let settings = Settings::from_env();
    // RUST_LOG, when set, replaces the LOG_LEVEL filter (e.g. to see the AWS SDK).
    let filter = std::env::var("RUST_LOG")
        .ok()
        .and_then(|f| tracing_subscriber::EnvFilter::try_new(f).ok())
        .or_else(|| {
            tracing_subscriber::EnvFilter::try_new(baileys_sqs::config::log_filter(
                &settings.log_level,
            ))
            .ok()
        })
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new("debug"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    baileys_sqs::run(settings).await
}
