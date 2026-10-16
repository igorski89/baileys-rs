//! The connector: `cargo run --release` with the variables in `.env.example`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let settings = baileys_sqs::config::Settings::from_env();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(baileys_sqs::config::log_filter(
                &settings.log_level,
            ))
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
        )
        .init();
    baileys_sqs::run(settings).await
}
