//! The connector: `cargo run --release` with the variables in `.env.example`.
use baileys_sqs::config::{Settings, USAGE};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // baileys-rs's TLS uses ring and the AWS SDK's aws-lc-rs; with both
    // linked, rustls has no process default and the WhatsApp connection
    // would panic.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return Ok(());
    }
    let settings = match Settings::from_env_and_args(&args) {
        Ok(settings) => settings,
        Err(err) => {
            eprintln!("❌ ERROR: {err}");
            std::process::exit(1);
        }
    };
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
