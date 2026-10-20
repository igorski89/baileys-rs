//! CLI that queues `send_text` commands on INPUT_QUEUE.
//!
//! ```sh
//! sender "+1234567890" "Hello, World!"
//! sender            # interactive
//! ```
use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};

fn help() {
    println!(
        r#"
📱 WhatsApp SQS Sender

Usage:
  sender "+1234567890" "Hello, World!"
  sender 1234567890 "How are you?"

Interactive mode (no args):
  sender

Environment variables:
  INPUT_QUEUE     - SQS queue URL (required)
  AWS_REGION      - AWS region (default: us-east-1)
  AWS_ENDPOINT_URL - Optional endpoint for local ElasticMQ or RusticMQ
"#
    );
}

struct Sender {
    sqs: aws_sdk_sqs::Client,
    queue: String,
    fifo: bool,
}

impl Sender {
    async fn send_text(&self, to: &str, text: &str) -> bool {
        // Remove spaces and the + prefix.
        let clean: String = to
            .chars()
            .filter(|c| *c != '+' && !baileys_sqs::js::is_space(*c))
            .collect();
        let body = serde_json::json!({"type":"send_text","to":clean,"text":text});
        let mut request = self
            .sqs
            .send_message()
            .queue_url(&self.queue)
            .message_body(body.to_string());
        if self.fifo {
            // Grouped by recipient; a random dedup id, so an intentional
            // identical resend is not swallowed.
            request = request
                .message_group_id(&clean)
                .message_deduplication_id(uuid::Uuid::new_v4().to_string());
        }
        match request.send().await {
            Ok(_) => {
                let preview = baileys_sqs::js::slice(text, 0, 50);
                let more = if baileys_sqs::js::utf16_len(text) > 50 {
                    "..."
                } else {
                    ""
                };
                println!("✅ Message queued: {to} - \"{preview}{more}\"");
                true
            }
            Err(err) => {
                eprintln!(
                    "❌ Failed to send: {}",
                    aws_sdk_sqs::error::DisplayErrorContext(&err)
                );
                false
            }
        }
    }
}

async fn question(
    lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
    prompt: &str,
) -> Option<String> {
    print!("{prompt}");
    std::io::stdout().flush().ok();
    lines.next_line().await.ok().flatten()
}

#[tokio::main]
async fn main() {
    let queue = std::env::var("INPUT_QUEUE").unwrap_or_default();
    if queue.is_empty() {
        eprintln!("❌ ERROR: INPUT_QUEUE environment variable is required");
        help();
        std::process::exit(1);
    }
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(
        aws_config::Region::new(var("AWS_REGION").unwrap_or_else(|| "us-east-1".into())),
    );
    if let Some(endpoint) = var("AWS_ENDPOINT_URL") {
        loader = loader.endpoint_url(endpoint);
    }
    let sender = Sender {
        sqs: aws_sdk_sqs::Client::new(&loader.load().await),
        fifo: baileys_sqs::output::is_fifo_queue(&queue),
        queue,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() >= 2 {
        sender.send_text(&args[0], &args[1..].join(" ")).await;
        std::process::exit(0);
    }
    if args.iter().any(|a| a == "--help" || a == "-h") {
        help();
        std::process::exit(0);
    }
    println!("📱 WhatsApp SQS Sender");
    println!("Connected to: {}", sender.queue);
    println!("Enter messages in format: \"phone number\" \"message body\"");
    println!("Type \"quit\" or \"exit\" to stop\n");
    let pattern = regex::Regex::new(r#"^["']?([^"']+)["']?\s+["']?(.+?)["']?$"#).expect("pattern");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        let Some(input) = question(&mut lines, "> ").await else {
            break;
        };
        if input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("exit") {
            println!("👋 Goodbye!");
            std::process::exit(0);
        }
        if input.trim().is_empty() {
            continue;
        }
        if let Some(captures) = pattern.captures(&input) {
            sender.send_text(&captures[1], &captures[2]).await;
        } else {
            let Some(phone) = question(&mut lines, "Phone number: ")
                .await
                .filter(|p| !p.is_empty())
            else {
                continue;
            };
            let Some(message) = question(&mut lines, "Message: ")
                .await
                .filter(|m| !m.is_empty())
            else {
                continue;
            };
            sender.send_text(&phone, &message).await;
        }
    }
}
