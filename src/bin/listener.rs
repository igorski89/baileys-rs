//! Output queue listener: renders QR codes and pretty-prints events.
//!
//! Reads OUTPUT_QUEUE; AWS_REGION defaults to us-east-1, and the AWS SDK's
//! own variables (AWS_ENDPOINT_URL, credentials) apply. DEBUG=true prints
//! the full JSON of events without a dedicated view.
use baileys_sqs::js::{pad_end, slice, utf16_len};
use qrcode::{QrCode, render::unicode};
use serde_json::Value;
use std::time::Duration;

const RESET: &str = "\x1b[0m";

fn color(name: &str) -> &'static str {
    match name {
        "bright" => "\x1b[1m",
        "dim" => "\x1b[2m",
        "red" => "\x1b[31m",
        "green" => "\x1b[32m",
        "yellow" => "\x1b[33m",
        "blue" => "\x1b[34m",
        "magenta" => "\x1b[35m",
        "cyan" => "\x1b[36m",
        _ => RESET,
    }
}

/// console.log(color, ...args, reset) separates the arguments with spaces.
fn log(name: &str, text: &str) {
    println!("{} {text} {RESET}", color(name));
}

fn clear() {
    print!("\x1b[2J\x1b[3J\x1b[H");
}

fn render_qr(data: &str) {
    clear();
    println!("\n");
    log(
        "cyan",
        "╔════════════════════════════════════════════════════════════╗",
    );
    log(
        "cyan",
        "║                                                            ║",
    );
    log(
        "cyan",
        "║              📱 SCAN QR CODE WITH WHATSAPP                 ║",
    );
    log(
        "cyan",
        "║                                                            ║",
    );
    log(
        "cyan",
        "╚════════════════════════════════════════════════════════════╝",
    );
    println!("\n");
    match QrCode::new(data.as_bytes()) {
        Ok(code) => println!(
            "{}",
            code.render::<unicode::Dense1x2>()
                .dark_color(unicode::Dense1x2::Light)
                .light_color(unicode::Dense1x2::Dark)
                .build()
        ),
        Err(err) => log("red", &format!("QR rendering failed: {err}")),
    }
    println!("\n");
    log(
        "yellow",
        "   Open WhatsApp → Settings → Linked Devices → Link a Device",
    );
    log("dim", "   Waiting for scan...");
    println!("\n");
}

fn text(value: &Value) -> Option<&str> {
    value.as_str().filter(|s| !s.is_empty())
}

fn message_content(message: &Value) -> String {
    let m = &message["message"];
    let present = |name: &str| m.get(name).is_some_and(|v| !v.is_null());
    if let Some(text) = text(&m["conversation"]) {
        return text.into();
    }
    if let Some(text) = text(&m["extendedTextMessage"]["text"]) {
        return text.into();
    }
    if present("imageMessage") {
        return "[📷 Image]".into();
    }
    if present("videoMessage") {
        return "[🎥 Video]".into();
    }
    if present("audioMessage") {
        return if m["audioMessage"]["ptt"] == true {
            "[🎤 Voice Message]".into()
        } else {
            "[🎵 Audio]".into()
        };
    }
    if present("documentMessage") {
        return format!(
            "[📄 Document: {}]",
            text(&m["documentMessage"]["fileName"]).unwrap_or("unnamed")
        );
    }
    if present("stickerMessage") {
        return "[🏷️ Sticker]".into();
    }
    if present("locationMessage") {
        return "[📍 Location]".into();
    }
    if present("contactMessage") || present("contactsArrayMessage") {
        return "[👤 Contact]".into();
    }
    if present("liveLocationMessage") {
        return "[📍 Live Location]".into();
    }
    if present("pollCreationMessage") {
        return format!(
            "[📊 Poll: {}]",
            m["pollCreationMessage"]["name"]
                .as_str()
                .unwrap_or("undefined")
        );
    }
    if present("reactionMessage") {
        return format!(
            "[❤️ Reaction: {}]",
            m["reactionMessage"]["text"].as_str().unwrap_or("undefined")
        );
    }
    "[Unknown message type]".into()
}

fn local_time(seconds: Option<f64>) -> String {
    use chrono::{Local, TimeZone};
    let time = match seconds {
        Some(s) => Local.timestamp_millis_opt((s * 1000.0) as i64).single(),
        None => Some(Local::now()),
    };
    time.map_or_else(
        || "Invalid Date".into(),
        |t| t.format("%-m/%-d/%Y, %-I:%M:%S %p").to_string(),
    )
}

fn pretty_print_message(message: &Value) {
    let seconds = match &message["messageTimestamp"] {
        Value::Number(n) => n.as_f64().filter(|n| *n != 0.0),
        Value::String(s) => s.parse::<f64>().ok().filter(|n| *n != 0.0),
        _ => None,
    };
    let timestamp = local_time(seconds);
    let from = text(&message["key"]["remoteJid"]).unwrap_or("unknown");
    let from_me = message["key"]["fromMe"] == true;
    let sender = if from_me {
        "ME".to_string()
    } else {
        text(&message["pushName"])
            .map(str::to_owned)
            .unwrap_or_else(|| from.split('@').next().unwrap_or_default().to_owned())
    };
    let direction = if from_me { "➤ OUT" } else { "➤ IN" };
    let c = if from_me { "green" } else { "blue" };
    let id = text(&message["key"]["id"])
        .map(|id| {
            let len = utf16_len(id);
            slice(id, len.saturating_sub(8), len)
        })
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| "????".into());
    let content = message_content(message);
    let media = message.get("_media").filter(|m| !m.is_null());
    let storage = media.and_then(|m| text(&m["storage"])).unwrap_or("base64");

    println!("\n");
    log(
        c,
        "┌─────────────────────────────────────────────────────────────┐",
    );
    log(
        c,
        &format!(
            "│ {direction}  ID: {id} {}│",
            " ".repeat(39 - utf16_len(&id))
        ),
    );
    log(
        c,
        "├─────────────────────────────────────────────────────────────┤",
    );
    log(c, &format!("│ From: {}│", pad_end(&sender, 51)));
    log(c, &format!("│ Time: {}│", pad_end(&timestamp, 51)));
    log(
        c,
        "├─────────────────────────────────────────────────────────────┤",
    );

    // Wrap content to fit in the box (59 characters per line).
    let max = 59;
    let mut lines: Vec<String> = vec![];
    if utf16_len(&content) <= max {
        lines.push(content.clone());
    } else {
        let mut current = String::new();
        for word in content.split(' ') {
            if utf16_len(&format!("{current} {word}")) > max {
                if !current.is_empty() {
                    lines.push(current.trim().to_owned());
                }
                current = word.to_owned();
            } else {
                current = if current.is_empty() {
                    word.to_owned()
                } else {
                    format!("{current} {word}")
                };
            }
        }
        if !current.is_empty() {
            lines.push(current.trim().to_owned());
        }
    }
    for line in lines.iter().take(10) {
        log(c, &format!("│ {}│", pad_end(line, 59)));
    }
    if lines.len() > 10 {
        log(
            c,
            &format!(
                "{}│",
                pad_end(&format!("│ ... and {} more lines", lines.len() - 10), 60)
            ),
        );
    }
    if let Some(media) = media {
        log(c, &format!("│ {}│", " ".repeat(58)));
        let kind = media["type"].as_str().unwrap_or("undefined");
        let mimetype = media["mimetype"].as_str().unwrap_or("undefined");
        log(
            c,
            &format!("{}│", pad_end(&format!("│  📎 [{kind} - {mimetype}]"), 60)),
        );
        log(c, &format!("│ Storage: {}│", pad_end(storage, 50)));
        if let Some(url) = text(&media["url"]) {
            let label = "URL: ";
            let available = max - label.len();
            if utf16_len(url) <= available {
                log(c, &format!("│ {label}{}│", pad_end(url, 50)));
            } else {
                log(
                    c,
                    &format!("│ {label}{}│", pad_end(&slice(url, 0, available), 50)),
                );
                log(
                    c,
                    &format!("│ {}│", pad_end(&slice(url, available, usize::MAX), 59)),
                );
            }
        }
    }
    log(
        c,
        "└─────────────────────────────────────────────────────────────┘",
    );
}

fn pretty_print_connection(status: &str) {
    if status == "connected" {
        clear();
        log(
            "green",
            "\n╔════════════════════════════════════════════════════════════╗",
        );
        log(
            "green",
            "║                                                            ║",
        );
        log(
            "green",
            "║              ✅ WHATSAPP CONNECTED                         ║",
        );
        log(
            "green",
            "║                                                            ║",
        );
        log(
            "green",
            "╚════════════════════════════════════════════════════════════╝\n",
        );
    } else if status == "disconnected" {
        log(
            "red",
            "\n╔════════════════════════════════════════════════════════════╗",
        );
        log(
            "red",
            "║                                                            ║",
        );
        log(
            "red",
            "║              ❌ WHATSAPP DISCONNECTED                      ║",
        );
        log(
            "red",
            "║                                                            ║",
        );
        log(
            "red",
            "╚════════════════════════════════════════════════════════════╝\n",
        );
    }
}

fn pretty_print_event(event: &Value) {
    let name = text(&event["event"])
        .or_else(|| text(&event["type"]))
        .unwrap_or("unknown");
    let time = chrono::Local::now().format("%-I:%M:%S %p").to_string();
    match name {
        "qr" => render_qr(event["payload"]["qr"].as_str().unwrap_or_default()),
        "connection" => {
            pretty_print_connection(event["payload"]["status"].as_str().unwrap_or_default());
            if let Some(error) = text(&event["payload"]["error"]) {
                let code = match &event["payload"]["code"] {
                    Value::Number(n) if n.as_f64() != Some(0.0) => n.to_string(),
                    _ => "unknown".into(),
                };
                log("red", &format!("   Error: {error} (Code: {code})"));
            }
        }
        "messages.upsert" => {
            for message in event["payload"]["messages"]
                .as_array()
                .into_iter()
                .flatten()
            {
                pretty_print_message(message);
            }
        }
        "messages.update" => {
            for update in event["meta"]["updates"].as_array().into_iter().flatten() {
                log(
                    "magenta",
                    &format!(
                        "[{time}] 📨 Message {} status: {}",
                        update
                            .get("message_id")
                            .map(baileys_sqs::js::string)
                            .unwrap_or_else(|| "undefined".into()),
                        update
                            .get("status")
                            .map(baileys_sqs::js::string)
                            .unwrap_or_else(|| "undefined".into())
                    ),
                );
            }
        }
        "presence.update" => {
            let presence = &event["payload"];
            let icon = if presence["presence"] == "available" {
                "🟢"
            } else {
                "⚪"
            };
            log(
                "cyan",
                &format!(
                    "[{time}] {icon} {} is {}",
                    presence["id"].as_str().unwrap_or("undefined"),
                    presence["presence"].as_str().unwrap_or("undefined")
                ),
            );
        }
        other => {
            log("dim", &format!("[{time}] 📦 Event: {other}"));
            if std::env::var("DEBUG").as_deref() == Ok("true") {
                println!(
                    "{}",
                    serde_json::to_string_pretty(event).unwrap_or_default()
                );
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let Ok(queue) = std::env::var("OUTPUT_QUEUE").map(|q| q.trim().to_owned()) else {
        eprintln!("❌ ERROR: OUTPUT_QUEUE environment variable is required");
        eprintln!("   Set it to your SQS output queue URL");
        std::process::exit(1);
    };
    if queue.is_empty() {
        eprintln!("❌ ERROR: OUTPUT_QUEUE environment variable is required");
        eprintln!("   Set it to your SQS output queue URL");
        std::process::exit(1);
    }
    let region = std::env::var("AWS_REGION")
        .ok()
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| "us-east-1".into());
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;
    let sqs = aws_sdk_sqs::Client::new(&config);
    log("cyan", "🚀 Starting Baileys Output Queue Listener...");
    log("cyan", &format!("📍 Queue: {queue}"));
    log("cyan", "👂 Listening for events...\n");
    loop {
        let received = sqs
            .receive_message()
            .queue_url(&queue)
            .max_number_of_messages(10)
            .wait_time_seconds(10)
            .message_attribute_names("All")
            .message_system_attribute_names(aws_sdk_sqs::types::MessageSystemAttributeName::All)
            .send()
            .await;
        let messages = match received {
            Ok(output) => output.messages.unwrap_or_default(),
            Err(err) => {
                log(
                    "red",
                    &format!(
                        "SQS Error: {}",
                        aws_sdk_sqs::error::DisplayErrorContext(&err)
                    ),
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        for message in messages {
            match serde_json::from_str::<Value>(message.body().unwrap_or_default()) {
                Ok(body) => pretty_print_event(&body),
                Err(err) => log("red", &format!("Error processing message: {err}")),
            }
            // Delete after processing, and also on failure to avoid a poison pill.
            if let Err(err) = sqs
                .delete_message()
                .queue_url(&queue)
                .receipt_handle(message.receipt_handle().unwrap_or_default())
                .send()
                .await
            {
                log(
                    "red",
                    &format!(
                        "SQS Error: {}",
                        aws_sdk_sqs::error::DisplayErrorContext(&err)
                    ),
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                break;
            }
        }
    }
}
