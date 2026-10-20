//! The INPUT_QUEUE poll loop.
use crate::commands::Commands;
use aws_sdk_sqs::{Client, error::DisplayErrorContext};
use serde_json::Value;
use std::time::Duration;

/// Receive up to five commands at a time with a ten-second long poll and run
/// them in order. Every message is deleted afterwards, even when it fails, so
/// a malformed command cannot block the queue.
pub async fn poll_loop(client: Client, queue_url: String, commands: Commands) {
    tracing::info!("Starting input queue poll loop");
    loop {
        let received = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(5)
            .wait_time_seconds(10)
            .send()
            .await;
        let messages = match received {
            Ok(output) => output.messages.unwrap_or_default(),
            Err(err) => {
                tracing::error!(err = %DisplayErrorContext(&err), "SQS poll error");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        for message in messages {
            let body = message.body().unwrap_or_default();
            let result = match serde_json::from_str::<Value>(body) {
                Ok(cmd) => {
                    tracing::debug!(cmd_type = ?cmd.get("type"), "Received command");
                    commands.dispatch(&cmd).await.map_err(|e| e.0)
                }
                Err(err) => Err(err.to_string()),
            };
            if let Err(err) = result {
                tracing::error!(err, msg_body = body, "Command error");
            }
            let deleted = client
                .delete_message()
                .queue_url(&queue_url)
                .receipt_handle(message.receipt_handle().unwrap_or_default())
                .send()
                .await;
            if let Err(err) = deleted {
                // The TypeScript loop lets this escape to its outer handler.
                tracing::error!(err = %DisplayErrorContext(&err), "SQS poll error");
                tokio::time::sleep(Duration::from_secs(5)).await;
                break;
            }
        }
    }
}
