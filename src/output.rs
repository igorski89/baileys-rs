//! Publishing to OUTPUT_QUEUE, with the FIFO group and deduplication ids.
use crate::js;
use aws_sdk_sqs::Client;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// FIFO queues (name contains "fifo", e.g. ending in ".fifo") require
/// MessageGroupId + MessageDeduplicationId on every send. Standard queues
/// reject these fields, so they're only added when the queue is FIFO.
pub fn is_fifo_queue(queue_url: &str) -> bool {
    queue_url.to_lowercase().contains("fifo")
}

fn first_entry_key(body: &Value) -> Option<&Value> {
    body.get("payload")?.as_array()?.first()?.get("key")
}

/// Groups events by chat so ordering is preserved per-conversation while
/// independent chats can still be processed in parallel by the consumer.
/// Falls back to the event type for events with no chat (qr, connection, ...).
/// A messages.upsert batch spanning several chats is grouped by the first
/// message's chat.
pub fn fifo_group_id(body: &Value) -> String {
    let upsert = body
        .pointer("/payload/messages/0/key/remoteJid")
        .filter(|v| js::truthy(Some(v)));
    if let Some(jid) = upsert {
        return js::string(jid);
    }
    if let Some(jid) = first_entry_key(body)
        .and_then(|key| key.get("remoteJid"))
        .filter(|v| js::truthy(Some(v)))
    {
        return js::string(jid);
    }
    if body.get("event").and_then(Value::as_str) == Some("presence.update")
        && let Some(id) = body.pointer("/payload/id").filter(|v| js::truthy(Some(v)))
    {
        return js::string(id);
    }
    if body.get("type").and_then(Value::as_str) == Some("command_ack")
        && let Some(to) = body.pointer("/payload/to").filter(|v| js::truthy(Some(v)))
    {
        return js::string(to);
    }
    fallback(body)
}

fn fallback(body: &Value) -> String {
    if body.get("type").and_then(Value::as_str) == Some("baileys_event") {
        body.get("event")
            .map(js::string)
            .unwrap_or_else(|| "undefined".into())
    } else {
        js::get(body, "type")
            .map(js::string)
            .unwrap_or_else(|| "system".into())
    }
}

/// Deterministic `JSON.stringify` with sorted object keys.
pub fn canonical_stringify(value: &Value) -> String {
    match value {
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_stringify)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            // Array.prototype.sort compares UTF-16 code units.
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            format!(
                "{{{}}}",
                keys.iter()
                    .map(|k| format!(
                        "{}:{}",
                        Value::String((*k).clone()),
                        canonical_stringify(&fields[*k])
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        other => js::stringify(other),
    }
}

/// Only alphanumerics and "@ . : _ -" are kept in the readable dedup prefix;
/// every other UTF-16 unit becomes "_".
pub fn sanitize_for_dedup_id(value: &str) -> String {
    value
        .encode_utf16()
        .map(|unit| match char::from_u32(u32::from(unit)) {
            Some(c) if c.is_ascii_alphanumeric() || "@.:_-".contains(c) => c,
            _ => '_',
        })
        .collect()
}

fn text(value: Option<&Value>) -> String {
    value.map(js::string).unwrap_or_else(|| "undefined".into())
}

fn text_or_empty(value: Option<&Value>) -> String {
    value
        .filter(|v| js::truthy(Some(v)))
        .map(js::string)
        .unwrap_or_default()
}

/// Human-readable prefix for the dedup id, so a dropped or duplicate message
/// is identifiable at a glance in a dead-letter queue or the SQS console.
pub fn dedup_prefix(body: &Value) -> String {
    if let Some(message) = body.pointer("/payload/messages/0")
        && let Some(id) = message.pointer("/key/id").filter(|v| js::truthy(Some(v)))
    {
        return format!(
            "messages.upsert:{}:{}",
            sanitize_for_dedup_id(&text_or_empty(message.pointer("/key/remoteJid"))),
            sanitize_for_dedup_id(&js::string(id))
        );
    }
    if let Some(entry) = body
        .get("payload")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        && let Some(id) = entry.pointer("/key/id").filter(|v| js::truthy(Some(v)))
    {
        let status = entry
            .pointer("/update/status")
            .filter(|v| !v.is_null())
            .map(js::string)
            .unwrap_or_default();
        return format!(
            "messages.update:{}:{}:{}",
            sanitize_for_dedup_id(&text_or_empty(entry.pointer("/key/remoteJid"))),
            sanitize_for_dedup_id(&js::string(id)),
            sanitize_for_dedup_id(&status)
        );
    }
    if body.get("event").and_then(Value::as_str) == Some("presence.update")
        && let Some(id) = body.pointer("/payload/id").filter(|v| js::truthy(Some(v)))
    {
        return format!("presence.update:{}", sanitize_for_dedup_id(&js::string(id)));
    }
    if body.get("type").and_then(Value::as_str) == Some("command_ack")
        && let Some(to) = body.pointer("/payload/to").filter(|v| js::truthy(Some(v)))
    {
        let payload = &body["payload"];
        let reference = js::get(payload, "correlation_id")
            .or_else(|| js::get(payload, "message_id"))
            .map(js::string)
            .unwrap_or_default();
        return format!(
            "command_ack:{}:{}:{}",
            sanitize_for_dedup_id(&js::string(to)),
            sanitize_for_dedup_id(&text_or_empty(payload.get("command_type"))),
            sanitize_for_dedup_id(&reference)
        );
    }
    let name = if body.get("type").and_then(Value::as_str) == Some("baileys_event") {
        text(body.get("event"))
    } else {
        fallback(body)
    };
    sanitize_for_dedup_id(&name)
}

/// Content-based dedup id: a readable prefix (capped at 55 characters) and
/// the SHA-256 of the canonical body without its per-call `id`.
pub fn compute_dedup_id(body: &Value) -> String {
    let mut content = body.clone();
    if let Some(fields) = content.as_object_mut() {
        fields.remove("id");
    }
    let hash = Sha256::digest(canonical_stringify(&content).as_bytes());
    let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    let prefix = js::slice(&dedup_prefix(body), 0, 55);
    format!("{prefix}-{hex}")
}

/// The output queue.
#[derive(Clone)]
pub struct Output {
    client: Client,
    queue_url: String,
    fifo: bool,
}

impl Output {
    pub fn new(client: Client, queue_url: impl Into<String>) -> Self {
        let queue_url = queue_url.into();
        Self {
            fifo: is_fifo_queue(&queue_url),
            client,
            queue_url,
        }
    }

    /// Send one body; failures are logged, as the connector never retries.
    pub async fn send(&self, body: &Value) {
        let mut request = self
            .client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(js::stringify(body));
        if self.fifo {
            request = request
                .message_group_id(fifo_group_id(body))
                .message_deduplication_id(compute_dedup_id(body));
        }
        if let Err(err) = request.send().await {
            tracing::error!(err = %aws_sdk_sqs::error::DisplayErrorContext(&err), "Failed to send message to queue");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn group_and_dedup_ids_follow_the_event_shape() {
        let upsert = json!({"id":"x","type":"baileys_event","event":"messages.upsert","payload":{"messages":[{"key":{"remoteJid":"1@s.whatsapp.net","id":"ABC"}}]}});
        assert_eq!(fifo_group_id(&upsert), "1@s.whatsapp.net");
        assert_eq!(
            dedup_prefix(&upsert),
            "messages.upsert:1@s.whatsapp.net:ABC"
        );
        let update = json!({"type":"baileys_event","event":"messages.update","payload":[{"key":{"remoteJid":"1@s.whatsapp.net","id":"ABC"},"update":{"status":3}}]});
        assert_eq!(
            dedup_prefix(&update),
            "messages.update:1@s.whatsapp.net:ABC:3"
        );
        let ack = json!({"type":"command_ack","payload":{"to":"1@s.whatsapp.net","command_type":"send_text","correlation_id":"c 1"}});
        assert_eq!(fifo_group_id(&ack), "1@s.whatsapp.net");
        assert_eq!(
            dedup_prefix(&ack),
            "command_ack:1@s.whatsapp.net:send_text:c_1"
        );
        assert_eq!(fifo_group_id(&json!({"type":"qr","payload":{}})), "qr");
        assert_eq!(fifo_group_id(&json!({})), "system");
        assert_eq!(sanitize_for_dedup_id("a😀b"), "a__b");
        let id = compute_dedup_id(&upsert);
        let mut other = upsert.clone();
        other["id"] = json!("y");
        assert_eq!(id, compute_dedup_id(&other));
        assert!(id.len() <= 128);
    }

    #[test]
    fn canonical_json_sorts_keys_and_prints_javascript_numbers() {
        assert_eq!(
            canonical_stringify(&json!({"b":1.0,"a":{"d":[2,"x"],"c":null}})),
            r#"{"a":{"c":null,"d":[2,"x"]},"b":1}"#
        );
    }
}
