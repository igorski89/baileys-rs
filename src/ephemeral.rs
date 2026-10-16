//! Per-chat disappearing-messages durations, persisted next to the session.
//!
//! Outgoing sends must carry the chat's ephemeral expiration explicitly;
//! neither the protocol nor the library applies a 1:1 chat's setting by
//! itself. The cache learns the setting from `chats.update` and from every
//! message's context info, and is written through on every real change.
use baileys_rs::{messages::generation, proto, proto_json::JsonField};
use serde_json::{Map, Value};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct EphemeralCache {
    path: PathBuf,
    entries: Arc<Mutex<Map<String, Value>>>,
}

impl EphemeralCache {
    /// Load `ephemeral-cache.json`; a missing or corrupt file starts empty.
    pub fn load(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut entries = Map::new();
        match std::fs::read_to_string(&path) {
            Ok(raw) => match serde_json::from_str::<Value>(&raw) {
                Ok(Value::Object(parsed)) => {
                    for (jid, expiration) in parsed {
                        if expiration.is_number() {
                            entries.insert(jid, expiration);
                        }
                    }
                    tracing::info!(count = entries.len(), path = %path.display(), "loaded ephemeral cache from disk");
                }
                Ok(_) => {
                    tracing::info!(count = 0, path = %path.display(), "loaded ephemeral cache from disk");
                }
                Err(err) => {
                    tracing::warn!(%err, path = %path.display(), "failed to load ephemeral cache - starting empty");
                }
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(%err, path = %path.display(), "failed to load ephemeral cache - starting empty");
            }
        }
        Self {
            path,
            entries: Arc::new(Mutex::new(entries)),
        }
    }

    fn persist(&self, entries: &Map<String, Value>) {
        if let Err(err) = std::fs::write(&self.path, Value::Object(entries.clone()).to_string()) {
            tracing::warn!(%err, path = %self.path.display(), "failed to persist ephemeral cache");
        }
    }

    /// `None` means no signal (the field was absent); `Some(None)` and zero
    /// both mean disappearing messages were turned off.
    pub fn update(&self, jid: Option<&str>, expiration: Option<Option<f64>>) {
        let (Some(jid), Some(expiration)) = (jid.filter(|j| !j.is_empty()), expiration) else {
            return;
        };
        let mut entries = self.entries.lock().expect("ephemeral cache mutex");
        match expiration.filter(|e| *e != 0.0 && !e.is_nan()) {
            Some(expiration) => {
                let value = serde_json::Number::from_f64(expiration)
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
                let value = if expiration.fract() == 0.0 && expiration.abs() < 9e15 {
                    Value::from(expiration as i64)
                } else {
                    value
                };
                if entries.get(jid) != Some(&value) {
                    tracing::debug!(jid, expiration, "ephemeral cache: enabled/updated");
                    entries.insert(jid.into(), value);
                    self.persist(&entries);
                }
            }
            None => {
                if entries.remove(jid).is_some() {
                    tracing::debug!(jid, "ephemeral cache: disabled");
                    self.persist(&entries);
                }
            }
        }
    }

    /// The known expiration for a chat, if disappearing messages are on.
    pub fn get(&self, jid: &str) -> Option<Value> {
        self.entries
            .lock()
            .expect("ephemeral cache mutex")
            .get(jid)
            .filter(|v| crate::js::truthy(Some(v)))
            .cloned()
    }

    /// Learn from a chats.update entry's `ephemeralExpiration`.
    pub fn update_from_chat(&self, chat: &Value) {
        let expiration = chat
            .get("ephemeralExpiration")
            .map(|v| if v.is_null() { None } else { v.as_f64() });
        self.update(chat.get("id").and_then(Value::as_str), expiration);
    }

    /// Learn from a message's own `contextInfo.expiration`, when present.
    pub fn update_from_message(&self, message: &proto::WebMessageInfo) {
        if let Some(expiration) = message_expiration(message) {
            let jid = message.key.as_ref().and_then(|k| k.remote_jid.as_deref());
            self.update(jid, Some(Some(expiration)));
        }
    }
}

/// `extractMessageContent` then the content field's `contextInfo.expiration`.
pub fn message_expiration(message: &proto::WebMessageInfo) -> Option<f64> {
    let content = generation::extract(message.message.as_ref())?;
    let kind = generation::content_type(Some(&content))?;
    content.to_value()[kind]
        .get("contextInfo")?
        .get("expiration")?
        .as_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn changes_are_written_through_and_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ephemeral-cache.json");
        let cache = EphemeralCache::load(&path);
        cache.update(Some("a@s.whatsapp.net"), Some(Some(86400.0)));
        cache.update(Some("b@s.whatsapp.net"), Some(Some(604800.0)));
        cache.update(Some("b@s.whatsapp.net"), Some(Some(0.0)));
        cache.update(Some("a@s.whatsapp.net"), None);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            r#"{"a@s.whatsapp.net":86400}"#
        );
        let restarted = EphemeralCache::load(&path);
        assert_eq!(restarted.get("a@s.whatsapp.net"), Some(json!(86400)));
        restarted.update_from_chat(&json!({"id":"a@s.whatsapp.net","ephemeralExpiration":null}));
        assert_eq!(restarted.get("a@s.whatsapp.net"), None);
        std::fs::write(&path, "not json").unwrap();
        assert_eq!(EphemeralCache::load(&path).get("a@s.whatsapp.net"), None);
    }

    #[test]
    fn message_context_reveals_the_setting() {
        let message = proto::WebMessageInfo {
            key: Some(proto::MessageKey {
                remote_jid: Some("a@s.whatsapp.net".into()),
                ..Default::default()
            }),
            message: Some(proto::Message {
                extended_text_message: Some(Box::new(proto::message::ExtendedTextMessage {
                    text: Some("hi".into()),
                    context_info: Some(Box::new(proto::ContextInfo {
                        expiration: Some(7776000),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(message_expiration(&message), Some(7776000.0));
    }
}
