//! Per-chat disappearing-messages durations, persisted in the session's
//! SQLite file.
//!
//! Outgoing sends must carry the chat's ephemeral expiration explicitly;
//! neither the protocol nor the library applies a 1:1 chat's setting by
//! itself. The cache learns the setting from `chats.update` and from every
//! message's context info, and is written through on every real change.
use crate::config::SessionDb;
use baileys_rs::{messages::generation, proto, proto_json::JsonField};
use rusqlite::{Connection, OptionalExtension, types::Value as Sql};
use serde_json::{Map, Value};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

/// The connector's table in `auth.sqlite3`, beside baileys-rs's own tables.
const TABLE: &str = "baileys_sqs_ephemeral";

#[derive(Clone)]
pub struct EphemeralCache {
    connection: Arc<Mutex<Connection>>,
    entries: Arc<Mutex<Map<String, Value>>>,
}

fn to_sql(value: &Value) -> Sql {
    match value.as_i64() {
        Some(n) => Sql::Integer(n),
        None => Sql::Real(value.as_f64().unwrap_or(f64::NAN)),
    }
}

fn from_sql(value: Sql) -> Option<Value> {
    match value {
        Sql::Integer(n) => Some(Value::from(n)),
        Sql::Real(n) => serde_json::Number::from_f64(n).map(Value::Number),
        _ => None,
    }
}

/// The numeric entries of the TypeScript connector's `ephemeral-cache.json`;
/// a missing or corrupt file has none.
fn legacy_entries(path: &Path) -> Map<String, Value> {
    let mut entries = Map::new();
    match std::fs::read_to_string(path) {
        Ok(raw) => match serde_json::from_str::<Value>(&raw) {
            Ok(Value::Object(parsed)) => {
                for (jid, expiration) in parsed {
                    if expiration.is_number() {
                        entries.insert(jid, expiration);
                    }
                }
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(%err, path = %path.display(), "failed to import ephemeral cache - starting empty");
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            tracing::warn!(%err, path = %path.display(), "failed to import ephemeral cache - starting empty");
        }
    }
    entries
}

impl EphemeralCache {
    /// Open the cache in the session database. The first time, the
    /// TypeScript connector's `ephemeral-cache.json` in the database's folder
    /// is imported; the file itself is left in place. An in-memory session
    /// keeps the cache in a memory database of its own.
    pub fn open(db: &SessionDb) -> Result<Self, rusqlite::Error> {
        let mut connection = match db {
            SessionDb::Memory => Connection::open_in_memory()?,
            SessionDb::File(path) => Connection::open(path)?,
        };
        // The same file is written by baileys-rs's own connection.
        connection.busy_timeout(Duration::from_secs(30))?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let created = tx
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                [TABLE],
                |_| Ok(()),
            )
            .optional()?
            .is_none();
        if created {
            tx.execute_batch(&format!(
                "CREATE TABLE {TABLE} (jid TEXT PRIMARY KEY, expiration NUMERIC NOT NULL)"
            ))?;
            let legacy = db.folder().map(|f| f.join("ephemeral-cache.json"));
            let imported = legacy.as_deref().map(legacy_entries).unwrap_or_default();
            for (jid, expiration) in &imported {
                tx.execute(
                    &format!("INSERT INTO {TABLE} (jid, expiration) VALUES (?1, ?2)"),
                    rusqlite::params![jid, to_sql(expiration)],
                )?;
            }
            if !imported.is_empty() {
                tracing::info!(count = imported.len(), path = ?legacy, "imported ephemeral cache");
            }
        }
        let mut entries = Map::new();
        {
            let mut rows = tx.prepare(&format!(
                "SELECT jid, expiration FROM {TABLE} ORDER BY rowid"
            ))?;
            for row in rows.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Sql>(1)?)))? {
                let (jid, expiration) = row?;
                if let Some(expiration) = from_sql(expiration) {
                    entries.insert(jid, expiration);
                }
            }
        }
        tx.commit()?;
        tracing::info!(count = entries.len(), "loaded ephemeral cache");
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            entries: Arc::new(Mutex::new(entries)),
        })
    }

    fn persist(&self, jid: &str, expiration: Option<&Value>) {
        let connection = self.connection.lock().expect("ephemeral cache connection");
        let result = match expiration {
            Some(expiration) => connection.execute(
                &format!("INSERT OR REPLACE INTO {TABLE} (jid, expiration) VALUES (?1, ?2)"),
                rusqlite::params![jid, to_sql(expiration)],
            ),
            None => connection.execute(&format!("DELETE FROM {TABLE} WHERE jid=?1"), [jid]),
        };
        if let Err(err) = result {
            tracing::warn!(%err, jid, "failed to persist ephemeral cache");
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
                    self.persist(jid, Some(&value));
                    entries.insert(jid.into(), value);
                }
            }
            None => {
                if entries.remove(jid).is_some() {
                    tracing::debug!(jid, "ephemeral cache: disabled");
                    self.persist(jid, None);
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
        let store = baileys_rs::auth::SqliteAuthStore::open(dir.path()).unwrap();
        let db = SessionDb::File(dir.path().join("auth.sqlite3"));
        let cache = EphemeralCache::open(&db).unwrap();
        cache.update(Some("a@s.whatsapp.net"), Some(Some(86400.0)));
        cache.update(Some("b@s.whatsapp.net"), Some(Some(604800.0)));
        cache.update(Some("b@s.whatsapp.net"), Some(Some(0.0)));
        cache.update(Some("a@s.whatsapp.net"), None);
        cache.update(Some("c@s.whatsapp.net"), Some(Some(1.5)));
        drop(cache);
        let restarted = EphemeralCache::open(&db).unwrap();
        assert_eq!(restarted.get("a@s.whatsapp.net"), Some(json!(86400)));
        assert_eq!(restarted.get("b@s.whatsapp.net"), None);
        assert_eq!(restarted.get("c@s.whatsapp.net"), Some(json!(1.5)));
        restarted.update_from_chat(&json!({"id":"a@s.whatsapp.net","ephemeralExpiration":null}));
        assert_eq!(
            EphemeralCache::open(&db).unwrap().get("a@s.whatsapp.net"),
            None
        );
        // The library's own store still opens the shared file.
        drop(store);
        baileys_rs::auth::SqliteAuthStore::open(dir.path()).unwrap();
    }

    #[test]
    fn the_typescript_cache_file_is_imported_once() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("ephemeral-cache.json");
        std::fs::write(
            &legacy,
            r#"{"a@s.whatsapp.net":86400,"b@s.whatsapp.net":"x","c@g.us":7776000}"#,
        )
        .unwrap();
        let db = SessionDb::File(dir.path().join("auth.sqlite3"));
        let cache = EphemeralCache::open(&db).unwrap();
        assert_eq!(cache.get("a@s.whatsapp.net"), Some(json!(86400)));
        assert_eq!(cache.get("b@s.whatsapp.net"), None);
        assert_eq!(cache.get("c@g.us"), Some(json!(7776000)));
        cache.update(Some("a@s.whatsapp.net"), Some(None));
        drop(cache);
        // The file stays, but is not read again.
        assert!(legacy.exists());
        assert_eq!(
            EphemeralCache::open(&db).unwrap().get("a@s.whatsapp.net"),
            None
        );

        let corrupt = tempfile::tempdir().unwrap();
        std::fs::write(corrupt.path().join("ephemeral-cache.json"), "not json").unwrap();
        assert_eq!(
            EphemeralCache::open(&SessionDb::File(corrupt.path().join("auth.sqlite3")))
                .unwrap()
                .get("a@s.whatsapp.net"),
            None
        );
    }

    #[test]
    fn an_in_memory_session_keeps_the_cache_in_memory() {
        let cache = EphemeralCache::open(&SessionDb::Memory).unwrap();
        cache.update(Some("a@s.whatsapp.net"), Some(Some(86400.0)));
        assert_eq!(cache.get("a@s.whatsapp.net"), Some(json!(86400)));
        assert_eq!(
            EphemeralCache::open(&SessionDb::Memory)
                .unwrap()
                .get("a@s.whatsapp.net"),
            None
        );
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
