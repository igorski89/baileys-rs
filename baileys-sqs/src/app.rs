//! The WhatsApp connection, its events and the reconnect loop.
use crate::{
    commands::Commands,
    config::{SessionDb, Settings},
    js,
    media::{self, S3Storage},
    whatsapp::{Live, WhatsApp},
};
use baileys_rs::{
    auth::SqliteAuthStore,
    cache::{Cache, MemoryStore},
    socket::{Config, ConnectionState, Event, Socket},
};
use serde_json::{Map, Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};

/// Open the session database. When the file is first created, a session
/// the TypeScript connector wrote to its folder (`useMultiFileAuthState`,
/// with `creds.json`) is imported, so an existing pairing keeps working.
pub fn open_session(db: &SessionDb) -> Result<SqliteAuthStore, baileys_rs::auth::Error> {
    let (SessionDb::File(path), Some(folder)) = (db, db.folder()) else {
        return SqliteAuthStore::in_memory();
    };
    let import = folder.join("creds.json").is_file() && !path.exists();
    let store = SqliteAuthStore::open_file(path)?;
    if import {
        if let Err(err) = store.import_baileys_folder(folder, &BTreeMap::new()) {
            // Leave no empty database behind: the next start would skip the
            // import and pair a new device.
            drop(store);
            for suffix in ["", "-wal", "-shm"] {
                let mut file = path.clone().into_os_string();
                file.push(suffix);
                let _ = std::fs::remove_file(file);
            }
            return Err(err);
        }
        tracing::info!(folder = %folder.display(), "imported the Baileys multi-file session");
    }
    Ok(store)
}

/// `WHATSAPP_VERSION` when it parses, otherwise the latest published version.
pub async fn whatsapp_version(settings: &Settings, http: &reqwest::Client) -> [u32; 3] {
    if let Some(raw) = &settings.whatsapp_version {
        match serde_json::from_str::<[u32; 3]>(raw) {
            Ok(version) => {
                tracing::debug!(version = ?version, "using WHATSAPP_VERSION from env");
                return version;
            }
            Err(err) => {
                tracing::error!(%err, whatsapp_version = raw, "Failed to parse WHATSAPP_VERSION, falling back to latest");
            }
        }
    }
    let latest = baileys_rs::version::fetch_latest_baileys_version(http).await;
    tracing::debug!(version = ?latest.version, is_latest = latest.is_latest, "using latest WA version");
    latest.version
}

/// Move events into an unbounded queue as they arrive, so slow handling
/// (media downloads and uploads) never makes the broadcast channel drop
/// events; upstream's `ev.process` queues the same way. Raw stanzas, which
/// produce no output, are left out.
pub fn queue(mut events: broadcast::Receiver<Event>) -> mpsc::UnboundedReceiver<Event> {
    let (sender, receiver) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(Event::Node(_)) => {}
                Ok(event) => {
                    if sender.send(event).is_err() {
                        return;
                    }
                }
                Err(RecvError::Lagged(missed)) => {
                    tracing::warn!(missed, "event subscriber lagged; events were dropped");
                }
                Err(RecvError::Closed) => return,
            }
        }
    });
    receiver
}

pub struct WhatsAppLoop {
    pub settings: Settings,
    pub store: SqliteAuthStore,
    pub version: [u32; 3],
    pub commands: Commands,
    pub storage: Option<S3Storage>,
    pub missing: crate::missing::MissingContent,
}

impl WhatsAppLoop {
    fn config(&self) -> Config {
        let mut config = Config::default();
        if let Some(url) = &self.settings.socket_url {
            config.url = url.clone();
        }
        config.payload.version = self.version;
        // Retry counts when decryption or encryption fails, as a 100 s TTL cache.
        config.msg_retry_counter_cache =
            Some(Cache::new(MemoryStore::new(Duration::from_secs(100))));
        // The TypeScript connector installs link-preview-js and asks for
        // high-quality previews.
        config.link_previews.enabled = true;
        config.link_previews.high_quality = true;
        config
    }

    /// Connect, process events until the connection closes, and reconnect
    /// after five seconds unless the account logged out.
    pub async fn run(self) {
        loop {
            tracing::info!(
                "Starting WhatsApp with SESSION_DIR: {}",
                self.settings
                    .session_db
                    .folder()
                    .map_or(":memory:".into(), |f| f.display().to_string())
            );
            let closed = match Socket::connect(self.config(), self.store.clone()).await {
                Ok((socket, events)) => {
                    let live = Arc::new(Live {
                        socket,
                        store: self.store.clone(),
                    });
                    *self.commands.socket.write().await = Some(live.clone() as Arc<dyn WhatsApp>);
                    self.process(live, events).await
                }
                Err(err) => {
                    tracing::error!(%err, "WhatsApp connection failed");
                    None
                }
            };
            let code = closed.as_ref().map(|(code, _)| *code);
            let should_reconnect = code != Some(401);
            tracing::error!(status_code = ?code, should_reconnect, "Connection closed");
            let mut payload = json!({"status":"disconnected"});
            if let Some((code, message)) = closed {
                // `lastDisconnect.error.message`, upstream's Boom message.
                payload["error"] = json!(message);
                payload["code"] = json!(code);
            }
            self.commands
                .output
                .send(&json!({"type":"connection","payload":payload}))
                .await;
            if !should_reconnect {
                tracing::error!("Connection closed. You are logged out.");
                return;
            }
            tracing::info!("Reconnecting in 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    /// Returns the close code and message, or `None` if the event stream
    /// ended without one.
    async fn process(
        &self,
        live: Arc<Live>,
        events: broadcast::Receiver<Event>,
    ) -> Option<(u16, String)> {
        let mut queue = queue(events);
        // One pairing code per connection: every request is a new link
        // attempt, which replaces the previous code, and WhatsApp limits how
        // many a number may make. QR refreshes come every 20 seconds.
        let mut pairing_requested = false;
        while let Some(event) = queue.recv().await {
            match &event {
                Event::Qr(qr) => {
                    self.commands
                        .output
                        .send(&json!({"type":"qr","payload":{"qr":qr}}))
                        .await;
                    if !pairing_requested {
                        pairing_requested = self.pairing_code(&live).await;
                    }
                }
                Event::Connection(ConnectionState::Closed { code, message }) => {
                    return Some((*code, message.clone()));
                }
                Event::Connection(ConnectionState::Open) => {
                    tracing::info!("WhatsApp connection opened");
                    self.commands
                        .output
                        .send(&json!({"type":"connection","payload":{"status":"connected"}}))
                        .await;
                }
                Event::CredentialsUpdated => tracing::debug!("creds saved"),
                _ => {}
            }
            self.forward(live.as_ref(), &event).await;
        }
        None
    }

    /// Request a pairing code when configured and unregistered; returns
    /// whether it did.
    async fn pairing_code(&self, live: &Live) -> bool {
        if !self.settings.use_pairing_code {
            return false;
        }
        let registered = self
            .store
            .load_credentials()
            .map(|c| c.registered)
            .unwrap_or(false);
        if registered {
            return false;
        }
        tracing::info!("Requesting pairing code...");
        let Some(phone) = &self.settings.phone_number else {
            return true;
        };
        match live.socket.request_pairing_code(phone.clone(), None).await {
            Ok(code) => {
                tracing::info!(code, "Pairing code generated");
                self.commands
                    .output
                    .send(
                        &json!({"type":"pairing_code","payload":{"code":code,"phoneNumber":phone}}),
                    )
                    .await;
            }
            Err(err) => tracing::error!(%err, "Failed to get pairing code"),
        }
        true
    }

    /// Forward the event's upstream entries, except connection and
    /// credential updates, which are handled above.
    pub async fn forward(&self, whatsapp: &dyn WhatsApp, event: &Event) {
        let entries = match event.upstream() {
            Ok(entries) => entries,
            Err(err) => {
                tracing::error!(%err, ?event, "event conversion failed");
                return;
            }
        };
        for (name, mut data) in entries {
            if name == "connection.update" || name == "creds.update" {
                continue;
            }
            let mut meta = Map::new();
            if name == "chats.update"
                && let Some(chats) = data.as_array()
            {
                for chat in chats {
                    self.commands.ephemeral.update_from_chat(chat);
                }
            }
            if name == "messages.upsert" {
                let upsert = match event {
                    Event::MessagesUpserted(upsert) => Some(upsert),
                    _ => None,
                };
                let typed = upsert.map_or(&[][..], |u| u.messages.as_slice());
                let mut messages_meta = vec![];
                let count = data["messages"].as_array().map_or(0, Vec::len);
                for i in 0..count {
                    let mut has_media = false;
                    if let Some(message) = typed.get(i) {
                        if let Some(found) =
                            media::extract(whatsapp, self.storage.as_ref(), message).await
                        {
                            // Upstream, only received envelopes are plain
                            // objects; own sends and notifications are
                            // protobufjs WebMessageInfo objects, whose JSON
                            // drops the `_media` the TypeScript connector sets
                            // on them, though it downloads the media.
                            if upsert
                                .and_then(|u| u.metadata_for(i))
                                .is_some_and(|m| !m.protobuf)
                            {
                                data["messages"][i]["_media"] = found;
                            }
                            has_media = true;
                        }
                        self.missing.observe(message);
                        self.commands.ephemeral.update_from_message(message);
                        self.commands
                            .polls
                            .learn(message, &data["messages"][i]["key"]);
                    }
                    let message = &data["messages"][i];
                    let mut entry = Map::new();
                    let mut set = |field: &str, value: Option<&Value>| {
                        if let Some(value) = value {
                            entry.insert(field.into(), value.clone());
                        }
                    };
                    set("message_id", message.pointer("/key/id"));
                    set("from", message.pointer("/key/remoteJid"));
                    set(
                        "direction",
                        Some(&json!(if js::truthy(message.pointer("/key/fromMe")) {
                            "outgoing"
                        } else {
                            "incoming"
                        })),
                    );
                    set("has_media", Some(&json!(has_media)));
                    let timestamp = message.get("messageTimestamp").map(raw_timestamp);
                    set("timestamp", timestamp.as_ref());
                    messages_meta.push(Value::Object(entry));
                }
                tracing::debug!(count = messages_meta.len(), "messages.upsert");
                meta.insert("messages".into(), json!(messages_meta));
                meta.insert("type".into(), data["type"].clone());
            }
            if name == "messages.update" {
                let updates: Vec<Value> = data
                    .as_array()
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|u| {
                                let mut update = Map::new();
                                if let Some(id) = u.pointer("/key/id") {
                                    update.insert("message_id".into(), id.clone());
                                }
                                if let Some(status) = u.pointer("/update/status") {
                                    update.insert("status".into(), status.clone());
                                }
                                Value::Object(update)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                meta.insert("updates".into(), json!(updates));
            }
            if name == "presence.update" {
                meta.insert("presence".into(), data.clone());
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis());
            let random = js::string(&json!(rand::random::<f64>()));
            self.commands
                .output
                .send(&json!({
                    "id": format!("{name}-{now}-{random}"),
                    "type": "baileys_event",
                    "event": name,
                    "meta": meta,
                    "payload": data,
                }))
                .await;
        }
    }
}

/// `messageTimestamp` as index.ts copies it into `meta`: the raw value, not
/// its toJSON form. A protobufjs message (the payload wrote the uint64 as a
/// string) holds a Long, which JSON.stringify writes as {low, high, unsigned}.
fn raw_timestamp(value: &Value) -> Value {
    match value.as_str().and_then(|s| s.parse::<u64>().ok()) {
        Some(v) => {
            json!({"low": v as u32 as i32, "high": (v >> 32) as u32 as i32, "unsigned": true})
        }
        None => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::raw_timestamp;
    use serde_json::json;

    #[test]
    fn meta_timestamps_are_the_raw_values_index_ts_copies() {
        assert_eq!(raw_timestamp(&json!(1792589730)), json!(1792589730));
        assert_eq!(
            raw_timestamp(&json!("1792589697")),
            json!({"low": 1792589697, "high": 0, "unsigned": true})
        );
        // Long.js keeps both halves as signed 32-bit integers.
        assert_eq!(
            raw_timestamp(&json!("4294967295")),
            json!({"low": -1, "high": 0, "unsigned": true})
        );
        assert_eq!(raw_timestamp(&json!(null)), json!(null));
    }
}
