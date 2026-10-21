//! Polls seen in `messages.upsert`, kept in the session's SQLite file so
//! `vote_poll` can encrypt a vote without the caller passing the secret.
//!
//! A vote is encrypted with the poll's `messageContextInfo.messageSecret`,
//! which only the poll creation message carries. Rust-only: the TypeScript
//! connector has no `vote_poll`.
use crate::{config::SessionDb, whatsapp::User};
use baileys_rs::{jid, proto, relay};
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

/// The connector's table in `auth.sqlite3`, beside baileys-rs's own tables.
const TABLE: &str = "baileys_sqs_polls";

/// A poll as it was received or sent.
#[derive(Debug, Clone, PartialEq)]
pub struct Poll {
    /// The message key as events carry it (with `addressingMode` and the
    /// `*Alt` JIDs).
    pub key: Value,
    pub secret: Vec<u8>,
    pub options: Vec<String>,
}

#[derive(Clone)]
pub struct PollCache {
    connection: Arc<Mutex<Connection>>,
}

/// The poll creation message inside `message`, with its secret.
fn creation(message: &proto::Message) -> Option<(Vec<String>, Vec<u8>)> {
    let content = relay::normalize(Some(message))?;
    let poll = content
        .poll_creation_message
        .as_deref()
        .or(content.poll_creation_message_v2.as_deref())
        .or(content.poll_creation_message_v3.as_deref())
        .or(content.poll_creation_message_v5.as_deref())?;
    let secret = [content, message]
        .into_iter()
        .find_map(|m| m.message_context_info.as_ref()?.message_secret.clone())?;
    let options = poll
        .options
        .iter()
        .map(|o| o.option_name.clone().unwrap_or_default())
        .collect();
    Some((options, secret))
}

impl PollCache {
    pub fn open(db: &SessionDb) -> Result<Self, rusqlite::Error> {
        let connection = match db {
            SessionDb::Memory => Connection::open_in_memory()?,
            SessionDb::File(path) => Connection::open(path)?,
        };
        // The same file is written by baileys-rs's own connection.
        connection.busy_timeout(Duration::from_secs(30))?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {TABLE} (id TEXT PRIMARY KEY, key TEXT NOT NULL, secret BLOB NOT NULL, options TEXT NOT NULL)"
        ))?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Remember the message if it creates a poll; `key` is its JSON key.
    pub fn learn(&self, message: &proto::WebMessageInfo, key: &Value) {
        let Some((options, secret)) = message.message.as_ref().and_then(creation) else {
            return;
        };
        let Some(id) = key.get("id").and_then(Value::as_str) else {
            return;
        };
        let connection = self.connection.lock().expect("poll cache connection");
        let result = connection.execute(
            &format!(
                "INSERT OR REPLACE INTO {TABLE} (id, key, secret, options) VALUES (?1, ?2, ?3, ?4)"
            ),
            rusqlite::params![
                id,
                key.to_string(),
                secret,
                Value::from(options).to_string()
            ],
        );
        match result {
            Ok(_) => tracing::debug!(id, "poll cache: stored"),
            Err(err) => tracing::warn!(%err, id, "failed to persist poll cache"),
        }
    }

    pub fn get(&self, id: &str) -> Option<Poll> {
        let connection = self.connection.lock().expect("poll cache connection");
        let row = connection
            .query_row(
                &format!("SELECT key, secret, options FROM {TABLE} WHERE id=?1"),
                [id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )
            .optional();
        let (key, secret, options) = match row {
            Ok(row) => row?,
            Err(err) => {
                tracing::warn!(%err, id, "failed to read poll cache");
                return None;
            }
        };
        Some(Poll {
            key: serde_json::from_str(&key).ok()?,
            secret,
            options: serde_json::from_str(&options).ok()?,
        })
    }
}

fn text<'a>(key: &'a Value, field: &str) -> Option<&'a str> {
    key.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn is_lid(jid: &str) -> bool {
    jid::Jid::parse(jid).is_some_and(|j| j.is_lid_user())
}

/// The poll creator's and this account's JIDs for the vote's encryption:
/// deviceless, and both in the chat's addressing mode, which is what the
/// creator's client decrypts with (a phone in a LID chat uses the LIDs).
pub fn vote_identities(key: &Value, me: &User) -> Result<(String, String), String> {
    let group = text(key, "remoteJid").is_some_and(|j| j.ends_with("@g.us"));
    let (author, alt) = if group {
        (text(key, "participant"), text(key, "participantAlt"))
    } else {
        (text(key, "remoteJid"), text(key, "remoteJidAlt"))
    };
    let lid = match text(key, "addressingMode") {
        Some(mode) => mode == "lid",
        None => author.is_some_and(is_lid),
    };
    let own = if lid {
        me.lid
            .as_deref()
            .ok_or("vote_poll: the account's LID is unknown")?
    } else {
        me.id.as_str()
    };
    let own = jid::normalized_user(own);
    let from_me = key.get("fromMe").and_then(Value::as_bool) == Some(true);
    let creator = if from_me {
        own.clone()
    } else {
        let creator = [author, alt]
            .into_iter()
            .flatten()
            .find(|j| is_lid(j) == lid)
            .ok_or("vote_poll: the poll's creator is unknown; pass \"poll_creator_jid\"")?;
        jid::normalized_user(creator)
    };
    Ok((creator, own))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn me() -> User {
        User {
            id: "41767767829:5@s.whatsapp.net".into(),
            lid: Some("111:5@lid".into()),
        }
    }

    #[test]
    fn identities_follow_the_chat_addressing_mode() {
        let lid_chat = json!({"remoteJid":"222@lid","remoteJidAlt":"41767906626@s.whatsapp.net","fromMe":false,"id":"A","addressingMode":"lid"});
        assert_eq!(
            vote_identities(&lid_chat, &me()).unwrap(),
            ("222@lid".into(), "111@lid".into())
        );
        let pn_chat = json!({"remoteJid":"41767906626@s.whatsapp.net","remoteJidAlt":"222@lid","fromMe":false,"id":"A","addressingMode":"pn"});
        assert_eq!(
            vote_identities(&pn_chat, &me()).unwrap(),
            (
                "41767906626@s.whatsapp.net".into(),
                "41767767829@s.whatsapp.net".into()
            )
        );
        let own = json!({"remoteJid":"222@lid","fromMe":true,"id":"A"});
        assert_eq!(
            vote_identities(&own, &me()).unwrap(),
            ("111@lid".into(), "111@lid".into())
        );
        let group = json!({"remoteJid":"1@g.us","participant":"41767906626:2@s.whatsapp.net","participantAlt":"222:2@lid","fromMe":false,"id":"A","addressingMode":"lid"});
        assert_eq!(
            vote_identities(&group, &me()).unwrap(),
            ("222@lid".into(), "111@lid".into())
        );
    }

    #[test]
    fn polls_are_kept_with_their_secret_and_options() {
        let dir = tempfile::tempdir().unwrap();
        let db = SessionDb::File(dir.path().join("auth.sqlite3"));
        let cache = PollCache::open(&db).unwrap();
        let poll = proto::message::PollCreationMessage {
            name: Some("T&C".into()),
            options: ["Yes", "No"]
                .map(|name| proto::message::poll_creation_message::Option {
                    option_name: Some(name.into()),
                    ..Default::default()
                })
                .to_vec(),
            ..Default::default()
        };
        let message = proto::WebMessageInfo {
            message: Some(proto::Message {
                ephemeral_message: Some(Box::new(proto::message::FutureProofMessage {
                    message: Some(Box::new(proto::Message {
                        poll_creation_message_v3: Some(Box::new(poll)),
                        message_context_info: Some(proto::MessageContextInfo {
                            message_secret: Some(vec![9; 32]),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        let key = json!({"remoteJid":"222@lid","fromMe":false,"id":"P1"});
        cache.learn(&message, &key);
        cache.learn(&proto::WebMessageInfo::default(), &json!({"id":"T1"}));
        drop(cache);
        let cache = PollCache::open(&db).unwrap();
        assert_eq!(
            cache.get("P1"),
            Some(Poll {
                key,
                secret: vec![9; 32],
                options: vec!["Yes".into(), "No".into()],
            })
        );
        assert_eq!(cache.get("T1"), None);
    }
}
