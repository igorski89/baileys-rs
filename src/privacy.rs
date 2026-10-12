//! Trusted-contact privacy-token storage, indexing, expiry and issuance requests.
use crate::{
    auth::{
        AuthTransaction, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore, TcToken,
        TokenTimestamp,
    },
    binary::{BinaryNode, NodeContent},
    jid::{self, Jid},
    signal::{Error, mapping},
};
use indexmap::IndexSet;
pub const INDEX_KEY: &str = "__index";
pub const BUCKET_SECONDS: u64 = 604800;
fn trim(s: &str) -> &str {
    s.trim_matches(|c: char| matches!(c, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'))
}
fn prefix_int(s: &str) -> Option<f64> {
    let mut text = trim(s);
    let sign = if let Some(s) = text.strip_prefix('-') {
        text = s;
        -1.0
    } else {
        if let Some(s) = text.strip_prefix('+') {
            text = s;
        }
        1.0
    };
    let radix = if text.starts_with("0x") || text.starts_with("0X") {
        text = &text[2..];
        16
    } else {
        10
    };
    let mut digits = 0;
    let mut result = 0.0;
    for c in text.chars() {
        let Some(d) = c.to_digit(radix).filter(|_| c.is_ascii()) else {
            break;
        };
        result = result * f64::from(radix) + f64::from(d);
        digits += 1;
    }
    (digits > 0).then_some(sign * result)
}
pub(crate) fn number(s: &str) -> f64 {
    let s = trim(s);
    if s.is_empty() {
        return 0.0;
    }
    if matches!(s, "Infinity" | "+Infinity") {
        return f64::INFINITY;
    }
    if s == "-Infinity" {
        return f64::NEG_INFINITY;
    }
    for (prefix, radix) in [
        ("0x", 16),
        ("0X", 16),
        ("0b", 2),
        ("0B", 2),
        ("0o", 8),
        ("0O", 8),
    ] {
        if let Some(digits) = s.strip_prefix(prefix) {
            if digits.is_empty() {
                return f64::NAN;
            }
            let mut result = 0.0;
            for c in digits.chars() {
                let Some(d) = c.to_digit(radix).filter(|_| c.is_ascii()) else {
                    return f64::NAN;
                };
                result = result * f64::from(radix) + f64::from(d);
            }
            return result;
        }
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'e' | b'E'))
    {
        return f64::NAN;
    }
    s.parse().unwrap_or(f64::NAN)
}
fn timestamp_value(timestamp: &TokenTimestamp, prefix: bool) -> f64 {
    match timestamp {
        TokenTimestamp::Text(text) => {
            if prefix {
                prefix_int(text).unwrap_or(f64::NAN)
            } else {
                number(text)
            }
        }
        TokenTimestamp::Number(value) => value.as_f64().unwrap_or(f64::NAN),
    }
}
fn timestamp_string(timestamp: &TokenTimestamp) -> String {
    match timestamp {
        TokenTimestamp::Text(text) => text.clone(),
        TokenTimestamp::Number(value) => {
            let number = value.as_f64().unwrap_or_default();
            if number == 0.0 {
                return "0".into();
            }
            if number.abs() >= 1e21 || number.abs() < 1e-6 {
                let formatted = format!("{number:e}");
                let (mantissa, exponent) = formatted.split_once('e').unwrap();
                let exponent: i32 = exponent.parse().unwrap();
                format!("{mantissa}e{exponent:+}")
            } else {
                number.to_string()
            }
        }
    }
}
pub fn is_expired(timestamp: Option<&TokenTimestamp>, now: u64) -> bool {
    let Some(timestamp) = timestamp else {
        return true;
    };
    let value = timestamp_value(timestamp, true);
    value.is_nan()
        || value < ((i128::from(now / BUCKET_SECONDS) - 3) * i128::from(BUCKET_SECONDS)) as f64
}
pub fn should_issue(sender_timestamp: Option<u64>, now: u64) -> bool {
    sender_timestamp.is_none_or(|sender| now / BUCKET_SECONDS > sender / BUCKET_SECONDS)
}
pub fn is_regular_user(value: &str) -> bool {
    let Some(jid) = Jid::parse(value) else {
        return false;
    };
    let user = value.split('@').next().unwrap_or_default();
    let bot = (user.len() == 11 && user.bytes().all(|c| c.is_ascii_digit()))
        && (user.starts_with("1313555") || user.starts_with("131655500"));
    user != "0"
        && !bot
        && !jid.is_meta_ai()
        && (jid.is_pn_user()
            || jid.is_lid_user()
            || jid.is_hosted_pn_user()
            || jid.is_hosted_lid_user()
            || jid.server == "c.us")
}
fn read_index(value: Option<&KeyValue>) -> Vec<String> {
    let Some(KeyValue::Tctoken(entry)) = value else {
        return vec![];
    };
    let Ok(serde_json::Value::Array(entries)) = serde_json::from_slice(&entry.token) else {
        return vec![];
    };
    entries
        .into_iter()
        .filter_map(|v| {
            v.as_str()
                .filter(|s| !s.is_empty() && *s != INDEX_KEY)
                .map(str::to_owned)
        })
        .collect()
}
fn index_write(jids: impl IntoIterator<Item = String>) -> Result<KeyMutation, Error> {
    let jids = jids
        .into_iter()
        .filter(|s| !s.is_empty() && s != INDEX_KEY)
        .collect::<IndexSet<_>>();
    Ok(KeyMutation::put(
        INDEX_KEY,
        KeyValue::Tctoken(TcToken {
            token: serde_json::to_vec(&jids)?,
            ..Default::default()
        }),
    ))
}
fn load(tx: &mut AuthTransaction<'_>, jid: &str) -> Result<Option<TcToken>, Error> {
    Ok(tx
        .get(KeyCategory::Tctoken, &[jid.into()])?
        .remove(jid)
        .and_then(|value| match value {
            KeyValue::Tctoken(value) => Some(value),
            _ => None,
        }))
}
fn clear_entry(jid: &str, entry: &TcToken) -> KeyMutation {
    match entry.sender_timestamp {
        Some(sender) => KeyMutation::put(
            jid,
            KeyValue::Tctoken(TcToken {
                sender_timestamp: Some(sender),
                ..Default::default()
            }),
        ),
        None => KeyMutation::delete(KeyCategory::Tctoken, jid),
    }
}
#[derive(Clone)]
pub struct Repository {
    store: SqliteAuthStore,
    mappings: mapping::Store,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Pruned {
    pub mutated: usize,
    pub remaining: usize,
}
impl Repository {
    pub fn new(store: SqliteAuthStore) -> Self {
        Self {
            mappings: mapping::Store::new(store.clone()),
            store,
        }
    }
    pub fn with_mappings(store: SqliteAuthStore, mappings: mapping::Store) -> Self {
        Self { store, mappings }
    }
    pub fn storage_jid(&self, jid: &str) -> Result<String, Error> {
        if Jid::parse(jid).is_some_and(|j| j.is_lid_user()) {
            return Ok(jid.into());
        }
        Ok(self
            .mappings
            .get_lid_for_pn(jid)?
            .unwrap_or_else(|| jid.into()))
    }
    pub fn issuance_jid(&self, jid: &str, to_lid: bool) -> Result<String, Error> {
        if to_lid {
            return self.storage_jid(jid);
        }
        if Jid::parse(jid).is_some_and(|j| j.is_lid_user()) {
            return Ok(self
                .mappings
                .get_pn_for_lid(jid)?
                .unwrap_or_else(|| jid.into()));
        }
        Ok(jid.into())
    }
    /// The sender timestamp recorded for a storage JID, if any.
    pub fn sender_timestamp(&self, storage_jid: &str) -> Result<Option<u64>, Error> {
        self.store.transaction_with_error(|tx| {
            Ok(load(tx, storage_jid)?.and_then(|entry| entry.sender_timestamp))
        })
    }
    pub fn read_index(&self) -> Result<Vec<String>, Error> {
        let values = self.store.get(KeyCategory::Tctoken, &[INDEX_KEY.into()])?;
        Ok(read_index(values.get(INDEX_KEY)))
    }
    /// Relay attaches no timestamp attribute and only clears nonempty expired tokens.
    /// Cleanup failure suppresses the expired token while retaining the persisted record.
    pub fn relay_token(&self, jid: &str, now: u64) -> Result<Option<Vec<u8>>, Error> {
        let storage = self.storage_jid(jid)?;
        self.store.transaction_with_error(|tx| {
            let Some(entry) = load(tx, &storage)? else {
                return Ok(None);
            };
            if entry.token.is_empty() {
                return Ok(None);
            }
            if is_expired(entry.timestamp.as_ref(), now) {
                let _ = tx.set(&[clear_entry(&storage, &entry)]);
                Ok(None)
            } else {
                Ok(Some(entry.token))
            }
        })
    }
    pub fn merged_index_write(&self, added: &[String]) -> Result<KeyMutation, Error> {
        index_write(self.read_index()?.into_iter().chain(added.iter().cloned()))
    }
    /// Mirrors the source helper's best-effort behavior, retaining base content on failure.
    pub fn build_content(&self, jid: &str, mut base: Vec<BinaryNode>, now: u64) -> Vec<BinaryNode> {
        let result = (|| {
            let storage = self.storage_jid(jid)?;
            self.store.transaction_with_error(|tx| {
                let Some(entry) = load(tx, &storage)? else {
                    return Ok(None);
                };
                if entry.token.is_empty() || is_expired(entry.timestamp.as_ref(), now) {
                    tx.set(&[clear_entry(&storage, &entry)])?;
                    return Ok(None);
                }
                Ok::<_, Error>(Some(
                    BinaryNode::new("tctoken")
                        .attr("t", timestamp_string(entry.timestamp.as_ref().unwrap()))
                        .with_content(NodeContent::Bytes(entry.token)),
                ))
            })
        })();
        if let Ok(Some(node)) = result {
            base.push(node);
        }
        base
    }
    /// Preserve sender timestamps and unknown metadata; reject older incoming tokens.
    pub fn store_from_iq(&self, result: &BinaryNode, fallback: &str) -> Result<Vec<String>, Error> {
        let Some(tokens) = result.child("tokens") else {
            return Ok(vec![]);
        };
        let mut incoming = vec![];
        for token in tokens.children("token") {
            if token
                .attrs
                .get("type")
                .is_none_or(|t| t != "trusted_contact")
            {
                continue;
            }
            let Some(NodeContent::Bytes(bytes)) = &token.content else {
                continue;
            };
            let raw = jid::normalized_user(if !fallback.is_empty() {
                fallback
            } else {
                token
                    .attrs
                    .get("jid")
                    .map(String::as_str)
                    .unwrap_or_default()
            });
            if !is_regular_user(&raw) {
                continue;
            }
            let storage = self.storage_jid(&raw)?;
            let timestamp = token.attrs.get("t").cloned().unwrap_or_default();
            let value = number(&timestamp);
            if value == 0.0 || value.is_nan() {
                continue;
            }
            incoming.push((storage, bytes.clone(), timestamp, value));
        }
        self.store.transaction_with_error(|tx| {
            let mut stored = vec![];
            for (jid, token, timestamp, value) in incoming {
                let mut entry = load(tx, &jid)?.unwrap_or_default();
                let existing = entry
                    .timestamp
                    .as_ref()
                    .map(|t| timestamp_value(t, false))
                    .unwrap_or(0.0);
                if existing > 0.0 && existing > value {
                    continue;
                }
                entry.token = token;
                entry.timestamp = Some(timestamp.into());
                tx.set(&[KeyMutation::put(&jid, KeyValue::Tctoken(entry))])?;
                stored.push(jid);
            }
            Ok(stored)
        })
    }
    /// History token candidates compare against the pre-batch snapshot, so the
    /// last duplicate replaces earlier candidates even if its timestamp is older.
    pub fn store_from_history(&self, chats: &[crate::history::Chat]) -> Result<Vec<String>, Error> {
        let mut candidates = vec![];
        for chat in chats {
            let chat = &chat.conversation;
            let ts = chat.tc_token_timestamp.unwrap_or(0) as f64;
            if let Some(token) = chat.tc_token.as_ref().filter(|token| !token.is_empty())
                && ts > 0.0
            {
                let jid = self.storage_jid(&jid::normalized_user(&chat.id))?;
                candidates.push((jid, token.clone(), ts, chat.tc_token_sender_timestamp));
            }
        }
        if candidates.is_empty() {
            return Ok(vec![]);
        }
        let ids: Vec<_> = candidates.iter().map(|c| c.0.clone()).collect();
        let existing = self.store.get(KeyCategory::Tctoken, &ids)?;
        let mut entries = indexmap::IndexMap::new();
        for (jid, token, ts, sender) in candidates {
            let mut entry = match existing.get(&jid) {
                Some(KeyValue::Tctoken(entry)) => entry.clone(),
                _ => Default::default(),
            };
            let old = entry
                .timestamp
                .as_ref()
                .map(|t| timestamp_value(t, false))
                .unwrap_or(0.0);
            if old > 0.0 && old >= ts {
                continue;
            }
            entry.token = token;
            entry.timestamp = Some(ts.to_string().into());
            if let Some(sender) = sender {
                entry.sender_timestamp = Some(sender);
            }
            entries.insert(jid, entry);
        }
        if entries.is_empty() {
            return Ok(vec![]);
        }
        let ids: Vec<_> = entries.keys().cloned().collect();
        let write = (|| {
            let old_index = read_index(
                self.store
                    .get(KeyCategory::Tctoken, &[INDEX_KEY.into()])?
                    .get(INDEX_KEY),
            );
            let mut writes: Vec<_> = entries
                .into_iter()
                .map(|(id, entry)| KeyMutation::put(id, KeyValue::Tctoken(entry)))
                .collect();
            writes.push(index_write(
                old_index.into_iter().chain(ids.iter().cloned()),
            )?);
            self.store.set(&writes)?;
            Ok::<_, Error>(())
        })();
        match write {
            Ok(()) => Ok(ids),
            Err(error) => {
                tracing::warn!(?error, "failed to store history privacy tokens");
                Ok(vec![])
            }
        }
    }
    pub fn mark_issued(&self, storage_jid: &str, timestamp: u64) -> Result<(), Error> {
        self.store.transaction_with_error(|tx| {
            let mut entry = load(tx, storage_jid)?.unwrap_or_default();
            entry.sender_timestamp = Some(timestamp);
            let mut jids = read_index(
                tx.get(KeyCategory::Tctoken, &[INDEX_KEY.into()])?
                    .get(INDEX_KEY),
            );
            jids.push(storage_jid.into());
            tx.set(&[
                KeyMutation::put(storage_jid, KeyValue::Tctoken(entry)),
                index_write(jids)?,
            ])?;
            Ok(())
        })
    }
    /// Prune the union of in-memory and persisted tracked JIDs in one transaction.
    pub fn prune(&self, known: &[String], now: u64) -> Result<Pruned, Error> {
        self.store.transaction_with_error(|tx| {
            let index = read_index(
                tx.get(KeyCategory::Tctoken, &[INDEX_KEY.into()])?
                    .get(INDEX_KEY),
            );
            let all = known
                .iter()
                .cloned()
                .chain(index)
                .filter(|s| !s.is_empty() && s != INDEX_KEY)
                .collect::<IndexSet<_>>();
            let mut survivors = vec![];
            let mut writes = vec![];
            let mut mutated = 0;
            for jid in all {
                let Some(entry) = load(tx, &jid)? else {
                    mutated += 1;
                    continue;
                };
                let peer = !entry.token.is_empty();
                let peer_expired = peer && is_expired(entry.timestamp.as_ref(), now);
                let sender = entry
                    .sender_timestamp
                    .is_some_and(|t| !is_expired(Some(&t.into()), now));
                if (!peer || peer_expired) && !sender {
                    writes.push(KeyMutation::delete(KeyCategory::Tctoken, &jid));
                    mutated += 1;
                } else {
                    if peer_expired {
                        writes.push(clear_entry(&jid, &entry));
                        mutated += 1;
                    }
                    survivors.push(jid);
                }
            }
            let remaining = survivors.len();
            if mutated > 0 {
                writes.push(index_write(survivors)?);
                tx.set(&writes)?;
            }
            Ok(Pruned { mutated, remaining })
        })
    }
}
pub fn issuance_query(jids: &[String], timestamp: u64) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("to", "@s.whatsapp.net")
        .attr("type", "set")
        .attr("xmlns", "privacy")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("tokens").with_content(NodeContent::Children(
                jids.iter()
                    .map(|jid| {
                        BinaryNode::new("token")
                            .attr("jid", jid::normalized_user(jid))
                            .attr("t", timestamp.to_string())
                            .attr("type", "trusted_contact")
                    })
                    .collect(),
            )),
        ]))
}
