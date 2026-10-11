//! Device-list-driven PN to LID session migration, matching the pinned repository.
use super::{Address, Error, direct::Record};
use crate::{
    auth::{KeyCategory, KeyMutation, KeyValue, SqliteAuthStore},
    jid::{self, Jid},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct Config {
    pub ttl: Duration,
    /// Rust bounds this cache; eviction only causes a fresh persistent lookup.
    pub capacity: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(3 * 24 * 60 * 60),
            capacity: 16384,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Result {
    pub migrated: usize,
    pub skipped: usize,
    pub total: usize,
}
struct Entry {
    expires: Instant,
    used: Instant,
}
pub(super) struct Cache {
    entries: HashMap<String, Entry>,
    config: Config,
}
impl Cache {
    pub(super) fn new(config: Config) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            entries: HashMap::new(),
            config,
        }))
    }
    pub(super) fn clear(&mut self) {
        self.entries.clear();
    }
    fn contains(&mut self, key: &str) -> bool {
        let now = Instant::now();
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        if entry.expires <= now {
            self.entries.remove(key);
            return false;
        }
        entry.used = now;
        // Upstream uses LRUCache.has, whose default updateAgeOnHas is false.
        // Its updateAgeOnGet setting does not extend this migration TTL.
        true
    }
    fn insert(&mut self, key: String) {
        if self.config.capacity == 0 || self.config.ttl.is_zero() {
            return;
        }
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.expires > now);
        if !self.entries.contains_key(&key)
            && self.entries.len() >= self.config.capacity
            && let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(
            key,
            Entry {
                expires: now.checked_add(self.config.ttl).unwrap_or(now),
                used: now,
            },
        );
    }
}

/// Copies open sessions and deletes their PN records in one SQLite transaction.
/// Existing LID sessions are overwritten, identity keys are left in place, and
/// the requesting device is appended to the local plan without persisting it.
/// The initial lookup uses user.device keys even for hosted users, as upstream does.
pub(super) fn migrate(
    store: &SqliteAuthStore,
    cache: &Mutex<Cache>,
    from: &str,
    to: &str,
) -> std::result::Result<Result, Error> {
    if from.is_empty() || !Jid::parse(to).is_some_and(|j| j.is_lid_user() || j.is_hosted_lid_user())
    {
        return Ok(Result::default());
    }
    let Some(from) = Jid::parse(from) else {
        return Ok(Result {
            total: 1,
            ..Default::default()
        });
    };
    if !from.is_pn_user() && !from.is_hosted_pn_user() {
        return Ok(Result {
            total: 1,
            ..Default::default()
        });
    }
    let mut cache = cache.lock().map_err(|_| Error::Invalid)?;
    let (result, completed) = store.transaction_with_error(|tx| {
        let mut devices = tx.get(KeyCategory::DeviceList, std::slice::from_ref(&from.user))?;
        let Some(KeyValue::DeviceList(mut devices)) = devices.remove(&from.user) else {
            return Ok((Result::default(), Vec::new()));
        };
        let requested = from.device.unwrap_or(0).to_string();
        if !devices.contains(&requested) {
            devices.push(requested);
        }
        let keys = devices
            .iter()
            .map(|d| format!("{}.{d}", from.user))
            .filter(|key| !cache.contains(key))
            .collect::<Vec<_>>();
        let discovered = tx.get(KeyCategory::Session, &keys)?;
        let mut operations = Vec::new();
        // Object.entries on upstream's discovered object collapses duplicate keys.
        let mut seen = std::collections::HashSet::new();
        for key in &keys {
            if !seen.insert(key) {
                continue;
            }
            if !discovered.contains_key(key) {
                continue;
            }
            let device = key
                .rsplit_once('.')
                .ok_or(Error::Address)?
                .1
                .parse::<u16>()
                .map_err(|_| Error::Address)?;
            let pn_jid = jid::encode(
                &from.user,
                if device == 99 {
                    "hosted"
                } else {
                    "s.whatsapp.net"
                },
                Some(device),
                None,
            );
            let lid_jid = jid::transfer_device(&pn_jid, to).ok_or(Error::Address)?;
            operations.push((
                Address::from_jid(&pn_jid)?.to_string(),
                Address::from_jid(&lid_jid)?.to_string(),
                format!("{}.{device}", from.user),
            ));
        }
        let source_keys = operations
            .iter()
            .map(|(pn, _, _)| pn.clone())
            .collect::<Vec<_>>();
        let sources = tx.get(KeyCategory::Session, &source_keys)?;
        let mut updates = Vec::new();
        let mut completed = Vec::new();
        let total = operations.len();
        for (pn, lid, cache_key) in operations {
            let Some(KeyValue::Session(value)) = sources.get(&pn) else {
                continue;
            };
            let record = Record::from_json(value.clone())?;
            if !record.has_open_session() {
                continue;
            }
            updates.push(KeyMutation::put(lid, KeyValue::Session(record.to_json()?)));
            updates.push(KeyMutation::delete(KeyCategory::Session, pn));
            completed.push(cache_key);
        }
        tx.set(&updates)?;
        Ok::<_, Error>((
            Result {
                migrated: completed.len(),
                skipped: total - completed.len(),
                total,
            },
            completed,
        ))
    })?;
    // A failed transaction never publishes a successful migration in the cache.
    for key in completed {
        cache.insert(key);
    }
    Ok(result)
}
