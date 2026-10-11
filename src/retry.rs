//! In-memory message retry state corresponding to upstream MessageRetryManager.
pub mod request;
use crate::proto;
use indexmap::IndexMap;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const RECENT_TTL: u64 = 300_000;
const RETRY_TTL: u64 = 900_000;
const RECREATE_TIMEOUT: u64 = 3_600_000;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("retry state lock poisoned")]
    Poisoned,
    #[error("phone request scheduling requires a Tokio runtime")]
    NoRuntime,
}
#[derive(Debug, Clone, PartialEq)]
pub struct RecentMessage {
    pub message: proto::Message,
    pub timestamp: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Statistics {
    pub total_retries: u64,
    pub successful_retries: u64,
    pub failed_retries: u64,
    pub media_retries: u64,
    pub session_recreations: u64,
    pub phone_requests: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Recreation {
    pub reason: String,
    pub recreate: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Reason {
    UnknownError = 0,
    SignalErrorNoSession = 1,
    SignalErrorInvalidKey = 2,
    SignalErrorInvalidKeyId = 3,
    SignalErrorInvalidMessage = 4,
    SignalErrorInvalidSignature = 5,
    SignalErrorFutureMessage = 6,
    SignalErrorBadMac = 7,
    SignalErrorInvalidSession = 8,
    SignalErrorInvalidMsgKey = 9,
    BadBroadcastEphemeralSetting = 10,
    UnknownCompanionNoPrekey = 11,
    AdvFailure = 12,
    StatusRevokeDelay = 13,
}
impl Reason {
    pub fn is_mac_error(self) -> bool {
        matches!(
            self,
            Self::SignalErrorInvalidMessage | Self::SignalErrorBadMac
        )
    }
    fn mac_name(self) -> &'static str {
        match self {
            Self::SignalErrorInvalidMessage => "SignalErrorInvalidMessage",
            Self::SignalErrorBadMac => "SignalErrorBadMac",
            _ => "",
        }
    }
}
/// Decimal JS parseInt semantics, including trailing text and unknown numeric codes.
pub fn parse_error_code(attribute: Option<&str>) -> Option<Reason> {
    let s = attribute?.trim_start_matches(|c: char| matches!(c, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'));
    let (negative, s) = if let Some(s) = s.strip_prefix('-') {
        (true, s)
    } else {
        (false, s.strip_prefix('+').unwrap_or(s))
    };
    let mut digits = s.bytes().take_while(u8::is_ascii_digit).peekable();
    digits.peek()?;
    let value = digits.fold(0u8, |v, d| v.saturating_mul(10).saturating_add(d - b'0'));
    if negative && value != 0 {
        return Some(Reason::UnknownError);
    }
    Some(match value {
        1 => Reason::SignalErrorNoSession,
        2 => Reason::SignalErrorInvalidKey,
        3 => Reason::SignalErrorInvalidKeyId,
        4 => Reason::SignalErrorInvalidMessage,
        5 => Reason::SignalErrorInvalidSignature,
        6 => Reason::SignalErrorFutureMessage,
        7 => Reason::SignalErrorBadMac,
        8 => Reason::SignalErrorInvalidSession,
        9 => Reason::SignalErrorInvalidMsgKey,
        10 => Reason::BadBroadcastEphemeralSetting,
        11 => Reason::UnknownCompanionNoPrekey,
        12 => Reason::AdvFailure,
        13 => Reason::StatusRevokeDelay,
        _ => Reason::UnknownError,
    })
}
struct Entry<V> {
    value: V,
    start: u64,
}
struct Cache<V> {
    entries: IndexMap<String, Entry<V>>,
    ttl: u64,
    max: Option<usize>,
    sliding: bool,
}
impl<V: Clone> Cache<V> {
    fn new(ttl: u64, max: Option<usize>, sliding: bool) -> Self {
        Self {
            entries: IndexMap::new(),
            ttl,
            max,
            sliding,
        }
    }
    fn purge(&mut self, now: u64) -> Vec<String> {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| e.start != 0 && now.saturating_sub(e.start) > self.ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired {
            self.entries.shift_remove(key);
        }
        expired
    }
    fn set(&mut self, key: String, value: V, now: u64) -> Vec<String> {
        let mut removed = vec![];
        if self.entries.shift_remove(&key).is_some() {
            removed.push(key.clone());
        }
        self.entries.insert(key, Entry { value, start: now });
        if self.max.is_some_and(|max| self.entries.len() > max) {
            removed.push(self.entries.shift_remove_index(0).unwrap().0);
        }
        removed
    }
    fn get(&mut self, key: &str, now: u64) -> Option<V> {
        let mut entry = self.entries.shift_remove(key)?;
        if self.sliding {
            entry.start = now;
        }
        let value = entry.value.clone();
        self.entries.insert(key.to_owned(), entry);
        Some(value)
    }
}
struct Pending {
    generation: u64,
    task: tokio::task::AbortHandle,
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct State {
    recent: Cache<RecentMessage>,
    index: HashMap<String, String>,
    history: Cache<u64>,
    counts: Cache<u64>,
    base: Cache<Vec<u8>>,
    pending: HashMap<String, Pending>,
    generation: u64,
    statistics: Statistics,
}
impl State {
    fn dispose(&mut self, keys: Vec<String>) {
        for key in keys {
            if let Some((_, id)) = key.rsplit_once('\0') {
                self.index.remove(id);
            }
        }
    }
    fn purge(&mut self, now: u64) {
        let keys = self.recent.purge(now);
        self.dispose(keys);
        self.history.purge(now);
        self.counts.purge(now);
        self.base.purge(now);
    }
    fn remove_recent(&mut self, id: &str) {
        if let Some(key) = self.index.remove(id) {
            self.recent.entries.shift_remove(&key);
            self.dispose(vec![key]);
        }
    }
}
struct Inner {
    state: Mutex<State>,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    max_retries: u64,
}
/// Cloneable shared state. Expired entries are purged on every operation; no background
/// cache timer is required. Phone callbacks are cancellable Tokio tasks.
#[derive(Clone)]
pub struct Manager(Arc<Inner>);
impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetryManager")
            .field("max_retries", &self.0.max_retries)
            .finish_non_exhaustive()
    }
}
impl Manager {
    pub fn new(max_retries: u64) -> Self {
        Self::with_clock(max_retries, || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX)
        })
    }
    /// Inject a millisecond clock, including for deterministic TTL/recreation tests.
    pub fn with_clock(max_retries: u64, clock: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        Self(Arc::new(Inner {
            clock: Arc::new(clock),
            max_retries,
            state: Mutex::new(State {
                recent: Cache::new(RECENT_TTL, Some(512), false),
                index: HashMap::new(),
                history: Cache::new(RECREATE_TIMEOUT * 2, None, false),
                counts: Cache::new(RETRY_TTL, None, true),
                base: Cache::new(RETRY_TTL, Some(1024), false),
                pending: HashMap::new(),
                generation: 0,
                statistics: Default::default(),
            }),
        }))
    }
    fn work<T>(&self, f: impl FnOnce(&mut State, u64) -> T) -> Result<T, Error> {
        let now = (self.0.clock)();
        let mut state = self.0.state.lock().map_err(|_| Error::Poisoned)?;
        state.purge(now);
        Ok(f(&mut state, now))
    }
    pub fn add_recent_message(
        &self,
        to: &str,
        id: &str,
        message: proto::Message,
    ) -> Result<(), Error> {
        self.work(|s, now| {
            let key = format!("{to}\0{id}");
            let removed = s.recent.set(
                key.clone(),
                RecentMessage {
                    message,
                    timestamp: now,
                },
                now,
            );
            s.dispose(removed);
            s.index.insert(id.to_owned(), key);
        })
    }
    pub fn get_recent_message(&self, to: &str, id: &str) -> Result<Option<RecentMessage>, Error> {
        self.work(|s, now| s.recent.get(&format!("{to}\0{id}"), now))
    }
    pub fn should_recreate_session(
        &self,
        jid: &str,
        has_session: bool,
        error: Option<Reason>,
    ) -> Result<Recreation, Error> {
        self.work(|s, now| {
            let reason = if !has_session {
                Some("we don't have a Signal session with them".to_owned())
            } else if let Some(error) = error.filter(|e| e.is_mac_error()) {
                Some(format!(
                    "MAC error (code {}: {}), immediate session recreation",
                    error as u8,
                    error.mac_name()
                ))
            } else if s
                .history
                .get(jid, now)
                .is_none_or(|prev| prev == 0 || now.saturating_sub(prev) > RECREATE_TIMEOUT)
            {
                Some("retry count > 1 and over an hour since last recreation".to_owned())
            } else {
                None
            };
            if let Some(reason) = reason {
                s.history.set(jid.into(), now, now);
                s.statistics.session_recreations += 1;
                Recreation {
                    reason,
                    recreate: true,
                }
            } else {
                Recreation {
                    reason: String::new(),
                    recreate: false,
                }
            }
        })
    }
    pub fn increment_retry_count(&self, id: &str) -> Result<u64, Error> {
        self.work(|s, now| {
            let count = s.counts.get(id, now).unwrap_or(0).saturating_add(1);
            s.counts.set(id.into(), count, now);
            s.statistics.total_retries += 1;
            count
        })
    }
    pub fn get_retry_count(&self, id: &str) -> Result<u64, Error> {
        self.work(|s, now| s.counts.get(id, now).unwrap_or(0))
    }
    pub fn has_exceeded_max_retries(&self, id: &str) -> Result<bool, Error> {
        Ok(self.get_retry_count(id)? >= self.0.max_retries)
    }
    pub fn mark_retry_success(&self, id: &str) -> Result<(), Error> {
        self.finish(id, true)
    }
    pub fn mark_retry_failed(&self, id: &str) -> Result<(), Error> {
        self.finish(id, false)
    }
    fn finish(&self, id: &str, success: bool) -> Result<(), Error> {
        self.work(|s, _| {
            if success {
                s.statistics.successful_retries += 1;
            } else {
                s.statistics.failed_retries += 1;
            }
            s.counts.entries.shift_remove(id);
            s.pending.remove(id);
            s.remove_recent(id);
        })
    }
    pub fn save_base_key(&self, address: &str, id: &str, key: &[u8]) -> Result<(), Error> {
        self.work(|s, now| {
            s.base.set(format!("{address}:{id}"), key.to_vec(), now);
        })
    }
    pub fn has_same_base_key(&self, address: &str, id: &str, key: &[u8]) -> Result<bool, Error> {
        self.work(|s, now| {
            s.base
                .get(&format!("{address}:{id}"), now)
                .is_some_and(|stored| stored == key)
        })
    }
    pub fn delete_base_key(&self, address: &str, id: &str) -> Result<(), Error> {
        self.work(|s, _| {
            s.base.entries.shift_remove(&format!("{address}:{id}"));
        })
    }
    pub fn statistics(&self) -> Result<Statistics, Error> {
        self.work(|s, _| s.statistics.clone())
    }
    pub fn clear(&self) -> Result<(), Error> {
        self.work(|s, _| {
            s.recent.entries.clear();
            s.index.clear();
            s.history.entries.clear();
            s.counts.entries.clear();
            s.base.entries.clear();
            s.pending.clear();
            s.statistics = Default::default();
        })
    }
    pub fn cancel_pending_phone_request(&self, id: &str) -> Result<(), Error> {
        self.work(|s, _| {
            s.pending.remove(id);
        })
    }
    /// Default delay is three seconds. Node clamps zero and delays beyond i32::MAX
    /// milliseconds to one millisecond; this API preserves that behavior.
    pub fn schedule_phone_request(
        &self,
        id: &str,
        callback: impl FnOnce() + Send + 'static,
        delay: Option<Duration>,
    ) -> Result<(), Error> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        let mut state = self.0.state.lock().map_err(|_| Error::Poisoned)?;
        state.pending.remove(id);
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        let id = id.to_owned();
        let task_id = id.clone();
        let inner = Arc::downgrade(&self.0);
        let ms = delay.unwrap_or(Duration::from_secs(3)).as_millis();
        let ms = if ms == 0 || ms > i32::MAX as u128 {
            1
        } else {
            ms as u64
        };
        let task = runtime.spawn(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let fire = if let Ok(mut state) = inner.state.lock() {
                if state
                    .pending
                    .get(&task_id)
                    .is_some_and(|p| p.generation == generation)
                {
                    // Dropping this task's abort handle cannot interrupt synchronous callback work.
                    state.pending.remove(&task_id);
                    state.statistics.phone_requests += 1;
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if fire {
                callback();
            }
        });
        state.pending.insert(
            id,
            Pending {
                generation,
                task: task.abort_handle(),
            },
        );
        Ok(())
    }
}
