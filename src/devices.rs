//! Device-discovery cache and query planning for message relay.
use crate::{
    jid::{self, Jid},
    signal::Error,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct Config {
    pub ttl: Duration,
    pub capacity: usize,
    /// Keep device lists in an external store instead of the built-in cache.
    /// The store applies its own expiry and capacity.
    pub store: Option<ExternalStore>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            capacity: 16384,
            store: None,
        }
    }
}
/// Storage for per-user device lists, keyed by the user part of the JID.
/// Calls are made while the cache is locked and must not block for long.
pub trait Store: Send + Sync {
    fn get(&self, user: &str) -> Option<Vec<Device>>;
    fn set(&self, user: &str, devices: Vec<Device>);
    fn del(&self, user: &str);
    fn flush_all(&self);
}
#[derive(Clone)]
pub struct ExternalStore(Arc<dyn Store>);
impl ExternalStore {
    pub fn new(store: impl Store + 'static) -> Self {
        Self(Arc::new(store))
    }
}
impl std::fmt::Debug for ExternalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalStore").finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    pub user: String,
    pub device: u16,
    pub jid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_type: Option<u8>,
}
impl From<Jid> for Device {
    fn from(jid: Jid) -> Self {
        Self {
            jid: jid::encode(&jid.user, &jid.server, jid.device, None),
            user: jid.user,
            device: jid.device.unwrap_or(0),
            server: Some(jid.server),
            domain_type: Some(jid.domain_type),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    Add,
    Remove,
    Update,
    Unknown,
}
/// A parsed `devices` notification grouped by user in arrival order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListChange {
    pub change: Change,
    pub users: IndexMap<String, Vec<(String, Jid)>>,
}
/// Parse a device-list notification; None when it carries no decodable devices.
pub fn parse_notification(node: &crate::binary::BinaryNode) -> Option<ListChange> {
    let child = node.all_children().first()?;
    let mut users = IndexMap::<String, Vec<(String, Jid)>>::new();
    for device in child.children("device") {
        let Some(raw) = device.attrs.get("jid").filter(|s| !s.is_empty()) else {
            continue;
        };
        let Some(jid) = Jid::parse(raw) else { continue };
        users
            .entry(jid.user.clone())
            .or_default()
            .push((raw.clone(), jid));
    }
    (!users.is_empty()).then(|| ListChange {
        change: match child.tag.as_str() {
            "add" => Change::Add,
            "remove" => Change::Remove,
            "update" => Change::Update,
            _ => Change::Unknown,
        },
        users,
    })
}
#[derive(Debug)]
pub struct Plan {
    pub devices: Vec<Device>,
    pub to_fetch: Vec<String>,
}
struct Entry {
    devices: Vec<Device>,
    expires: Instant,
    used: Instant,
}
struct State {
    entries: HashMap<String, Entry>,
    config: Config,
}
impl State {
    fn purge(&mut self, now: Instant) {
        self.entries.retain(|_, entry| entry.expires > now);
    }
    fn get(&mut self, user: &str, now: Instant) -> Option<Vec<Device>> {
        if let Some(store) = &self.config.store {
            return store.0.get(user);
        }
        let entry = self.entries.get_mut(user).filter(|e| e.expires > now)?;
        entry.used = now;
        Some(entry.devices.clone())
    }
    fn set(&mut self, user: &str, devices: Vec<Device>, now: Instant) {
        if let Some(store) = &self.config.store {
            return store.0.set(user, devices);
        }
        if self.config.capacity == 0 || self.config.ttl.is_zero() {
            return;
        }
        if !self.entries.contains_key(user)
            && self.entries.len() >= self.config.capacity
            && let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
        {
            self.entries.remove(&oldest);
        }
        let expires = now.checked_add(self.config.ttl).unwrap_or(now);
        self.entries.insert(
            user.into(),
            Entry {
                devices,
                expires,
                used: now,
            },
        );
    }
    fn del(&mut self, user: &str) {
        match &self.config.store {
            Some(store) => store.0.del(user),
            None => {
                self.entries.remove(user);
            }
        }
    }
}
#[derive(Clone)]
pub struct Cache {
    state: Arc<Mutex<State>>,
}
impl Cache {
    pub fn new(config: Config) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                entries: HashMap::new(),
                config,
            })),
        }
    }
    /// Explicit devices bypass discovery and filtering; remaining requests keep input order.
    pub fn plan(&self, inputs: &[String], use_cache: bool) -> Result<Plan, Error> {
        let mut plan = Plan {
            devices: vec![],
            to_fetch: vec![],
        };
        let mut users = vec![];
        for input in inputs {
            let jid = Jid::parse(input)
                .filter(|j| !j.user.is_empty())
                .ok_or(Error::Address)?;
            if let Some(device) = jid.device {
                plan.devices.push(Device {
                    user: jid.user,
                    device,
                    jid: input.clone(),
                    server: None,
                    domain_type: None,
                });
            } else {
                users.push((jid.normalized(), jid.user));
            }
        }
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        let now = Instant::now();
        state.purge(now);
        for (jid, user) in users {
            if use_cache && let Some(devices) = state.get(&user, now) {
                plan.devices.extend(devices);
            } else {
                plan.to_fetch.push(jid);
            }
        }
        Ok(plan)
    }
    /// Cache only users with extracted devices. Empty server results are not negative-cached.
    /// The first fetch's zero/own-device filtering is retained on subsequent cache hits.
    pub fn update(&self, devices: Vec<Jid>) -> Result<IndexMap<String, Vec<Device>>, Error> {
        let mut groups = IndexMap::<String, Vec<Device>>::new();
        for device in devices {
            groups
                .entry(device.user.clone())
                .or_default()
                .push(device.into());
        }
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        let now = Instant::now();
        state.purge(now);
        for (user, devices) in &groups {
            state.set(user, devices.clone(), now);
        }
        Ok(groups)
    }
    /// Unexpired cached devices for one user.
    pub fn cached(&self, user: &str) -> Result<Option<Vec<Device>>, Error> {
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        Ok(state.get(user, Instant::now()))
    }
    /// Apply one user's device-list notification delta. Users without a cached
    /// baseline are left for a later full USync refresh.
    pub fn apply_change(&self, user: &str, change: Change, devices: &[Jid]) -> Result<(), Error> {
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        let now = Instant::now();
        state.purge(now);
        if change == Change::Update {
            state.del(user);
            return Ok(());
        }
        let Some(existing) = state.get(user, now).filter(|d| !d.is_empty()) else {
            return Ok(());
        };
        // A device-less JID matches no cached device number, as upstream's undefined.
        let mut updated: Vec<_> = existing
            .into_iter()
            .filter(|d| !devices.iter().any(|j| j.device == Some(d.device)))
            .collect();
        match change {
            Change::Add => updated.extend(devices.iter().cloned().map(Device::from)),
            Change::Remove => (),
            _ => return Ok(()),
        }
        if updated.is_empty() {
            state.del(user);
        } else {
            state.set(user, updated, now);
        }
        Ok(())
    }
    pub fn invalidate_users(&self, users: &[String]) -> Result<(), Error> {
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        for user in users {
            state.del(user);
        }
        Ok(())
    }
    pub fn clear(&self) -> Result<(), Error> {
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        match &state.config.store {
            Some(store) => store.0.flush_all(),
            None => state.entries.clear(),
        }
        Ok(())
    }
}
