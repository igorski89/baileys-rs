//! Device-discovery cache and query planning for message relay.
use crate::{
    jid::{self, Jid},
    signal::Error,
};
use futures_util::future::BoxFuture;
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
/// The batched calls are upstream's optional mget and mset; by default they
/// go through the single-key calls one user at a time.
pub trait Store: Send + Sync {
    fn get<'a>(&'a self, user: &'a str) -> BoxFuture<'a, Option<Vec<Device>>>;
    fn set<'a>(&'a self, user: &'a str, devices: Vec<Device>) -> BoxFuture<'a, ()>;
    fn del<'a>(&'a self, user: &'a str) -> BoxFuture<'a, ()>;
    fn flush_all(&self) -> BoxFuture<'_, ()>;
    fn get_many<'a>(&'a self, users: &'a [String]) -> BoxFuture<'a, HashMap<String, Vec<Device>>> {
        Box::pin(async move {
            let mut found = HashMap::new();
            for user in users {
                if let Some(devices) = self.get(user).await {
                    found.insert(user.clone(), devices);
                }
            }
            found
        })
    }
    fn set_many<'a>(&'a self, entries: &'a [(String, Vec<Device>)]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            for (user, devices) in entries {
                self.set(user, devices.clone()).await;
            }
        })
    }
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
    /// Absent for a JID without a device part, as upstream's undefined device.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<u16>,
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
            device: jid.device,
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
        let entry = self.entries.get_mut(user).filter(|e| e.expires > now)?;
        entry.used = now;
        Some(entry.devices.clone())
    }
    fn set(&mut self, user: &str, devices: Vec<Device>, now: Instant) {
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
        self.entries.remove(user);
    }
}
#[derive(Clone)]
pub struct Cache {
    state: Arc<Mutex<State>>,
    store: Option<ExternalStore>,
}
impl Cache {
    pub fn new(config: Config) -> Self {
        Self {
            store: config.store.clone(),
            state: Arc::new(Mutex::new(State {
                entries: HashMap::new(),
                config,
            })),
        }
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        state.purge(Instant::now());
        Ok(state)
    }
    async fn get(&self, user: &str) -> Result<Option<Vec<Device>>, Error> {
        match &self.store {
            Some(store) => Ok(store.0.get(user).await),
            None => Ok(self.state()?.get(user, Instant::now())),
        }
    }
    async fn set(&self, user: &str, devices: Vec<Device>) -> Result<(), Error> {
        match &self.store {
            Some(store) => store.0.set(user, devices).await,
            None => self.state()?.set(user, devices, Instant::now()),
        }
        Ok(())
    }
    async fn del(&self, user: &str) -> Result<(), Error> {
        match &self.store {
            Some(store) => store.0.del(user).await,
            None => self.state()?.del(user),
        }
        Ok(())
    }
    /// Explicit devices bypass discovery and filtering; remaining requests keep input order.
    pub async fn plan(&self, inputs: &[String], use_cache: bool) -> Result<Plan, Error> {
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
                    device: Some(device),
                    jid: input.clone(),
                    server: None,
                    domain_type: None,
                });
            } else {
                users.push((jid.normalized(), jid.user));
            }
        }
        // An external store is asked for all users at once, as upstream's mget.
        let mut found = match &self.store {
            Some(store) if use_cache => {
                let names: Vec<_> = users.iter().map(|(_, user)| user.clone()).collect();
                Some(store.0.get_many(&names).await)
            }
            _ => None,
        };
        for (jid, user) in users {
            let devices = match &mut found {
                Some(found) => found.get(&user).cloned(),
                None if use_cache => self.get(&user).await?,
                None => None,
            };
            match devices {
                Some(devices) => plan.devices.extend(devices),
                None => plan.to_fetch.push(jid),
            }
        }
        Ok(plan)
    }
    /// Cache only users with extracted devices. Empty server results are not negative-cached.
    /// The first fetch's zero/own-device filtering is retained on subsequent cache hits.
    pub async fn update(&self, devices: Vec<Jid>) -> Result<IndexMap<String, Vec<Device>>, Error> {
        let mut groups = IndexMap::<String, Vec<Device>>::new();
        for device in devices {
            groups
                .entry(device.user.clone())
                .or_default()
                .push(device.into());
        }
        match &self.store {
            Some(store) => {
                let entries: Vec<_> = groups
                    .iter()
                    .map(|(user, devices)| (user.clone(), devices.clone()))
                    .collect();
                store.0.set_many(&entries).await;
            }
            None => {
                for (user, devices) in &groups {
                    self.set(user, devices.clone()).await?;
                }
            }
        }
        Ok(groups)
    }
    /// Unexpired cached devices for one user.
    pub async fn cached(&self, user: &str) -> Result<Option<Vec<Device>>, Error> {
        self.get(user).await
    }
    /// Apply one user's device-list notification delta. Users without a cached
    /// baseline are left for a later full USync refresh. Callers serialize the
    /// writers, as upstream's devices mutex does.
    pub async fn apply_change(
        &self,
        user: &str,
        change: Change,
        devices: &[Jid],
    ) -> Result<(), Error> {
        if change == Change::Update {
            return self.del(user).await;
        }
        let Some(existing) = self.get(user).await?.filter(|d| !d.is_empty()) else {
            return Ok(());
        };
        // A device-less JID only matches a cached entry that has no device either.
        let mut updated: Vec<_> = existing
            .into_iter()
            .filter(|d| !devices.iter().any(|j| j.device == d.device))
            .collect();
        match change {
            Change::Add => updated.extend(devices.iter().cloned().map(Device::from)),
            Change::Remove => (),
            _ => return Ok(()),
        }
        if updated.is_empty() {
            self.del(user).await
        } else {
            self.set(user, updated).await
        }
    }
    pub async fn invalidate_users(&self, users: &[String]) -> Result<(), Error> {
        for user in users {
            self.del(user).await?;
        }
        Ok(())
    }
    pub async fn clear(&self) -> Result<(), Error> {
        match &self.store {
            Some(store) => store.0.flush_all().await,
            None => self.state()?.entries.clear(),
        }
        Ok(())
    }
}
