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
}
impl Default for Config {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            capacity: 16384,
        }
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
        state.entries.retain(|_, entry| entry.expires > now);
        for (jid, user) in users {
            if use_cache && let Some(entry) = state.entries.get_mut(&user) {
                entry.used = now;
                plan.devices.extend(entry.devices.clone());
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
        state.entries.retain(|_, entry| entry.expires > now);
        if state.config.capacity > 0 && !state.config.ttl.is_zero() {
            for (user, devices) in &groups {
                if !state.entries.contains_key(user)
                    && state.entries.len() >= state.config.capacity
                    && let Some(oldest) = state
                        .entries
                        .iter()
                        .min_by_key(|(_, e)| e.used)
                        .map(|(k, _)| k.clone())
                {
                    state.entries.remove(&oldest);
                }
                let expires = now.checked_add(state.config.ttl).unwrap_or(now);
                state.entries.insert(
                    user.clone(),
                    Entry {
                        devices: devices.clone(),
                        expires,
                        used: now,
                    },
                );
            }
        }
        Ok(groups)
    }
    pub fn invalidate_users(&self, users: &[String]) -> Result<(), Error> {
        let mut state = self.state.lock().map_err(|_| Error::Invalid)?;
        for user in users {
            state.entries.remove(user);
        }
        Ok(())
    }
    pub fn clear(&self) -> Result<(), Error> {
        self.state
            .lock()
            .map_err(|_| Error::Invalid)?
            .entries
            .clear();
        Ok(())
    }
}
