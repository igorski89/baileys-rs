//! Persistent PN/LID mappings with batched resolution and shared concurrent lookups.
use super::{Address, Error};
use crate::{
    auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    jid::{self, Jid},
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mapping {
    pub lid: String,
    pub pn: String,
}
/// Resolver inputs are normalized PN JIDs without device IDs. Implementations
/// may perform USync; results must associate each returned LID with that PN.
pub trait Resolver: Send + Sync {
    fn resolve(&self, pns: &[String]) -> Result<Vec<Mapping>, Error>;
}
impl<F: Fn(&[String]) -> Result<Vec<Mapping>, Error> + Send + Sync> Resolver for F {
    fn resolve(&self, pns: &[String]) -> Result<Vec<Mapping>, Error> {
        self(pns)
    }
}
#[derive(Debug, Clone)]
pub struct Config {
    pub ttl: Duration,
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
struct Entry {
    value: String,
    expires: Instant,
    used: Instant,
}
struct Cache {
    entries: HashMap<String, Entry>,
    config: Config,
}
impl Cache {
    fn get(&mut self, key: &str) -> Option<String> {
        let now = Instant::now();
        let entry = self.entries.get_mut(key)?;
        if entry.expires <= now {
            self.entries.remove(key);
            return None;
        }
        entry.expires = now.checked_add(self.config.ttl).unwrap_or(now);
        entry.used = now;
        Some(entry.value.clone())
    }
    fn set(&mut self, key: String, value: String) {
        if self.config.capacity == 0 || self.config.ttl.is_zero() {
            return;
        }
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.expires > now);
        if !self.entries.contains_key(&key)
            && self.entries.len() >= self.config.capacity
            && let Some(old) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone())
        {
            self.entries.remove(&old);
        }
        self.entries.insert(
            key,
            Entry {
                value,
                expires: now.checked_add(self.config.ttl).unwrap_or(now),
                used: now,
            },
        );
    }
}
type LookupKey = (bool, Vec<String>);
type Flights = HashMap<LookupKey, Arc<Flight>>;
type Outcome = Result<Vec<Mapping>, Arc<Error>>;
#[derive(Default)]
struct Flight {
    result: Mutex<Option<Outcome>>,
    ready: Condvar,
}
struct Inner {
    store: SqliteAuthStore,
    cache: Mutex<Cache>,
    resolver: Option<Arc<dyn Resolver>>,
    flights: Mutex<Flights>,
}
/// Synchronous operations can run on a blocking executor. Clones share caches
/// and flights. `close` clears cached mappings; persisted mappings remain.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}
struct Leader<'a> {
    inner: &'a Inner,
    key: LookupKey,
    flight: Arc<Flight>,
}
impl Drop for Leader<'_> {
    fn drop(&mut self) {
        // Wake followers even if a user-supplied resolver unwinds.
        if let Ok(mut result) = self.flight.result.lock() {
            if result.is_none() {
                *result = Some(Err(Arc::new(Error::ResolverAborted)));
            }
            self.flight.ready.notify_all();
        }
        if let Ok(mut flights) = self.inner.flights.lock() {
            flights.remove(&self.key);
        }
    }
}
impl Store {
    pub fn new(store: SqliteAuthStore) -> Self {
        Self::with_resolver(store, Config::default(), None)
    }
    pub fn with_resolver(
        store: SqliteAuthStore,
        config: Config,
        resolver: Option<Arc<dyn Resolver>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                cache: Mutex::new(Cache {
                    entries: HashMap::new(),
                    config,
                }),
                resolver,
                flights: Mutex::new(HashMap::new()),
            }),
        }
    }
    fn cached(&self, key: &str) -> Result<Option<String>, Error> {
        Ok(self
            .inner
            .cache
            .lock()
            .map_err(|_| crate::auth::Error::Poisoned)?
            .get(key))
    }
    fn cache_pair(&self, pn: &str, lid: &str) -> Result<(), Error> {
        let mut cache = self
            .inner
            .cache
            .lock()
            .map_err(|_| crate::auth::Error::Poisoned)?;
        cache.set(format!("pn:{pn}"), lid.into());
        cache.set(format!("lid:{lid}"), pn.into());
        Ok(())
    }
    pub fn close(&self) -> Result<(), Error> {
        self.inner
            .cache
            .lock()
            .map_err(|_| crate::auth::Error::Poisoned)?
            .entries
            .clear();
        Ok(())
    }
    pub fn store_mappings(&self, pairs: &[Mapping]) -> Result<(), Error> {
        let validated: Vec<_> = pairs
            .iter()
            .filter_map(|pair| {
                let pn = Jid::parse(&pair.pn)?;
                let lid = Jid::parse(&pair.lid)?;
                // Preserve upstream's accepted reversed-field case; it does not swap
                // the fields after validation. Hosted pairs are not accepted here.
                if !((pn.is_pn_user() && lid.is_lid_user())
                    || (pn.is_lid_user() && lid.is_pn_user()))
                {
                    return None;
                }
                Some((pn.user, lid.user))
            })
            .collect();
        let mut existing = HashMap::new();
        let mut missing = vec![];
        for (pn, _) in &validated {
            if let Some(lid) = self.cached(&format!("pn:{pn}"))? {
                existing.insert(pn.clone(), lid);
            } else if !missing.contains(pn) {
                missing.push(pn.clone());
            }
        }
        let stored = self.inner.store.get(KeyCategory::LidMapping, &missing)?;
        for (pn, value) in stored {
            if let KeyValue::LidMapping(lid) = value {
                self.cache_pair(&pn, &lid)?;
                existing.insert(pn, lid);
            }
        }
        let mut updates = IndexMap::new();
        for (pn, lid) in validated {
            if existing.get(&pn) != Some(&lid) {
                updates.insert(pn, lid);
            }
        }
        if updates.is_empty() {
            return Ok(());
        }
        let mutations: Vec<_> = updates
            .iter()
            .flat_map(|(pn, lid)| {
                [
                    KeyMutation::put(pn, KeyValue::LidMapping(lid.clone())),
                    KeyMutation::put(format!("{lid}_reverse"), KeyValue::LidMapping(pn.clone())),
                ]
            })
            .collect();
        self.inner.store.transaction(|tx| tx.set(&mutations))?;
        for (pn, lid) in updates {
            self.cache_pair(&pn, &lid)?;
        }
        Ok(())
    }
    pub fn get_lid_for_pn(&self, pn: &str) -> Result<Option<String>, Error> {
        Ok(self
            .get_lids_for_pns(&[pn.into()])?
            .into_iter()
            .next()
            .map(|p| p.lid))
    }
    pub fn get_pn_for_lid(&self, lid: &str) -> Result<Option<String>, Error> {
        Ok(self
            .get_pns_for_lids(&[lid.into()])?
            .into_iter()
            .next()
            .map(|p| p.pn))
    }
    pub fn get_lids_for_pns(&self, pns: &[String]) -> Result<Vec<Mapping>, Error> {
        self.lookup(pns, true)
    }
    pub fn get_pns_for_lids(&self, lids: &[String]) -> Result<Vec<Mapping>, Error> {
        self.lookup(lids, false)
    }
    fn lookup(&self, jids: &[String], forward: bool) -> Result<Vec<Mapping>, Error> {
        if jids.is_empty() {
            return Ok(vec![]);
        }
        let mut normalized = jids.to_vec();
        normalized.sort();
        normalized.dedup();
        let key = (forward, normalized);
        let (flight, leader) = {
            let mut flights = self
                .inner
                .flights
                .lock()
                .map_err(|_| crate::auth::Error::Poisoned)?;
            if let Some(flight) = flights.get(&key) {
                (flight.clone(), false)
            } else {
                let flight = Arc::new(Flight::default());
                flights.insert(key.clone(), flight.clone());
                (flight, true)
            }
        };
        if leader {
            let guard = Leader {
                inner: &self.inner,
                key,
                flight: flight.clone(),
            };
            let result = if forward {
                self.forward(jids)
            } else {
                self.reverse(jids)
            }
            .map_err(Arc::new);
            *flight
                .result
                .lock()
                .map_err(|_| crate::auth::Error::Poisoned)? = Some(result.clone());
            drop(guard);
            return result.map_err(Error::Shared);
        }
        let mut result = flight
            .result
            .lock()
            .map_err(|_| crate::auth::Error::Poisoned)?;
        while result.is_none() {
            result = flight
                .ready
                .wait(result)
                .map_err(|_| crate::auth::Error::Poisoned)?;
        }
        result
            .as_ref()
            .expect("completed flight")
            .clone()
            .map_err(Error::Shared)
    }
    fn forward(&self, pns: &[String]) -> Result<Vec<Mapping>, Error> {
        let mut results = IndexMap::new();
        let mut pending = vec![];
        let add = |results: &mut IndexMap<String, Mapping>, pn: &str, decoded: &Jid, lid: &str| {
            if !lid.is_empty() {
                let jid = jid::encode(
                    lid,
                    if decoded.is_hosted_pn_user() {
                        "hosted.lid"
                    } else {
                        "lid"
                    },
                    decoded.device,
                    None,
                );
                results.insert(
                    pn.into(),
                    Mapping {
                        lid: jid,
                        pn: pn.into(),
                    },
                );
            }
        };
        for pn in pns {
            let Some(decoded) = Jid::parse(pn).filter(|j| j.is_pn_user() || j.is_hosted_pn_user())
            else {
                continue;
            };
            if let Some(lid) = self.cached(&format!("pn:{}", decoded.user))? {
                add(&mut results, pn, &decoded, &lid);
            } else {
                pending.push((pn, decoded));
            }
        }
        let mut users: Vec<_> = pending.iter().map(|(_, j)| j.user.clone()).collect();
        users.sort();
        users.dedup();
        let stored = self.inner.store.get(KeyCategory::LidMapping, &users)?;
        for (pn, value) in &stored {
            if let KeyValue::LidMapping(lid) = value {
                self.cache_pair(pn, lid)?;
            }
        }
        let mut fetch: IndexMap<String, Vec<u16>> = IndexMap::new();
        for (pn, decoded) in pending {
            // With a disabled cache, consume the DB result directly.
            let value = self.cached(&format!("pn:{}", decoded.user))?.or_else(|| {
                match stored.get(&decoded.user) {
                    Some(KeyValue::LidMapping(lid)) => Some(lid.clone()),
                    _ => None,
                }
            });
            if let Some(lid) = value {
                add(&mut results, pn, &decoded, &lid);
            } else {
                let normalized = if decoded.is_hosted_pn_user() {
                    jid::encode(&decoded.user, "s.whatsapp.net", None, None)
                } else {
                    decoded.normalized()
                };
                fetch
                    .entry(normalized)
                    .or_default()
                    .push(decoded.device.unwrap_or(0));
            }
        }
        if !fetch.is_empty()
            && let Some(resolver) = &self.inner.resolver
        {
            let resolved = resolver.resolve(&fetch.keys().cloned().collect::<Vec<_>>())?;
            self.store_mappings(&resolved)?;
            for pair in resolved {
                let (Some(pn), Some(lid), Some(devices)) = (
                    Jid::parse(&pair.pn),
                    Jid::parse(&pair.lid),
                    fetch.get(&pair.pn),
                ) else {
                    continue;
                };
                if pn.user.is_empty() || lid.user.is_empty() {
                    continue;
                }
                for device in devices {
                    let hosted = *device == 99;
                    let pn = jid::encode(
                        &pn.user,
                        if hosted { "hosted" } else { "s.whatsapp.net" },
                        Some(*device),
                        None,
                    );
                    let lid = jid::encode(
                        &lid.user,
                        if hosted { "hosted.lid" } else { "lid" },
                        Some(*device),
                        None,
                    );
                    results.insert(pn.clone(), Mapping { pn, lid });
                }
            }
        }
        Ok(results.into_values().collect())
    }
    fn reverse(&self, lids: &[String]) -> Result<Vec<Mapping>, Error> {
        let mut results = IndexMap::new();
        let mut pending = vec![];
        let add = |results: &mut IndexMap<String, Mapping>, lid: &str, decoded: &Jid, pn: &str| {
            if !pn.is_empty() {
                results.insert(
                    lid.into(),
                    Mapping {
                        lid: lid.into(),
                        pn: format!("{pn}:{}@s.whatsapp.net", decoded.device.unwrap_or(0)),
                    },
                );
            }
        };
        // Upstream currently excludes hosted.lid from reverse lookup.
        for lid in lids {
            let Some(decoded) = Jid::parse(lid).filter(Jid::is_lid_user) else {
                continue;
            };
            if let Some(pn) = self.cached(&format!("lid:{}", decoded.user))? {
                add(&mut results, lid, &decoded, &pn);
            } else {
                pending.push((lid, decoded));
            }
        }
        let mut ids: Vec<_> = pending
            .iter()
            .map(|(_, j)| format!("{}_reverse", j.user))
            .collect();
        ids.sort();
        ids.dedup();
        let stored = self.inner.store.get(KeyCategory::LidMapping, &ids)?;
        for (lid, decoded) in pending {
            let pn = self.cached(&format!("lid:{}", decoded.user))?.or_else(|| {
                match stored.get(&format!("{}_reverse", decoded.user)) {
                    Some(KeyValue::LidMapping(pn)) => Some(pn.clone()),
                    _ => None,
                }
            });
            if let Some(pn) = pn {
                self.cache_pair(&pn, &decoded.user)?;
                add(&mut results, lid, &decoded, &pn);
            }
        }
        Ok(results.into_values().collect())
    }
    pub fn resolve_address(&self, address: &Address) -> Result<Address, Error> {
        let (user, domain) = match address.user.rsplit_once('_') {
            Some((user, domain)) => (user, domain.parse::<u8>().map_err(|_| Error::Address)?),
            None => (address.user.as_str(), 0),
        };
        if matches!(domain, 1 | 129) {
            return Ok(address.clone());
        }
        let pn = jid::encode(
            user,
            if domain == 128 {
                "hosted"
            } else {
                "s.whatsapp.net"
            },
            Some(address.device),
            None,
        );
        match self.get_lid_for_pn(&pn)? {
            Some(lid) => Address::from_jid(&lid),
            None => Ok(address.clone()),
        }
    }
}
