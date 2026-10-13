//! Caller-replaceable key/value caches, the typed form of upstream's CacheStore.
use futures_util::future::BoxFuture;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// A cache the socket reads and writes through. A caller-supplied store applies
/// its own expiry and is never flushed or closed by the socket.
pub trait CacheStore<T>: Send + Sync {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Option<T>>;
    fn set<'a>(&'a self, key: &'a str, value: T) -> BoxFuture<'a, ()>;
    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, ()>;
    fn flush_all(&self) -> BoxFuture<'_, ()>;
}
pub struct Cache<T>(Arc<dyn CacheStore<T>>);
impl<T> Cache<T> {
    pub fn new(store: impl CacheStore<T> + 'static) -> Self {
        Self(Arc::new(store))
    }
    pub fn from_arc(store: Arc<dyn CacheStore<T>>) -> Self {
        Self(store)
    }
    // The store's boxed futures are returned as they are, so large values are
    // not copied into the caller's own future.
    pub fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Option<T>> {
        self.0.get(key)
    }
    pub fn set<'a>(&'a self, key: &'a str, value: T) -> BoxFuture<'a, ()> {
        self.0.set(key, value)
    }
    pub fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, ()> {
        self.0.del(key)
    }
    pub fn flush_all(&self) -> BoxFuture<'_, ()> {
        self.0.flush_all()
    }
}
impl<T> Clone for Cache<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> std::fmt::Debug for Cache<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish_non_exhaustive()
    }
}
/// The built-in store: entries expire a fixed time after they were last set.
pub struct MemoryStore<T> {
    entries: Mutex<HashMap<String, (T, Instant)>>,
    ttl: Duration,
}
impl<T> MemoryStore<T> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
        }
    }
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}
impl<T: Clone + Send> CacheStore<T> for MemoryStore<T> {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Option<T>> {
        Box::pin(async move {
            let mut entries = self.entries.lock().ok()?;
            let now = Instant::now();
            entries.retain(|_, (_, expires)| *expires > now);
            entries.get(key).map(|(value, _)| value.clone())
        })
    }
    fn set<'a>(&'a self, key: &'a str, value: T) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Ok(mut entries) = self.entries.lock() {
                entries.insert(key.into(), (value, Instant::now() + self.ttl));
            }
        })
    }
    fn del<'a>(&'a self, key: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Ok(mut entries) = self.entries.lock() {
                entries.remove(key);
            }
        })
    }
    fn flush_all(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.clear() })
    }
}
