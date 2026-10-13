use super::{
    BlobDownloader, CollectionRequest, CollectionResponse, Error, KeyProvider, MutationMap,
    decode_patches_with_downloads, decode_snapshot, new_lthash_state,
};
use crate::auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, LtHashState};
use futures_util::future::BoxFuture;
use indexmap::{IndexMap, IndexSet};
use std::{collections::HashMap, sync::Arc, time::Duration};

pub const ALL_COLLECTIONS: [&str; 5] = [
    "critical_block",
    "critical_unblock_low",
    "regular_high",
    "regular_low",
    "regular",
];
pub const MAX_SYNC_ATTEMPTS: usize = 2;

type Query = dyn Fn(
        Vec<CollectionRequest>,
    ) -> BoxFuture<'static, Result<IndexMap<String, CollectionResponse>, Error>>
    + Send
    + Sync;
#[derive(Clone)]
pub struct CollectionTransport(Arc<Query>);
impl CollectionTransport {
    pub fn new<F, Fut>(query: F) -> Self
    where
        F: Fn(Vec<CollectionRequest>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<IndexMap<String, CollectionResponse>, Error>>
            + Send
            + 'static,
    {
        Self(Arc::new(move |requests| Box::pin(query(requests))))
    }
}

#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub verify_snapshot: bool,
    pub verify_patch: bool,
    pub max_commit_attempts: usize,
    pub commit_retry_delay: Duration,
}
impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            verify_snapshot: false,
            verify_patch: false,
            max_commit_attempts: 10,
            commit_retry_delay: Duration::from_secs(3),
        }
    }
}
pub struct SyncResult {
    pub mutations: MutationMap,
    pub states: IndexMap<String, LtHashState>,
    pub blocked: Vec<String>,
    pub gave_up: Vec<String>,
    pub attempts: IndexMap<String, usize>,
}
/// Serialized app-state synchronization with a transaction overlay. Database
/// writes commit atomically only after all collection requests have completed.
/// Applying returned sync actions is a separate stage.
pub struct Synchronizer {
    store: Arc<dyn KeyStore>,
    keys: KeyProvider,
    downloader: BlobDownloader,
    transport: CollectionTransport,
    blocked: tokio::sync::Mutex<IndexSet<String>>,
    run_lock: tokio::sync::Mutex<()>,
}
impl Synchronizer {
    pub fn new(
        store: Arc<dyn KeyStore>,
        keys: KeyProvider,
        downloader: BlobDownloader,
        transport: CollectionTransport,
    ) -> Self {
        Self {
            store,
            keys,
            downloader,
            transport,
            blocked: tokio::sync::Mutex::new(IndexSet::new()),
            run_lock: tokio::sync::Mutex::new(()),
        }
    }
    pub async fn blocked_collections(&self) -> Vec<String> {
        self.blocked.lock().await.iter().cloned().collect()
    }
    /// Remove parked collections for a new-key retry, matching upstream's
    /// clear-before-resync behavior. The retry uses a fresh per-call key cache.
    pub async fn take_blocked_collections(&self) -> Vec<String> {
        self.blocked.lock().await.drain(..).collect()
    }
    pub async fn resync(
        &self,
        collections: &[String],
        options: &SyncOptions,
    ) -> Result<SyncResult, Error> {
        self.resync_with(collections, options, true).await
    }
    /// Synchronize without persisting the new versions; they are returned in
    /// `SyncResult::states` for a caller whose own write decides the outcome,
    /// as a resync nested in an upstream transaction.
    pub async fn resync_staged(
        &self,
        collections: &[String],
        options: &SyncOptions,
    ) -> Result<SyncResult, Error> {
        self.resync_with(collections, options, false).await
    }
    async fn resync_with(
        &self,
        collections: &[String],
        options: &SyncOptions,
        commit: bool,
    ) -> Result<SyncResult, Error> {
        let _serial = self.run_lock.lock().await;
        let mut remaining: IndexSet<String> = collections.iter().cloned().collect();
        let mut cached_states: HashMap<String, Option<LtHashState>> = HashMap::new();
        let mut initial_versions = HashMap::new();
        let mut staged = IndexMap::new();
        let mut mutations = MutationMap::new();
        let mut attempts = IndexMap::new();
        let mut gave_up = Vec::new();
        let mut force_snapshot = IndexSet::new();
        let key_cache = Arc::new(tokio::sync::Mutex::new(HashMap::<
            String,
            Option<crate::proto::message::AppStateSyncKeyData>,
        >::new()));
        let underlying = self.keys.clone();
        let keys = KeyProvider::new(move |id| {
            let cache = key_cache.clone();
            let provider = underlying.clone();
            async move {
                let mut cache = cache.lock().await;
                if let Some(value) = cache.get(&id) {
                    return Ok(value.clone());
                }
                let value = provider.get(id.clone()).await?;
                cache.insert(id, value.clone());
                Ok(value)
            }
        });
        while !remaining.is_empty() {
            let mut requests = Vec::new();
            let mut states = HashMap::new();
            for name in &remaining {
                if !cached_states.contains_key(name) {
                    let store = self.store.clone();
                    let id = name.clone();
                    let value = tokio::task::spawn_blocking(move || {
                        Ok::<_, crate::auth::Error>(
                            match store
                                .get(KeyCategory::AppStateSyncVersion, std::slice::from_ref(&id))?
                                .remove(&id)
                            {
                                Some(KeyValue::AppStateSyncVersion(state)) => Some(state),
                                _ => None,
                            },
                        )
                    })
                    .await
                    .map_err(|_| Error::Task)??;
                    cached_states.insert(name.clone(), value);
                }
                let state = if let Some(state) = cached_states[name].as_ref() {
                    initial_versions
                        .entry(name.clone())
                        .or_insert(state.version);
                    state.clone()
                } else {
                    new_lthash_state()
                };
                requests.push(CollectionRequest {
                    name: name.clone(),
                    version: state.version,
                    force_snapshot: force_snapshot.shift_remove(name),
                });
                states.insert(name.clone(), state);
            }
            // Query and snapshot-extraction errors abort the whole overlay,
            // rather than entering individual collection retry handling.
            let response = (self.transport.0)(requests).await?;
            for (name, mut collection) in response {
                let decoded = async {
                    if let Some(snapshot) = collection.snapshot {
                        let decoded = decode_snapshot(
                            &snapshot,
                            &name,
                            &keys,
                            initial_versions.get(&name).copied(),
                            options.verify_snapshot,
                        )
                        .await?;
                        states.insert(name.clone(), decoded.state.clone());
                        mutations.extend(decoded.mutations);
                        cached_states.insert(name.clone(), Some(decoded.state.clone()));
                        staged.insert(name.clone(), decoded.state);
                    }
                    if !collection.patches.is_empty() {
                        let initial = states
                            .get(&name)
                            .ok_or(Error::Input("unrequested collection state"))?;
                        let decoded = decode_patches_with_downloads(
                            &mut collection.patches,
                            &name,
                            initial,
                            &keys,
                            initial_versions.get(&name).copied(),
                            options.verify_patch,
                            &self.downloader,
                        )
                        .await?;
                        // Source deliberately does not update states[name] here;
                        // the next request reloads the staged version instead.
                        initial_versions.insert(name.clone(), decoded.state.version);
                        cached_states.insert(name.clone(), Some(decoded.state.clone()));
                        staged.insert(name.clone(), decoded.state);
                        mutations.extend(decoded.mutations);
                    }
                    Ok::<_, Error>(())
                }
                .await;
                match decoded {
                    Ok(()) => {
                        if !collection.has_more_patches {
                            remaining.shift_remove(&name);
                        }
                    }
                    Err(error) => {
                        // Upstream's failure handler reads states[name].version;
                        // a failed unsolicited collection therefore aborts the
                        // transaction rather than retrying an unknown name.
                        if !states.contains_key(&name) {
                            return Err(Error::Input("unrequested collection state"));
                        }
                        let attempt = attempts.entry(name.clone()).or_insert(0);
                        *attempt += 1;
                        if error.is_missing_key() && *attempt >= MAX_SYNC_ATTEMPTS {
                            self.blocked.lock().await.insert(name.clone());
                            remaining.shift_remove(&name);
                        } else if error.is_missing_key() {
                            force_snapshot.insert(name.clone());
                        } else if *attempt >= MAX_SYNC_ATTEMPTS || matches!(error, Error::Input(_))
                        {
                            gave_up.push(name.clone());
                            remaining.shift_remove(&name);
                        } else {
                            force_snapshot.insert(name.clone());
                        }
                        tracing::warn!(
                            name,
                            attempt = *attempt,
                            ?error,
                            "app-state collection synchronization failed"
                        );
                    }
                }
            }
        }
        if commit && !staged.is_empty() {
            let writes: Vec<_> = staged
                .iter()
                .map(|(name, state)| {
                    KeyMutation::put(name, KeyValue::AppStateSyncVersion(state.clone()))
                })
                .collect();
            let mut attempt = 0;
            loop {
                attempt += 1;
                let store = self.store.clone();
                let writes = writes.clone();
                match tokio::task::spawn_blocking(move || store.set(&writes))
                    .await
                    .map_err(|_| Error::Task)?
                {
                    Ok(()) => break,
                    Err(error) if attempt >= options.max_commit_attempts.max(1) => {
                        return Err(error.into());
                    }
                    Err(error) => {
                        tracing::warn!(attempt, ?error, "retrying app-state transaction commit");
                        tokio::time::sleep(options.commit_retry_delay).await;
                    }
                }
            }
        }
        Ok(SyncResult {
            mutations,
            states: staged,
            blocked: self.blocked_collections().await,
            gave_up,
            attempts,
        })
    }
}
