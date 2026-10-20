use super::{Credentials, Error};
use crate::{crypto::KeyPair, proto};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use zeroize::Zeroizing;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KeyCategory {
    PreKey,
    Session,
    SenderKey,
    SenderKeyMemory,
    AppStateSyncKey,
    AppStateSyncVersion,
    LidMapping,
    DeviceList,
    Tctoken,
    IdentityKey,
}
impl KeyCategory {
    pub const ALL: [Self; 10] = [
        Self::PreKey,
        Self::Session,
        Self::SenderKey,
        Self::SenderKeyMemory,
        Self::AppStateSyncKey,
        Self::AppStateSyncVersion,
        Self::LidMapping,
        Self::DeviceList,
        Self::Tctoken,
        Self::IdentityKey,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreKey => "pre-key",
            Self::Session => "session",
            Self::SenderKey => "sender-key",
            Self::SenderKeyMemory => "sender-key-memory",
            Self::AppStateSyncKey => "app-state-sync-key",
            Self::AppStateSyncVersion => "app-state-sync-version",
            Self::LidMapping => "lid-mapping",
            Self::DeviceList => "device-list",
            Self::Tctoken => "tctoken",
            Self::IdentityKey => "identity-key",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TokenTimestamp {
    Text(String),
    Number(serde_json::Number),
}
impl From<String> for TokenTimestamp {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}
impl From<&str> for TokenTimestamp {
    fn from(value: &str) -> Self {
        Self::Text(value.into())
    }
}
impl From<u64> for TokenTimestamp {
    fn from(value: u64) -> Self {
        Self::Number(value.into())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LtHashState {
    pub version: u64,
    #[serde(with = "crate::serialization::bytes")]
    pub hash: Vec<u8>,
    pub index_value_map: BTreeMap<String, ValueMac>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValueMac {
    #[serde(with = "crate::serialization::bytes")]
    pub value_mac: Vec<u8>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TcToken {
    #[serde(with = "crate::serialization::bytes")]
    pub token: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<TokenTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_timestamp: Option<u64>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "category", content = "value", rename_all = "kebab-case")]
pub enum KeyValue {
    PreKey(KeyPair),
    // libsignal-node actually serializes SessionRecord to a JSON object, despite
    // the upstream public type declaring Uint8Array. Preserve its opaque shape.
    Session(serde_json::Value),
    SenderKey(#[serde(with = "crate::serialization::bytes")] Vec<u8>),
    SenderKeyMemory(BTreeMap<String, bool>),
    AppStateSyncKey(proto::message::AppStateSyncKeyData),
    AppStateSyncVersion(LtHashState),
    LidMapping(String),
    DeviceList(Vec<String>),
    Tctoken(TcToken),
    IdentityKey(#[serde(with = "crate::serialization::bytes")] Vec<u8>),
}
impl std::fmt::Debug for KeyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyValue")
            .field("category", &self.category())
            .finish_non_exhaustive()
    }
}
impl KeyValue {
    pub fn category(&self) -> KeyCategory {
        match self {
            Self::PreKey(_) => KeyCategory::PreKey,
            Self::Session(_) => KeyCategory::Session,
            Self::SenderKey(_) => KeyCategory::SenderKey,
            Self::SenderKeyMemory(_) => KeyCategory::SenderKeyMemory,
            Self::AppStateSyncKey(_) => KeyCategory::AppStateSyncKey,
            Self::AppStateSyncVersion(_) => KeyCategory::AppStateSyncVersion,
            Self::LidMapping(_) => KeyCategory::LidMapping,
            Self::DeviceList(_) => KeyCategory::DeviceList,
            Self::Tctoken(_) => KeyCategory::Tctoken,
            Self::IdentityKey(_) => KeyCategory::IdentityKey,
        }
    }
    pub fn from_baileys_json(
        category: KeyCategory,
        value: serde_json::Value,
    ) -> Result<Self, Error> {
        let tagged = serde_json::json!({"category":category.as_str(),"value":value});
        Ok(serde_json::from_value(tagged)?)
    }
    pub fn to_baileys_json(&self) -> Result<serde_json::Value, Error> {
        Ok(serde_json::to_value(self)?["value"].take())
    }
}
#[derive(Clone, Debug)]
pub struct KeyMutation {
    pub category: KeyCategory,
    pub id: String,
    pub value: Option<KeyValue>,
}
impl KeyMutation {
    pub fn put(id: impl Into<String>, value: KeyValue) -> Self {
        Self {
            category: value.category(),
            id: id.into(),
            value: Some(value),
        }
    }
    pub fn delete(category: KeyCategory, id: impl Into<String>) -> Self {
        Self {
            category,
            id: id.into(),
            value: None,
        }
    }
}
/// Synchronous storage contract; async clients can run database work on a blocking
/// executor. Missing keys are omitted and an entire mutation batch must be atomic.
pub trait KeyStore: Send + Sync {
    fn get(
        &self,
        category: KeyCategory,
        ids: &[String],
    ) -> Result<BTreeMap<String, KeyValue>, Error>;
    fn set(&self, mutations: &[KeyMutation]) -> Result<(), Error>;
    fn clear(&self) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct SqliteAuthStore {
    connection: Arc<Mutex<Connection>>,
    path: Option<PathBuf>,
}
impl SqliteAuthStore {
    /// Create auth.sqlite3 under a private directory, or resume existing state.
    /// A corrupt credential record is an error, never an implicit identity reset.
    pub fn open(folder: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_file(folder.as_ref().join("auth.sqlite3"))
    }
    /// Create or resume the database at `path`, creating a missing parent
    /// directory privately as `open` does.
    pub fn open_file(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        if let Some(folder) = path.parent().filter(|f| !f.as_os_str().is_empty())
            && !folder.exists()
        {
            let mut builder = fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(folder)?;
        }
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(file) => drop(file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if !fs::symlink_metadata(path)?.file_type().is_file() {
                    return Err(Error::FileName);
                }
            }
            Err(e) => return Err(e.into()),
        }
        Self::initialize(Connection::open(path)?, Some(path.to_owned()))
    }
    pub fn in_memory() -> Result<Self, Error> {
        Self::initialize(Connection::open_in_memory()?, None)
    }
    fn initialize(mut connection: Connection, path: Option<PathBuf>) -> Result<Self, Error> {
        connection.busy_timeout(Duration::from_secs(30))?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        let version: u32 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version > 1 {
            return Err(Error::SchemaVersion);
        }
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS credentials (id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS signal_keys (category TEXT NOT NULL,id TEXT NOT NULL,data BLOB NOT NULL,PRIMARY KEY(category,id)); PRAGMA user_version=1;")?;
        let existing: Option<Vec<u8>> = tx
            .query_row("SELECT data FROM credentials WHERE id=1", [], |r| r.get(0))
            .optional()?;
        if let Some(data) = existing {
            let credentials: Credentials = serde_json::from_slice(&Zeroizing::new(data))?;
            credentials.validate()?;
        } else {
            let data = Zeroizing::new(serde_json::to_vec(&Credentials::generate()?)?);
            tx.execute(
                "INSERT INTO credentials (id,data) VALUES (1,?1)",
                [data.as_slice()],
            )?;
        }
        tx.commit()?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            path,
        })
    }
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
    pub fn load_credentials(&self) -> Result<Credentials, Error> {
        let connection = self.connection.lock().map_err(|_| Error::Poisoned)?;
        read_credentials(&connection)
    }
    pub fn save_credentials(&self, credentials: &Credentials) -> Result<(), Error> {
        self.transaction(|tx| tx.save_credentials(credentials))
    }
    pub fn transaction<T>(
        &self,
        work: impl FnOnce(&mut AuthTransaction<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.transaction_with_error(work)
    }
    /// Run application work atomically while preserving its own typed errors.
    pub fn transaction_with_error<T, E: From<Error>>(
        &self,
        work: impl FnOnce(&mut AuthTransaction<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| E::from(Error::Poisoned))?;
        let sql = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::from)?;
        let mut tx = AuthTransaction {
            sql,
            cache: BTreeMap::new(),
        };
        let result = work(&mut tx)?;
        tx.sql.commit().map_err(Error::from)?;
        Ok(result)
    }
    /// Import a snapshot from the TypeScript multi-file adapter. IDs containing
    /// slash/colon cannot be recovered from its lossy filenames; an explicit map
    /// resolves those names. All imported records and credentials commit together.
    pub fn import_baileys_folder(
        &self,
        folder: impl AsRef<Path>,
        id_map: &BTreeMap<String, String>,
    ) -> Result<(), Error> {
        let folder = folder.as_ref();
        let data = Zeroizing::new(fs::read(folder.join("creds.json"))?);
        let credentials: Credentials = serde_json::from_slice(&data)?;
        credentials.validate()?;
        let mut mutations = Vec::new();
        for entry in fs::read_dir(folder)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::FileName)?;
            if name == "creds.json" || !name.ends_with(".json") {
                continue;
            }
            if !entry.file_type()?.is_file() {
                return Err(Error::FileName);
            }
            let Some(category) = KeyCategory::ALL
                .into_iter()
                .filter(|c| name.starts_with(&format!("{}-", c.as_str())))
                .max_by_key(|c| c.as_str().len())
            else {
                return Err(Error::FileName);
            };
            let id = &name[category.as_str().len() + 1..name.len() - 5];
            let id = if let Some(mapped) = id_map.get(&name) {
                mapped.as_str()
            } else {
                if id.contains("__") || id.contains('-') {
                    return Err(Error::FileName);
                }
                id
            };
            let data = Zeroizing::new(fs::read(entry.path())?);
            let value = KeyValue::from_baileys_json(category, serde_json::from_slice(&data)?)?;
            mutations.push(KeyMutation::put(id, value));
        }
        self.transaction(|tx| {
            tx.sql.execute("DELETE FROM signal_keys", [])?;
            tx.set(&mutations)?;
            tx.save_credentials(&credentials)
        })
    }
    /// Export into a new directory. Existing destinations are rejected to avoid
    /// mixing snapshots; a filename collision is detected before creating it.
    pub fn export_baileys_folder(
        &self,
        folder: impl AsRef<Path>,
    ) -> Result<BTreeMap<String, String>, Error> {
        let (credentials, keys) = self.transaction(|tx| {
            let credentials = Zeroizing::new(serde_json::to_vec(&tx.load_credentials()?)?);
            let mut stmt = tx
                .sql
                .prepare("SELECT category,id,data FROM signal_keys ORDER BY category,id")?;
            let rows = stmt.query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })?;
            let mut keys = Vec::new();
            for row in rows {
                let (category, id, data) = row?;
                let value: KeyValue = serde_json::from_slice(&Zeroizing::new(data))?;
                if value.category().as_str() != category {
                    return Err(Error::CategoryMismatch);
                }
                keys.push((category, id, value));
            }
            Ok((credentials, keys))
        })?;
        let mut id_map = BTreeMap::new();
        let mut files = Vec::new();
        for (category, id, value) in keys {
            let name = format!("{category}-{id}.json")
                .replace('/', "__")
                .replace(':', "-");
            if name.contains('\\') || id_map.insert(name.clone(), id).is_some() {
                return Err(Error::FileName);
            }
            files.push((
                name,
                Zeroizing::new(serde_json::to_vec(&value.to_baileys_json()?)?),
            ));
        }
        let folder = folder.as_ref();
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(folder)?;
        private_write(&folder.join("creds.json"), &credentials)?;
        for (name, data) in files {
            private_write(&folder.join(name), &data)?;
        }
        Ok(id_map)
    }
}
fn private_write(path: &Path, data: &[u8]) -> Result<(), Error> {
    use std::io::Write;
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}
fn read_credentials(connection: &Connection) -> Result<Credentials, Error> {
    let data: Vec<u8> =
        connection.query_row("SELECT data FROM credentials WHERE id=1", [], |r| r.get(0))?;
    let credentials: Credentials = serde_json::from_slice(&Zeroizing::new(data))?;
    credentials.validate()?;
    Ok(credentials)
}
fn read_key(
    connection: &Connection,
    category: KeyCategory,
    id: &str,
) -> Result<Option<KeyValue>, Error> {
    let data: Option<Vec<u8>> = connection
        .query_row(
            "SELECT data FROM signal_keys WHERE category=?1 AND id=?2",
            params![category.as_str(), id],
            |r| r.get(0),
        )
        .optional()?;
    data.map(|data| {
        let value: KeyValue = serde_json::from_slice(&Zeroizing::new(data))?;
        if value.category() != category {
            return Err(Error::CategoryMismatch);
        }
        Ok(value)
    })
    .transpose()
}
fn write_key(connection: &Connection, mutation: &KeyMutation) -> Result<(), Error> {
    if let Some(value) = &mutation.value {
        if value.category() != mutation.category {
            return Err(Error::CategoryMismatch);
        }
        let data = Zeroizing::new(serde_json::to_vec(value)?);
        connection.execute("INSERT INTO signal_keys (category,id,data) VALUES (?1,?2,?3) ON CONFLICT(category,id) DO UPDATE SET data=excluded.data",params![mutation.category.as_str(),mutation.id,data.as_slice()])?;
    } else {
        connection.execute(
            "DELETE FROM signal_keys WHERE category=?1 AND id=?2",
            params![mutation.category.as_str(), mutation.id],
        )?;
    }
    Ok(())
}

pub struct AuthTransaction<'a> {
    sql: rusqlite::Transaction<'a>,
    cache: BTreeMap<(KeyCategory, String), Option<KeyValue>>,
}
impl AuthTransaction<'_> {
    pub fn load_credentials(&self) -> Result<Credentials, Error> {
        read_credentials(&self.sql)
    }
    pub fn save_credentials(&mut self, credentials: &Credentials) -> Result<(), Error> {
        credentials.validate()?;
        let data = Zeroizing::new(serde_json::to_vec(credentials)?);
        self.sql.execute(
            "UPDATE credentials SET data=?1 WHERE id=1",
            [data.as_slice()],
        )?;
        Ok(())
    }
    pub fn get(
        &mut self,
        category: KeyCategory,
        ids: &[String],
    ) -> Result<BTreeMap<String, KeyValue>, Error> {
        let mut values = BTreeMap::new();
        for id in ids {
            let address = (category, id.clone());
            if !self.cache.contains_key(&address) {
                self.cache
                    .insert(address.clone(), read_key(&self.sql, category, id)?);
            }
            if let Some(Some(value)) = self.cache.get(&address) {
                values.insert(id.clone(), value.clone());
            }
        }
        Ok(values)
    }
    pub fn set(&mut self, mutations: &[KeyMutation]) -> Result<(), Error> {
        for mutation in mutations {
            let address = (mutation.category, mutation.id.clone());
            if mutation.category == KeyCategory::PreKey
                && mutation.value.is_none()
                && !matches!(self.cache.get(&address), Some(Some(_)))
            {
                continue;
            }
            write_key(&self.sql, mutation)?;
            self.cache.insert(address, mutation.value.clone());
        }
        Ok(())
    }
    /// Nested work shares this transaction, matching upstream's reused context.
    pub fn transaction<T>(
        &mut self,
        work: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        work(self)
    }
    /// Advance the upload cursor only after the server acknowledges the batch.
    pub fn mark_prekeys_uploaded(&mut self, last_id: u32) -> Result<(), Error> {
        let mut credentials = self.load_credentials()?;
        let next = last_id.checked_add(1).ok_or(Error::PreKeyRange)?;
        if next > credentials.next_pre_key_id {
            return Err(Error::PreKeyRange);
        }
        credentials.first_unuploaded_pre_key_id = credentials.first_unuploaded_pre_key_id.max(next);
        self.save_credentials(&credentials)
    }
    /// Persist newly allocated prekeys and advance their allocation counter together.
    pub fn allocate_prekeys(&mut self, count: u32) -> Result<super::PreKeyBatch, Error> {
        let mut credentials = self.load_credentials()?;
        let batch = credentials.generate_or_get_prekeys(count)?;
        let mutations: Vec<_> = batch
            .new_pre_keys
            .iter()
            .map(|(id, pair)| KeyMutation::put(id.to_string(), KeyValue::PreKey(pair.clone())))
            .collect();
        self.set(&mutations)?;
        credentials.next_pre_key_id = credentials.next_pre_key_id.max(
            batch
                .last_pre_key_id
                .checked_add(1)
                .ok_or(Error::PreKeyRange)?,
        );
        self.save_credentials(&credentials)?;
        Ok(batch)
    }
}
impl KeyStore for SqliteAuthStore {
    fn get(
        &self,
        category: KeyCategory,
        ids: &[String],
    ) -> Result<BTreeMap<String, KeyValue>, Error> {
        let connection = self.connection.lock().map_err(|_| Error::Poisoned)?;
        let mut result = BTreeMap::new();
        for id in ids {
            if let Some(value) = read_key(&connection, category, id)? {
                result.insert(id.clone(), value);
            }
        }
        Ok(result)
    }
    fn set(&self, mutations: &[KeyMutation]) -> Result<(), Error> {
        self.transaction(|tx| {
            let ids: Vec<_> = mutations
                .iter()
                .filter(|m| m.category == KeyCategory::PreKey && m.value.is_none())
                .map(|m| m.id.clone())
                .collect();
            tx.get(KeyCategory::PreKey, &ids)?;
            tx.set(mutations)
        })
    }
    fn clear(&self) -> Result<(), Error> {
        self.transaction(|tx| {
            tx.sql.execute("DELETE FROM signal_keys", [])?;
            Ok(())
        })
    }
}

struct CacheEntry {
    value: Option<KeyValue>,
    expires: Instant,
}
/// Bounded TTL cache for an exclusively-owned key store. External writers require
/// explicit invalidation; backend failures never publish uncommitted cache data.
pub struct CachedKeyStore<S> {
    store: S,
    cache: Mutex<BTreeMap<(KeyCategory, String), CacheEntry>>,
    ttl: Duration,
    capacity: usize,
}
impl<S: KeyStore> CachedKeyStore<S> {
    pub fn new(store: S, ttl: Duration, capacity: usize) -> Self {
        Self {
            store,
            cache: Mutex::new(BTreeMap::new()),
            ttl,
            capacity,
        }
    }
    pub fn invalidate(&self) -> Result<(), Error> {
        self.cache.lock().map_err(|_| Error::Poisoned)?.clear();
        Ok(())
    }
    fn trim(&self, cache: &mut BTreeMap<(KeyCategory, String), CacheEntry>) {
        cache.retain(|_, entry| entry.expires > Instant::now());
        while cache.len() > self.capacity {
            let key = cache
                .iter()
                .min_by_key(|(_, e)| e.expires)
                .map(|(key, _)| key.clone());
            if let Some(key) = key {
                cache.remove(&key);
            }
        }
    }
}
impl<S: KeyStore> KeyStore for CachedKeyStore<S> {
    fn get(
        &self,
        category: KeyCategory,
        ids: &[String],
    ) -> Result<BTreeMap<String, KeyValue>, Error> {
        let mut cache = self.cache.lock().map_err(|_| Error::Poisoned)?;
        self.trim(&mut cache);
        let missing: Vec<_> = ids
            .iter()
            .filter(|id| !cache.contains_key(&(category, (*id).clone())))
            .cloned()
            .collect();
        let fetched = self.store.get(category, &missing)?;
        let mut result = BTreeMap::new();
        for id in ids {
            let value = cache
                .get(&(category, id.clone()))
                .map(|e| e.value.clone())
                .unwrap_or_else(|| fetched.get(id).cloned());
            if let Some(value) = &value {
                result.insert(id.clone(), value.clone());
            }
            cache.entry((category, id.clone())).or_insert(CacheEntry {
                value,
                expires: Instant::now() + self.ttl,
            });
        }
        self.trim(&mut cache);
        Ok(result)
    }
    fn set(&self, mutations: &[KeyMutation]) -> Result<(), Error> {
        let mut cache = self.cache.lock().map_err(|_| Error::Poisoned)?;
        self.store.set(mutations)?;
        for m in mutations {
            cache.insert(
                (m.category, m.id.clone()),
                CacheEntry {
                    value: m.value.clone(),
                    expires: Instant::now() + self.ttl,
                },
            );
        }
        self.trim(&mut cache);
        Ok(())
    }
    fn clear(&self) -> Result<(), Error> {
        let mut cache = self.cache.lock().map_err(|_| Error::Poisoned)?;
        self.store.clear()?;
        cache.clear();
        Ok(())
    }
}
