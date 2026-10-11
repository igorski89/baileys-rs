//! Native X3DH/Double Ratchet engine, independently implemented from the Signal
//! specifications. Legacy JSON and wire parameters are verified with runtime
//! interoperability fixtures; no libsignal implementation source is translated.
use super::Error;
use crate::{
    crypto::{self, KeyPair},
    proto,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};
mod b64 {
    use super::*;
    pub fn serialize<S: serde::Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        STANDARD
            .decode(String::deserialize(d)?)
            .map_err(serde::de::Error::custom)
    }
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct Pair {
    #[serde(with = "b64")]
    pub_key: Vec<u8>,
    #[serde(with = "b64")]
    priv_key: Vec<u8>,
}
impl Pair {
    fn from(pair: &KeyPair) -> Self {
        Self {
            pub_key: crypto::signal_public_key(&pair.public)
                .expect("key length")
                .to_vec(),
            priv_key: pair.private.to_vec(),
        }
    }
    fn key(&self) -> Result<KeyPair, Error> {
        Ok(KeyPair::from_private(
            self.priv_key
                .as_slice()
                .try_into()
                .map_err(|_| Error::Invalid)?,
        ))
    }
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct Ratchet {
    ephemeral_key_pair: Pair,
    #[serde(with = "b64")]
    last_remote_ephemeral_key: Vec<u8>,
    previous_counter: u32,
    #[serde(with = "b64")]
    root_key: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct Index {
    #[serde(with = "b64")]
    base_key: Vec<u8>,
    base_key_type: u8,
    closed: i64,
    used: u64,
    created: u64,
    #[serde(with = "b64")]
    remote_identity_key: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
struct ChainKey {
    counter: i64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_b64"
    )]
    key: Option<Vec<u8>>,
}
mod optional_b64 {
    use super::*;
    pub fn serialize<S: serde::Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(v) => b64::serialize(v, s),
            None => s.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(d)?
            .map(|s| STANDARD.decode(s).map_err(serde::de::Error::custom))
            .transpose()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Chain {
    chain_key: ChainKey,
    chain_type: u8,
    #[serde(with = "message_keys")]
    message_keys: BTreeMap<u32, Vec<u8>>,
}
impl Drop for Chain {
    fn drop(&mut self) {
        self.chain_key.zeroize();
        for key in self.message_keys.values_mut() {
            key.zeroize();
        }
    }
}
mod message_keys {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        v: &BTreeMap<u32, Vec<u8>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        v.iter()
            .map(|(k, v)| (*k, STANDARD.encode(v)))
            .collect::<BTreeMap<_, _>>()
            .serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<u32, Vec<u8>>, D::Error> {
        BTreeMap::<u32, String>::deserialize(d)?
            .into_iter()
            .map(|(k, v)| {
                STANDARD
                    .decode(v)
                    .map(|v| (k, v))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct Pending {
    signed_key_id: u32,
    #[serde(with = "b64")]
    base_key: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pre_key_id: Option<u32>,
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
struct Session {
    registration_id: u32,
    current_ratchet: Ratchet,
    index_info: Index,
    #[serde(rename = "_chains")]
    #[zeroize(skip)]
    chains: BTreeMap<String, Chain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_pre_key: Option<Pending>,
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct Record {
    #[serde(rename = "_sessions")]
    #[zeroize(skip)]
    sessions: BTreeMap<String, Session>,
    version: String,
}
impl Default for Record {
    fn default() -> Self {
        Self {
            sessions: BTreeMap::new(),
            version: "v1".into(),
        }
    }
}
impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectSessionRecord")
            .field("sessions", &self.sessions.len())
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone)]
pub struct PublicPreKey {
    pub id: u32,
    pub public: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct Bundle {
    pub registration_id: u32,
    pub identity: Vec<u8>,
    pub signed: PublicPreKey,
    pub signature: Vec<u8>,
    pub pre_key: Option<PublicPreKey>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    PreKey,
    Signal,
}
pub struct Encrypted {
    pub kind: MessageType,
    pub ciphertext: Vec<u8>,
}
impl std::fmt::Debug for Encrypted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectCiphertext")
            .field("kind", &self.kind)
            .field("bytes", &self.ciphertext.len())
            .finish()
    }
}
const MAX_WIRE: usize = 16 * 1024 * 1024;
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn public(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    Ok(crypto::signal_public_key(bytes)?.to_vec())
}
fn kdf(input: &[u8], salt: &[u8], info: &[u8], length: usize) -> Result<Zeroizing<Vec<u8>>, Error> {
    Ok(Zeroizing::new(crypto::hkdf(
        input,
        length,
        Some(salt),
        info,
    )?))
}
fn dh(pair: &KeyPair, remote: &[u8]) -> Result<Zeroizing<[u8; 32]>, Error> {
    Ok(Zeroizing::new(pair.shared_key(remote)?))
}
fn root_chain(root: &[u8], pair: &KeyPair, remote: &[u8]) -> Result<(Vec<u8>, Chain), Error> {
    let shared = dh(pair, remote)?;
    let keys = kdf(shared.as_slice(), root, b"WhisperRatchet", 64)?;
    Ok((
        keys[..32].to_vec(),
        Chain {
            chain_type: 1,
            chain_key: ChainKey {
                counter: -1,
                key: Some(keys[32..].to_vec()),
            },
            message_keys: BTreeMap::new(),
        },
    ))
}
impl Record {
    pub fn from_json(value: serde_json::Value) -> Result<Self, Error> {
        let record: Self = serde_json::from_value(value)?;
        if record.version != "v1" || record.sessions.len() > 40 {
            return Err(Error::Invalid);
        }
        for (base, session) in &record.sessions {
            if STANDARD.decode(base).map_err(|_| Error::Invalid)? != session.index_info.base_key {
                return Err(Error::Invalid);
            }
            public(&session.index_info.base_key)?;
            public(&session.index_info.remote_identity_key)?;
            public(&session.current_ratchet.last_remote_ephemeral_key)?;
            let pair = session.current_ratchet.ephemeral_key_pair.key()?;
            if public(&pair.public)? != session.current_ratchet.ephemeral_key_pair.pub_key
                || session.current_ratchet.root_key.len() != 32
                || session.chains.len() > 512
            {
                return Err(Error::Invalid);
            }
            for (name, chain) in &session.chains {
                public(&STANDARD.decode(name).map_err(|_| Error::Invalid)?)?;
                if !matches!(chain.chain_type, 1 | 2)
                    || !(-1..=i64::from(u32::MAX)).contains(&chain.chain_key.counter)
                    || chain
                        .chain_key
                        .key
                        .as_ref()
                        .is_some_and(|key| key.len() != 32)
                    || chain.message_keys.len() > 2000
                    || chain.message_keys.values().any(|key| key.len() != 32)
                {
                    return Err(Error::Invalid);
                }
            }
        }
        Ok(record)
    }
    pub fn to_json(&self) -> Result<serde_json::Value, Error> {
        Ok(serde_json::to_value(self)?)
    }
    pub fn has_open_session(&self) -> bool {
        self.sessions.values().any(|s| s.index_info.closed == -1)
    }
    pub fn session_info(&self) -> Option<(Vec<u8>, u32)> {
        self.sessions
            .values()
            .find(|s| s.index_info.closed == -1)
            .map(|s| (s.index_info.base_key.clone(), s.registration_id))
    }
    fn archive(&mut self) {
        for session in self.sessions.values_mut() {
            if session.index_info.closed == -1 {
                session.index_info.closed = now() as i64;
            }
        }
    }
    fn insert(&mut self, session: Session) {
        self.sessions
            .insert(STANDARD.encode(&session.index_info.base_key), session);
        while self.sessions.len() > 40 {
            if let Some(old) = self
                .sessions
                .iter()
                .filter(|(_, s)| s.index_info.closed != -1)
                .min_by_key(|(_, s)| s.index_info.closed)
                .map(|(k, _)| k.clone())
            {
                self.sessions.remove(&old);
            } else {
                break;
            }
        }
    }
    pub fn initialize(&mut self, identity: &KeyPair, bundle: &Bundle) -> Result<(), Error> {
        self.initialize_with_keys(
            identity,
            bundle,
            &KeyPair::generate()?,
            &KeyPair::generate()?,
        )
    }
    pub fn initialize_with_keys(
        &mut self,
        identity: &KeyPair,
        bundle: &Bundle,
        base: &KeyPair,
        ratchet: &KeyPair,
    ) -> Result<(), Error> {
        let remote_identity = public(&bundle.identity)?;
        let signed = public(&bundle.signed.public)?;
        if !crypto::verify_signature(&remote_identity, &signed, &bundle.signature) {
            return Err(Error::Signature);
        }
        let mut shared = Zeroizing::new(vec![255; 32]);
        shared.extend(dh(identity, &signed)?.as_slice());
        shared.extend(dh(base, &remote_identity)?.as_slice());
        shared.extend(dh(base, &signed)?.as_slice());
        if let Some(pre) = &bundle.pre_key {
            shared.extend(dh(base, &pre.public)?.as_slice());
        }
        let master = kdf(&shared, &[0; 32], b"WhisperText", 96)?;
        let (root, chain) = root_chain(&master[..32], ratchet, &signed)?;
        let base_public = public(&base.public)?;
        let time = now();
        let session = Session {
            registration_id: bundle.registration_id,
            current_ratchet: Ratchet {
                ephemeral_key_pair: Pair::from(ratchet),
                last_remote_ephemeral_key: signed,
                previous_counter: 0,
                root_key: root,
            },
            index_info: Index {
                base_key: base_public.clone(),
                base_key_type: 1,
                closed: -1,
                used: time,
                created: time,
                remote_identity_key: remote_identity,
            },
            chains: BTreeMap::from([(STANDARD.encode(public(&ratchet.public)?), chain)]),
            pending_pre_key: Some(Pending {
                signed_key_id: bundle.signed.id,
                base_key: base_public,
                pre_key_id: bundle.pre_key.as_ref().map(|k| k.id),
            }),
        };
        self.archive();
        self.insert(session);
        Ok(())
    }
    pub fn encrypt(
        &mut self,
        identity: &KeyPair,
        registration: u32,
        plaintext: &[u8],
    ) -> Result<Encrypted, Error> {
        if plaintext.len() > MAX_WIRE - 256 {
            return Err(Error::Invalid);
        }
        let mut updated = self.clone();
        let session = updated
            .sessions
            .values_mut()
            .find(|s| s.index_info.closed == -1)
            .ok_or(Error::NoSession)?;
        let ratchet = public(&session.current_ratchet.ephemeral_key_pair.pub_key)?;
        let chain = session
            .chains
            .get_mut(&STANDARD.encode(&ratchet))
            .ok_or(Error::NoSession)?;
        if chain.chain_type != 1 {
            return Err(Error::Invalid);
        }
        let counter = u32::try_from(
            chain
                .chain_key
                .counter
                .checked_add(1)
                .ok_or(Error::Counter)?,
        )
        .map_err(|_| Error::Counter)?;
        let seed = take_message_key(chain, counter)?;
        let keys = kdf(&seed, &[0; 32], b"WhisperMessageKeys", 80)?;
        let ciphertext = crypto::aes_encrypt_cbc_with_iv(plaintext, &keys[..32], &keys[64..80])?;
        let message = proto::SignalMessage {
            ratchet_key: Some(ratchet),
            counter: Some(counter),
            previous_counter: Some(session.current_ratchet.previous_counter),
            ciphertext: Some(ciphertext),
        };
        let mut wire = vec![0x33];
        wire.extend(message.encode_to_vec());
        let mut authenticated = public(&identity.public)?;
        authenticated.extend(&session.index_info.remote_identity_key);
        authenticated.extend(&wire);
        wire.extend(&crypto::hmac_sha256(&authenticated, &keys[32..64])[..8]);
        let kind = if let Some(pending) = &session.pending_pre_key {
            let message = proto::PreKeySignalMessage {
                registration_id: Some(registration),
                pre_key_id: pending.pre_key_id,
                signed_pre_key_id: Some(pending.signed_key_id),
                base_key: Some(pending.base_key.clone()),
                identity_key: Some(public(&identity.public)?),
                message: Some(wire),
            };
            wire = vec![0x33];
            wire.extend(message.encode_to_vec());
            MessageType::PreKey
        } else {
            MessageType::Signal
        };
        *self = updated;
        Ok(Encrypted {
            kind,
            ciphertext: wire,
        })
    }
    pub fn decrypt(
        &mut self,
        identity: &KeyPair,
        signed: &KeyPair,
        pre_key: Option<&KeyPair>,
        kind: MessageType,
        wire: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.decrypt_with_rng(identity, signed, pre_key, kind, wire, KeyPair::generate)
    }
    pub fn decrypt_with_rng(
        &mut self,
        identity: &KeyPair,
        signed: &KeyPair,
        pre_key: Option<&KeyPair>,
        kind: MessageType,
        wire: &[u8],
        mut generate: impl FnMut() -> Result<KeyPair, crypto::Error>,
    ) -> Result<Vec<u8>, Error> {
        check_wire(wire)?;
        let mut updated = self.clone();
        if kind == MessageType::PreKey {
            let message = proto::PreKeySignalMessage::decode(&wire[1..])?;
            let base = public(message.base_key.as_deref().ok_or(Error::Invalid)?)?;
            let remote_identity = public(message.identity_key.as_deref().ok_or(Error::Invalid)?)?;
            let name = STANDARD.encode(&base);
            if !updated.sessions.contains_key(&name) {
                if message.pre_key_id.is_some() && pre_key.is_none() {
                    return Err(Error::MissingPreKey);
                }
                let mut shared = Zeroizing::new(vec![255; 32]);
                shared.extend(dh(signed, &remote_identity)?.as_slice());
                shared.extend(dh(identity, &base)?.as_slice());
                shared.extend(dh(signed, &base)?.as_slice());
                if message.pre_key_id.is_some()
                    && let Some(pre) = pre_key
                {
                    shared.extend(dh(pre, &base)?.as_slice());
                }
                let master = kdf(&shared, &[0; 32], b"WhisperText", 96)?;
                let time = now();
                updated.archive();
                updated.insert(Session {
                    registration_id: message.registration_id.ok_or(Error::Invalid)?,
                    current_ratchet: Ratchet {
                        ephemeral_key_pair: Pair::from(signed),
                        last_remote_ephemeral_key: base.clone(),
                        previous_counter: 0,
                        root_key: master[..32].to_vec(),
                    },
                    index_info: Index {
                        base_key: base,
                        base_key_type: 2,
                        closed: -1,
                        used: time,
                        created: time,
                        remote_identity_key: remote_identity,
                    },
                    chains: BTreeMap::new(),
                    pending_pre_key: None,
                });
            }
            let session = updated.sessions.get_mut(&name).ok_or(Error::NoSession)?;
            let plaintext = decrypt_session(
                session,
                identity,
                message.message.as_deref().ok_or(Error::Invalid)?,
                &mut generate,
            )?;
            *self = updated;
            return Ok(plaintext);
        }
        let mut candidates: Vec<_> = updated
            .sessions
            .iter()
            .map(|(k, s)| (s.index_info.closed == -1, s.index_info.used, k.clone()))
            .collect();
        candidates.sort_by(|a, b| b.cmp(a));
        let mut error = Error::NoSession;
        for (_, _, name) in candidates {
            let mut session = updated.sessions[&name].clone();
            match decrypt_session(&mut session, identity, wire, &mut generate) {
                Ok(plaintext) => {
                    updated.sessions.insert(name, session);
                    *self = updated;
                    return Ok(plaintext);
                }
                Err(e) => error = e,
            }
        }
        Err(error)
    }
}
fn check_wire(wire: &[u8]) -> Result<(), Error> {
    if wire.is_empty() || wire.len() > MAX_WIRE {
        return Err(Error::Invalid);
    }
    if wire[0] >> 4 != 3 || wire[0] & 15 > 3 {
        return Err(Error::Version);
    }
    Ok(())
}
fn fill(chain: &mut Chain, counter: u32) -> Result<(), Error> {
    if i64::from(counter) <= chain.chain_key.counter {
        return Ok(());
    }
    if i64::from(counter) - chain.chain_key.counter > 2000 {
        return Err(Error::Future);
    }
    while chain.chain_key.counter < i64::from(counter) {
        let key = chain.chain_key.key.as_ref().ok_or(Error::Replay)?;
        let seed = crypto::hmac_sha256(&[1], key);
        let next = crypto::hmac_sha256(&[2], key);
        chain.chain_key.counter += 1;
        chain.chain_key.key.zeroize();
        chain.chain_key.key = Some(next.to_vec());
        chain
            .message_keys
            .insert(chain.chain_key.counter as u32, seed.to_vec());
        while chain.message_keys.len() > 2000 {
            if let Some((_, mut key)) = chain.message_keys.pop_first() {
                key.zeroize();
            }
        }
    }
    Ok(())
}
fn take_message_key(chain: &mut Chain, counter: u32) -> Result<Zeroizing<Vec<u8>>, Error> {
    fill(chain, counter)?;
    Ok(Zeroizing::new(
        chain.message_keys.remove(&counter).ok_or(Error::Replay)?,
    ))
}
fn decrypt_session(
    session: &mut Session,
    identity: &KeyPair,
    wire: &[u8],
    generate: &mut impl FnMut() -> Result<KeyPair, crypto::Error>,
) -> Result<Vec<u8>, Error> {
    check_wire(wire)?;
    if wire.len() < 9 {
        return Err(Error::Invalid);
    }
    let signed_wire = &wire[..wire.len() - 8];
    let tag = &wire[wire.len() - 8..];
    let message = proto::SignalMessage::decode(&signed_wire[1..])?;
    let remote = public(message.ratchet_key.as_deref().ok_or(Error::Invalid)?)?;
    let name = STANDARD.encode(&remote);
    if !session.chains.contains_key(&name) {
        let old_remote = STANDARD.encode(&session.current_ratchet.last_remote_ephemeral_key);
        if let Some(chain) = session.chains.get_mut(&old_remote) {
            fill(chain, message.previous_counter.ok_or(Error::Invalid)?)?;
            chain.chain_key.key.zeroize();
            chain.chain_key.key = None;
        }
        let old_local = STANDARD.encode(&session.current_ratchet.ephemeral_key_pair.pub_key);
        if let Some(chain) = session.chains.remove(&old_local) {
            session.current_ratchet.previous_counter =
                u32::try_from(chain.chain_key.counter.max(0)).map_err(|_| Error::Counter)?;
        }
        let local = session.current_ratchet.ephemeral_key_pair.key()?;
        let (root, mut receive) = root_chain(&session.current_ratchet.root_key, &local, &remote)?;
        receive.chain_type = 2;
        session.chains.insert(name.clone(), receive);
        let fresh = generate()?;
        let (root, send) = root_chain(&root, &fresh, &remote)?;
        session
            .chains
            .insert(STANDARD.encode(public(&fresh.public)?), send);
        session.current_ratchet.root_key.zeroize();
        session.current_ratchet.root_key = root;
        session.current_ratchet.ephemeral_key_pair = Pair::from(&fresh);
        session.current_ratchet.last_remote_ephemeral_key = remote;
    }
    let chain = session.chains.get_mut(&name).ok_or(Error::NoSession)?;
    if chain.chain_type != 2 {
        return Err(Error::Invalid);
    }
    let seed = take_message_key(chain, message.counter.ok_or(Error::Invalid)?)?;
    let keys = kdf(&seed, &[0; 32], b"WhisperMessageKeys", 80)?;
    let mut authenticated = session.index_info.remote_identity_key.clone();
    authenticated.extend(public(&identity.public)?);
    authenticated.extend(signed_wire);
    let expected = crypto::hmac_sha256(&authenticated, &keys[32..64]);
    if expected[..8]
        .iter()
        .zip(tag)
        .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
        != 0
    {
        return Err(Error::Mac);
    }
    let plaintext = crypto::aes_decrypt_cbc_with_iv(
        message.ciphertext.as_deref().ok_or(Error::Invalid)?,
        &keys[..32],
        &keys[64..80],
    )?;
    session.pending_pre_key = None;
    session.index_info.used = now();
    Ok(plaintext)
}

use super::{Address, mapping};
use crate::auth::{AuthTransaction, KeyCategory, KeyMutation, KeyValue, SqliteAuthStore};
/// Persistent direct-message repository. Each cipher operation commits sessions,
/// identity changes, and one-time prekey consumption together. Async clients
/// should invoke these synchronous operations on a blocking executor.
#[derive(Clone)]
pub struct Repository {
    store: SqliteAuthStore,
    mappings: mapping::Store,
    migrations: std::sync::Arc<std::sync::Mutex<super::migration::Cache>>,
}
fn load_record(tx: &mut AuthTransaction<'_>, id: &str) -> Result<Record, Error> {
    let mut keys = tx.get(KeyCategory::Session, &[id.into()])?;
    match keys.remove(id) {
        Some(KeyValue::Session(value)) => Record::from_json(value),
        None => Ok(Record::default()),
        _ => Err(Error::Invalid),
    }
}
fn save_record(tx: &mut AuthTransaction<'_>, id: &str, record: &Record) -> Result<(), Error> {
    tx.set(&[KeyMutation::put(id, KeyValue::Session(record.to_json()?))])?;
    Ok(())
}
impl Repository {
    pub fn new(store: SqliteAuthStore) -> Self {
        Self {
            mappings: mapping::Store::new(store.clone()),
            store,
            migrations: super::migration::Cache::new(Default::default()),
        }
    }
    pub fn with_mappings(store: SqliteAuthStore, mappings: mapping::Store) -> Self {
        Self {
            store,
            mappings,
            migrations: super::migration::Cache::new(Default::default()),
        }
    }
    /// Replace this handle's migration cache; subsequent clones share the new cache.
    pub fn with_migration_config(mut self, config: super::migration::Config) -> Self {
        self.migrations = super::migration::Cache::new(config);
        self
    }
    pub fn lid_mapping(&self) -> &mapping::Store {
        &self.mappings
    }
    /// Migrate device-list-backed open PN sessions to LID addresses atomically.
    /// Does not create a mapping; store it separately before resolving PN aliases.
    pub fn migrate_session(&self, from: &str, to: &str) -> Result<super::migration::Result, Error> {
        super::migration::migrate(&self.store, &self.migrations, from, to)
    }
    /// Clear ephemeral caches; persistent sessions and mappings are retained.
    pub fn close(&self) -> Result<(), Error> {
        self.migrations.lock().map_err(|_| Error::Invalid)?.clear();
        self.mappings.close()
    }
    pub(crate) fn address(&self, jid: &str) -> Result<String, Error> {
        Ok(self
            .mappings
            .resolve_address(&Address::from_jid(jid)?)?
            .to_string())
    }
    pub fn inject_session(&self, jid: &str, bundle: &Bundle) -> Result<(), Error> {
        self.inject_session_with_keys(jid, bundle, &KeyPair::generate()?, &KeyPair::generate()?)
    }
    pub fn inject_session_with_keys(
        &self,
        jid: &str,
        bundle: &Bundle,
        base: &KeyPair,
        ratchet: &KeyPair,
    ) -> Result<(), Error> {
        let id = self.address(jid)?;
        self.store.transaction_with_error(|tx| {
            let creds = tx.load_credentials()?;
            let mut record = load_record(tx, &id)?;
            record.initialize_with_keys(&creds.signed_identity_key, bundle, base, ratchet)?;
            save_record(tx, &id, &record)
        })
    }
    pub fn encrypt(&self, jid: &str, plaintext: &[u8]) -> Result<Encrypted, Error> {
        let id = self.address(jid)?;
        self.store
            .transaction_with_error(|tx| Self::encrypt_in_transaction(tx, &id, plaintext))
    }
    pub(crate) fn encrypt_in_transaction(
        tx: &mut AuthTransaction<'_>,
        id: &str,
        plaintext: &[u8],
    ) -> Result<Encrypted, Error> {
        let creds = tx.load_credentials()?;
        let mut record = load_record(tx, id)?;
        let result = record.encrypt(
            &creds.signed_identity_key,
            u32::from(creds.registration_id),
            plaintext,
        )?;
        save_record(tx, id, &record)?;
        Ok(result)
    }
    pub fn decrypt(&self, jid: &str, kind: MessageType, wire: &[u8]) -> Result<Vec<u8>, Error> {
        self.decrypt_with_rng(jid, kind, wire, KeyPair::generate)
    }
    pub fn decrypt_with_rng(
        &self,
        jid: &str,
        kind: MessageType,
        wire: &[u8],
        generate: impl FnMut() -> Result<KeyPair, crypto::Error>,
    ) -> Result<Vec<u8>, Error> {
        let id = self.address(jid)?;
        self.store.transaction_with_error(|tx| {
            let creds = tx.load_credentials()?;
            let mut record = load_record(tx, &id)?;
            let mut consume = None;
            let mut pre_key = None;
            if kind == MessageType::PreKey {
                check_wire(wire)?;
                let message = proto::PreKeySignalMessage::decode(&wire[1..])?;
                let identity = public(message.identity_key.as_deref().ok_or(Error::Invalid)?)?;
                let existing = tx.get(KeyCategory::IdentityKey, std::slice::from_ref(&id))?;
                if let Some(KeyValue::IdentityKey(old)) = existing.get(&id)
                    && *old != identity
                {
                    record = Record::default();
                }
                tx.set(&[KeyMutation::put(&id, KeyValue::IdentityKey(identity))])?;
                let base = message.base_key.as_deref().ok_or(Error::Invalid)?;
                if !record
                    .sessions
                    .contains_key(&STANDARD.encode(public(base)?))
                    && let Some(pre_id) = message.pre_key_id
                {
                    let mut keys = tx.get(KeyCategory::PreKey, &[pre_id.to_string()])?;
                    let Some(KeyValue::PreKey(key)) = keys.remove(&pre_id.to_string()) else {
                        return Err(Error::MissingPreKey);
                    };
                    pre_key = Some(key);
                    consume = Some(pre_id);
                }
            }
            let plaintext = record.decrypt_with_rng(
                &creds.signed_identity_key,
                &creds.signed_pre_key.key_pair,
                pre_key.as_ref(),
                kind,
                wire,
                generate,
            )?;
            save_record(tx, &id, &record)?;
            if let Some(pre_id) = consume {
                tx.set(&[KeyMutation::delete(KeyCategory::PreKey, pre_id.to_string())])?;
            }
            Ok(plaintext)
        })
    }
    pub fn session_info(&self, jid: &str) -> Result<Option<(Vec<u8>, u32)>, Error> {
        let id = self.address(jid)?;
        self.store
            .transaction_with_error(|tx| Ok(load_record(tx, &id)?.session_info()))
    }
    pub fn validate_session(&self, jid: &str) -> Result<bool, Error> {
        Ok(self.session_info(jid)?.is_some())
    }
    /// Matches upstream's explicit deletion addresses, without PN/LID resolution.
    pub fn delete_sessions(&self, jids: &[String]) -> Result<(), Error> {
        let mutations = jids
            .iter()
            .map(|jid| {
                Ok(KeyMutation::delete(
                    KeyCategory::Session,
                    Address::from_jid(jid)?.to_string(),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        self.store.transaction(|tx| tx.set(&mutations))?;
        Ok(())
    }
}
