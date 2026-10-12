//! App-state mutation integrity and snapshot/patch codecs.
mod actions;
mod codec;
mod network;
mod sync;
use crate::{
    auth::{LtHashState, ValueMac},
    crypto, proto,
};
pub use actions::*;
use base64::{Engine, engine::general_purpose::STANDARD};
pub use codec::*;
pub use network::*;
use std::collections::BTreeMap;
pub use sync::*;
use zeroize::Zeroizing;
pub const HASH_SIZE: usize = 128;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("LT-hash must contain 128 bytes")]
    HashLength,
    #[error("app-state MAC version exceeds the source uint32 boundary")]
    Version,
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Auth(#[from] crate::auth::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Media(#[from] crate::media::Error),
    #[error("missing app-state key {0}")]
    MissingKey(String),
    #[error("invalid app-state {0}")]
    Input(&'static str),
    #[error("app-state {0} MAC verification failed")]
    Mac(&'static str),
    #[error("app-state key lookup failed: {0}")]
    Lookup(String),
    #[error("app-state transport failed: {message}")]
    Transport {
        message: String,
        status_code: Option<u16>,
    },
    #[error("app-state key worker failed")]
    Task,
}
#[derive(Clone)]
pub struct MutationKeys {
    pub index_key: Zeroizing<[u8; 32]>,
    pub value_encryption_key: Zeroizing<[u8; 32]>,
    pub value_mac_key: Zeroizing<[u8; 32]>,
    pub snapshot_mac_key: Zeroizing<[u8; 32]>,
    pub patch_mac_key: Zeroizing<[u8; 32]>,
}
impl std::fmt::Debug for MutationKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutationKeys").finish_non_exhaustive()
    }
}
impl MutationKeys {
    pub fn derive(secret: &[u8]) -> Result<Self, Error> {
        let expanded = Zeroizing::new(crypto::hkdf(secret, 160, None, b"WhatsApp Mutation Keys")?);
        Ok(Self {
            index_key: Zeroizing::new(expanded[0..32].try_into().unwrap()),
            value_encryption_key: Zeroizing::new(expanded[32..64].try_into().unwrap()),
            value_mac_key: Zeroizing::new(expanded[64..96].try_into().unwrap()),
            snapshot_mac_key: Zeroizing::new(expanded[96..128].try_into().unwrap()),
            patch_mac_key: Zeroizing::new(expanded[128..160].try_into().unwrap()),
        })
    }
}
pub fn new_lthash_state() -> LtHashState {
    LtHashState {
        version: 0,
        hash: vec![0; HASH_SIZE],
        index_value_map: BTreeMap::new(),
    }
}
/// 64 little-endian uint16 lanes, with modular addition/subtraction.
pub fn subtract_then_add(
    base: &[u8],
    subtract: &[Vec<u8>],
    add: &[Vec<u8>],
) -> Result<[u8; HASH_SIZE], Error> {
    let mut hash = <[u8; HASH_SIZE]>::try_from(base).map_err(|_| Error::HashLength)?;
    for (values, remove) in [(subtract, true), (add, false)] {
        for value in values {
            let expanded = crypto::hkdf(value, HASH_SIZE, None, b"WhatsApp Patch Integrity")?;
            for (lane, delta) in hash
                .as_chunks_mut::<2>()
                .0
                .iter_mut()
                .zip(expanded.as_chunks::<2>().0)
            {
                let before = u16::from_le_bytes(*lane);
                let delta = u16::from_le_bytes(*delta);
                *lane = if remove {
                    before.wrapping_sub(delta)
                } else {
                    before.wrapping_add(delta)
                }
                .to_le_bytes();
            }
        }
    }
    Ok(hash)
}
pub struct LtHashGenerator {
    version: u64,
    hash: Vec<u8>,
    index_value_map: BTreeMap<String, ValueMac>,
    additions: Vec<Vec<u8>>,
    subtractions: Vec<Vec<u8>>,
}
impl LtHashGenerator {
    pub fn new(state: &LtHashState) -> Result<Self, Error> {
        if state.hash.len() != HASH_SIZE {
            return Err(Error::HashLength);
        }
        Ok(Self {
            version: state.version,
            hash: state.hash.clone(),
            index_value_map: state.index_value_map.clone(),
            additions: vec![],
            subtractions: vec![],
        })
    }
    /// Unknown operation values follow the source generator's SET fallback.
    pub fn mix(&mut self, index_mac: &[u8], value_mac: &[u8], operation: i32) {
        let id = STANDARD.encode(index_mac);
        let previous = self.index_value_map.get(&id).cloned();
        if operation == proto::syncd_mutation::SyncdOperation::Remove as i32 {
            if previous.is_none() {
                return;
            }
            self.index_value_map.remove(&id);
        } else {
            self.additions.push(value_mac.to_vec());
            self.index_value_map.insert(
                id,
                ValueMac {
                    value_mac: value_mac.to_vec(),
                },
            );
        }
        if let Some(value) = previous {
            self.subtractions.push(value.value_mac);
        }
    }
    /// Finishing is repeatable and never mutates the caller's starting state.
    pub fn finish(&self) -> Result<LtHashState, Error> {
        Ok(LtHashState {
            version: self.version,
            hash: subtract_then_add(&self.hash, &self.subtractions, &self.additions)?.to_vec(),
            index_value_map: self.index_value_map.clone(),
        })
    }
}
/// Source truncates the key-prefix length into the final byte of an 8-byte suffix.
pub fn value_mac(operation: i32, data: &[u8], key_id: &[u8], key: &[u8]) -> [u8; 32] {
    let mut input = Vec::with_capacity(1 + key_id.len() + data.len() + 8);
    input.push(
        if operation == proto::syncd_mutation::SyncdOperation::Set as i32 {
            1
        } else {
            2
        },
    );
    input.extend_from_slice(key_id);
    input.extend_from_slice(data);
    input.extend_from_slice(&[0; 7]);
    input.push(key_id.len().wrapping_add(1) as u8);
    crypto::hmac_sha512(&input, key)[..32].try_into().unwrap()
}
fn version_bytes(version: u64) -> Result<[u8; 8], Error> {
    let value = u32::try_from(version).map_err(|_| Error::Version)?;
    let mut bytes = [0; 8];
    bytes[4..].copy_from_slice(&value.to_be_bytes());
    Ok(bytes)
}
pub fn snapshot_mac(hash: &[u8], version: u64, name: &str, key: &[u8]) -> Result<[u8; 32], Error> {
    let mut input = hash.to_vec();
    input.extend_from_slice(&version_bytes(version)?);
    input.extend_from_slice(name.as_bytes());
    Ok(crypto::hmac_sha256(&input, key))
}
pub fn patch_mac(
    snapshot: &[u8],
    values: &[Vec<u8>],
    version: u64,
    name: &str,
    key: &[u8],
) -> Result<[u8; 32], Error> {
    let mut input = snapshot.to_vec();
    for value in values {
        input.extend_from_slice(value);
    }
    input.extend_from_slice(&version_bytes(version)?);
    input.extend_from_slice(name.as_bytes());
    Ok(crypto::hmac_sha256(&input, key))
}

impl Error {
    pub fn is_missing_key(&self) -> bool {
        matches!(self, Self::MissingKey(_))
    }
}
