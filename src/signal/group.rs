//! Sender-key distribution, chain derivation, signatures and persistent group cipher.
use super::{Error, SenderKeyName};
use crate::{
    auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    crypto::{self, KeyPair},
    proto,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};
const MAX_STATES: usize = 5;
const MAX_SKIPPED: usize = 2000;
const MAX_MESSAGE: usize = 16 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct Chain {
    iteration: u32,
    #[serde(with = "crate::serialization::bytes32")]
    seed: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
struct SigningKey {
    #[serde(rename = "public", with = "crate::serialization::bytes")]
    public: Vec<u8>,
    #[serde(default, rename = "private", with = "crate::serialization::bytes")]
    private: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "camelCase")]
struct State {
    sender_key_id: u32,
    sender_chain_key: Chain,
    sender_signing_key: SigningKey,
    #[serde(default)]
    sender_message_keys: Vec<Chain>,
}
/// JSON/BufferJSON compatible with Baileys' SenderKeyRecord, rather than the
/// unused protobuf record type. Secret material is redacted and zeroized on drop.
#[derive(Clone, Default, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(transparent)]
pub struct Record {
    states: Vec<State>,
}
impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SenderKeyRecord")
            .field("states", &self.states.len())
            .finish_non_exhaustive()
    }
}
impl Record {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_MESSAGE {
            return Err(Error::Invalid);
        }
        let record: Self = serde_json::from_slice(bytes)?;
        if record.states.len() > MAX_STATES {
            return Err(Error::Invalid);
        }
        for state in &record.states {
            crypto::signal_public_key(&state.sender_signing_key.public)?;
            let private = &state.sender_signing_key.private;
            if !private.is_empty() {
                let pair = KeyPair::from_private(
                    private.as_slice().try_into().map_err(|_| Error::Invalid)?,
                );
                if crypto::signal_public_key(&pair.public)?.as_slice()
                    != crypto::signal_public_key(&state.sender_signing_key.public)?.as_slice()
                {
                    return Err(Error::Invalid);
                }
            }
            if state.sender_message_keys.len() > MAX_SKIPPED {
                return Err(Error::Invalid);
            }
            let mut iterations = std::collections::HashSet::new();
            for key in &state.sender_message_keys {
                if key.iteration >= state.sender_chain_key.iteration
                    || !iterations.insert(key.iteration)
                {
                    return Err(Error::Invalid);
                }
            }
        }
        Ok(record)
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(self)?)
    }
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
    pub fn state_count(&self) -> usize {
        self.states.len()
    }
    pub fn latest_iteration(&self) -> Option<u32> {
        self.states.last().map(|s| s.sender_chain_key.iteration)
    }
    /// Deterministic reset for fixture verification or explicitly supplied fresh
    /// material. Reusing a chain/signing key combination can reuse message keys.
    pub fn reset(&mut self, id: u32, iteration: u32, chain: [u8; 32], signing: &KeyPair) {
        self.states.clear();
        self.states.push(State {
            sender_key_id: id,
            sender_chain_key: Chain {
                iteration,
                seed: chain,
            },
            sender_signing_key: SigningKey {
                public: crypto::signal_public_key(&signing.public)
                    .expect("32-byte public key")
                    .to_vec(),
                private: signing.private.to_vec(),
            },
            sender_message_keys: vec![],
        });
    }
    pub fn create_distribution(&mut self) -> Result<Vec<u8>, Error> {
        if self.is_empty() {
            let mut bytes = [0; 4];
            let id = loop {
                getrandom::fill(&mut bytes).map_err(|_| crypto::Error::Random)?;
                let id = u32::from_le_bytes(bytes) & 0x7fffffff;
                if id < 0x7fffffff {
                    break id;
                }
            };
            let mut seed = [0; 32];
            getrandom::fill(&mut seed).map_err(|_| crypto::Error::Random)?;
            let signing = KeyPair::generate()?;
            self.reset(id, 0, seed, &signing);
            seed.zeroize();
        }
        self.distribution()
    }
    pub fn distribution(&self) -> Result<Vec<u8>, Error> {
        let state = self.states.last().ok_or(Error::NoSession)?;
        let message = proto::SenderKeyDistributionMessage {
            id: Some(state.sender_key_id),
            iteration: Some(state.sender_chain_key.iteration),
            chain_key: Some(state.sender_chain_key.seed.to_vec()),
            signing_key: Some(
                crypto::signal_public_key(&state.sender_signing_key.public)?.to_vec(),
            ),
        };
        let mut bytes = vec![0x33];
        bytes.extend(message.encode_to_vec());
        Ok(bytes)
    }
    pub fn process_distribution(&mut self, bytes: &[u8]) -> Result<(), Error> {
        version(bytes)?;
        let message = proto::SenderKeyDistributionMessage::decode(&bytes[1..])?;
        let seed = message
            .chain_key
            .as_deref()
            .ok_or(Error::Invalid)?
            .try_into()
            .map_err(|_| Error::Invalid)?;
        let public =
            crypto::signal_public_key(message.signing_key.as_deref().ok_or(Error::Invalid)?)?
                .to_vec();
        let state = State {
            sender_key_id: message.id.ok_or(Error::Invalid)?,
            sender_chain_key: Chain {
                iteration: message.iteration.ok_or(Error::Invalid)?,
                seed,
            },
            sender_signing_key: SigningKey {
                public,
                private: vec![],
            },
            sender_message_keys: vec![],
        };
        // Upstream appends duplicate IDs; lookup chooses the oldest matching state.
        self.states.push(state);
        if self.states.len() > MAX_STATES {
            self.states.remove(0);
        }
        Ok(())
    }
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        if plaintext.len() > MAX_MESSAGE - 128 {
            return Err(Error::Invalid);
        }
        let mut updated = self.clone();
        let state = updated.states.last_mut().ok_or(Error::NoSession)?;
        let signing = KeyPair::from_private(
            state
                .sender_signing_key
                .private
                .as_slice()
                .try_into()
                .map_err(|_| Error::NoSigningKey)?,
        );
        // Preserve upstream's actual counter sequence: 0,2,4,... . Each send
        // after the first retains the intervening message key as a skipped key.
        let current = state.sender_chain_key.iteration;
        let iteration = if current == 0 {
            0
        } else {
            current.checked_add(1).ok_or(Error::Counter)?
        };
        let seed = Zeroizing::new(message_seed(state, iteration)?);
        let (iv, key) = derive_message_key(seed.as_slice())?;
        let ciphertext = crypto::aes_encrypt_cbc_with_iv(plaintext, key.as_slice(), &iv)?;
        let message = proto::SenderKeyMessage {
            id: Some(state.sender_key_id),
            iteration: Some(iteration),
            ciphertext: Some(ciphertext),
        };
        let mut bytes = vec![0x33];
        bytes.extend(message.encode_to_vec());
        bytes.extend(signing.sign(&bytes));
        *self = updated;
        Ok(bytes)
    }
    pub fn decrypt(&mut self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        version(bytes)?;
        if bytes.len() < 65 {
            return Err(Error::Invalid);
        }
        let signed = &bytes[..bytes.len() - 64];
        let signature = &bytes[bytes.len() - 64..];
        let message = proto::SenderKeyMessage::decode(&signed[1..])?;
        let mut updated = self.clone();
        let state = updated
            .states
            .iter_mut()
            .find(|s| Some(s.sender_key_id) == message.id)
            .ok_or(Error::NoSession)?;
        if !crypto::verify_signature(&state.sender_signing_key.public, signed, signature) {
            return Err(Error::Signature);
        }
        let seed = Zeroizing::new(message_seed(
            state,
            message.iteration.ok_or(Error::Invalid)?,
        )?);
        let (iv, key) = derive_message_key(seed.as_slice())?;
        let plaintext = crypto::aes_decrypt_cbc_with_iv(
            message.ciphertext.as_deref().ok_or(Error::Invalid)?,
            key.as_slice(),
            &iv,
        )?;
        *self = updated;
        Ok(plaintext)
    }
}
fn version(bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() || bytes.len() > MAX_MESSAGE {
        return Err(Error::Invalid);
    }
    if bytes[0] >> 4 != 3 {
        return Err(Error::Version);
    }
    Ok(())
}
fn derive_message_key(seed: &[u8]) -> Result<([u8; 16], Zeroizing<Vec<u8>>), Error> {
    let expanded = Zeroizing::new(crypto::hkdf(seed, 64, Some(&[0; 32]), b"WhisperGroup")?);
    Ok((
        expanded[..16].try_into().expect("HKDF length"),
        Zeroizing::new(expanded[16..48].to_vec()),
    ))
}
fn message_seed(state: &mut State, iteration: u32) -> Result<[u8; 32], Error> {
    let current = state.sender_chain_key.iteration;
    if current > iteration {
        let index = state
            .sender_message_keys
            .iter()
            .position(|k| k.iteration == iteration)
            .ok_or(Error::Replay)?;
        return Ok(state.sender_message_keys.remove(index).seed);
    }
    if iteration - current > MAX_SKIPPED as u32 {
        return Err(Error::Future);
    }
    iteration.checked_add(1).ok_or(Error::Counter)?;
    while state.sender_chain_key.iteration < iteration {
        let skipped = Chain {
            iteration: state.sender_chain_key.iteration,
            seed: crypto::hmac_sha256(&[1], &state.sender_chain_key.seed),
        };
        state.sender_message_keys.push(skipped);
        if state.sender_message_keys.len() > MAX_SKIPPED {
            state.sender_message_keys.remove(0);
        }
        state.sender_chain_key.seed = crypto::hmac_sha256(&[2], &state.sender_chain_key.seed);
        state.sender_chain_key.iteration += 1;
    }
    let seed = crypto::hmac_sha256(&[1], &state.sender_chain_key.seed);
    state.sender_chain_key.seed = crypto::hmac_sha256(&[2], &state.sender_chain_key.seed);
    state.sender_chain_key.iteration += 1;
    Ok(seed)
}
/// Synchronous cryptographic operations run in one SQLite transaction, so failed
/// authentication/decryption never consumes persisted chain or skipped keys.
/// Async callers should use a blocking executor.
#[derive(Clone)]
pub struct Repository {
    store: SqliteAuthStore,
}
impl Repository {
    pub fn new(store: SqliteAuthStore) -> Self {
        Self { store }
    }
    fn update<T>(
        &self,
        name: &SenderKeyName,
        work: impl FnOnce(&mut Record) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.store.transaction_with_error(|tx| {
            let id = name.to_string();
            let mut keys = tx.get(KeyCategory::SenderKey, std::slice::from_ref(&id))?;
            let mut record = match keys.remove(&id) {
                Some(KeyValue::SenderKey(bytes)) => Record::from_bytes(&bytes)?,
                None => Record::default(),
                _ => return Err(Error::Invalid),
            };
            let result = work(&mut record)?;
            let bytes = record.to_bytes()?;
            tx.set(&[KeyMutation::put(id, KeyValue::SenderKey(bytes))])?;
            Ok(result)
        })
    }
    pub fn has_sender_key(&self, name: &SenderKeyName) -> Result<bool, Error> {
        let id = name.to_string();
        Ok(self
            .store
            .get(KeyCategory::SenderKey, std::slice::from_ref(&id))?
            .contains_key(&id))
    }
    pub fn distribution(&self, name: &SenderKeyName) -> Result<Vec<u8>, Error> {
        self.update(name, Record::create_distribution)
    }
    pub fn process_distribution(&self, name: &SenderKeyName, bytes: &[u8]) -> Result<(), Error> {
        self.update(name, |r| r.process_distribution(bytes))
    }
    pub fn encrypt(
        &self,
        name: &SenderKeyName,
        plaintext: &[u8],
    ) -> Result<EncryptedGroupMessage, Error> {
        self.update(name, |record| {
            let distribution = record.create_distribution()?;
            let ciphertext = record.encrypt(plaintext)?;
            Ok(EncryptedGroupMessage {
                ciphertext,
                distribution,
            })
        })
    }
    pub fn decrypt(&self, name: &SenderKeyName, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        self.update(name, |r| r.decrypt(bytes))
    }
}
/// Distribution is captured before encryption, matching the upstream repository.
pub struct EncryptedGroupMessage {
    pub ciphertext: Vec<u8>,
    pub distribution: Vec<u8>,
}
impl std::fmt::Debug for EncryptedGroupMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedGroupMessage")
            .field("ciphertext_bytes", &self.ciphertext.len())
            .finish_non_exhaustive()
    }
}
