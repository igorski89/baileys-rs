//! Authentication credentials and persistent typed Signal key storage.
mod store;
use crate::{
    crypto::{self, KeyPair, SignedKeyPair},
    proto,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
pub use store::*;
use zeroize::Zeroize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error("socket is not authenticated (creds.me.id is missing)")]
    Unauthenticated,
    #[error("auth database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("invalid auth serialization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("auth file operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("auth store lock is poisoned")]
    Poisoned,
    #[error("key value does not match its category")]
    CategoryMismatch,
    #[error("invalid credential state: {0}")]
    InvalidCredentials(&'static str),
    #[error("invalid prekey range")]
    PreKeyRange,
    #[error("unsupported auth database schema version")]
    SchemaVersion,
    #[error("auth import filename is ambiguous or unsafe")]
    FileName,
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        if matches!(self, Self::Unauthenticated) {
            Some(401)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Contact {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notify: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_name: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nullable_string"
    )]
    pub img_url: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}
fn nullable_string<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(de).map(Some)
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolAddress {
    pub name: String,
    pub device_id: u32,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalIdentity {
    pub identifier: ProtocolAddress,
    #[serde(with = "crate::serialization::bytes")]
    pub identifier_key: Vec<u8>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisappearingMode {
    pub ephemeral_expiration: Option<u32>,
    #[serde(default, with = "crate::serialization::optional_i64")]
    pub ephemeral_setting_timestamp: Option<i64>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSettings {
    pub unarchive_chats: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_disappearing_mode: Option<DisappearingMode>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MinimalMessage {
    pub key: Option<proto::MessageKey>,
    #[serde(default, with = "crate::serialization::optional_u64")]
    pub message_timestamp: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials {
    pub noise_key: KeyPair,
    pub pairing_ephemeral_key_pair: KeyPair,
    pub signed_identity_key: KeyPair,
    pub signed_pre_key: SignedKeyPair,
    pub registration_id: u16,
    pub adv_secret_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub me: Option<Contact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<proto::AdvSignedDeviceIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_identities: Option<Vec<SignalIdentity>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub my_app_state_key_id: Option<String>,
    pub first_unuploaded_pre_key_id: u32,
    pub next_pre_key_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_account_sync_timestamp: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    pub processed_history_messages: Vec<MinimalMessage>,
    pub account_sync_counter: u32,
    pub account_settings: AccountSettings,
    pub registered: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairing_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_prop_hash: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serialization::optional_bytes"
    )]
    pub routing_info: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_data: Option<serde_json::Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("registered", &self.registered)
            .field("next_pre_key_id", &self.next_pre_key_id)
            .finish_non_exhaustive()
    }
}
impl Drop for Credentials {
    fn drop(&mut self) {
        self.adv_secret_key.zeroize();
        if let Some(code) = &mut self.pairing_code {
            code.zeroize();
        }
    }
}
impl Credentials {
    pub fn generate() -> Result<Self, Error> {
        let signed_identity_key = KeyPair::generate()?;
        let mut secret = zeroize::Zeroizing::new([0; 32]);
        getrandom::fill(secret.as_mut()).map_err(|_| crypto::Error::Random)?;
        let mut registration = [0; 2];
        getrandom::fill(&mut registration).map_err(|_| crypto::Error::Random)?;
        Ok(Self {
            noise_key: KeyPair::generate()?,
            pairing_ephemeral_key_pair: KeyPair::generate()?,
            signed_pre_key: crypto::signed_key_pair(&signed_identity_key, 1)?,
            signed_identity_key,
            // Upstream Uint16Array.from(randomBytes(2))[0] reads the first byte.
            registration_id: u16::from(registration[0]),
            adv_secret_key: STANDARD.encode(secret.as_slice()),
            me: None,
            account: None,
            signal_identities: None,
            my_app_state_key_id: None,
            first_unuploaded_pre_key_id: 1,
            next_pre_key_id: 1,
            last_account_sync_timestamp: None,
            platform: None,
            processed_history_messages: Vec::new(),
            account_sync_counter: 0,
            account_settings: AccountSettings::default(),
            registered: false,
            pairing_code: None,
            last_prop_hash: None,
            routing_info: None,
            additional_data: None,
            extra: BTreeMap::new(),
        })
    }
    pub fn me_id(&self) -> Result<&str, Error> {
        self.me
            .as_ref()
            .map(|me| me.id.as_str())
            .filter(|id| !id.is_empty())
            .ok_or(Error::Unauthenticated)
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.registration_id > 16383 {
            return Err(Error::InvalidCredentials("registration ID"));
        }
        if STANDARD
            .decode(&self.adv_secret_key)
            .map_or(true, |v| v.len() != 32)
        {
            return Err(Error::InvalidCredentials("ADV secret"));
        }
        for pair in [
            &self.noise_key,
            &self.pairing_ephemeral_key_pair,
            &self.signed_identity_key,
            &self.signed_pre_key.key_pair,
        ] {
            if KeyPair::from_private(pair.private).public != pair.public {
                return Err(Error::InvalidCredentials("key pair mismatch"));
            }
        }
        if !crypto::verify_signature(
            &self.signed_identity_key.public,
            &crypto::signal_public_key(&self.signed_pre_key.key_pair.public)?,
            &self.signed_pre_key.signature,
        ) {
            return Err(Error::InvalidCredentials("signed prekey signature"));
        }
        if self.first_unuploaded_pre_key_id > self.next_pre_key_id {
            return Err(Error::InvalidCredentials("prekey counters"));
        }
        Ok(())
    }
    pub fn generate_or_get_prekeys(&self, count: u32) -> Result<PreKeyBatch, Error> {
        let available = self
            .next_pre_key_id
            .checked_sub(self.first_unuploaded_pre_key_id)
            .ok_or(Error::PreKeyRange)?;
        let last = self
            .first_unuploaded_pre_key_id
            .checked_add(count)
            .and_then(|n| n.checked_sub(1))
            .ok_or(Error::PreKeyRange)?;
        if count == 0 || last > 0xffffff {
            return Err(Error::PreKeyRange);
        }
        let mut new_pre_keys = BTreeMap::new();
        if count > available {
            for id in self.next_pre_key_id..=last {
                new_pre_keys.insert(id, KeyPair::generate()?);
            }
        }
        Ok(PreKeyBatch {
            new_pre_keys,
            last_pre_key_id: last,
            first_pre_key_id: self.first_unuploaded_pre_key_id,
            count,
        })
    }
}
#[derive(Debug)]
pub struct PreKeyBatch {
    pub new_pre_keys: BTreeMap<u32, KeyPair>,
    pub last_pre_key_id: u32,
    pub first_pre_key_id: u32,
    pub count: u32,
}
