use super::{
    Error, LtHashGenerator, MutationKeys, new_lthash_state, patch_mac, snapshot_mac, value_mac,
};
use crate::{
    auth::{KeyCategory, KeyStore, KeyValue, LtHashState, SqliteAuthStore, ValueMac},
    crypto, proto,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::future::BoxFuture;
use indexmap::IndexMap;
use prost::Message as _;
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;
type Fetch = dyn Fn(String) -> BoxFuture<'static, Result<Option<proto::message::AppStateSyncKeyData>, Error>>
    + Send
    + Sync;
#[derive(Clone)]
pub struct KeyProvider(Arc<Fetch>);
impl std::fmt::Debug for KeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyProvider").finish_non_exhaustive()
    }
}
impl KeyProvider {
    pub fn new<F, Fut>(fetch: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<proto::message::AppStateSyncKeyData>, Error>>
            + Send
            + 'static,
    {
        Self(Arc::new(move |id| Box::pin(fetch(id))))
    }
    pub async fn get(
        &self,
        id: String,
    ) -> Result<Option<proto::message::AppStateSyncKeyData>, Error> {
        (self.0)(id).await
    }
    /// Read persistent keys on a blocking worker without holding a transaction across awaits.
    pub fn from_store(store: SqliteAuthStore) -> Self {
        Self::new(move |id| {
            let store = store.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    let mut keys =
                        store.get(KeyCategory::AppStateSyncKey, std::slice::from_ref(&id))?;
                    Ok(match keys.remove(&id) {
                        Some(KeyValue::AppStateSyncKey(data)) => Some(data),
                        _ => None,
                    })
                })
                .await
                .map_err(|_| Error::Task)?
            }
        })
    }
}
#[derive(Debug, Clone)]
pub struct PatchCreate {
    pub name: String,
    pub index: Vec<String>,
    pub sync_action: proto::SyncActionValue,
    pub api_version: i32,
    pub operation: i32,
}
pub struct EncodedPatch {
    pub patch: proto::SyncdPatch,
    pub state: LtHashState,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ChatMutation {
    pub sync_action: proto::SyncActionData,
    pub index: Value,
}
pub type MutationMap = IndexMap<String, ChatMutation>;
pub struct DecodedState {
    pub state: LtHashState,
    pub mutations: MutationMap,
    /// Snapshot MAC mismatches are resilient partial-state results in upstream.
    pub hash_verified: Option<bool>,
}
async fn keys(provider: &KeyProvider, id: &[u8]) -> Result<MutationKeys, Error> {
    let id = STANDARD.encode(id);
    let data = provider
        .get(id.clone())
        .await?
        .ok_or(Error::MissingKey(id))?;
    MutationKeys::derive(data.key_data.as_deref().ok_or(Error::Input("key data"))?)
}
fn equal(left: &[u8], right: &[u8]) -> bool {
    bool::from(left.ct_eq(right))
}
pub async fn encode_patch(
    request: &PatchCreate,
    key_id: &str,
    state: &LtHashState,
    provider: &KeyProvider,
) -> Result<EncodedPatch, Error> {
    encode_inner(request, key_id, state, provider, None).await
}
pub async fn encode_patch_with_iv(
    request: &PatchCreate,
    key_id: &str,
    state: &LtHashState,
    provider: &KeyProvider,
    iv: &[u8; 16],
) -> Result<EncodedPatch, Error> {
    encode_inner(request, key_id, state, provider, Some(*iv)).await
}
async fn encode_inner(
    request: &PatchCreate,
    key_id: &str,
    state: &LtHashState,
    provider: &KeyProvider,
    provided_iv: Option<[u8; 16]>,
) -> Result<EncodedPatch, Error> {
    if key_id.is_empty() {
        return Err(Error::MissingKey(key_id.into()));
    }
    let data = provider
        .get(key_id.into())
        .await?
        .ok_or_else(|| Error::MissingKey(key_id.into()))?;
    let material = MutationKeys::derive(data.key_data.as_deref().ok_or(Error::Input("key data"))?)?;
    let id = crate::media::decode_base64(key_id).map_err(|_| Error::Input("Base64 key id"))?;
    let index = serde_json::to_vec(&request.index).map_err(Error::from)?;
    let action = proto::SyncActionData {
        index: Some(index.clone()),
        value: Some(request.sync_action.clone()),
        padding: Some(vec![]),
        version: Some(request.api_version),
    };
    let encoded = Zeroizing::new(action.encode_to_vec());
    let iv = if let Some(iv) = provided_iv {
        iv
    } else {
        let mut iv = [0; 16];
        getrandom::fill(&mut iv).map_err(|_| crypto::Error::Random)?;
        iv
    };
    let mut encrypted = iv.to_vec();
    encrypted.extend(crypto::aes_encrypt_cbc_with_iv(
        &encoded,
        material.value_encryption_key.as_ref(),
        &iv,
    )?);
    let value = value_mac(
        request.operation,
        &encrypted,
        &id,
        material.value_mac_key.as_ref(),
    );
    let index_mac = crypto::hmac_sha256(&index, material.index_key.as_ref());
    let mut generator = LtHashGenerator::new(state)?;
    generator.mix(&index_mac, &value, request.operation);
    let mut next = generator.finish()?;
    next.version = next.version.checked_add(1).ok_or(Error::Version)?;
    let snapshot = snapshot_mac(
        &next.hash,
        next.version,
        &request.name,
        material.snapshot_mac_key.as_ref(),
    )?;
    let mac = patch_mac(
        &snapshot,
        &[value.to_vec()],
        next.version,
        &request.name,
        material.patch_mac_key.as_ref(),
    )?;
    encrypted.extend(value);
    let patch = proto::SyncdPatch {
        patch_mac: Some(mac.to_vec()),
        snapshot_mac: Some(snapshot.to_vec()),
        key_id: Some(proto::KeyId {
            id: Some(id.clone()),
        }),
        mutations: vec![proto::SyncdMutation {
            operation: Some(request.operation),
            record: Some(proto::SyncdRecord {
                index: Some(proto::SyncdIndex {
                    blob: Some(index_mac.to_vec()),
                }),
                value: Some(proto::SyncdValue {
                    blob: Some(encrypted),
                }),
                key_id: Some(proto::KeyId { id: Some(id) }),
            }),
        }],
        ..Default::default()
    };
    // Source encoder reinserts this entry even for REMOVE, after computing MACs.
    next.index_value_map.insert(
        STANDARD.encode(index_mac),
        ValueMac {
            value_mac: value.to_vec(),
        },
    );
    Ok(EncodedPatch { patch, state: next })
}
/// Snapshot records imply SET; mutation protobufs retain their operation field.
pub enum Record<'a> {
    Snapshot(&'a proto::SyncdRecord),
    Mutation(&'a proto::SyncdMutation),
}
pub async fn decode_mutations(
    records: &[Record<'_>],
    initial: &LtHashState,
    provider: &KeyProvider,
    on_mutation: &mut dyn FnMut(ChatMutation) -> Result<(), Error>,
    validate: bool,
) -> Result<LtHashState, Error> {
    let mut generator = LtHashGenerator::new(initial)?;
    let mut cache = HashMap::<String, MutationKeys>::new();
    for item in records {
        let (operation, record) = match item {
            Record::Snapshot(r) => (0, Some(*r)),
            Record::Mutation(m) => (m.operation.unwrap_or(0), m.record.as_ref()),
        };
        let Some(record) = record else {
            continue;
        };
        let Some(id) = record.key_id.as_ref().and_then(|k| k.id.as_deref()) else {
            continue;
        };
        let encoded_id = STANDARD.encode(id);
        if !cache.contains_key(&encoded_id) {
            match keys(provider, id).await {
                Ok(key) => {
                    cache.insert(encoded_id.clone(), key);
                }
                Err(e) if e.is_missing_key() => return Err(e),
                Err(_) => continue,
            }
        }
        let key = &cache[&encoded_id];
        let content = record
            .value
            .as_ref()
            .and_then(|v| v.blob.as_deref())
            .ok_or(Error::Input("record value"))?;
        let (encrypted, mac) = content.split_at(content.len().saturating_sub(32));
        if validate
            && !equal(
                &value_mac(operation, encrypted, id, key.value_mac_key.as_ref()),
                mac,
            )
        {
            continue;
        }
        let plaintext = match crypto::aes_decrypt(encrypted, key.value_encryption_key.as_ref()) {
            Ok(bytes) => Zeroizing::new(bytes),
            Err(_) => continue,
        };
        let action = proto::SyncActionData::decode(plaintext.as_slice())?;
        if validate {
            let index = action
                .index
                .as_deref()
                .ok_or(Error::Input("action index"))?;
            let expected = record
                .index
                .as_ref()
                .and_then(|v| v.blob.as_deref())
                .ok_or(Error::Input("record index"))?;
            if !equal(
                &crypto::hmac_sha256(index, key.index_key.as_ref()),
                expected,
            ) {
                return Err(Error::Mac("index"));
            }
        }
        let text = String::from_utf8_lossy(
            action
                .index
                .as_deref()
                .ok_or(Error::Input("action index"))?,
        );
        let index: Value = serde_json::from_str(&text)?;
        on_mutation(ChatMutation {
            sync_action: action,
            index,
        })?;
        let index = record
            .index
            .as_ref()
            .and_then(|v| v.blob.as_deref())
            .ok_or(Error::Input("record index"))?;
        generator.mix(index, mac, operation);
    }
    generator.finish()
}
pub async fn decode_patch(
    patch: &proto::SyncdPatch,
    name: &str,
    initial: &LtHashState,
    provider: &KeyProvider,
    on_mutation: &mut dyn FnMut(ChatMutation) -> Result<(), Error>,
    validate: bool,
) -> Result<LtHashState, Error> {
    if validate {
        let id = patch
            .key_id
            .as_ref()
            .and_then(|k| k.id.as_deref())
            .ok_or(Error::Input("patch key id"))?;
        let material = keys(provider, id).await?;
        let macs: Vec<_> = patch
            .mutations
            .iter()
            .map(|m| {
                let value = m
                    .record
                    .as_ref()
                    .and_then(|r| r.value.as_ref())
                    .and_then(|v| v.blob.as_deref())
                    .ok_or(Error::Input("patch record value"))?;
                Ok(value[value.len().saturating_sub(32)..].to_vec())
            })
            .collect::<Result<_, Error>>()?;
        let version = patch
            .version
            .as_ref()
            .and_then(|v| v.version)
            .ok_or(Error::Input("patch version"))?;
        let mac = patch_mac(
            patch
                .snapshot_mac
                .as_deref()
                .ok_or(Error::Input("snapshot MAC"))?,
            &macs,
            version,
            name,
            material.patch_mac_key.as_ref(),
        )?;
        if !equal(
            &mac,
            patch
                .patch_mac
                .as_deref()
                .ok_or(Error::Input("patch MAC"))?,
        ) {
            return Err(Error::Mac("patch"));
        }
    }
    decode_mutations(
        &patch
            .mutations
            .iter()
            .map(Record::Mutation)
            .collect::<Vec<_>>(),
        initial,
        provider,
        on_mutation,
        validate,
    )
    .await
}
pub async fn decode_snapshot(
    snapshot: &proto::SyncdSnapshot,
    name: &str,
    provider: &KeyProvider,
    minimum_version: Option<u64>,
    validate: bool,
) -> Result<DecodedState, Error> {
    let mut initial = new_lthash_state();
    initial.version = snapshot
        .version
        .as_ref()
        .and_then(|v| v.version)
        .ok_or(Error::Input("snapshot version"))?;
    let required = minimum_version.is_none_or(|v| initial.version > v);
    let mut mutations = MutationMap::new();
    let state = decode_mutations(
        &snapshot
            .records
            .iter()
            .map(Record::Snapshot)
            .collect::<Vec<_>>(),
        &initial,
        provider,
        &mut |mutation| {
            if required {
                mutations.insert(
                    String::from_utf8_lossy(
                        mutation.sync_action.index.as_deref().unwrap_or_default(),
                    )
                    .into_owned(),
                    mutation,
                );
            }
            Ok(())
        },
        validate,
    )
    .await?;
    let verified = if validate {
        let id = snapshot
            .key_id
            .as_ref()
            .and_then(|k| k.id.as_deref())
            .ok_or(Error::Input("snapshot key id"))?;
        let material = keys(provider, id).await?;
        let mac = snapshot_mac(
            &state.hash,
            state.version,
            name,
            material.snapshot_mac_key.as_ref(),
        )?;
        let verified = equal(
            snapshot
                .mac
                .as_deref()
                .ok_or(Error::Input("snapshot MAC"))?,
            &mac,
        );
        if !verified {
            tracing::warn!(
                name,
                version = state.version,
                "LT-hash mismatch on snapshot, retaining partial state"
            );
        }
        Some(verified)
    } else {
        None
    };
    Ok(DecodedState {
        state,
        mutations,
        hash_verified: verified,
    })
}
/// Decode an in-memory patch sequence. External mutations must be downloaded
/// and appended by the caller, then the reference cleared before this stage.
pub async fn decode_patches(
    patches: &mut [proto::SyncdPatch],
    name: &str,
    initial: &LtHashState,
    provider: &KeyProvider,
    minimum_version: Option<u64>,
    validate: bool,
) -> Result<DecodedState, Error> {
    decode_patches_inner(
        patches,
        name,
        initial,
        provider,
        minimum_version,
        validate,
        None,
    )
    .await
}

pub(crate) async fn decode_patches_inner(
    patches: &mut [proto::SyncdPatch],
    name: &str,
    initial: &LtHashState,
    provider: &KeyProvider,
    minimum_version: Option<u64>,
    validate: bool,
    downloader: Option<&super::BlobDownloader>,
) -> Result<DecodedState, Error> {
    let mut state = initial.clone();
    let mut mutations = MutationMap::new();
    let mut verified = None;
    for patch in patches {
        if let Some(reference) = &patch.external_mutations {
            let downloader = downloader.ok_or(Error::Input("unresolved external mutations"))?;
            let external = downloader.download_patch(reference.clone()).await?;
            patch.mutations.extend(external.mutations);
        }
        let version = patch
            .version
            .as_ref()
            .and_then(|v| v.version)
            .ok_or(Error::Input("patch version"))?;
        state.version = version;
        let required = minimum_version.is_none_or(|v| version > v);
        let decoded = decode_patch(
            patch,
            name,
            &state,
            provider,
            &mut |mutation| {
                if required {
                    mutations.insert(
                        String::from_utf8_lossy(
                            mutation.sync_action.index.as_deref().unwrap_or_default(),
                        )
                        .into_owned(),
                        mutation,
                    );
                }
                Ok(())
            },
            validate,
        )
        .await;
        match decoded {
            Ok(next) => state = next,
            Err(e) if e.is_missing_key() => return Err(e),
            Err(error) => {
                tracing::warn!(
                    name,
                    version,
                    ?error,
                    "failed to decode app-state patch, skipping"
                );
                continue;
            }
        }
        if validate {
            let id = patch
                .key_id
                .as_ref()
                .and_then(|k| k.id.as_deref())
                .ok_or(Error::Input("patch key id"))?;
            let material = keys(provider, id).await?;
            let expected = snapshot_mac(
                &state.hash,
                state.version,
                name,
                material.snapshot_mac_key.as_ref(),
            )?;
            let valid = equal(
                patch
                    .snapshot_mac
                    .as_deref()
                    .ok_or(Error::Input("snapshot MAC"))?,
                &expected,
            );
            verified = Some(valid);
            if !valid {
                tracing::warn!(
                    name,
                    version,
                    "LT-hash mismatch, skipping remaining patches"
                );
                break;
            }
        }
        // Source releases successfully consumed mutation buffers in place.
        patch.mutations.clear();
    }
    Ok(DecodedState {
        state,
        mutations,
        hash_verified: verified,
    })
}
