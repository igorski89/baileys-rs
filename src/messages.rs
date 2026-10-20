//! Message envelopes and protobuf payloads, independent of socket receipt processing.
pub mod encrypted;
pub use encrypted::*;
pub mod generation;
pub mod group_effects;
pub mod incoming;
pub mod own;
pub mod protocol;
pub mod updates;
use crate::{
    binary::{BinaryNode, NodeContent},
    crypto,
    jid::{self, Jid},
    proto, signal,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid message stanza: {0}")]
    Envelope(&'static str),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid message padding")]
    Padding,
    #[error("invalid message ID input")]
    Id,
    #[error("unknown encrypted message type")]
    EncryptionType,
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Signal(#[from] signal::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
}

pub fn pad_with_byte(message: &[u8], random_byte: u8) -> Vec<u8> {
    let length = usize::from((random_byte & 15) + 1);
    let mut result = Vec::with_capacity(message.len() + length);
    result.extend_from_slice(message);
    result.resize(message.len() + length, length as u8);
    result
}

/// Source-compatible: checks the final length byte, without validating every pad byte.
pub fn unpad(message: &[u8]) -> Result<&[u8], Error> {
    let length = usize::from(*message.last().ok_or(Error::Padding)?);
    message
        .get(..message.len().checked_sub(length).ok_or(Error::Padding)?)
        .ok_or(Error::Padding)
}

pub fn encode(message: &proto::Message) -> Result<Vec<u8>, Error> {
    let mut byte = [0];
    getrandom::fill(&mut byte).map_err(|_| crypto::Error::Random)?;
    Ok(pad_with_byte(&message.encode_to_vec(), byte[0]))
}

fn hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    bytes
        .iter()
        .flat_map(|b| {
            [
                char::from(HEX[usize::from(b >> 4)]),
                char::from(HEX[usize::from(b & 15)]),
            ]
        })
        .collect()
}

pub fn generate_id() -> Result<String, Error> {
    let mut bytes = [0; 18];
    getrandom::fill(&mut bytes).map_err(|_| crypto::Error::Random)?;
    Ok(format!("3EB0{}", hex_upper(&bytes)))
}

/// Deterministic equivalent of generateMessageIDV2 for a timestamp in seconds.
pub fn id_v2_with(user: Option<&str>, timestamp: u64, random: &[u8; 16]) -> Result<String, Error> {
    let mut data = [0; 44];
    data[..8].copy_from_slice(&timestamp.to_be_bytes());
    if let Some(id) = user.and_then(Jid::parse).filter(|j| !j.user.is_empty()) {
        // Buffer.write uses a UTF-16 character offset for the suffix, but UTF-8 bytes for the user.
        let offset = 8 + id.user.encode_utf16().count();
        if offset > data.len() {
            return Err(Error::Id);
        }
        write_utf8(&mut data[8..], &id.user);
        write_utf8(&mut data[offset..], "@c.us");
    }
    data[28..].copy_from_slice(random);
    Ok(format!("3EB0{}", hex_upper(&crypto::sha256(&data)[..9])))
}

fn write_utf8(destination: &mut [u8], text: &str) {
    let mut length = text.len().min(destination.len());
    while !text.is_char_boundary(length) {
        length -= 1;
    }
    destination[..length].copy_from_slice(&text.as_bytes()[..length]);
}

pub fn generate_id_v2(user: Option<&str>) -> Result<String, Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| crypto::Error::Random)?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Id)?
        .as_secs();
    id_v2_with(user, timestamp, &bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressingContext {
    pub addressing_mode: String,
    pub sender_alt: Option<String>,
    pub recipient_alt: Option<String>,
}
fn attr(node: &BinaryNode, name: &str) -> Option<String> {
    node.attrs.get(name).cloned()
}
fn truthy_attr(node: &BinaryNode, name: &str) -> Option<String> {
    attr(node, name).filter(|s| !s.is_empty())
}
pub fn addressing_context(node: &BinaryNode) -> AddressingContext {
    let sender = truthy_attr(node, "participant").or_else(|| attr(node, "from"));
    let mode = truthy_attr(node, "addressing_mode").unwrap_or_else(|| {
        if sender.is_some_and(|s| s.ends_with("lid")) {
            "lid"
        } else {
            "pn"
        }
        .into()
    });
    let suffix = if mode == "lid" { "pn" } else { "lid" };
    AddressingContext {
        addressing_mode: mode,
        sender_alt: ["participant", "sender", "peer_recipient"]
            .iter()
            .find_map(|prefix| truthy_attr(node, &format!("{prefix}_{suffix}"))),
        recipient_alt: attr(node, &format!("recipient_{suffix}")),
    }
}

/// Runtime key properties absent from the WAProto MessageKey schema.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct KeyMetadata {
    pub remote_jid_alt: Option<String>,
    pub remote_jid_username: Option<String>,
    pub participant_alt: Option<String>,
    pub participant_username: Option<String>,
    pub addressing_mode: String,
    pub is_view_once: bool,
    #[serde(alias = "server_id")]
    pub server_id: Option<String>,
}
impl KeyMetadata {
    const NAMES: [&str; 7] = [
        "remoteJidAlt",
        "remoteJidUsername",
        "participantAlt",
        "participantUsername",
        "addressingMode",
        "isViewOnce",
        "server_id",
    ];
    /// Write the properties onto a JSON message key as upstream's runtime
    /// key carries them: unset ones are absent, `isViewOnce` appears only
    /// when true and the newsletter id keeps its stanza name `server_id`.
    pub fn apply(&self, key: &mut serde_json::Value) {
        let Some(key) = key.as_object_mut() else {
            return;
        };
        let text = [
            &self.remote_jid_alt,
            &self.remote_jid_username,
            &self.participant_alt,
            &self.participant_username,
        ];
        for (name, value) in Self::NAMES.iter().zip(text) {
            if let Some(value) = value {
                key.insert((*name).into(), value.clone().into());
            }
        }
        if !self.addressing_mode.is_empty() {
            key.insert("addressingMode".into(), self.addressing_mode.clone().into());
        }
        if self.is_view_once {
            key.insert("isViewOnce".into(), true.into());
        }
        if let Some(id) = &self.server_id {
            key.insert("server_id".into(), id.clone().into());
        }
    }
    /// The properties `apply` wrote onto a JSON key; `None` when it has none.
    pub fn from_key(key: &serde_json::Value) -> Option<Self> {
        let fields = key.as_object()?;
        if !Self::NAMES.iter().any(|name| fields.contains_key(*name)) {
            return None;
        }
        serde_json::from_value(key.clone()).ok()
    }
}

/// Runtime receive properties associated with a protobuf message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EnvelopeMetadata {
    pub key_metadata: KeyMetadata,
    pub category: Option<String>,
    pub retry_count: Option<u32>,
    pub author: String,
    pub sender: String,
}
impl From<&Envelope> for EnvelopeMetadata {
    fn from(envelope: &Envelope) -> Self {
        Self {
            key_metadata: envelope.key_metadata.clone(),
            category: envelope.category.clone(),
            retry_count: envelope.retry_count,
            author: envelope.author.clone(),
            sender: envelope.sender.clone(),
        }
    }
}
#[derive(Clone, PartialEq)]
pub struct Envelope {
    pub message: proto::WebMessageInfo,
    pub key_metadata: KeyMetadata,
    pub category: Option<String>,
    pub retry_count: Option<u32>,
    pub author: String,
    pub sender: String,
}
impl std::fmt::Debug for Envelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Envelope")
            .field("key", &self.message.key)
            .field("author", &self.author)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}

pub fn decode_node(node: &BinaryNode, me: &str, me_lid: &str) -> Result<Envelope, Error> {
    let id = truthy_attr(node, "id").ok_or(Error::Envelope("missing id"))?;
    let from = truthy_attr(node, "from").ok_or(Error::Envelope("missing from"))?;
    let jid = Jid::parse(&from).ok_or(Error::Envelope("unknown message type"))?;
    let participant = attr(node, "participant");
    let recipient = truthy_attr(node, "recipient");
    let is_me = |s: &str| jid::are_same_user(s, me) || jid::are_same_user(s, me_lid);
    let (chat, author, from_me, is_chat) = if jid.is_pn_user()
        || jid.is_lid_user()
        || jid.is_hosted_pn_user()
        || jid.is_hosted_lid_user()
    {
        let chat = if let Some(recipient) =
            recipient.filter(|s| !Jid::parse(s).is_some_and(|j| j.is_meta_ai()))
        {
            if !is_me(&from) {
                return Err(Error::Envelope(
                    "recipient present, but message not from me",
                ));
            }
            recipient
        } else {
            from.clone()
        };
        (chat, from.clone(), is_me(&from), true)
    } else if jid.is_group() || jid.is_broadcast() {
        let author = participant
            .clone()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Envelope("missing participant"))?;
        // Broadcast source matching intentionally only uses the PN identity.
        let from_me = if jid.is_group() {
            is_me(&author)
        } else {
            jid::are_same_user(&author, me)
        };
        (from.clone(), author, from_me, false)
    } else if jid.is_newsletter() {
        (from.clone(), from.clone(), is_me(&from), false)
    } else {
        return Err(Error::Envelope("unknown message type"));
    };
    let context = addressing_context(node);
    let group = Jid::parse(&chat).is_some_and(|j| j.is_group());
    // Rust's uint64 protobuf timestamp cannot represent JavaScript NaN/negative values.
    let timestamp = node
        .attrs
        .get("t")
        .ok_or(Error::Envelope("missing timestamp"))?
        .parse()
        .map_err(|_| Error::Envelope("invalid timestamp"))?;
    let key_metadata = KeyMetadata {
        remote_jid_alt: (!group).then(|| context.sender_alt.clone()).flatten(),
        remote_jid_username: (!group)
            .then(|| {
                truthy_attr(node, "peer_recipient_username")
                    .or_else(|| attr(node, "recipient_username"))
            })
            .flatten(),
        participant_alt: group.then_some(context.sender_alt).flatten(),
        participant_username: participant
            .as_ref()
            .filter(|s| !s.is_empty())
            .and_then(|_| attr(node, "participant_username")),
        addressing_mode: context.addressing_mode,
        server_id: jid
            .is_newsletter()
            .then(|| truthy_attr(node, "server_id"))
            .flatten(),
        ..Default::default()
    };
    Ok(Envelope {
        message: proto::WebMessageInfo {
            key: Some(proto::MessageKey {
                remote_jid: Some(chat.clone()),
                from_me: Some(from_me),
                id: Some(id),
                participant,
            }),
            message_timestamp: Some(timestamp),
            push_name: attr(node, "notify"),
            broadcast: Some(jid.is_broadcast()),
            status: from_me.then_some(proto::web_message_info::Status::ServerAck as i32),
            ..Default::default()
        },
        key_metadata,
        category: attr(node, "category"),
        retry_count: None,
        author: author.clone(),
        sender: if is_chat { author } else { chat },
    })
}

/// Synchronous decryption backend. Call from a blocking executor in async applications.
pub trait Decryptor {
    /// Resolve the author first; return nonfatal mapping/migration diagnostics.
    fn prepare_envelope(
        &self,
        _node: &BinaryNode,
        _author: &str,
        _encrypted: bool,
    ) -> Result<Vec<Error>, Error> {
        Ok(Vec::new())
    }
    fn decrypt_direct(
        &self,
        author: &str,
        kind: signal::direct::MessageType,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error>;
    fn decrypt_group(
        &self,
        sender: &str,
        author: &str,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error>;
    fn process_distribution(
        &self,
        author: &str,
        distribution: &proto::message::SenderKeyDistributionMessage,
    ) -> Result<(), Error>;
}

/// Learns PN/LID envelope mappings and migrates device-list-backed open sessions.
pub struct SignalDecryptor {
    pub direct: signal::direct::Repository,
    pub group: signal::group::Repository,
}
impl SignalDecryptor {
    pub fn new(store: crate::auth::SqliteAuthStore) -> Self {
        Self {
            direct: signal::direct::Repository::new(store.clone()),
            group: signal::group::Repository::new(store),
        }
    }
}
impl Decryptor for SignalDecryptor {
    fn prepare_envelope(
        &self,
        node: &BinaryNode,
        author: &str,
        encrypted: bool,
    ) -> Result<Vec<Error>, Error> {
        let Some(jid) = Jid::parse(author) else {
            return Ok(Vec::new());
        };
        if jid.is_lid_user() || jid.is_hosted_lid_user() {
            return Ok(Vec::new());
        }
        let mapping = self.direct.lid_mapping();
        let existing = mapping.get_lid_for_pn(author)?;
        let Some(alt) = addressing_context(node)
            .sender_alt
            .filter(|s| Jid::parse(s).is_some_and(|j| j.is_lid_user()))
        else {
            return Ok(Vec::new());
        };
        if !encrypted || !jid.is_pn_user() || existing.is_some() {
            return Ok(Vec::new());
        }
        // Upstream persists the mapping before migration and retains it if migration fails.
        let result = (|| {
            mapping.store_mappings(&[signal::mapping::Mapping {
                pn: author.into(),
                lid: alt.clone(),
            }])?;
            self.direct.migrate_session(author, &alt)?;
            Ok::<_, signal::Error>(())
        })();
        Ok(result.err().map(Error::Signal).into_iter().collect())
    }
    fn decrypt_direct(
        &self,
        author: &str,
        kind: signal::direct::MessageType,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        Ok(self.direct.decrypt(author, kind, ciphertext)?)
    }
    fn decrypt_group(
        &self,
        sender: &str,
        author: &str,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        Ok(self
            .group
            .decrypt(&signal::SenderKeyName::new(sender, author)?, ciphertext)?)
    }
    fn process_distribution(
        &self,
        author: &str,
        distribution: &proto::message::SenderKeyDistributionMessage,
    ) -> Result<(), Error> {
        let group = distribution
            .group_id
            .as_deref()
            .ok_or(Error::Envelope("missing distribution group"))?;
        let bytes = distribution
            .axolotl_sender_key_distribution_message
            .as_deref()
            .ok_or(Error::Envelope("missing distribution bytes"))?;
        Ok(self
            .group
            .process_distribution(&signal::SenderKeyName::new(group, author)?, bytes)?)
    }
}

/// Distribution failures are reported separately; source behavior keeps the decoded content.
#[derive(Debug)]
pub struct Decrypted {
    pub envelope: Envelope,
    pub distribution_errors: Vec<Error>,
    pub mapping_errors: Vec<Error>,
}

pub fn decrypt_node(
    node: &BinaryNode,
    me: &str,
    me_lid: &str,
    repository: &impl Decryptor,
) -> Result<Decrypted, Error> {
    let mut envelope = decode_node(node, me, me_lid)?;
    let mut decryptables = 0;
    let mut distribution_errors = Vec::new();
    let mut mapping_errors = Vec::new();
    for child in node.all_children() {
        if child.tag == "unavailable" && child.attrs.get("type").is_some_and(|s| s == "view_once") {
            envelope.key_metadata.is_view_once = true;
        }
        if child.tag == "enc"
            && let Some(count) = truthy_attr(child, "count")
        {
            envelope.retry_count = Some(
                count
                    .parse()
                    .map_err(|_| Error::Envelope("invalid retry count"))?,
            );
        }
        let Some(NodeContent::Bytes(bytes)) = &child.content else {
            continue;
        };
        if child.tag == "verified_name" {
            let certificate = proto::VerifiedNameCertificate::decode(bytes.as_slice())?;
            let details = certificate
                .details
                .ok_or(Error::Envelope("missing verified name details"))?;
            envelope.message.verified_biz_name =
                proto::verified_name_certificate::Details::decode(details.as_slice())?
                    .verified_name;
        }
        if child.tag != "enc" && child.tag != "plaintext" {
            continue;
        }
        decryptables += 1;
        mapping_errors.extend(repository.prepare_envelope(
            node,
            &envelope.author,
            child.tag != "plaintext",
        )?);
        let result = (|| {
            let plaintext = child.tag == "plaintext"
                || child.attrs.get("type").is_some_and(|s| s == "plaintext");
            let buffer = if plaintext {
                bytes.clone()
            } else {
                match child.attrs.get("type").map(String::as_str) {
                    Some("skmsg") => {
                        repository.decrypt_group(&envelope.sender, &envelope.author, bytes)?
                    }
                    Some("pkmsg") => repository.decrypt_direct(
                        &envelope.author,
                        signal::direct::MessageType::PreKey,
                        bytes,
                    )?,
                    Some("msg") => repository.decrypt_direct(
                        &envelope.author,
                        signal::direct::MessageType::Signal,
                        bytes,
                    )?,
                    _ => return Err(Error::EncryptionType),
                }
            };
            let payload = if plaintext {
                buffer.as_slice()
            } else {
                unpad(&buffer)?
            };
            let mut message = proto::Message::decode(payload)?;
            if let Some(inner) = message
                .device_sent_message
                .as_mut()
                .and_then(|d| d.message.take())
            {
                message = *inner;
            }
            if let Some(distribution) = &message.sender_key_distribution_message
                && let Err(error) = repository.process_distribution(&envelope.author, distribution)
            {
                distribution_errors.push(error);
            }
            if let Some(existing) = &mut envelope.message.message {
                merge_message(existing, message);
            } else {
                envelope.message.message = Some(message);
            }
            Ok::<_, Error>(())
        })();
        if let Err(error) = result {
            let reason = if matches!(error, Error::Signal(signal::Error::MissingPreKey)) {
                incoming::MISSING_KEYS.into()
            } else {
                error.to_string()
            };
            ciphertext_stub(&mut envelope, reason);
        }
    }
    if decryptables == 0 && !envelope.key_metadata.is_view_once {
        ciphertext_stub(&mut envelope, "Message absent from node".into());
    }
    Ok(Decrypted {
        envelope,
        distribution_errors,
        mapping_errors,
    })
}
fn ciphertext_stub(envelope: &mut Envelope, error: String) {
    envelope.message.message_stub_type = Some(proto::web_message_info::StubType::Ciphertext as i32);
    envelope.message.message_stub_parameters = vec![error];
}

include!("messages_merge.rs");
