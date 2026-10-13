//! Recipient fanout and wire metadata used by the message relay orchestrator.
mod stanza;
mod transaction;
use crate::{
    binary::{BinaryNode, NodeContent},
    crypto,
    jid::Jid,
    messages, proto,
    signal::direct,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use indexmap::IndexMap;
use prost::Message as _;
pub use stanza::*;
pub use transaction::*;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Auth(#[from] crate::auth::Error),
    #[error(transparent)]
    Message(#[from] messages::Error),
    #[error(transparent)]
    Signal(#[from] crate::signal::Error),
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Binary(#[from] crate::binary::Error),
    #[error("invalid recipient JID")]
    Recipient,
    #[error("missing signed device identity")]
    Identity,
    #[error("message patch failed: {0}")]
    Patch(String),
    #[error("All encryptions failed")]
    AllEncryptionsFailed { failures: Vec<RecipientFailure> },
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Auth(e) => e.status_code(),
            Self::Binary(crate::binary::Error::Stanza { code, .. }) => {
                code.and_then(|n| n.try_into().ok())
            }
            Self::AllEncryptionsFailed { .. } => Some(500),
            Self::Signal(e) => e.status_code(),
            _ => None,
        }
    }
}
type PatchCallback =
    dyn Fn(&proto::Message, Option<&[String]>) -> Result<Patch, Error> + Send + Sync;
/// Message patch executed on the relay blocking worker; see [`MessagePatch::new_async`]
/// for a hook that awaits.
/// Group payloads receive `None`; newsletters receive an empty recipient slice.
/// Do not access the same auth store or re-enter relay from this callback.
#[derive(Clone)]
pub struct MessagePatch(std::sync::Arc<PatchCallback>);
impl MessagePatch {
    pub fn new(
        callback: impl Fn(&proto::Message, Option<&[String]>) -> Result<Patch, Error>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self(std::sync::Arc::new(callback))
    }
    /// An asynchronous patch, as upstream allows the hook to return a promise.
    /// It is awaited on the relay blocking worker, so it must not wait on the
    /// relay it is patching.
    pub fn new_async<F, Fut>(callback: F) -> Self
    where
        F: Fn(proto::Message, Option<Vec<String>>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Patch, Error>>,
    {
        Self::new(move |message, recipients| {
            let runtime = tokio::runtime::Handle::try_current()
                .map_err(|_| Error::Patch("no async runtime for message patch".into()))?;
            runtime.block_on(callback(
                message.clone(),
                recipients.map(<[String]>::to_vec),
            ))
        })
    }
    pub fn apply(
        &self,
        message: &proto::Message,
        recipients: Option<&[String]>,
    ) -> Result<Patch, Error> {
        (self.0)(message, recipients)
    }
}
impl Default for MessagePatch {
    fn default() -> Self {
        Self::new(|message, _| Ok(Patch::Shared(Box::new(message.clone()))))
    }
}
impl std::fmt::Debug for MessagePatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessagePatch").finish_non_exhaustive()
    }
}
pub struct PatchedMessage {
    pub recipient_jid: Option<String>,
    pub message: proto::Message,
}
pub enum Patch {
    Shared(Box<proto::Message>),
    PerRecipient(Vec<PatchedMessage>),
}
/// Upstream selects the per-recipient wrapper itself for reporting. That wrapper
/// has no root message context, so per-recipient patches suppress report tokens.
pub fn reporting_patch_message(patch: Patch) -> Option<proto::Message> {
    match patch {
        Patch::Shared(message) => Some(*message),
        Patch::PerRecipient(_) => None,
    }
}
#[derive(Debug)]
pub struct RecipientFailure {
    pub jid: String,
    pub error: Error,
}
#[derive(Debug, Default)]
pub struct ParticipantNodes {
    pub nodes: Vec<BinaryNode>,
    pub should_include_device_identity: bool,
    pub failures: Vec<RecipientFailure>,
}
pub trait Encryptor {
    fn encrypt(&self, jid: &str, data: &[u8]) -> Result<direct::Encrypted, Error>;
}
impl Encryptor for direct::Repository {
    fn encrypt(&self, jid: &str, data: &[u8]) -> Result<direct::Encrypted, Error> {
        Ok(self.encrypt(jid, data)?)
    }
}

/// Unwrap at most five future-proof layers, in the upstream priority order.
pub fn normalize(mut message: Option<&proto::Message>) -> Option<&proto::Message> {
    for _ in 0..5 {
        let content = message?;
        let wrapper = content
            .ephemeral_message
            .as_deref()
            .or(content.view_once_message.as_deref())
            .or(content.document_with_caption_message.as_deref())
            .or(content.view_once_message_v2.as_deref())
            .or(content.view_once_message_v2_extension.as_deref())
            .or(content.edited_message.as_deref())
            .or(content.associated_child_message.as_deref())
            .or(content.group_status_message.as_deref())
            .or(content.group_status_message_v2.as_deref());
        let Some(wrapper) = wrapper else {
            break;
        };
        message = wrapper.message.as_deref();
    }
    message
}
/// Raw payload classification: wrapping is intentionally the caller's choice.
pub fn media_type(message: &proto::Message) -> &'static str {
    if message.image_message.is_some() {
        "image"
    } else if let Some(video) = &message.video_message {
        if video.gif_playback == Some(true) {
            "gif"
        } else {
            "video"
        }
    } else if let Some(audio) = &message.audio_message {
        if audio.ptt == Some(true) {
            "ptt"
        } else {
            "audio"
        }
    } else if message.contact_message.is_some() {
        "vcard"
    } else if message.document_message.is_some() {
        "document"
    } else if message.contacts_array_message.is_some() {
        "contact_array"
    } else if message.live_location_message.is_some() {
        "livelocation"
    } else if message.sticker_message.is_some() {
        "sticker"
    } else if message.list_message.is_some() {
        "list"
    } else if message.list_response_message.is_some() {
        "list_response"
    } else if message.buttons_response_message.is_some() {
        "buttons_response"
    } else if message.order_message.is_some() {
        "order"
    } else if message.product_message.is_some() {
        "product"
    } else if message.interactive_response_message.is_some() {
        "native_flow_response"
    } else if message.group_invite_message.is_some() {
        "url"
    } else {
        ""
    }
}
pub fn message_type(message: Option<&proto::Message>) -> &'static str {
    let Some(message) = normalize(message) else {
        return "text";
    };
    if message.reaction_message.is_some() || message.enc_reaction_message.is_some() {
        "reaction"
    } else if message.poll_creation_message.is_some()
        || message.poll_creation_message_v2.is_some()
        || message.poll_creation_message_v3.is_some()
        || message.poll_update_message.is_some()
    {
        "poll"
    } else if message.event_message.is_some() {
        "event"
    } else if !media_type(message).is_empty() {
        "media"
    } else {
        "text"
    }
}
pub fn participant_hash_v2(participants: &mut [String]) -> String {
    participants.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
    let hash = STANDARD.encode(crypto::sha256(participants.join("").as_bytes()));
    format!("2:{}", &hash[..6])
}
pub fn device_sent_message(destination: &str, message: &proto::Message) -> proto::Message {
    proto::Message {
        device_sent_message: Some(Box::new(proto::message::DeviceSentMessage {
            destination_jid: Some(destination.into()),
            message: Some(Box::new(message.clone())),
            phash: None,
        })),
        message_context_info: message.message_context_info.clone(),
        ..Default::default()
    }
}
pub fn encode_device_identity(
    account: &proto::AdvSignedDeviceIdentity,
    include_signature_key: bool,
) -> Vec<u8> {
    let mut account = account.clone();
    if !include_signature_key
        || account
            .account_signature_key
            .as_ref()
            .is_none_or(Vec::is_empty)
    {
        account.account_signature_key = None;
    }
    account.encode_to_vec()
}
pub fn encode_newsletter_message(message: &proto::Message) -> Vec<u8> {
    message.encode_to_vec()
}

pub struct Fanout<'a> {
    pub recipients: &'a [String],
    pub message: &'a proto::Message,
    pub me: &'a str,
    pub me_lid: Option<&'a str>,
    pub extra_attrs: &'a IndexMap<String, String>,
    pub device_sent_message: Option<&'a proto::Message>,
}
pub fn create_participant_nodes(
    input: Fanout<'_>,
    repository: &impl Encryptor,
) -> Result<ParticipantNodes, Error> {
    create_participant_nodes_patched(input, repository, |message, _| {
        Ok(Patch::Shared(Box::new(message.clone())))
    })
}
pub fn create_participant_nodes_patched(
    input: Fanout<'_>,
    repository: &impl Encryptor,
    patch: impl FnOnce(&proto::Message, &[String]) -> Result<Patch, Error>,
) -> Result<ParticipantNodes, Error> {
    create_participant_nodes_with(input, repository, patch, || {
        let mut byte = [0];
        getrandom::fill(&mut byte).map_err(|_| crypto::Error::Random)?;
        Ok(byte[0])
    })
}
/// Patches once and supports per-recipient payloads. Own-device DSM replaces the patch,
/// except for the exact sender device. Partial failures preserve other recipients.
/// Database-backed encryptors serialize ratchet counters; invoke on a blocking executor.
pub fn create_participant_nodes_with(
    input: Fanout<'_>,
    repository: &impl Encryptor,
    patch: impl FnOnce(&proto::Message, &[String]) -> Result<Patch, Error>,
    mut random_byte: impl FnMut() -> Result<u8, crypto::Error>,
) -> Result<ParticipantNodes, Error> {
    let mut result = ParticipantNodes {
        nodes: vec![],
        should_include_device_identity: false,
        failures: vec![],
    };
    if input.recipients.is_empty() {
        return Ok(result);
    }
    let patch = patch(input.message, input.recipients)?;
    let patched: Vec<_> = match &patch {
        Patch::Shared(message) => input
            .recipients
            .iter()
            .map(|jid| (Some(jid.as_str()), message.as_ref()))
            .collect(),
        Patch::PerRecipient(messages) => messages
            .iter()
            .map(|item| (item.recipient_jid.as_deref(), &item.message))
            .collect(),
    };
    for (jid, patched_message) in patched {
        let Some(jid) = jid.filter(|j| !j.is_empty()) else {
            continue;
        };
        let encryption = (|| {
            let mut message = patched_message;
            if let Some(dsm) = input.device_sent_message {
                let target = Jid::parse(jid).ok_or(Error::Recipient)?;
                let own_pn = Jid::parse(input.me).ok_or(Error::Recipient)?;
                let own_lid = input.me_lid.and_then(Jid::parse);
                let own =
                    target.user == own_pn.user || own_lid.is_some_and(|me| me.user == target.user);
                let exact = jid == input.me || input.me_lid == Some(jid);
                if own && !exact {
                    message = dsm;
                }
            }
            let bytes = messages::pad_with_byte(&message.encode_to_vec(), random_byte()?);
            let encrypted = repository.encrypt(jid, &bytes)?;
            if encrypted.kind == direct::MessageType::PreKey {
                result.should_include_device_identity = true;
            }
            let mut enc = BinaryNode::new("enc").attr("v", "2").attr(
                "type",
                if encrypted.kind == direct::MessageType::PreKey {
                    "pkmsg"
                } else {
                    "msg"
                },
            );
            enc.attrs.extend(input.extra_attrs.clone());
            Ok::<_, Error>(BinaryNode::new("to").attr("jid", jid).with_content(
                NodeContent::Children(vec![
                    enc.with_content(NodeContent::Bytes(encrypted.ciphertext)),
                ]),
            ))
        })();
        match encryption {
            Ok(node) => result.nodes.push(node),
            Err(error) => result.failures.push(RecipientFailure {
                jid: jid.into(),
                error,
            }),
        }
    }
    if result.nodes.is_empty() {
        return Err(Error::AllEncryptionsFailed {
            failures: result.failures,
        });
    }
    Ok(result)
}

type LookupFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Option<proto::Message>, Error>> + Send>,
>;
type LookupCallback = dyn Fn(proto::MessageKey) -> LookupFuture + Send + Sync;
/// Async fallback for retry messages absent from the recent-message cache.
#[derive(Clone)]
pub struct MessageLookup(std::sync::Arc<LookupCallback>);
impl MessageLookup {
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn(proto::MessageKey) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<proto::Message>, Error>> + Send + 'static,
    {
        Self(std::sync::Arc::new(move |key| Box::pin(callback(key))))
    }
    pub async fn get(&self, key: proto::MessageKey) -> Result<Option<proto::Message>, Error> {
        (self.0)(key).await
    }
}
impl Default for MessageLookup {
    fn default() -> Self {
        Self::new(|_| async { Ok(None) })
    }
}
impl std::fmt::Debug for MessageLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageLookup").finish_non_exhaustive()
    }
}
