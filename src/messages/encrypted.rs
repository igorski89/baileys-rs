//! Secret-derived encrypted poll votes and event responses.
use super::{Error, KeyMetadata};
use crate::{crypto, proto};
use prost::Message as _;
use zeroize::Zeroizing;
#[derive(Clone, Copy)]
pub struct PollContext<'a> {
    pub poll_creator_jid: &'a str,
    pub poll_msg_id: &'a str,
    pub poll_enc_key: &'a [u8],
    pub voter_jid: &'a str,
}
#[derive(Clone, Copy)]
pub struct EventContext<'a> {
    pub event_creator_jid: &'a str,
    pub event_msg_id: &'a str,
    pub event_enc_key: &'a [u8],
    pub responder_jid: &'a str,
}
impl std::fmt::Debug for PollContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollContext")
            .field("poll_creator_jid", &self.poll_creator_jid)
            .field("poll_msg_id", &self.poll_msg_id)
            .field("voter_jid", &self.voter_jid)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for EventContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventContext")
            .field("event_creator_jid", &self.event_creator_jid)
            .field("event_msg_id", &self.event_msg_id)
            .field("responder_jid", &self.responder_jid)
            .finish_non_exhaustive()
    }
}
/// The AES-GCM key and additional data of a secret-derived payload.
fn secret_key(
    secret: &[u8],
    id: &str,
    creator: &str,
    actor: &str,
    label: &str,
) -> (Zeroizing<[u8; 32]>, Vec<u8>) {
    let key0 = Zeroizing::new(crypto::hmac_sha256(secret, &[0; 32]));
    let mut sign = Vec::new();
    for part in [id, creator, actor, label] {
        sign.extend_from_slice(part.as_bytes());
    }
    sign.push(1);
    let key = Zeroizing::new(crypto::hmac_sha256(&sign, key0.as_ref()));
    let mut aad = id.as_bytes().to_vec();
    aad.push(0);
    aad.extend_from_slice(actor.as_bytes());
    (key, aad)
}
fn decrypt(
    payload: &[u8],
    iv: &[u8],
    secret: &[u8],
    id: &str,
    creator: &str,
    actor: &str,
    label: &str,
) -> Result<Zeroizing<Vec<u8>>, Error> {
    let (key, aad) = secret_key(secret, id, creator, actor, label);
    Ok(Zeroizing::new(crypto::aes_decrypt_gcm(
        payload,
        key.as_ref(),
        iv,
        &aad,
    )?))
}
/// Encrypt a vote as the voter's client does, with a random 12-byte IV;
/// the inverse of [`decrypt_poll_vote`]. Upstream has no counterpart.
pub fn encrypt_poll_vote(
    vote: &proto::message::PollVoteMessage,
    ctx: &PollContext<'_>,
) -> Result<proto::message::PollEncValue, Error> {
    let (key, aad) = secret_key(
        ctx.poll_enc_key,
        ctx.poll_msg_id,
        ctx.poll_creator_jid,
        ctx.voter_jid,
        "Poll Vote",
    );
    let mut iv = vec![0; 12];
    getrandom::fill(&mut iv).map_err(|_| crypto::Error::Random)?;
    let plaintext = Zeroizing::new(vote.encode_to_vec());
    let payload = crypto::aes_encrypt_gcm(&plaintext, key.as_ref(), &iv, &aad)?;
    Ok(proto::message::PollEncValue {
        enc_payload: Some(payload),
        enc_iv: Some(iv),
    })
}
pub fn decrypt_poll_vote(
    value: &proto::message::PollEncValue,
    ctx: &PollContext<'_>,
) -> Result<proto::message::PollVoteMessage, Error> {
    let payload = value
        .enc_payload
        .as_deref()
        .ok_or(Error::Envelope("missing encrypted vote payload"))?;
    let iv = value
        .enc_iv
        .as_deref()
        .ok_or(Error::Envelope("missing encrypted vote IV"))?;
    let bytes = decrypt(
        payload,
        iv,
        ctx.poll_enc_key,
        ctx.poll_msg_id,
        ctx.poll_creator_jid,
        ctx.voter_jid,
        "Poll Vote",
    )?;
    Ok(proto::message::PollVoteMessage::decode(bytes.as_slice())?)
}
pub fn decrypt_event_response(
    value: &proto::message::PollEncValue,
    ctx: &EventContext<'_>,
) -> Result<proto::message::EventResponseMessage, Error> {
    let payload = value
        .enc_payload
        .as_deref()
        .ok_or(Error::Envelope("missing encrypted response payload"))?;
    let iv = value
        .enc_iv
        .as_deref()
        .ok_or(Error::Envelope("missing encrypted response IV"))?;
    let bytes = decrypt(
        payload,
        iv,
        ctx.event_enc_key,
        ctx.event_msg_id,
        ctx.event_creator_jid,
        ctx.responder_jid,
        "Event Response",
    )?;
    Ok(proto::message::EventResponseMessage::decode(
        bytes.as_slice(),
    )?)
}
/// Upstream author precedence, including runtime PN/LID alternate keys.
pub fn key_author(
    key: Option<&proto::MessageKey>,
    metadata: Option<&KeyMetadata>,
    me: &str,
) -> String {
    let Some(key) = key else {
        return String::new();
    };
    if key.from_me == Some(true) {
        return me.into();
    }
    metadata
        .and_then(|m| m.participant_alt.as_deref())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            metadata
                .and_then(|m| m.remote_jid_alt.as_deref())
                .filter(|s| !s.is_empty())
        })
        .or(key.participant.as_deref().filter(|s| !s.is_empty()))
        .or(key.remote_jid.as_deref())
        .unwrap_or("")
        .into()
}
#[derive(Debug, Clone, PartialEq)]
pub struct EventResponseUpdate {
    pub key: proto::MessageKey,
    pub event_response_message_key: proto::MessageKey,
    pub sender_timestamp_ms: Option<i64>,
    pub response: proto::message::EventResponseMessage,
}
