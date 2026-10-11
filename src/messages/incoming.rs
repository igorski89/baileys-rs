//! Incoming message cleanup and receive policy, independent of socket I/O.
use super::{Envelope, Error, generation};
use crate::{binary::BinaryNode, jid, proto, receipts};
pub const NO_MESSAGE: &str = "Message absent from node";
pub const MISSING_KEYS: &str = "Key used already or never filled";
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    Drop {
        nack: Option<u32>,
    },
    Placeholder,
    Retry,
    Newsletter,
    Delivery {
        kind: Option<receipts::Type>,
        participant: Option<String>,
        history: bool,
    },
}
/// Choose source acknowledgements/retries after decryption, before cleanup.
pub fn plan(envelope: &Envelope, node: &BinaryNode, active: bool, now: u64) -> Plan {
    if node
        .child("enc")
        .is_some_and(|n| n.attrs.get("type").is_some_and(|t| t == "msmsg"))
    {
        return Plan::Drop { nack: Some(495) };
    }
    let msg = &envelope.message;
    let key = msg.key.as_ref();
    let remote = key.and_then(|k| k.remote_jid.as_deref()).unwrap_or("");
    let peer = envelope.category.as_deref() == Some("peer");
    if msg.message_stub_type == Some(proto::web_message_info::StubType::Ciphertext as i32) && !peer
    {
        let reason = msg.message_stub_parameters.first().map(String::as_str);
        if reason == Some(MISSING_KEYS) {
            return Plan::Drop { nack: Some(487) };
        }
        let age = now as i128 - i128::from(msg.message_timestamp.unwrap_or(0));
        if reason == Some(NO_MESSAGE) {
            let excluded = node
                .child("unavailable")
                .and_then(|n| n.attrs.get("type"))
                .is_some_and(|s| {
                    matches!(
                        s.as_str(),
                        "bot_unavailable_fanout"
                            | "hosted_unavailable_fanout"
                            | "view_once_unavailable_fanout"
                    )
                });
            if excluded || age > 14 * 86400 {
                return Plan::Drop { nack: None };
            }
            return Plan::Placeholder;
        }
        if remote == "status@broadcast" && age > 86400 {
            return Plan::Drop { nack: None };
        }
        return Plan::Retry;
    }
    if remote.ends_with("@newsletter") {
        return Plan::Newsletter;
    }
    let from_me = key.and_then(|k| k.from_me) == Some(true);
    let participant = if !peer
        && from_me
        && (remote.ends_with("@lid")
            || envelope
                .key_metadata
                .remote_jid_alt
                .as_deref()
                .is_some_and(|s| s.ends_with("@lid")))
    {
        Some(envelope.author.clone())
    } else {
        key.and_then(|k| k.participant.clone())
    };
    let kind = if peer {
        Some(receipts::Type::PeerMessage)
    } else if from_me {
        Some(receipts::Type::Sender)
    } else if !active {
        Some(receipts::Type::Inactive)
    } else {
        None
    };
    let history = crate::relay::normalize(msg.message.as_ref())
        .and_then(|m| m.protocol_message.as_deref())
        .and_then(|p| p.history_sync_notification.as_ref())
        .is_some();
    Plan::Delivery {
        kind,
        participant,
        history,
    }
}
fn clean_jid(value: Option<&str>) -> String {
    if let Some(j) = value.and_then(jid::Jid::parse)
        && matches!(j.server.as_str(), "hosted" | "hosted.lid")
    {
        return format!(
            "{}@{}",
            j.user,
            if j.server == "hosted" {
                "s.whatsapp.net"
            } else {
                "lid"
            }
        );
    }
    value.map(jid::normalized_user).unwrap_or_default()
}
/// Normalize received addresses and reaction/poll targets to our perspective.
pub fn clean(message: &mut proto::WebMessageInfo, me: &str, lid: &str) -> Result<(), Error> {
    let key = message
        .key
        .as_mut()
        .ok_or(Error::Envelope("missing received key"))?;
    key.remote_jid = Some(clean_jid(key.remote_jid.as_deref()));
    key.participant = Some(clean_jid(key.participant.as_deref()));
    let from_me = key.from_me == Some(true);
    let remote = key.remote_jid.clone();
    let participant = key.participant.clone();
    if let Some(content) = message
        .message
        .as_mut()
        .and_then(|m| generation::normalize_mut(m))
    {
        let normalize_key = |target: &mut proto::MessageKey| {
            if !from_me {
                let sender = target
                    .participant
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .or(target.remote_jid.as_deref())
                    .unwrap_or("");
                target.from_me = Some(
                    target.from_me != Some(true)
                        && (jid::are_same_user(sender, me) || jid::are_same_user(sender, lid)),
                );
                target.remote_jid = remote.clone();
                if target.participant.as_deref().is_none_or(|s| s.is_empty()) {
                    target.participant = participant.clone();
                }
            }
        };
        if let Some(reaction) = content.reaction_message.as_mut() {
            normalize_key(
                reaction
                    .key
                    .as_mut()
                    .ok_or(Error::Envelope("missing reaction key"))?,
            );
        }
        if let Some(poll) = content.poll_update_message.as_mut() {
            normalize_key(
                poll.poll_creation_message_key
                    .as_mut()
                    .ok_or(Error::Envelope("missing poll key"))?,
            );
        }
    }
    Ok(())
}
pub fn chat_id(key: &proto::MessageKey) -> Result<&str, Error> {
    let remote = key
        .remote_jid
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or(Error::Envelope("missing chat JID"))?;
    if remote.ends_with("@broadcast") && remote != "status@broadcast" && key.from_me != Some(true) {
        return key
            .participant
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or(Error::Envelope("missing broadcast participant"));
    }
    Ok(remote)
}
pub fn is_real(message: &proto::WebMessageInfo) -> bool {
    let content = crate::relay::normalize(message.message.as_ref());
    generation::content_type(content).is_some()
        && content.is_some_and(|m| {
            m.protocol_message.is_none()
                && m.reaction_message.is_none()
                && m.poll_update_message.is_none()
        })
}
pub fn increments_unread(message: &proto::WebMessageInfo) -> bool {
    message.key.as_ref().and_then(|k| k.from_me) != Some(true)
        && message.message_stub_type.unwrap_or(0) == 0
}

#[derive(Debug, Clone, PartialEq)]
pub struct StubUpdate {
    pub key: proto::MessageKey,
    pub parameters: Vec<String>,
}
