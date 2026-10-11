//! Common chat, protocol and reaction effects for generated and received messages.
use crate::{jid, messages, proto, relay};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertKind {
    Append,
    Notify,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Upsert {
    pub messages: Vec<proto::WebMessageInfo>,
    pub kind: UpsertKind,
    pub request_id: Option<String>,
    /// Runtime receive properties in message order; empty for generated own events.
    pub metadata: Vec<messages::EnvelopeMetadata>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Revoke {
        key: proto::MessageKey,
        original_key: proto::MessageKey,
    },
    Edit {
        key: proto::MessageKey,
        message: Box<proto::Message>,
        timestamp: f64,
    },
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReactionUpdate {
    pub key: Option<proto::MessageKey>,
    pub reaction: proto::Reaction,
}
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ChatUpdate {
    pub id: String,
    pub messages: Option<Vec<proto::WebMessageInfo>>,
    pub conversation_timestamp: Option<f64>,
    /// An unread-count delta, matching the upstream buffered chat update.
    pub unread_count: Option<i64>,
    pub archived: Option<bool>,
    pub read_only: Option<bool>,
    pub ephemeral_setting_timestamp: Option<f64>,
    /// None means unchanged; Some(None) disables expiration.
    pub ephemeral_expiration: Option<Option<u32>>,
}
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Effects {
    pub chat: Option<ChatUpdate>,
    pub mutation: Option<Mutation>,
    pub reaction: Option<ReactionUpdate>,
}
/// Derive chat and supported protocol/reaction effects for our outgoing envelopes.
/// History/app-state and encrypted response processing belongs to the receive pipeline.
pub fn effects(
    message: &proto::WebMessageInfo,
    unarchive: bool,
) -> Result<Effects, messages::Error> {
    let key = message
        .key
        .as_ref()
        .ok_or(messages::Error::Envelope("missing own message key"))?;
    if key.from_me != Some(true) {
        return Err(messages::Error::Envelope("expected own message"));
    }
    effects_received(message, unarchive)
}
/// Shared chat, edit, revoke and reaction effects after received-key cleanup.
pub fn effects_received(
    message: &proto::WebMessageInfo,
    unarchive: bool,
) -> Result<Effects, messages::Error> {
    let key = message
        .key
        .as_ref()
        .ok_or(messages::Error::Envelope("missing message key"))?;
    let remote = messages::incoming::chat_id(key)?;
    let content = relay::normalize(message.message.as_ref());
    let real = messages::incoming::is_real(message);
    let mut chat = ChatUpdate {
        id: jid::normalized_user(remote),
        ..Default::default()
    };
    let timestamp = message.message_timestamp.unwrap_or(0) as f64;
    if real {
        chat.messages = Some(vec![message.clone()]);
        chat.conversation_timestamp = Some(timestamp);
        if messages::incoming::increments_unread(message) {
            chat.unread_count = Some(1);
        }
    }
    if unarchive
        && (real
            || content
                .and_then(|m| m.reaction_message.as_ref())
                .and_then(|r| r.key.as_ref())
                .and_then(|k| k.from_me)
                == Some(true))
    {
        chat.archived = Some(false);
        chat.read_only = Some(false);
    }
    let mut result = Effects::default();
    if let Some(protocol) = content.and_then(|m| m.protocol_message.as_deref()) {
        use proto::message::protocol_message::Type;
        match protocol.r#type.and_then(|t| Type::try_from(t).ok()) {
            Some(Type::Revoke) => {
                let original = protocol
                    .key
                    .as_ref()
                    .ok_or(messages::Error::Envelope("missing revoke key"))?;
                let mut target = key.clone();
                target.id = original.id.clone();
                result.mutation = Some(Mutation::Revoke {
                    key: target,
                    original_key: key.clone(),
                });
            }
            Some(Type::MessageEdit) => {
                let mut target = key.clone();
                target.id = protocol.key.as_ref().and_then(|k| k.id.clone());
                result.mutation = Some(Mutation::Edit {
                    key: target,
                    message: Box::new(proto::Message {
                        edited_message: Some(Box::new(proto::message::FutureProofMessage {
                            message: protocol.edited_message.clone(),
                        })),
                        ..Default::default()
                    }),
                    // Explicit zero is a truthy protobufjs Long; absent timestamps still fall back.
                    timestamp: protocol
                        .timestamp_ms
                        .map(|v| (v as f64 / 1000.0).floor())
                        .unwrap_or(timestamp),
                });
            }
            Some(Type::EphemeralSetting) => {
                chat.ephemeral_setting_timestamp = Some(timestamp);
                chat.ephemeral_expiration = Some(protocol.ephemeral_expiration.filter(|v| *v != 0));
            }
            _ => {}
        }
    } else if let Some(reaction) = content.and_then(|m| m.reaction_message.as_ref()) {
        result.reaction = Some(ReactionUpdate {
            key: reaction.key.clone(),
            reaction: proto::Reaction {
                key: Some(key.clone()),
                text: reaction.text.clone(),
                grouping_key: reaction.grouping_key.clone(),
                sender_timestamp_ms: reaction.sender_timestamp_ms,
                unread: None,
            },
        });
    }
    if chat.messages.is_some() || chat.archived.is_some() || chat.ephemeral_expiration.is_some() {
        result.chat = Some(chat);
    }
    Ok(result)
}
