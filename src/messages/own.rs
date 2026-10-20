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
    /// Message indices for sparse metadata after buffered own/received merges.
    /// Empty means metadata is aligned from index zero.
    pub metadata_indices: Vec<usize>,
    /// Message-aligned runtime overrides (e.g. nullable fields or JS numbers).
    /// Empty when all messages are fully represented by their protobufs.
    pub runtime_fields: Vec<serde_json::Map<String, serde_json::Value>>,
}
impl Upsert {
    /// Protocol JSON with buffered runtime overrides applied.
    pub fn message_value(&self, index: usize) -> Option<serde_json::Value> {
        use crate::proto_json::JsonField;
        let mut value = self.messages.get(index)?.to_value();
        if let Some(runtime) = self.runtime_fields.get(index) {
            value
                .as_object_mut()
                .expect("message object")
                .extend(runtime.clone());
        }
        Some(value)
    }
    pub fn metadata_for(&self, index: usize) -> Option<&messages::EnvelopeMetadata> {
        if self.metadata_indices.is_empty() {
            self.metadata.get(index)
        } else {
            self.metadata_indices
                .iter()
                .position(|i| *i == index)
                .and_then(|position| self.metadata.get(position))
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Revoke {
        key: proto::MessageKey,
        original_key: proto::MessageKey,
        /// Runtime properties of the protocol message's key, which upstream
        /// spreads into both keys.
        key_metadata: Option<messages::KeyMetadata>,
    },
    Edit {
        key: proto::MessageKey,
        message: Box<proto::Message>,
        timestamp: f64,
        key_metadata: Option<messages::KeyMetadata>,
    },
}
/// One consolidated source messages.update entry. Protobuf fields retain their
/// native types; runtime overrides preserve explicit nulls and extension fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub key: proto::MessageKey,
    /// Runtime properties of `key`.
    pub key_metadata: Option<messages::KeyMetadata>,
    pub info: proto::WebMessageInfo,
    pub runtime_fields: serde_json::Map<String, serde_json::Value>,
}
impl Update {
    pub fn update_value(&self) -> serde_json::Value {
        use crate::proto_json::JsonField;
        let mut value = self.info.to_value();
        value
            .as_object_mut()
            .expect("message update object")
            .extend(self.runtime_fields.clone());
        value
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReactionUpdate {
    pub key: Option<proto::MessageKey>,
    pub reaction: proto::Reaction,
    /// Runtime properties of the reactor's key, used for PN/LID deduplication.
    pub key_metadata: Option<messages::KeyMetadata>,
}
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ChatUpdate {
    pub id: String,
    /// Some(None) represents a source-assigned undefined name/description.
    pub name: Option<Option<String>>,
    pub description: Option<Option<String>>,
    pub messages: Option<Vec<proto::WebMessageInfo>>,
    /// Runtime properties of the messages' keys.
    pub message_key_metadata: Option<Box<messages::KeyMetadata>>,
    pub conversation_timestamp: Option<f64>,
    /// An unread-count delta, matching the upstream buffered chat update.
    pub unread_count: Option<i64>,
    pub archived: Option<bool>,
    pub clear_archived: bool,
    pub mute_end_time: Option<Option<f64>>,
    pub pinned: Option<Option<f64>>,
    /// Source null clears the unread count during initial synchronization.
    pub clear_unread_count: bool,
    pub conditional: Option<crate::app_state::ChatCondition>,
    pub read_only: Option<bool>,
    pub ephemeral_setting_timestamp: Option<f64>,
    /// None means unchanged; Some(None) disables expiration.
    pub ephemeral_expiration: Option<Option<u32>>,
    /// Extension fields and runtime values outside the native field ranges.
    pub runtime_fields: Option<Box<serde_json::Map<String, serde_json::Value>>>,
}
/// A chat update's message: a received message is a plain envelope holding a
/// decoded protobufjs message, an own one a protobufjs WebMessageInfo.
fn chat_message(
    message: &proto::WebMessageInfo,
    key_metadata: Option<&messages::KeyMetadata>,
) -> serde_json::Value {
    use crate::proto_json::{self, JsonField};
    let Some(metadata) = key_metadata else {
        return proto_json::protobuf(|| message.to_value());
    };
    let mut value = proto_json::plain(|| message.to_value());
    if let Some(content) = &message.message {
        value["message"] = proto_json::protobuf(|| content.to_value());
    }
    metadata.apply(&mut value["key"]);
    value
}
impl crate::proto_json::JsonField for ChatUpdate {
    fn to_value(&self) -> serde_json::Value {
        use serde_json::json;
        let mut value = json!({"id": self.id});
        let fields = value.as_object_mut().expect("chat update object");
        macro_rules! field {
            ($member:ident, $name:literal) => {
                if let Some(value) = &self.$member {
                    fields.insert($name.into(), json!(value));
                }
            };
        }
        field!(name, "name");
        field!(description, "description");
        field!(conversation_timestamp, "conversationTimestamp");
        field!(unread_count, "unreadCount");
        field!(archived, "archived");
        field!(mute_end_time, "muteEndTime");
        field!(pinned, "pinned");
        field!(read_only, "readOnly");
        field!(ephemeral_setting_timestamp, "ephemeralSettingTimestamp");
        field!(ephemeral_expiration, "ephemeralExpiration");
        if let Some(messages) = &self.messages {
            fields.insert(
                "messages".into(),
                json!(
                    messages
                        .iter()
                        .map(|message| {
                            json!({ "message": chat_message(message, self.message_key_metadata.as_deref()) })
                        })
                        .collect::<Vec<_>>()
                ),
            );
        }
        if self.clear_archived {
            fields.insert("archived".into(), serde_json::Value::Null);
        }
        if self.clear_unread_count {
            fields.insert("unreadCount".into(), serde_json::Value::Null);
        }
        if let Some(runtime_fields) = &self.runtime_fields {
            fields.extend(runtime_fields.as_ref().clone());
        }
        value
    }
    fn from_value(value: &serde_json::Value) -> Result<Self, crate::proto_json::Error> {
        use crate::proto_json::{Error, object};
        let mut fields = object(value)?.clone();
        let id = fields
            .remove("id")
            .and_then(|v| v.as_str().map(str::to_owned))
            .ok_or(Error::Input("chat update id"))?;
        let mut result = Self {
            id,
            ..Default::default()
        };
        macro_rules! field {
            ($member:ident, $name:literal, $parse:expr) => {
                if let Some(value) = fields.get($name)
                    && let Some(parsed) = ($parse)(value)
                {
                    result.$member = Some(parsed);
                    fields.remove($name);
                }
            };
        }
        fn text(v: &serde_json::Value) -> Option<Option<String>> {
            if v.is_null() {
                Some(None)
            } else {
                v.as_str().map(|s| Some(s.into()))
            }
        }
        fn number(v: &serde_json::Value) -> Option<Option<f64>> {
            if v.is_null() {
                Some(None)
            } else {
                v.as_f64().map(Some)
            }
        }
        field!(name, "name", text);
        field!(description, "description", text);
        field!(
            conversation_timestamp,
            "conversationTimestamp",
            serde_json::Value::as_f64
        );
        field!(archived, "archived", serde_json::Value::as_bool);
        field!(read_only, "readOnly", serde_json::Value::as_bool);
        field!(mute_end_time, "muteEndTime", number);
        field!(pinned, "pinned", number);
        field!(
            ephemeral_setting_timestamp,
            "ephemeralSettingTimestamp",
            serde_json::Value::as_f64
        );
        field!(
            ephemeral_expiration,
            "ephemeralExpiration",
            |v: &serde_json::Value| {
                if v.is_null() {
                    Some(None)
                } else {
                    v.as_u64().and_then(|n| u32::try_from(n).ok()).map(Some)
                }
            }
        );
        field!(unread_count, "unreadCount", serde_json::Value::as_i64);
        if result.unread_count.is_none()
            && let Some(n) = fields
                .get("unreadCount")
                .and_then(serde_json::Value::as_f64)
            && n.fract() == 0.0
            && (-9223372036854775808.0..9223372036854775808.0).contains(&n)
        {
            result.unread_count = Some(n as i64);
            fields.remove("unreadCount");
        }
        if fields
            .get("archived")
            .is_some_and(serde_json::Value::is_null)
        {
            result.clear_archived = true;
            fields.remove("archived");
        }
        if fields
            .get("unreadCount")
            .is_some_and(serde_json::Value::is_null)
        {
            result.clear_unread_count = true;
            fields.remove("unreadCount");
        }
        if let Some(messages) = fields.get("messages") {
            result.message_key_metadata = messages
                .get(0)
                .and_then(|entry| messages::KeyMetadata::from_key(&entry["message"]["key"]))
                .map(Box::new);
            result.messages = Some(
                messages
                    .as_array()
                    .ok_or(Error::Input("chat messages"))?
                    .iter()
                    .map(|entry| {
                        proto::WebMessageInfo::from_value(
                            entry
                                .get("message")
                                .ok_or(Error::Input("chat message envelope"))?,
                        )
                    })
                    .collect::<Result<_, _>>()?,
            );
            // Preserve future wrapper fields rather than discard them.
            let canonical = result.to_value();
            if canonical.get("messages") == Some(messages) {
                fields.remove("messages");
            }
        }
        if !fields.is_empty() {
            result.runtime_fields = Some(Box::new(fields));
        }
        Ok(result)
    }
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
                    key_metadata: None,
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
                    key_metadata: None,
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
            key_metadata: None,
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
