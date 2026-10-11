//! Message construction from protobuf content, forwarding and content extraction.
use crate::{jid, messages, proto, relay};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no content in message")]
    MissingContent,
    #[error("message content cannot carry quote or expiration metadata")]
    InvalidContent,
    #[error(transparent)]
    Message(#[from] messages::Error),
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::MissingContent | Self::InvalidContent => Some(400),
            Self::Message(_) => None,
        }
    }
}

macro_rules! message_fields {
    ($($variant:ident, $field:ident, $name:literal, $content:literal;)*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Field { $($variant,)* }
        impl Field {
            fn first(message: &proto::Message, content_only: bool) -> Option<Self> {
                $(if (!content_only || $content) && message.$field.is_some() {
                    return Some(Self::$variant);
                })*
                None
            }
            fn name(self) -> &'static str {
                match self { $(Self::$variant => $name,)* }
            }
            fn isolate(self, message: &proto::Message) -> proto::Message {
                let mut result = proto::Message::default();
                match self { $(Self::$variant => result.$field = message.$field.clone(),)* }
                result
            }
        }
    };
}
macro_rules! context_fields {
    ($($variant:ident, $field:ident;)*) => {
        impl Field {
            fn context(self, message: &proto::Message) -> Option<&proto::ContextInfo> {
                match self {
                    $(Self::$variant => message.$field.as_ref()?.context_info.as_deref(),)*
                    _ => None,
                }
            }
            fn set_context(self, message: &mut proto::Message, context: Option<proto::ContextInfo>) {
                match self {
                    $(Self::$variant => {
                        if let Some(payload) = message.$field.as_mut() {
                            payload.context_info = context.map(Box::new);
                        }
                    },)*
                    // Upstream's runtime-only contextInfo is discarded by protobuf encoding.
                    _ => {},
                }
            }
        }
    };
}
include!("generation_fields.rs");

/// Select the first content field in protobuf schema order, excluding sender-key distribution.
pub fn content_type(message: Option<&proto::Message>) -> Option<&'static str> {
    Field::first(message?, true).map(Field::name)
}

pub fn disappearing_setting(expiration: Option<u32>) -> proto::Message {
    proto::Message {
        ephemeral_message: Some(Box::new(proto::message::FutureProofMessage {
            message: Some(Box::new(proto::Message {
                protocol_message: Some(Box::new(proto::message::ProtocolMessage {
                    r#type: Some(proto::message::protocol_message::Type::EphemeralSetting as i32),
                    ephemeral_expiration: Some(expiration.unwrap_or(0)),
                    ..Default::default()
                })),
                ..Default::default()
            })),
        })),
        ..Default::default()
    }
}

/// Normalize wrappers and replace context with only the upstream forwarding metadata.
pub fn forward(message: &proto::WebMessageInfo, force: bool) -> Result<proto::Message, Error> {
    let original = message.message.as_ref().ok_or(Error::MissingContent)?;
    let mut content = relay::normalize(Some(original))
        .ok_or(Error::InvalidContent)?
        .clone();
    let mut field = Field::first(&content, false).ok_or(Error::InvalidContent)?;
    let score = u64::from(
        field
            .context(&content)
            .and_then(|c| c.forwarding_score)
            .unwrap_or(0),
    ) + u64::from(force || message.key.as_ref().and_then(|k| k.from_me) != Some(true));
    if field == Field::Conversation {
        content.extended_text_message = Some(Box::new(proto::message::ExtendedTextMessage {
            text: content.conversation.take(),
            ..Default::default()
        }));
        field = Field::ExtendedTextMessage;
    }
    field.set_context(
        &mut content,
        Some(if score > 0 {
            proto::ContextInfo {
                forwarding_score: Some(score as u32),
                is_forwarded: Some(true),
                ..Default::default()
            }
        } else {
            proto::ContextInfo::default()
        }),
    );
    Ok(content)
}

#[derive(Debug, Default, Clone)]
pub struct Options {
    pub user_jid: String,
    pub message_id: Option<String>,
    pub timestamp_seconds: Option<u64>,
    pub quoted: Option<proto::WebMessageInfo>,
    pub ephemeral_expiration: Option<u32>,
}

fn normalize_mut(mut message: &mut proto::Message) -> Option<&mut proto::Message> {
    for _ in 0..5 {
        if message.ephemeral_message.is_none()
            && message.view_once_message.is_none()
            && message.document_with_caption_message.is_none()
            && message.view_once_message_v2.is_none()
            && message.view_once_message_v2_extension.is_none()
            && message.edited_message.is_none()
            && message.associated_child_message.is_none()
            && message.group_status_message.is_none()
            && message.group_status_message_v2.is_none()
        {
            break;
        }
        let wrapper = message
            .ephemeral_message
            .as_deref_mut()
            .or(message.view_once_message.as_deref_mut())
            .or(message.document_with_caption_message.as_deref_mut())
            .or(message.view_once_message_v2.as_deref_mut())
            .or(message.view_once_message_v2_extension.as_deref_mut())
            .or(message.edited_message.as_deref_mut())
            .or(message.associated_child_message.as_deref_mut())
            .or(message.group_status_message.as_deref_mut())
            .or(message.group_status_message_v2.as_deref_mut())?;
        message = wrapper.message.as_deref_mut()?;
    }
    Some(message)
}

/// Build an outgoing message, applying quote/expiration context to the normalized payload.
pub fn from_content(
    recipient: &str,
    mut message: proto::Message,
    options: &Options,
) -> Result<proto::WebMessageInfo, Error> {
    let newsletter = recipient.ends_with("@newsletter");
    let quoted = options.quoted.as_ref().filter(|_| !newsletter);
    let expiration = options
        .ephemeral_expiration
        .filter(|v| *v != 0 && !newsletter);
    if quoted.is_some() || expiration.is_some() {
        let inner = normalize_mut(&mut message).ok_or(Error::InvalidContent)?;
        let field = Field::first(inner, true).ok_or(Error::InvalidContent)?;
        if field == Field::Conversation {
            return Err(Error::InvalidContent);
        }
        let mut context = field.context(inner).cloned().unwrap_or_default();
        if let Some(quoted) = quoted {
            let key = quoted.key.as_ref().ok_or(Error::InvalidContent)?;
            let participant = if key.from_me == Some(true) {
                Some(options.user_jid.as_str())
            } else {
                quoted
                    .participant
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .or(key.participant.as_deref().filter(|s| !s.is_empty()))
                    .or(key.remote_jid.as_deref())
            };
            let quoted_inner =
                relay::normalize(quoted.message.as_ref()).ok_or(Error::InvalidContent)?;
            let quoted_message = if let Some(quoted_field) = Field::first(quoted_inner, true) {
                let mut content = quoted_field.isolate(quoted_inner);
                quoted_field.set_context(&mut content, None);
                content
            } else {
                proto::Message::default()
            };
            context.participant = Some(jid::normalized_user(participant.unwrap_or("")));
            context.stanza_id = key.id.clone();
            context.quoted_message = Some(Box::new(quoted_message));
            if Some(recipient) != key.remote_jid.as_deref() {
                context.remote_jid = key.remote_jid.clone();
            }
            field.set_context(inner, Some(context.clone()));
        }
        if let Some(expiration) = expiration
            && field != Field::ProtocolMessage
            && field != Field::EphemeralMessage
        {
            context.expiration = Some(expiration);
            field.set_context(inner, Some(context));
        }
    }
    let id = match options.message_id.as_ref().filter(|id| !id.is_empty()) {
        Some(id) => id.clone(),
        None => messages::generate_id_v2(None)?,
    };
    Ok(proto::WebMessageInfo {
        key: Some(proto::MessageKey {
            remote_jid: Some(recipient.into()),
            from_me: Some(true),
            id: Some(id),
            ..Default::default()
        }),
        message: Some(message),
        message_timestamp: Some(options.timestamp_seconds.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        })),
        participant: (recipient.ends_with("@g.us") || recipient == jid::STORIES_JID)
            .then(|| options.user_jid.clone()),
        status: Some(proto::web_message_info::Status::Pending as i32),
        ..Default::default()
    })
}

/// Extract media or text from buttons/templates after normalizing future-proof wrappers.
pub fn extract(message: Option<&proto::Message>) -> Option<proto::Message> {
    let content = relay::normalize(message)?;
    macro_rules! media {
        ($header:expr, $ty:path, $fallback:expr) => {{
            use $ty as Header;
            match $header {
                Some(Header::ImageMessage(value)) => proto::Message {
                    image_message: Some(value.clone()),
                    ..Default::default()
                },
                Some(Header::DocumentMessage(value)) => proto::Message {
                    document_message: Some(value.clone()),
                    ..Default::default()
                },
                Some(Header::VideoMessage(value)) => proto::Message {
                    video_message: Some(value.clone()),
                    ..Default::default()
                },
                Some(Header::LocationMessage(value)) => proto::Message {
                    location_message: Some(value.clone()),
                    ..Default::default()
                },
                _ => proto::Message {
                    conversation: $fallback,
                    ..Default::default()
                },
            }
        }};
    }
    if let Some(buttons) = &content.buttons_message {
        return Some(media!(
            buttons.header.as_ref(),
            proto::message::buttons_message::Header,
            buttons.content_text.clone()
        ));
    }
    if let Some(template) = &content.template_message {
        use proto::message::template_message::Format;
        let hydrated = match &template.format {
            Some(Format::HydratedFourRowTemplate(value)) => Some(value.as_ref()),
            _ => template.hydrated_template.as_deref(),
        };
        if let Some(hydrated) = hydrated {
            return Some(media!(
                hydrated.title.as_ref(),
                proto::message::template_message::hydrated_four_row_template::Title,
                hydrated.hydrated_content_text.clone()
            ));
        }
        if let Some(Format::FourRowTemplate(value)) = &template.format {
            return Some(media!(
                value.title.as_ref(),
                proto::message::template_message::four_row_template::Title,
                Some(String::new())
            ));
        }
    }
    Some(content.clone())
}
