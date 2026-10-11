use super::{Error, Field, Options, disappearing_setting, forward, from_content};
use crate::{crypto, messages, proto, reporting};
use std::{
    future::Future,
    pin::Pin,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

pub type HookFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;
/// Async enrichment plus clock/entropy hooks. Thumbnail hooks fetch the actual image bytes.
/// Link-preview failures are suppressed; thumbnail and call-link errors propagate.
pub trait ContentHooks: Send + Sync {
    fn prepare_media<'a>(
        &'a self,
        _request: &'a crate::media::Request,
        _options: &'a crate::media::PrepareOptions,
    ) -> HookFuture<'a, proto::Message> {
        Box::pin(async { Err(crate::media::Error::UploadUnavailable.into()) })
    }
    fn link_preview(&self, _url: String) -> HookFuture<'_, Option<LinkPreview>> {
        Box::pin(async { Ok(None) })
    }
    fn invite_thumbnail(&self, _jid: String) -> HookFuture<'_, Option<Vec<u8>>> {
        Box::pin(async { Ok(None) })
    }
    fn call_link(&self, _kind: CallKind, _start_time: i64) -> HookFuture<'_, Option<String>> {
        Box::pin(async { Ok(None) })
    }
    fn now_ms(&self) -> Result<i64, Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Hook(e.to_string()))?;
        now.as_millis()
            .try_into()
            .map_err(|_| Error::Hook("timestamp overflow".into()))
    }
    fn message_secret(&self) -> Result<Vec<u8>, Error> {
        let mut secret = vec![0; 32];
        getrandom::fill(&mut secret).map_err(|_| messages::Error::Crypto(crypto::Error::Random))?;
        Ok(secret)
    }
}
#[derive(Debug, Default)]
pub struct DefaultContentHooks;
impl ContentHooks for DefaultContentHooks {}

/// Cloneable socket configuration for asynchronous content enrichment.
#[derive(Clone)]
pub struct GenerationHooks(std::sync::Arc<dyn ContentHooks>);
impl GenerationHooks {
    pub fn new(hooks: impl ContentHooks + 'static) -> Self {
        Self(std::sync::Arc::new(hooks))
    }
    pub fn get(&self) -> &dyn ContentHooks {
        self.0.as_ref()
    }
}
impl Default for GenerationHooks {
    fn default() -> Self {
        Self::new(DefaultContentHooks)
    }
}
impl std::fmt::Debug for GenerationHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenerationHooks").finish_non_exhaustive()
    }
}

#[derive(Debug, Default, Clone)]
pub struct LinkPreview {
    pub matched_text: Option<String>,
    pub jpeg_thumbnail: Option<Vec<u8>>,
    pub description: Option<String>,
    pub title: Option<String>,
    pub high_quality_thumbnail: Option<proto::message::ImageMessage>,
}
#[derive(Debug, Default, Clone)]
pub enum Preview {
    #[default]
    Automatic,
    Disabled,
    Provided(Box<LinkPreview>),
}
#[derive(Debug, Default, Clone)]
pub struct Text {
    pub text: String,
    pub link_preview: Preview,
}
#[derive(Debug, Clone)]
pub enum Color {
    Hex(String),
    /// Preserves the upstream numeric-color helper's undefined return value.
    Number(f64),
}
#[derive(Debug, Default, Clone)]
pub struct ContentOptions {
    pub media: crate::media::PrepareOptions,
    pub background_color: Option<Color>,
    pub font: Option<i32>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    Audio,
    Video,
}
#[derive(Debug, Default, Clone)]
pub struct Event {
    pub name: String,
    pub description: Option<String>,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub location: Option<Box<proto::message::LocationMessage>>,
    pub call: Option<CallKind>,
    pub is_canceled: Option<bool>,
    pub is_schedule_call: Option<bool>,
    pub extra_guests_allowed: Option<bool>,
    pub message_secret: Option<Vec<u8>>,
}
#[derive(Debug, Default, Clone)]
pub struct Poll {
    pub name: String,
    pub values: Vec<String>,
    pub selectable_count: Option<i64>,
    pub to_announcement_group: bool,
    pub message_secret: Option<Vec<u8>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disappearing {
    Enabled,
    Disabled,
    Seconds(u32),
}
impl Disappearing {
    pub fn expiration(self) -> u32 {
        match self {
            Self::Enabled => 604800,
            Self::Disabled => 0,
            Self::Seconds(seconds) => seconds,
        }
    }
}
#[derive(Debug, Clone)]
pub enum Content {
    Text(Box<Text>),
    Contacts(Box<proto::message::ContactsArrayMessage>),
    Location(Box<proto::message::LocationMessage>),
    Reaction(Box<proto::message::ReactionMessage>),
    Delete(proto::MessageKey),
    Forward {
        message: Box<proto::WebMessageInfo>,
        force: bool,
    },
    Disappearing(Disappearing),
    GroupInvite(Box<proto::message::GroupInviteMessage>),
    Pin {
        key: proto::MessageKey,
        kind: i32,
        duration_seconds: Option<u32>,
    },
    TemplateButtonReply {
        id: String,
        display_text: String,
        index: u32,
    },
    ButtonReply {
        id: String,
        display_text: String,
    },
    ListReply(Box<proto::message::ListResponseMessage>),
    Event(Box<Event>),
    Poll(Box<Poll>),
    Album {
        expected_image_count: Option<u32>,
        expected_video_count: Option<u32>,
    },
    SharePhoneNumber,
    RequestPhoneNumber,
    LimitSharing(bool),
    /// Media already prepared by an external uploader; built-in preparation remains separate.
    PreparedMedia(Box<proto::Message>),
    Media(Box<crate::media::Request>),
    VideoNote(crate::media::Input),
    Product {
        image: crate::media::Input,
        template: Box<proto::message::ProductMessage>,
    },
}
impl Content {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text(Box::new(Text {
            text: text.into(),
            ..Default::default()
        }))
    }
}

macro_rules! context_patch_fields {
    ($($variant:ident, $field:ident, $kind:ident;)*) => {
        /// Fields that can be explicitly cleared while merging context metadata.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum ContextField { $($variant,)* }
        impl ContextPatch {
            fn apply(&self, target: &mut proto::ContextInfo) {
                $(context_patch_fields!(@merge target, self.value, $field, $kind);)*
                for field in &self.clear_fields {
                    match field { $(ContextField::$variant => target.$field = Default::default(),)* }
                }
            }
        }
    };
    (@merge $target:ident, $value:expr, $field:ident, optional) => {
        if $value.$field.is_some() { $target.$field = $value.$field.clone(); }
    };
    (@merge $target:ident, $value:expr, $field:ident, repeated) => {
        if !$value.$field.is_empty() { $target.$field = $value.$field.clone(); }
    };
}
/// Optional populated fields replace existing values; nonempty lists replace whole lists.
/// Use clear_fields for explicit nulls or empty lists that typed protobufs cannot distinguish.
#[derive(Debug, Default, Clone)]
pub struct ContextPatch {
    pub value: proto::ContextInfo,
    pub clear_fields: Vec<ContextField>,
}
include!("context_fields.rs");
#[derive(Debug, Default, Clone)]
pub struct Modifiers {
    pub view_once: bool,
    pub mentions: Vec<String>,
    pub mention_all: bool,
    pub edit: Option<proto::MessageKey>,
    pub context: Option<ContextPatch>,
    pub album_parent_key: Option<proto::MessageKey>,
}

// JS \s excludes U+0085 and includes the byte-order mark; preserve that character set.
const SPACE: &str =
    r"\x09-\x0d \u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}\u{feff}";
fn js_space(c: char) -> bool {
    matches!(c, '\u{0009}'..='\u{000d}' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
/// Return the first URL matched by the upstream HTTPS/domain expression.
pub fn extract_url(text: &str) -> Option<&str> {
    static REGEX: OnceLock<(regex::Regex, regex::Regex)> = OnceLock::new();
    let (url, auth) = REGEX.get_or_init(|| {
        (
            regex::Regex::new(&format!(
                r"https://[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}(:[0-9]+)?(/[^{}]*)?",
                SPACE
            ))
            .expect("URL pattern"),
            regex::Regex::new(&format!(r"^[^:@/{}]+:[^:@/{}]+@", SPACE, SPACE))
                .expect("auth pattern"),
        )
    });
    url.find_iter(text)
        .find(|m| !auth.is_match(&text[m.start() + 8..]))
        .map(|m| m.as_str())
}
/// Upstream strings use permissive hexadecimal prefix parsing; numeric colors are omitted.
pub fn color_argb(color: &Color) -> Option<u32> {
    let Color::Hex(value) = color else {
        return None;
    };
    if value.is_empty() {
        return None;
    }
    let mut text = value.trim_matches(js_space).replacen('#', "", 1);
    if text.encode_utf16().count() <= 6 {
        text = format!("FF{}{}", "0".repeat(6 - text.encode_utf16().count()), text);
    }
    let mut digits = text.trim_start_matches(js_space);
    let negative = digits.starts_with('-');
    if digits.starts_with(['-', '+']) {
        digits = &digits[1..];
    }
    if digits.starts_with("0x") || digits.starts_with("0X") {
        digits = &digits[2..];
    }
    // Convert the complete hex integer once, preserving JS Number rounding before uint32.
    let mut integer = 0u128;
    for digit in digits.chars().map_while(|c| c.to_digit(16)) {
        let Some(next) = integer
            .checked_mul(16)
            .and_then(|v| v.checked_add(u128::from(digit)))
        else {
            // Above 128 bits, every representable f64 integer is a multiple of 2^32.
            return Some(0);
        };
        integer = next;
    }
    let low = (integer as f64).rem_euclid(4294967296.0) as u32;
    Some(if negative { low.wrapping_neg() } else { low })
}

/// Generate content with async media/enrichment hooks and apply wrappers/context in the source's operation order.
pub async fn content(
    input: &Content,
    modifiers: &Modifiers,
    options: &ContentOptions,
    hooks: &dyn ContentHooks,
) -> Result<proto::Message, Error> {
    use proto::message as p;
    let mut m = proto::Message::default();
    // Track insertion order independently of protobuf field order (notably polls/events).
    let mut first = match input {
        Content::Text(text) => {
            let preview = match &text.link_preview {
                Preview::Provided(preview) => Some(preview.as_ref().clone()),
                Preview::Disabled => None,
                Preview::Automatic => {
                    if let Some(url) = extract_url(&text.text) {
                        match hooks.link_preview(url.into()).await {
                            Ok(preview) => preview,
                            Err(error) => {
                                tracing::debug!(?error, "url generation failed");
                                None
                            }
                        }
                    } else {
                        None
                    }
                }
            };
            let mut ext = p::ExtendedTextMessage {
                text: Some(text.text.clone()),
                ..Default::default()
            };
            if let Some(preview) = preview {
                ext.matched_text = preview.matched_text;
                ext.jpeg_thumbnail = preview.jpeg_thumbnail;
                ext.description = preview.description;
                ext.title = preview.title;
                ext.preview_type = Some(0);
                if let Some(image) = preview.high_quality_thumbnail {
                    ext.thumbnail_direct_path = image.direct_path;
                    ext.media_key = image.media_key;
                    ext.media_key_timestamp = image.media_key_timestamp;
                    ext.thumbnail_width = image.width;
                    ext.thumbnail_height = image.height;
                    ext.thumbnail_sha256 = image.file_sha256;
                    ext.thumbnail_enc_sha256 = image.file_enc_sha256;
                }
            }
            ext.background_argb = options.background_color.as_ref().and_then(color_argb);
            ext.font = options.font.filter(|v| *v != 0);
            m.extended_text_message = Some(Box::new(ext));
            Field::ExtendedTextMessage
        }
        Content::Contacts(contacts) => match contacts.contacts.len() {
            0 => return Err(Error::EmptyContacts),
            1 => {
                m.contact_message = Some(Box::new(contacts.contacts[0].clone()));
                Field::ContactMessage
            }
            _ => {
                m.contacts_array_message = Some(contacts.clone());
                Field::ContactsArrayMessage
            }
        },
        Content::Location(location) => {
            m.location_message = Some(location.clone());
            Field::LocationMessage
        }
        Content::Reaction(reaction) => {
            let mut reaction = reaction.clone();
            if reaction.sender_timestamp_ms.is_none_or(|v| v == 0) {
                reaction.sender_timestamp_ms = Some(hooks.now_ms()?);
            }
            m.reaction_message = Some(*reaction);
            Field::ReactionMessage
        }
        Content::Delete(key) => {
            m.protocol_message = Some(Box::new(p::ProtocolMessage {
                key: Some(key.clone()),
                r#type: Some(p::protocol_message::Type::Revoke as i32),
                ..Default::default()
            }));
            Field::ProtocolMessage
        }
        Content::Forward { message, force } => {
            m = forward(message, *force)?;
            Field::first(&m, false).ok_or(Error::InvalidContent)?
        }
        Content::Disappearing(value) => {
            m = disappearing_setting(Some(value.expiration()));
            Field::EphemeralMessage
        }
        Content::GroupInvite(invite) => {
            m.group_invite_message = Some(Box::new(p::GroupInviteMessage {
                invite_code: invite.invite_code.clone(),
                invite_expiration: invite.invite_expiration,
                caption: invite.caption.clone(),
                group_jid: invite.group_jid.clone(),
                group_name: invite.group_name.clone(),
                jpeg_thumbnail: hooks
                    .invite_thumbnail(invite.group_jid.clone().unwrap_or_default())
                    .await?,
                ..Default::default()
            }));
            Field::GroupInviteMessage
        }
        Content::Pin {
            key,
            kind,
            duration_seconds,
        } => {
            m.pin_in_chat_message = Some(p::PinInChatMessage {
                key: Some(key.clone()),
                r#type: Some(*kind),
                sender_timestamp_ms: Some(hooks.now_ms()?),
            });
            m.message_context_info = Some(proto::MessageContextInfo {
                message_add_on_duration_in_secs: Some(if *kind == 1 {
                    duration_seconds.filter(|v| *v != 0).unwrap_or(86400)
                } else {
                    0
                }),
                ..Default::default()
            });
            Field::PinInChatMessage
        }
        Content::TemplateButtonReply {
            id,
            display_text,
            index,
        } => {
            m.template_button_reply_message = Some(Box::new(p::TemplateButtonReplyMessage {
                selected_id: Some(id.clone()),
                selected_display_text: Some(display_text.clone()),
                selected_index: Some(*index),
                ..Default::default()
            }));
            Field::TemplateButtonReplyMessage
        }
        Content::ButtonReply { id, display_text } => {
            m.buttons_response_message = Some(Box::new(p::ButtonsResponseMessage {
                selected_button_id: Some(id.clone()),
                r#type: Some(p::buttons_response_message::Type::DisplayText as i32),
                response: Some(p::buttons_response_message::Response::SelectedDisplayText(
                    display_text.clone(),
                )),
                ..Default::default()
            }));
            Field::ButtonsResponseMessage
        }
        Content::ListReply(reply) => {
            m.list_response_message = Some(reply.clone());
            Field::ListResponseMessage
        }
        Content::Event(event) => {
            let join_link = if let Some(kind) = event.call {
                hooks.call_link(kind, event.start_time).await?.map(|token| {
                    format!(
                        "https://call.whatsapp.com/{}/{token}",
                        if kind == CallKind::Audio {
                            "voice"
                        } else {
                            "video"
                        }
                    )
                })
            } else {
                None
            };
            m.message_context_info = Some(proto::MessageContextInfo {
                message_secret: Some(match &event.message_secret {
                    Some(secret) => secret.clone(),
                    None => hooks.message_secret()?,
                }),
                ..Default::default()
            });
            m.event_message = Some(Box::new(p::EventMessage {
                name: Some(event.name.clone()),
                description: event.description.clone(),
                start_time: Some(event.start_time),
                end_time: event.end_time,
                location: event.location.clone(),
                join_link,
                is_canceled: Some(event.is_canceled.unwrap_or(false)),
                extra_guests_allowed: event.extra_guests_allowed,
                is_schedule_call: Some(event.is_schedule_call.unwrap_or(false)),
                ..Default::default()
            }));
            Field::EventMessage
        }
        Content::Poll(poll) => {
            let count = poll.selectable_count.unwrap_or(0);
            if count < 0 || count as u64 > poll.values.len() as u64 || count > i64::from(u32::MAX) {
                return Err(Error::PollCount {
                    max: poll.values.len(),
                });
            }
            m.message_context_info = Some(proto::MessageContextInfo {
                message_secret: Some(match &poll.message_secret {
                    Some(secret) => secret.clone(),
                    None => hooks.message_secret()?,
                }),
                ..Default::default()
            });
            let creation = Box::new(p::PollCreationMessage {
                name: Some(poll.name.clone()),
                selectable_options_count: Some(count as u32),
                options: poll
                    .values
                    .iter()
                    .map(|value| p::poll_creation_message::Option {
                        option_name: Some(value.clone()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            });
            if poll.to_announcement_group {
                m.poll_creation_message_v2 = Some(creation);
            } else if count == 1 {
                m.poll_creation_message_v3 = Some(creation);
            } else {
                m.poll_creation_message = Some(creation);
            }
            Field::MessageContextInfo
        }
        Content::Album {
            expected_image_count,
            expected_video_count,
        } => {
            m.album_message = Some(Box::new(p::AlbumMessage {
                expected_image_count: *expected_image_count,
                expected_video_count: *expected_video_count,
                ..Default::default()
            }));
            Field::AlbumMessage
        }
        Content::SharePhoneNumber => {
            m.protocol_message = Some(Box::new(p::ProtocolMessage {
                r#type: Some(p::protocol_message::Type::SharePhoneNumber as i32),
                ..Default::default()
            }));
            Field::ProtocolMessage
        }
        Content::RequestPhoneNumber => {
            m.request_phone_number_message = Some(Box::default());
            Field::RequestPhoneNumberMessage
        }
        Content::LimitSharing(limited) => {
            m.protocol_message = Some(Box::new(p::ProtocolMessage {
                r#type: Some(p::protocol_message::Type::LimitSharing as i32),
                limit_sharing: Some(proto::LimitSharing {
                    sharing_limited: Some(*limited),
                    trigger: Some(1),
                    limit_sharing_setting_timestamp: Some(hooks.now_ms()?),
                    initiated_by_me: Some(true),
                }),
                ..Default::default()
            }));
            Field::ProtocolMessage
        }
        Content::VideoNote(input) => {
            let request = crate::media::Request {
                input: input.clone(),
                payload: crate::media::Payload::Video(Box::default()),
                video_note: false,
            };
            let prepared = hooks.prepare_media(&request, &options.media).await?;
            m.ptv_message = prepared.video_message;
            Field::PtvMessage
        }
        Content::Product { image, template } => {
            let request = crate::media::Request {
                input: image.clone(),
                payload: crate::media::Payload::Image(Box::default()),
                video_note: false,
            };
            let prepared = hooks.prepare_media(&request, &options.media).await?;
            let mut product = template.clone();
            product
                .product
                .get_or_insert_with(Box::default)
                .product_image = prepared.image_message;
            m.product_message = Some(product);
            Field::ProductMessage
        }
        Content::Media(request) => {
            let mut media_options = options.media.clone();
            media_options.background_color = options.background_color.clone();
            m = hooks.prepare_media(request, &media_options).await?;
            Field::first(&m, false).ok_or(Error::InvalidContent)?
        }
        Content::PreparedMedia(message) => {
            m = *message.clone();
            Field::first(&m, false).ok_or(Error::InvalidContent)?
        }
    };
    let context_member = matches!(
        input,
        Content::Contacts(_)
            | Content::Location(_)
            | Content::PreparedMedia(_)
            | Content::Media(_)
            | Content::VideoNote(_)
            | Content::Product { .. }
    ) || first.context(&m).is_some();
    if modifiers.view_once {
        m = proto::Message {
            view_once_message: Some(Box::new(p::FutureProofMessage {
                message: Some(Box::new(m)),
            })),
            ..Default::default()
        };
        first = Field::ViewOnceMessage;
    }
    if !modifiers.mentions.is_empty() || modifiers.mention_all {
        let existing = first.context(&m);
        let mut context = existing.cloned().unwrap_or_default();
        if !modifiers.mentions.is_empty() {
            context.mentioned_jid = modifiers.mentions.clone();
        }
        if modifiers.mention_all {
            context.non_jid_mentions = Some(1);
        } else if existing.is_none() && !context_member {
            context.non_jid_mentions = Some(0);
        }
        first.set_context(&mut m, Some(context));
    }
    if let Some(key) = &modifiers.edit {
        m = proto::Message {
            protocol_message: Some(Box::new(p::ProtocolMessage {
                key: Some(key.clone()),
                edited_message: Some(Box::new(m)),
                timestamp_ms: Some(hooks.now_ms()?),
                r#type: Some(p::protocol_message::Type::MessageEdit as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        first = Field::ProtocolMessage;
    }
    if let Some(patch) = &modifiers.context {
        let mut context = first.context(&m).cloned().unwrap_or_default();
        patch.apply(&mut context);
        first.set_context(&mut m, Some(context));
    }
    if let Some(parent) = &modifiers.album_parent_key {
        m.message_context_info
            .get_or_insert_default()
            .message_association = Some(proto::MessageAssociation {
            association_type: Some(proto::message_association::AssociationType::MediaAlbum as i32),
            parent_message_key: Some(parent.clone()),
            ..Default::default()
        });
    }
    if reporting::should_include_token(&m) {
        let context = m.message_context_info.get_or_insert_default();
        if context.message_secret.is_none() {
            context.message_secret = Some(hooks.message_secret()?);
        }
    }
    Ok(m)
}

/// Generate content and its outgoing envelope, ready for relay.
pub async fn generate(
    recipient: &str,
    input: &Content,
    modifiers: &Modifiers,
    content_options: &ContentOptions,
    options: &Options,
    hooks: &dyn ContentHooks,
) -> Result<proto::WebMessageInfo, Error> {
    from_content(
        recipient,
        content(input, modifiers, content_options, hooks).await?,
        options,
    )
}
