use super::{Error, Inner, RelayOptions, Socket, load_credentials};
use crate::{
    binary::BinaryNode,
    messages::{self, generation, own},
    proto, relay,
};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct SendOptions {
    pub message_id: Option<String>,
    pub timestamp_seconds: Option<u64>,
    pub quoted: Option<proto::WebMessageInfo>,
    pub ephemeral_expiration: Option<u32>,
    pub content: generation::ContentOptions,
    pub hooks: Option<generation::GenerationHooks>,
    pub media_hooks: Option<crate::media::PreparationHooks>,
    pub use_cached_group_metadata: bool,
    pub cached_group_metadata: Option<crate::groups::Metadata>,
    pub status_jid_list: Vec<String>,
}
impl Default for SendOptions {
    fn default() -> Self {
        Self {
            message_id: None,
            timestamp_seconds: None,
            quoted: None,
            ephemeral_expiration: None,
            content: Default::default(),
            hooks: None,
            media_hooks: None,
            use_cached_group_metadata: true,
            cached_group_metadata: None,
            status_jid_list: vec![],
        }
    }
}
impl SendOptions {
    /// Read upstream's `MiscMessageGenerationOptions` object as JSON: `quoted`
    /// (a full message), `ephemeralExpiration`, `messageId`, `timestamp`
    /// (milliseconds), `statusJidList`, `backgroundColor`, `font` and
    /// `useCachedGroupMetadata`. Other fields are ignored, as upstream ignores
    /// options it does not use.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, generation::Error> {
        use crate::proto_json::JsonField;
        use serde_json::Value;
        let invalid = |field: &str| generation::Error::Json(format!("invalid option {field}"));
        let options = value.as_object().ok_or_else(|| invalid("object"))?;
        let get = |name: &str| options.get(name).filter(|v| !v.is_null());
        let mut result = Self::default();
        if let Some(quoted) = get("quoted") {
            result.quoted = Some(
                proto::WebMessageInfo::from_value(quoted)
                    .map_err(|e| generation::Error::Json(format!("quoted: {e}")))?,
            );
        }
        if let Some(expiration) = get("ephemeralExpiration") {
            let seconds = match expiration {
                Value::String(s) => s.trim().parse::<f64>().ok(),
                other => other.as_f64(),
            }
            .ok_or_else(|| invalid("ephemeralExpiration"))?;
            result.ephemeral_expiration = Some(seconds as u32);
        }
        if let Some(id) = get("messageId") {
            result.message_id = Some(id.as_str().ok_or_else(|| invalid("messageId"))?.into());
        }
        if let Some(timestamp) = get("timestamp") {
            let ms = timestamp.as_f64().ok_or_else(|| invalid("timestamp"))?;
            result.timestamp_seconds = Some((ms / 1000.0).floor() as u64);
        }
        if let Some(list) = get("statusJidList") {
            result.status_jid_list = list
                .as_array()
                .ok_or_else(|| invalid("statusJidList"))?
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("statusJidList"))
                })
                .collect::<Result<_, _>>()?;
        }
        if let Some(color) = get("backgroundColor") {
            result.content.background_color = Some(match color {
                Value::String(s) => generation::Color::Hex(s.clone()),
                other => generation::Color::Number(
                    other.as_f64().ok_or_else(|| invalid("backgroundColor"))?,
                ),
            });
        }
        if let Some(font) = get("font") {
            result.content.font = Some(font.as_f64().ok_or_else(|| invalid("font"))? as i32);
        }
        if let Some(cached) = options.get("useCachedGroupMetadata") {
            result.use_cached_group_metadata = cached.as_bool().unwrap_or(false);
        }
        Ok(result)
    }
}
/// Source send-message metadata precedence: delete, edit, pin, poll, event.
pub fn send_metadata(
    content: &generation::Content,
    modifiers: &generation::Modifiers,
) -> relay::StanzaOptions {
    use generation::Content;
    let mut options = relay::StanzaOptions::default();
    if let Content::Delete(key) = content {
        options.additional_attributes.insert(
            "edit".into(),
            if key
                .remote_jid
                .as_deref()
                .is_some_and(|j| j.ends_with("@g.us"))
                && key.from_me != Some(true)
            {
                "8"
            } else {
                "7"
            }
            .into(),
        );
    } else if modifiers.edit.is_some() {
        options
            .additional_attributes
            .insert("edit".into(), "1".into());
    } else if matches!(content, Content::Pin { .. }) {
        options
            .additional_attributes
            .insert("edit".into(), "2".into());
    } else if matches!(content, Content::Poll(_)) {
        options
            .additional_nodes
            .push(BinaryNode::new("meta").attr("polltype", "creation"));
    } else if matches!(content, Content::Event(_)) {
        options
            .additional_nodes
            .push(BinaryNode::new("meta").attr("event_type", "creation"));
    }
    options
}
impl Socket {
    /// Generate and relay a message; group disappearing settings return None after their IQ.
    pub async fn send_message(
        &self,
        destination: &str,
        content: &generation::Content,
        modifiers: &generation::Modifiers,
        options: SendOptions,
    ) -> Result<Option<proto::WebMessageInfo>, Error> {
        let credentials = load_credentials(self.inner.store.clone()).await?;
        let user_jid = credentials.me_id()?.to_owned();
        if let generation::Content::Disappearing(value) = content
            && destination.ends_with("@g.us")
        {
            self.group_toggle_ephemeral(destination, u64::from(value.expiration()))
                .await?;
            return Ok(None);
        }
        let id = match options.message_id {
            Some(id) => id,
            None => messages::generate_id_v2(Some(&user_jid))?,
        };
        let hooks = options
            .hooks
            .as_ref()
            .unwrap_or(&self.inner.generation_hooks);
        let media_hooks = options
            .media_hooks
            .as_ref()
            .unwrap_or(&self.inner.media_preparation_hooks);
        let socket_hooks = SocketContentHooks {
            socket: self,
            base: hooks.get(),
            media: media_hooks.get(),
            destination,
        };
        // Keep protobuf-heavy generation and relay futures out of the caller's stack frame.
        let full = Box::pin(generation::generate(
            destination,
            content,
            modifiers,
            &options.content,
            &generation::Options {
                user_jid,
                message_id: Some(id),
                timestamp_seconds: options.timestamp_seconds,
                quoted: options.quoted,
                ephemeral_expiration: options.ephemeral_expiration,
            },
            &socket_hooks,
        ))
        .await?;
        Box::pin(
            self.relay_message(
                destination,
                full.message
                    .clone()
                    .ok_or(generation::Error::InvalidContent)?,
                RelayOptions {
                    message_id: full.key.as_ref().and_then(|k| k.id.clone()),
                    stanza: send_metadata(content, modifiers),
                    use_cached_group_metadata: options.use_cached_group_metadata,
                    cached_group_metadata: options.cached_group_metadata,
                    status_jid_list: options.status_jid_list,
                    ..Default::default()
                },
            ),
        )
        .await?;
        if let Some(sender) = &self.inner.own_messages {
            let _ = sender.send(full.clone());
        }
        Ok(Some(full))
    }
}
pub(super) fn spawn_own_worker(
    inner: &Arc<Inner>,
    mut receiver: mpsc::UnboundedReceiver<proto::WebMessageInfo>,
) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            let Some(inner) = weak.upgrade() else { break };
            let result = (Socket { inner })
                .publish_message(&message, None, own::UpsertKind::Append)
                .await;
            if let Err(error) = result {
                tracing::debug!(?error, "own message processing failed");
            }
        }
    });
}

struct SocketContentHooks<'a> {
    socket: &'a Socket,
    base: &'a dyn generation::ContentHooks,
    media: &'a dyn crate::media::PrepareHooks,
    destination: &'a str,
}
impl generation::ContentHooks for SocketContentHooks<'_> {
    fn link_preview(
        &self,
        url: String,
    ) -> generation::HookFuture<'_, Option<generation::LinkPreview>> {
        Box::pin(async move {
            // A configured hook takes precedence over the built-in generator.
            let provided = self.base.link_preview(url.clone()).await?;
            let config = &self.socket.inner.link_previews;
            if provided.is_some() || !config.enabled {
                return Ok(provided);
            }
            let info = crate::link_preview::url_info(
                &url,
                config.image_thumbnail_width,
                config.high_quality,
                self,
            )
            .await
            .map_err(|error| generation::Error::Hook(error.to_string()))?;
            Ok(info.map(|info| generation::LinkPreview {
                matched_text: Some(info.matched_text),
                jpeg_thumbnail: info.jpeg_thumbnail,
                description: info.description,
                title: Some(info.title),
                high_quality_thumbnail: info.high_quality_thumbnail,
            }))
        })
    }
    fn invite_thumbnail(&self, jid: String) -> generation::HookFuture<'_, Option<Vec<u8>>> {
        self.base.invite_thumbnail(jid)
    }
    fn call_link(
        &self,
        kind: generation::CallKind,
        time: i64,
    ) -> generation::HookFuture<'_, Option<String>> {
        self.base.call_link(kind, time)
    }
    fn now_ms(&self) -> Result<i64, generation::Error> {
        self.base.now_ms()
    }
    fn message_secret(&self) -> Result<Vec<u8>, generation::Error> {
        self.base.message_secret()
    }
    fn prepare_media<'a>(
        &'a self,
        request: &'a crate::media::Request,
        options: &'a crate::media::PrepareOptions,
    ) -> generation::HookFuture<'a, crate::proto::Message> {
        Box::pin(async move {
            let mut options = options.clone();
            options.destination = Some(self.destination.into());
            self.socket
                .prepare_media_message(request, &options, self.media)
                .await
                .map_err(|e| {
                    generation::Error::Media(crate::media::Error::Refresh {
                        status: e.status_code(),
                        message: e.to_string(),
                    })
                })
        })
    }
}

impl crate::link_preview::Source for SocketContentHooks<'_> {
    fn page<'a>(
        &'a self,
        url: &'a str,
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<crate::link_preview::Page, crate::link_preview::Error>,
    > {
        use crate::link_preview::{Error, Page};
        Box::pin(async move {
            let http = self.socket.link_preview_http().await?;
            let (url, content_type, body) = http
                .get_page(url, self.socket.inner.link_previews.timeout)
                .await
                .map_err(|error| Error::Fetch(error.to_string()))?;
            Ok(Page {
                url,
                content_type,
                body,
            })
        })
    }
    fn thumbnail<'a>(
        &'a self,
        url: &'a str,
        width: u32,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<u8>, crate::link_preview::Error>> {
        use crate::link_preview::Error;
        Box::pin(async move {
            let http = self.socket.link_preview_http().await?;
            // The image request has the same time allowance as the page request.
            let limit = self.socket.inner.link_previews.timeout;
            let image = tokio::time::timeout(limit, http.get(url, Default::default()))
                .await
                .map_err(|_| Error::Image("request timed out".into()))?
                .map_err(|error| Error::Image(error.to_string()))?
                .bytes()
                .await
                .map_err(|error| Error::Image(error.to_string()))?;
            tokio::task::spawn_blocking(move || {
                crate::media::extract_image_thumb(std::io::Cursor::new(image), width)
            })
            .await
            .map_err(|error| Error::Image(error.to_string()))?
            .map(|thumbnail| thumbnail.jpeg)
            .map_err(|error| Error::Image(error.to_string()))
        })
    }
    fn upload<'a>(
        &'a self,
        url: &'a str,
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<Option<crate::proto::message::ImageMessage>, crate::link_preview::Error>,
    > {
        Box::pin(async move {
            let request = crate::media::Request {
                input: crate::media::Input::Url(url.into()),
                payload: crate::media::Payload::Image(Default::default()),
                video_note: false,
            };
            let options = crate::media::PrepareOptions {
                media_type_override: Some(crate::media::MediaType::LinkThumbnail),
                ..Default::default()
            };
            let message = self
                .socket
                .prepare_media(&request, &options, self.media, false)
                .await
                .map_err(|error| crate::link_preview::Error::Image(error.to_string()))?;
            Ok(message.image_message.map(|image| *image))
        })
    }
}
impl Socket {
    async fn link_preview_http(
        &self,
    ) -> Result<&crate::media::HttpClient, crate::link_preview::Error> {
        self.inner
            .media_http
            .get_or_try_init(|| async { crate::media::HttpClient::new() })
            .await
            .map_err(|error| crate::link_preview::Error::Fetch(error.to_string()))
    }
}
