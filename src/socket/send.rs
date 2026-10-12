use super::{Error, Event, Inner, RelayOptions, Socket, load_credentials};
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
            let _ = inner.events.send(Event::MessagesUpserted(own::Upsert {
                messages: vec![message.clone()],
                kind: own::UpsertKind::Append,
                request_id: None,
                metadata: vec![],
            }));
            let result = async {
                let credentials = load_credentials(inner.store.clone()).await?;
                let effects = own::effects(&message, credentials.account_settings.unarchive_chats)?;
                if let Some(update) = effects.mutation {
                    let _ = inner.events.send(Event::MessageMutations(vec![update]));
                }
                if let Some(update) = effects.reaction {
                    let _ = inner.events.send(Event::MessageReactions(vec![update]));
                }
                (Socket {
                    inner: inner.clone(),
                })
                .process_phone_protocol(&message)
                .await?;
                (Socket {
                    inner: inner.clone(),
                })
                .process_event_response(&message, None, &credentials)
                .await?;
                if let Some(update) = effects.chat {
                    let _ = inner.events.send(Event::ChatsUpdated(vec![update]));
                }
                Ok::<_, Error>(())
            }
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
        self.base.link_preview(url)
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
