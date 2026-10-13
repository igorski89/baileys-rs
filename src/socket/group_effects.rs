use super::{Command, Error, Event, Socket};
use crate::auth::Contact;
use crate::{
    auth::Credentials,
    jid,
    messages::{self, KeyMetadata, group_effects, own},
    proto,
};
use tokio::sync::oneshot;
impl Socket {
    pub(super) fn process_group_effects(
        &self,
        message: &proto::WebMessageInfo,
        metadata: Option<&KeyMetadata>,
        credentials: &Credentials,
        effects: &mut own::Effects,
    ) -> Result<(), Error> {
        let had_chat = effects.chat.is_some();
        let mut chat = effects.chat.take().unwrap_or(own::ChatUpdate {
            id: jid::normalized_user(messages::incoming::chat_id(
                message
                    .key
                    .as_ref()
                    .ok_or(messages::Error::Envelope("missing group key"))?,
            )?),
            ..Default::default()
        });
        group_effects::process(
            message,
            metadata,
            credentials.me_id()?,
            &mut chat,
            |event| {
                let _ = self.inner.events.send(Event::GroupEffects(vec![event]));
            },
        )?;
        if had_chat || chat.name.is_some() || chat.description.is_some() || chat.read_only.is_some()
        {
            effects.chat = Some(chat);
        }
        Ok(())
    }
    /// Upsert a parsed notification/history envelope and apply supported effects.
    /// Recovered PDO messages retain their separate direct-emission semantics.
    pub async fn upsert_message(
        &self,
        envelope: &messages::Envelope,
        kind: own::UpsertKind,
    ) -> Result<(), Error> {
        let _serial = self.inner.message_lock.lock().await;
        self.publish_message(
            &envelope.message,
            Some(messages::EnvelopeMetadata::from(envelope)),
            kind,
        )
        .await
    }
    pub(super) async fn publish_message(
        &self,
        message: &proto::WebMessageInfo,
        metadata: Option<messages::EnvelopeMetadata>,
        kind: own::UpsertKind,
    ) -> Result<(), Error> {
        self.with_buffered_events(Box::pin(
            self.publish_message_inner(message, metadata, kind),
        ))
        .await
    }
    async fn publish_message_inner(
        &self,
        message: &proto::WebMessageInfo,
        metadata: Option<messages::EnvelopeMetadata>,
        kind: own::UpsertKind,
    ) -> Result<(), Error> {
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        self.inner
            .events
            .send(Event::MessagesUpserted(own::Upsert {
                messages: vec![message.clone()],
                kind,
                request_id: None,
                metadata: metadata.iter().cloned().collect(),
                metadata_indices: vec![],
                runtime_fields: vec![],
            }))?;
        if let Some(name) = message.push_name.as_ref().filter(|s| !s.is_empty()) {
            let key = message
                .key
                .as_ref()
                .ok_or(messages::Error::Envelope("missing upsert key"))?;
            if key.from_me != Some(true) {
                let target = key
                    .participant
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .or(key.remote_jid.as_deref())
                    .unwrap_or("");
                self.inner
                    .events
                    .send(Event::ContactsUpdated(vec![Contact {
                        id: jid::normalized_user(target),
                        notify: Some(name.clone()),
                        verified_name: message.verified_biz_name.clone(),
                        ..Default::default()
                    }]))?;
            } else {
                let (reply, response) = oneshot::channel();
                self.inner
                    .commands
                    .send(Command::PushName {
                        name: name.clone(),
                        reply,
                    })
                    .await
                    .map_err(|_| Error::Closed(428))?;
                response.await.map_err(|_| Error::Closed(428))??;
            }
        }
        let history_notification = crate::history::notification(message.message.as_ref());
        let history_eligible = history_notification
            .as_ref()
            .is_some_and(|notification| self.inner.history_policy.should_process(notification));
        if let Some(notification) = &history_notification {
            self.inner
                .events
                .history_notification(notification, history_eligible);
        }
        if history_notification.is_some() {
            self.inner.events.initial_history(history_eligible)?;
        }
        if history_eligible && self.initial_sync_phase() == super::initial_sync::Phase::Syncing {
            let sync_socket = self.clone();
            let mut sync =
                tokio::spawn(
                    async move { sync_socket.run_admitted_initial_app_state_sync().await },
                );
            let process_socket = self.clone();
            let process_message = message.clone();
            let process_metadata = metadata.clone();
            let process_credentials = credentials.clone();
            let mut processing = tokio::spawn(async move {
                process_socket
                    .process_published_message(
                        &process_message,
                        process_metadata.as_ref(),
                        &process_credentials,
                        history_eligible,
                    )
                    .await
            });
            // Promise.all rejects at the first error while the other promise
            // continues. Dropped JoinHandles detach their tasks rather than abort.
            tokio::select! {
                result=&mut sync=>{result.map_err(|_|Error::Task)??;processing.await.map_err(|_|Error::Task)??;},
                result=&mut processing=>{result.map_err(|_|Error::Task)??;sync.await.map_err(|_|Error::Task)??;},
            }
        } else {
            self.process_published_message(
                message,
                metadata.as_ref(),
                &credentials,
                history_eligible,
            )
            .await?;
        }
        if message
            .message
            .as_ref()
            .and_then(|m| m.protocol_message.as_ref())
            .is_some_and(|p| p.app_state_sync_key_share.is_some())
            && self.initial_sync_phase() == super::initial_sync::Phase::Syncing
        {
            self.run_initial_app_state_sync().await?;
        }
        Ok(())
    }
    async fn process_published_message(
        &self,
        message: &proto::WebMessageInfo,
        metadata: Option<&messages::EnvelopeMetadata>,
        credentials: &Credentials,
        history_eligible: bool,
    ) -> Result<(), Error> {
        let mut effects =
            own::effects_received(message, credentials.account_settings.unarchive_chats)?;
        if let Some(update) = effects.mutation.take() {
            self.inner
                .events
                .send(Event::MessageMutations(vec![update]))?;
        }
        if let Some(mut update) = effects.reaction.take() {
            update.key_metadata = metadata.as_ref().map(|m| m.key_metadata.clone());
            self.inner
                .events
                .send(Event::MessageReactions(vec![update]))?;
        }
        self.process_phone_protocol(message, history_eligible)
            .await?;
        self.process_event_response(
            message,
            metadata.as_ref().map(|m| &m.key_metadata),
            credentials,
        )
        .await?;
        self.process_group_effects(
            message,
            metadata.as_ref().map(|m| &m.key_metadata),
            credentials,
            &mut effects,
        )?;
        if let Some(update) = effects.chat {
            self.inner.events.send(Event::ChatsUpdated(vec![update]))?;
        }
        Ok(())
    }
}
