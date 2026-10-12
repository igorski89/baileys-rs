use super::{Command, Error, Event, Inner, Socket};
use crate::{
    app_state, auth,
    binary::BinaryNode,
    messages,
    notifications::{self, Effect},
    receipts,
};
use std::sync::Arc;
use tokio::sync::mpsc;

impl Socket {
    /// Dispatch a server_sync notification once. Disable automatic processing
    /// when dispatching raw Node events manually. ACK failure is warning-only,
    /// including when processing has already failed, as in upstream's finally.
    pub async fn handle_app_state_notification(&self, node: &BinaryNode) -> Result<bool, Error> {
        if node.tag != "notification" || node.attrs.get("type").is_none_or(|t| t != "server_sync") {
            return Ok(false);
        }
        let result = {
            let _serial = self.inner.notification_lock.lock().await;
            self.process_app_state_notification(node).await
        };
        self.finish_app_state_notification(node, result).await
    }
    /// Dispatch a server_sync, group, picture or account-sync notification once.
    /// Returns false for other stanzas. The ACK is attempted after success or failure.
    pub async fn handle_notification(&self, node: &BinaryNode) -> Result<bool, Error> {
        let kind = node.attrs.get("type").map(String::as_str);
        if node.tag != "notification"
            || !(matches!(kind, Some("server_sync" | "devices"))
                || notifications::is_supported(kind))
        {
            return Ok(false);
        }
        let result = {
            let _serial = self.inner.notification_lock.lock().await;
            self.process_notification(node).await
        };
        self.finish_app_state_notification(node, result).await
    }
    async fn process_notification(&self, node: &BinaryNode) -> Result<(), Error> {
        if node.attrs.get("type").is_some_and(|t| t == "server_sync") {
            return self.process_app_state_notification(node).await;
        }
        if node.attrs.get("type").is_some_and(|t| t == "devices") {
            // Upstream logs device-list failures without failing the notification.
            if let Err(error) = self.process_devices_notification(node).await {
                tracing::error!(?error, "failed to handle devices notification");
            }
            return Ok(());
        }
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        let me = credentials.me_id().map_err(Error::Auth)?.to_owned();
        let parsed = notifications::process(node, &me)?;
        for effect in parsed.effects {
            match effect {
                Effect::GroupCreated(group) => {
                    let mut chat = crate::history::Chat {
                        conversation: crate::proto::Conversation {
                            id: group.metadata.id.clone(),
                            name: group.metadata.subject.clone(),
                            ..Default::default()
                        },
                        last_message_recv_timestamp: None,
                        runtime_fields: Default::default(),
                    };
                    // Creation is a JS number upstream; keep values a uint64 cannot hold.
                    match group.metadata.creation {
                        Some(time)
                            if time >= 0.0 && time.fract() == 0.0 && time < 2f64.powi(64) =>
                        {
                            chat.conversation.conversation_timestamp = Some(time as u64)
                        }
                        time => {
                            chat.runtime_fields.insert(
                                "conversationTimestamp".into(),
                                time.and_then(serde_json::Number::from_f64)
                                    .map_or(serde_json::Value::Null, Into::into),
                            );
                        }
                    }
                    self.inner.events.send(Event::ChatsUpserted(vec![chat]))?;
                    self.inner
                        .events
                        .send(Event::GroupsUpserted(vec![*group]))?;
                }
                Effect::Picture { id, removed } => {
                    self.inner
                        .events
                        .send(Event::ContactsUpdated(vec![auth::Contact {
                            id,
                            img_url: Some(Some(if removed { "removed" } else { "changed" }.into())),
                            ..Default::default()
                        }]))?;
                }
                Effect::DisappearingMode {
                    duration,
                    timestamp,
                } => {
                    // Credentials hold integers; a NaN source value has no stored form.
                    let mode = auth::DisappearingMode {
                        ephemeral_expiration: (duration.is_finite()
                            && duration >= 0.0
                            && duration <= u32::MAX as f64)
                            .then_some(duration as u32),
                        ephemeral_setting_timestamp: timestamp
                            .is_finite()
                            .then_some(timestamp as i64),
                    };
                    let (reply, response) = tokio::sync::oneshot::channel();
                    self.inner
                        .commands
                        .send(Command::DisappearingMode { mode, reply })
                        .await
                        .map_err(|_| Error::Closed(428))?;
                    response.await.map_err(|_| Error::Closed(428))??;
                }
                Effect::Blocklist { jid, add } => {
                    self.inner.events.send(Event::BlocklistUpdated {
                        blocklist: vec![jid],
                        add,
                    })?;
                }
            }
        }
        if let Some(message) = parsed.message {
            let context = messages::addressing_context(node);
            let key = message.key.clone().unwrap_or_default();
            let author = key
                .participant
                .clone()
                .or_else(|| key.remote_jid.clone())
                .unwrap_or_default();
            let envelope = messages::Envelope {
                message,
                key_metadata: messages::KeyMetadata {
                    addressing_mode: context.addressing_mode,
                    ..Default::default()
                },
                category: None,
                retry_count: None,
                sender: author.clone(),
                author,
            };
            self.upsert_message(&envelope, messages::own::UpsertKind::Append)
                .await?;
        }
        Ok(())
    }
    async fn process_devices_notification(&self, node: &BinaryNode) -> Result<(), Error> {
        let Some(list) = crate::devices::parse_notification(node) else {
            return Ok(());
        };
        let _serial = self.inner.devices_lock.lock().await;
        for (user, entries) in &list.users {
            if list.change == crate::devices::Change::Remove {
                let repository = self.inner.decryptor.direct.clone();
                let jids: Vec<_> = entries.iter().map(|(raw, _)| raw.clone()).collect();
                tokio::task::spawn_blocking(move || repository.delete_sessions(&jids))
                    .await
                    .map_err(|_| Error::Task)??;
            }
            let devices: Vec<_> = entries.iter().map(|(_, jid)| jid.clone()).collect();
            self.inner
                .devices
                .apply_change(user, list.change, &devices)?;
        }
        Ok(())
    }
    async fn process_app_state_notification(&self, node: &BinaryNode) -> Result<(), Error> {
        if let Some(collection) = node.child("collection") {
            let name = collection
                .attrs
                .get("name")
                .ok_or(app_state::Error::Input("notification collection name"))?
                .clone();
            let options = self
                .inner
                .app_state_options
                .lock()
                .map_err(|_| Error::Config)?
                .clone();
            self.resync_app_state(&[name], false, &options).await?;
        }
        Ok(())
    }
    async fn finish_app_state_notification(
        &self,
        node: &BinaryNode,
        result: Result<(), Error>,
    ) -> Result<bool, Error> {
        if let Err(error) = self
            .send_node(receipts::acknowledgement(node, None, None))
            .await
        {
            tracing::warn!(?error, "failed to acknowledge app-state notification");
        }
        result?;
        Ok(true)
    }
}
pub(super) fn spawn_worker(
    inner: &Arc<Inner>,
    mut receiver: mpsc::UnboundedReceiver<super::dispatch::OnlineNode>,
) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        while let Some(online) = receiver.recv().await {
            let super::dispatch::OnlineNode { node, scope } = online;
            let Some(inner) = weak.upgrade() else { break };
            // Acquire in arrival order, but release before ACK completion so
            // queued processing can continue as in upstream's finally block.
            let guard = inner.notification_lock.clone().lock_owned().await;
            tokio::spawn(async move {
                let _scope = scope;
                let socket = Socket { inner };
                let result = socket.process_notification(&node).await;
                drop(guard);
                if let Err(error) = socket.finish_app_state_notification(&node, result).await {
                    tracing::warn!(?error, "app-state notification processing failed");
                }
            });
        }
    });
}
