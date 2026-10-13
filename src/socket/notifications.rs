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
    /// Dispatch a server_sync, group, picture, account-sync, device, encrypt,
    /// privacy-token, newsletter or MEX notification once.
    /// Returns false for other stanzas. The ACK is attempted after success or failure.
    pub async fn handle_notification(&self, node: &BinaryNode) -> Result<bool, Error> {
        let kind = node.attrs.get("type").map(String::as_str);
        if node.tag != "notification"
            || !(matches!(
                kind,
                Some(
                    "server_sync" | "devices" | "encrypt" | "privacy_token" | "newsletter" | "mex"
                )
            ) || notifications::is_supported(kind))
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
        if node.attrs.get("type").is_some_and(|t| t == "encrypt") {
            return self.process_encrypt_notification(node).await.map(|_| ());
        }
        if node.attrs.get("type").is_some_and(|t| t == "privacy_token") {
            return self.process_privacy_token_notification(node).await;
        }
        match node.attrs.get("type").map(String::as_str) {
            Some("newsletter") => {
                return self
                    .apply_newsletter_effects(crate::newsletters::notification(node))
                    .await;
            }
            Some("mex") => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0.0, |elapsed| elapsed.as_millis() as f64);
                return self
                    .apply_newsletter_effects(crate::newsletters::mex_notification(node, now)?)
                    .await;
            }
            _ => {}
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
    /// Server pre-key-low notices upload more keys; peer notices refresh a changed identity.
    async fn process_encrypt_notification(
        &self,
        node: &BinaryNode,
    ) -> Result<Option<IdentityChange>, Error> {
        if node
            .attrs
            .get("from")
            .is_none_or(|s| s != crate::jid::S_WHATSAPP_NET)
        {
            let result = self.handle_identity_change(node).await?;
            if result == IdentityChange::NoIdentityNode {
                tracing::info!("unknown encrypt notification");
            }
            return Ok(Some(result));
        }
        let id = node.attrs.get("id").filter(|s| !s.is_empty()).cloned();
        if let Some(id) = &id
            && self
                .inner
                .prekey_low
                .lock()
                .map_err(|_| Error::Task)?
                .contains(id)
        {
            return Ok(None);
        }
        let count = node
            .child("count")
            .ok_or(notifications::Error::MissingChild)?
            .attrs
            .get("value")
            .map_or(f64::NAN, |s| crate::privacy::number(s));
        if count < f64::from(crate::defaults::MIN_PREKEY_COUNT) {
            let _guard = id.map(|id| {
                if let Ok(mut set) = self.inner.prekey_low.lock() {
                    set.insert(id.clone());
                }
                PreKeyLow {
                    inner: self.inner.clone(),
                    id,
                }
            });
            self.upload_pre_keys(crate::defaults::MIN_PREKEY_COUNT)
                .await?;
        }
        Ok(None)
    }
    /// Generate and upload one-time pre-keys, waiting for an upload already in progress.
    pub async fn upload_pre_keys(&self, count: u32) -> Result<(), Error> {
        let (reply, response) = tokio::sync::oneshot::channel();
        self.inner
            .commands
            .send(Command::UploadPreKeys { count, reply })
            .await
            .map_err(|_| Error::Closed(428))?;
        response.await.map_err(|_| Error::Closed(428))?
    }
    /// Check that the server still holds our key bundle. Without one, pre-keys
    /// are uploaded again and the check still fails, as upstream does.
    pub async fn digest_key_bundle(&self) -> Result<(), Error> {
        let result = self
            .query(
                BinaryNode::new("iq")
                    .attr("to", crate::jid::S_WHATSAPP_NET)
                    .attr("type", "get")
                    .attr("xmlns", "encrypt")
                    .with_content(crate::binary::NodeContent::Children(vec![BinaryNode::new(
                        "digest",
                    )])),
                None,
            )
            .await?;
        if result.child("digest").is_none() {
            self.upload_pre_keys(crate::defaults::MIN_PREKEY_COUNT)
                .await?;
            return Err(Error::DigestMissing);
        }
        Ok(())
    }
    /// Replace the signed pre-key on the server with a new one under the next
    /// key ID, then store it in the credentials.
    pub async fn rotate_signed_pre_key(&self) -> Result<(), Error> {
        use crate::binary::NodeContent;
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        let key = crate::crypto::signed_key_pair(
            &credentials.signed_identity_key,
            credentials.signed_pre_key.key_id + 1,
        )?;
        let bytes = |tag: &str, value: Vec<u8>| {
            BinaryNode::new(tag).with_content(NodeContent::Bytes(value))
        };
        self.query(
            BinaryNode::new("iq")
                .attr("to", crate::jid::S_WHATSAPP_NET)
                .attr("type", "set")
                .attr("xmlns", "encrypt")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("rotate").with_content(NodeContent::Children(vec![
                        BinaryNode::new("skey").with_content(NodeContent::Children(vec![
                            bytes("id", key.key_id.to_be_bytes()[1..].to_vec()),
                            bytes("value", key.key_pair.public.to_vec()),
                            bytes("signature", key.signature.clone()),
                        ])),
                    ])),
                ])),
            None,
        )
        .await?;
        let (reply, response) = tokio::sync::oneshot::channel();
        self.inner
            .commands
            .send(Command::SignedPreKey {
                key: Box::new(key),
                reply,
            })
            .await
            .map_err(|_| Error::Closed(428))?;
        response.await.map_err(|_| Error::Closed(428))?
    }
    /// Refresh the session of a contact whose primary-device identity changed.
    pub async fn handle_identity_change(&self, node: &BinaryNode) -> Result<IdentityChange, Error> {
        let Some(from) = node.attrs.get("from").filter(|s| !s.is_empty()).cloned() else {
            return Ok(IdentityChange::InvalidNotification);
        };
        if node.child("identity").is_none() {
            return Ok(IdentityChange::NoIdentityNode);
        }
        if let Some(device) = crate::jid::Jid::parse(&from)
            .and_then(|jid| jid.device)
            .filter(|device| *device != 0)
        {
            return Ok(IdentityChange::SkippedCompanionDevice(device));
        }
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        if let Some(me) = &credentials.me
            && !me.id.is_empty()
            && (crate::jid::are_same_user(&from, &me.id)
                || me
                    .lid
                    .as_deref()
                    .is_some_and(|lid| !lid.is_empty() && crate::jid::are_same_user(&from, lid)))
        {
            return Ok(IdentityChange::SkippedSelfPrimary);
        }
        {
            let mut debounce = self
                .inner
                .identity_debounce
                .lock()
                .map_err(|_| Error::Task)?;
            let now = tokio::time::Instant::now();
            debounce.retain(|_, expires| *expires > now);
            if debounce.contains_key(&from) {
                return Ok(IdentityChange::Debounced);
            }
            debounce.insert(from.clone(), now + IDENTITY_DEBOUNCE);
        }
        let repository = self.inner.decryptor.direct.clone();
        let jid = from.clone();
        // Validation errors mean no usable session, as upstream's catch reports.
        let exists = tokio::task::spawn_blocking(move || repository.validate_session(&jid))
            .await
            .map_err(|_| Error::Task)?
            .unwrap_or(false);
        if !exists {
            return Ok(IdentityChange::SkippedNoSession);
        }
        if node.attrs.get("offline").is_some_and(|s| !s.is_empty()) {
            return Ok(IdentityChange::SkippedOffline);
        }
        // Token re-issuance runs alongside the session refresh, not after it.
        let socket = self.clone();
        let peer = from.clone();
        tokio::spawn(async move {
            if let Err(error) = socket.reissue_privacy_token(&peer).await {
                tracing::debug!(?error, "failed to re-issue tctoken after identity change");
            }
        });
        Ok(match self.assert_sessions(&[from], true).await {
            Ok(_) => IdentityChange::SessionRefreshed,
            Err(error) => {
                tracing::warn!(?error, "failed to assert sessions after identity change");
                IdentityChange::SessionRefreshFailed
            }
        })
    }
    fn privacy_repository(&self) -> crate::privacy::Repository {
        crate::privacy::Repository::with_mappings(
            self.inner.store.clone(),
            self.inner.decryptor.direct.lid_mapping().clone(),
        )
    }
    async fn reissue_privacy_token(&self, from: &str) -> Result<(), Error> {
        let normalized = crate::jid::normalized_user(from);
        let repo = self.privacy_repository();
        let to_lid = self.server_props()?.lid_trusted_token_issue_to_lid;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Config)?
            .as_secs();
        let lookup = repo.clone();
        let Some((storage, sender, issuance)) = tokio::task::spawn_blocking(move || {
            let storage = lookup.storage_jid(&normalized)?;
            let Some(sender) = lookup.sender_timestamp(&storage)? else {
                return Ok(None);
            };
            if crate::privacy::is_expired(Some(&auth::TokenTimestamp::Number(sender.into())), now) {
                return Ok(None);
            }
            let issuance = lookup.issuance_jid(&normalized, to_lid)?;
            Ok::<_, Error>(Some((storage, sender, issuance)))
        })
        .await
        .map_err(|_| Error::Task)??
        else {
            return Ok(());
        };
        let result = self.issue_privacy_tokens(&[issuance], Some(sender)).await?;
        tokio::task::spawn_blocking(move || repo.store_from_iq(&result, &storage))
            .await
            .map_err(|_| Error::Task)??;
        Ok(())
    }
    async fn process_privacy_token_notification(&self, node: &BinaryNode) -> Result<(), Error> {
        if node.child("tokens").is_none() {
            return Ok(());
        }
        let from = node
            .attrs
            .get("from")
            .map(|s| crate::jid::normalized_user(s))
            .unwrap_or_default();
        // The sender_lid attribute supplies the storage LID directly when present.
        let sender_lid = node
            .attrs
            .get("sender_lid")
            .map(|s| crate::jid::normalized_user(s))
            .filter(|s| s.ends_with("@lid"));
        let repo = self.privacy_repository();
        let node = node.clone();
        tokio::task::spawn_blocking(move || {
            let fallback = match sender_lid {
                Some(lid) => lid,
                None => repo.storage_jid(&from)?,
            };
            repo.store_from_iq(&node, &fallback)
        })
        .await
        .map_err(|_| Error::Task)??;
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
                .apply_change(user, list.change, &devices)
                .await?;
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
            self.resync_app_state(&[name], false).await?;
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
const IDENTITY_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(5);
/// Outcome of an identity-change notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityChange {
    NoIdentityNode,
    InvalidNotification,
    SkippedCompanionDevice(u16),
    SkippedSelfPrimary,
    Debounced,
    SkippedOffline,
    SkippedNoSession,
    SessionRefreshed,
    SessionRefreshFailed,
}
// Clears the in-flight pre-key-low stanza when the upload ends or is cancelled.
struct PreKeyLow {
    inner: Arc<Inner>,
    id: String,
}
impl Drop for PreKeyLow {
    fn drop(&mut self) {
        if let Ok(mut set) = self.inner.prekey_low.lock() {
            set.remove(&self.id);
        }
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
