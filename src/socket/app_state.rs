use super::{Error, Socket};
use crate::{app_state, media};
use indexmap::IndexMap;

impl Socket {
    pub(super) async fn run_initial_app_state_sync(&self) -> Result<(), Error> {
        if self.initial_sync_phase() != super::initial_sync::Phase::Syncing {
            return Ok(());
        }
        self.run_admitted_initial_app_state_sync().await
    }
    pub(super) async fn run_admitted_initial_app_state_sync(&self) -> Result<(), Error> {
        let sync = self.app_state_synchronizer().await?;
        sync.take_blocked_collections().await;
        let collections = [
            "critical_block",
            "critical_unblock_low",
            "regular_high",
            "regular_low",
            "regular",
        ]
        .map(str::to_owned);
        self.resync_app_state(&collections, true).await?;
        self.inner.events.initial_sync_complete()?;
        let (reply, response) = tokio::sync::oneshot::channel();
        self.inner
            .commands
            .send(super::Command::IncrementAccountSync { reply })
            .await
            .map_err(|_| Error::Closed(428))?;
        response.await.map_err(|_| Error::Closed(428))?
    }
    async fn app_state_synchronizer(&self) -> Result<&app_state::Synchronizer, Error> {
        self.inner
            .app_state_sync
            .get_or_try_init(|| async {
                let client = self
                    .inner
                    .media_http
                    .get_or_try_init(|| async { media::HttpClient::new() })
                    .await?
                    .clone();
                let downloader = app_state::BlobDownloader::from_http(client, Default::default());
                let weak = std::sync::Arc::downgrade(&self.inner);
                let transport = app_state::CollectionTransport::new(move |collections| {
                    let weak = weak.clone();
                    async move {
                        let inner = weak.upgrade().ok_or(app_state::Error::Transport {
                            message: "connection dropped".into(),
                            status_code: Some(428),
                        })?;
                        let socket = Socket { inner };
                        socket
                            .query_app_state_collections(&collections, Default::default())
                            .await
                            .map_err(|error| app_state::Error::Transport {
                                status_code: error.status_code(),
                                message: error.to_string(),
                            })
                    }
                });
                Ok::<_, Error>(app_state::Synchronizer::new(
                    std::sync::Arc::new(self.inner.store.clone()),
                    app_state::KeyProvider::from_store(self.inner.store.clone()),
                    downloader,
                    transport,
                ))
            })
            .await
    }
    /// Send one app-state patch: resynchronise its collection, encode against the
    /// resulting version, upload, persist the new version and, when own events are enabled,
    /// publish the patch's own effects.
    pub async fn app_patch(&self, patch: app_state::PatchCreate) -> Result<(), Error> {
        use crate::{
            auth::{KeyCategory, KeyMutation, KeyStore, KeyValue},
            binary::{BinaryNode, NodeContent},
        };
        use prost::Message as _;
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        let key_id = credentials
            .my_app_state_key_id
            .clone()
            .filter(|id| !id.is_empty())
            .ok_or(app_state::Error::Modification("App state key not present!"))?;
        let name = patch.name.clone();
        let provider = app_state::KeyProvider::from_store(self.inner.store.clone());
        let (initial, encoded) = {
            let _serial = self.inner.app_state_patch_lock.lock().await;
            // Upstream runs the resync and the upload in one key transaction, so the
            // resynced version is only stored together with the uploaded one; a failed
            // upload leaves the stored version where it was.
            let mut synced = self
                .with_buffered_events(self.resync_app_state_inner(
                    std::slice::from_ref(&name),
                    false,
                    false,
                ))
                .await?;
            let initial = match synced.states.shift_remove(&name) {
                Some(state) => state,
                None => {
                    let store = self.inner.store.clone();
                    let id = name.clone();
                    tokio::task::spawn_blocking(move || {
                        store
                            .get(KeyCategory::AppStateSyncVersion, std::slice::from_ref(&id))
                            .map(|mut values| match values.remove(&id) {
                                Some(KeyValue::AppStateSyncVersion(state)) => state,
                                _ => app_state::new_lthash_state(),
                            })
                    })
                    .await
                    .map_err(|_| Error::Task)??
                }
            };
            let encoded = app_state::encode_patch(&patch, &key_id, &initial, &provider).await?;
            let node = BinaryNode::new("iq")
                .attr("to", crate::jid::S_WHATSAPP_NET)
                .attr("type", "set")
                .attr("xmlns", "w:sync:app:state")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("sync").with_content(NodeContent::Children(vec![
                        BinaryNode::new("collection")
                            .attr("name", &name)
                            .attr("version", (encoded.state.version - 1).to_string())
                            .attr("return_snapshot", "false")
                            .with_content(NodeContent::Children(vec![
                                BinaryNode::new("patch").with_content(NodeContent::Bytes(
                                    encoded.patch.encode_to_vec(),
                                )),
                            ])),
                    ])),
                ]));
            self.query(node, None).await?;
            // The transaction commit, with upstream's retries.
            let options = &self.inner.app_state_options;
            let mut attempt = 0;
            loop {
                attempt += 1;
                let store = self.inner.store.clone();
                let state = encoded.state.clone();
                let id = name.clone();
                match tokio::task::spawn_blocking(move || {
                    store.set(&[KeyMutation::put(id, KeyValue::AppStateSyncVersion(state))])
                })
                .await
                .map_err(|_| Error::Task)?
                {
                    Ok(()) => break,
                    Err(error) if attempt >= options.max_commit_attempts.max(1) => {
                        return Err(error.into());
                    }
                    Err(error) => {
                        tracing::warn!(attempt, ?error, "retrying app-state patch commit");
                        tokio::time::sleep(options.commit_retry_delay).await;
                    }
                }
            }
            (initial, encoded)
        };
        if self.inner.own_messages.is_some() {
            let mut patches = [crate::proto::SyncdPatch {
                version: Some(crate::proto::SyncdVersion {
                    version: Some(encoded.state.version),
                }),
                ..encoded.patch
            }];
            let decoded =
                app_state::decode_patches(&mut patches, &name, &initial, &provider, None, true)
                    .await?;
            self.apply_app_state_mutations(&decoded.mutations, false)
                .await?;
        }
        Ok(())
    }
    /// Modify a chat or app setting (archive, mute, pin, read state, stars, labels,
    /// contacts, quick replies) through an app-state patch.
    pub async fn chat_modify(
        &self,
        modification: app_state::ChatModification,
        jid: &str,
    ) -> Result<(), Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Config)?
            .as_millis() as i64;
        self.app_patch(app_state::chat_modification_patch(modification, jid, now)?)
            .await
    }
    /// Enable or disable link previews in the account's privacy settings.
    pub async fn update_disable_link_previews_privacy(&self, disabled: bool) -> Result<(), Error> {
        self.chat_modify(
            app_state::ChatModification::DisableLinkPreviews(
                crate::proto::sync_action_value::PrivacySettingDisableLinkPreviewsAction {
                    is_previews_disabled: Some(disabled),
                },
            ),
            "",
        )
        .await
    }
    /// Star or unstar a message; only the first listed message is sent, as upstream.
    pub async fn star(
        &self,
        jid: &str,
        messages: Vec<app_state::StarredMessage>,
        star: bool,
    ) -> Result<(), Error> {
        self.chat_modify(app_state::ChatModification::Star { messages, star }, jid)
            .await
    }
    pub async fn add_or_edit_contact(
        &self,
        jid: &str,
        contact: crate::proto::sync_action_value::ContactAction,
    ) -> Result<(), Error> {
        self.chat_modify(app_state::ChatModification::Contact(Some(contact)), jid)
            .await
    }
    pub async fn remove_contact(&self, jid: &str) -> Result<(), Error> {
        self.chat_modify(app_state::ChatModification::Contact(None), jid)
            .await
    }
    pub async fn add_label(&self, jid: &str, label: app_state::LabelBody) -> Result<(), Error> {
        self.chat_modify(app_state::ChatModification::AddLabel(label), jid)
            .await
    }
    pub async fn add_chat_label(&self, jid: &str, label_id: &str) -> Result<(), Error> {
        self.chat_modify(
            app_state::ChatModification::AddChatLabel {
                label_id: label_id.into(),
            },
            jid,
        )
        .await
    }
    pub async fn remove_chat_label(&self, jid: &str, label_id: &str) -> Result<(), Error> {
        self.chat_modify(
            app_state::ChatModification::RemoveChatLabel {
                label_id: label_id.into(),
            },
            jid,
        )
        .await
    }
    pub async fn add_message_label(
        &self,
        jid: &str,
        message_id: &str,
        label_id: &str,
    ) -> Result<(), Error> {
        self.chat_modify(
            app_state::ChatModification::AddMessageLabel {
                message_id: message_id.into(),
                label_id: label_id.into(),
            },
            jid,
        )
        .await
    }
    pub async fn remove_message_label(
        &self,
        jid: &str,
        message_id: &str,
        label_id: &str,
    ) -> Result<(), Error> {
        self.chat_modify(
            app_state::ChatModification::RemoveMessageLabel {
                message_id: message_id.into(),
                label_id: label_id.into(),
            },
            jid,
        )
        .await
    }
    pub async fn add_or_edit_quick_reply(&self, reply: app_state::QuickReply) -> Result<(), Error> {
        self.chat_modify(app_state::ChatModification::QuickReply(reply), "")
            .await
    }
    pub async fn remove_quick_reply(&self, timestamp: &str) -> Result<(), Error> {
        self.chat_modify(
            app_state::ChatModification::QuickReply(app_state::QuickReply {
                timestamp: Some(timestamp.into()),
                deleted: Some(true),
                ..Default::default()
            }),
            "",
        )
        .await
    }
    /// Synchronize and atomically persist collection versions, returning actions
    /// for the caller to apply. Chat/contact/settings event application is separate.
    pub async fn sync_app_state(
        &self,
        collections: &[String],
    ) -> Result<app_state::SyncResult, Error> {
        self.sync_app_state_with(collections, true).await
    }
    async fn sync_app_state_with(
        &self,
        collections: &[String],
        commit: bool,
    ) -> Result<app_state::SyncResult, Error> {
        let sync = self.app_state_synchronizer().await?;
        let options = &self.inner.app_state_options;
        Ok(if commit {
            sync.resync(collections, options).await?
        } else {
            sync.resync_staged(collections, options).await?
        })
    }
    /// Synchronize collection state, then publish chat/contact/settings actions.
    pub async fn resync_app_state(
        &self,
        collections: &[String],
        initial_sync: bool,
    ) -> Result<app_state::SyncResult, Error> {
        self.with_buffered_events(self.resync_app_state_inner(collections, initial_sync, true))
            .await
    }
    async fn resync_app_state_inner(
        &self,
        collections: &[String],
        initial_sync: bool,
        commit: bool,
    ) -> Result<app_state::SyncResult, Error> {
        let _serial = self.inner.app_state_apply_lock.lock().await;
        let result = self.sync_app_state_with(collections, commit).await?;
        self.apply_app_state_actions(&result.mutations, initial_sync)
            .await?;
        Ok(result)
    }
    pub async fn apply_app_state_mutations(
        &self,
        mutations: &app_state::MutationMap,
        initial_sync: bool,
    ) -> Result<(), Error> {
        let _serial = self.inner.app_state_apply_lock.lock().await;
        self.apply_app_state_actions(mutations, initial_sync).await
    }
    async fn apply_app_state_actions(
        &self,
        mutations: &app_state::MutationMap,
        initial_sync: bool,
    ) -> Result<(), Error> {
        use super::{Command, Event};
        use app_state::ActionEvent;
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        let mut initial_settings = credentials.account_settings.clone();
        for mutation in mutations.values() {
            let credentials = super::load_credentials(self.inner.store.clone()).await?;
            let events = app_state::process_action(
                mutation,
                &mut app_state::ActionContext {
                    me: credentials.me.as_ref(),
                    initial_sync,
                    account_settings: initial_sync.then_some(&mut initial_settings),
                },
            )?;
            for event in events {
                let outgoing = match event {
                    ActionEvent::Chat(chat) => Event::ChatsUpdated(vec![chat]),
                    ActionEvent::DeleteMessages(keys) => Event::MessagesDeleted(keys),
                    ActionEvent::Contacts(contacts) => Event::ContactsUpserted(contacts),
                    ActionEvent::Stars(updates) => Event::MessagesStarred(updates),
                    ActionEvent::DeleteChats(ids) => Event::ChatsDeleted(ids),
                    ActionEvent::LabelEdit(label) => Event::LabelEdited(label),
                    ActionEvent::LabelAssociation(association) => {
                        Event::LabelAssociationUpdated(association)
                    }
                    ActionEvent::Setting(setting) => Event::SettingUpdated(setting),
                    ActionEvent::Lock(lock) => Event::ChatLocked(lock),
                    ActionEvent::PushName(name) => {
                        let (reply, response) = tokio::sync::oneshot::channel();
                        self.inner
                            .commands
                            .send(Command::PushName { name, reply })
                            .await
                            .map_err(|_| Error::Closed(428))?;
                        response.await.map_err(|_| Error::Closed(428))??;
                        continue;
                    }
                    ActionEvent::UnarchiveChats(value) => {
                        let (reply, response) = tokio::sync::oneshot::channel();
                        self.inner
                            .commands
                            .send(Command::UnarchiveChats { value, reply })
                            .await
                            .map_err(|_| Error::Closed(428))?;
                        response.await.map_err(|_| Error::Closed(428))??;
                        continue;
                    }
                    ActionEvent::LidMapping { lid, pn } => {
                        let _ = self.inner.events.send(Event::LidMappingUpdated {
                            lid: lid.clone(),
                            pn: pn.clone(),
                        });
                        if let Some(lid) = lid {
                            let mapping = self.inner.decryptor.direct.lid_mapping().clone();
                            match tokio::task::spawn_blocking(move || {
                                mapping
                                    .store_mappings(&[crate::signal::mapping::Mapping { lid, pn }])
                            })
                            .await
                            {
                                Ok(Ok(())) => {}
                                error => tracing::warn!(
                                    ?error,
                                    "failed to store app-state LID-PN mapping"
                                ),
                            }
                        }
                        continue;
                    }
                };
                let _ = self.inner.events.send(outgoing);
            }
        }
        Ok(())
    }
    pub(super) async fn schedule_app_state_retry(&self) {
        let Some(sync) = self.inner.app_state_sync.get() else {
            return;
        };
        match super::load_credentials(self.inner.store.clone()).await {
            Ok(creds)
                if creds
                    .my_app_state_key_id
                    .as_deref()
                    .is_some_and(|id| !id.is_empty()) => {}
            _ => return,
        }
        let collections = sync.take_blocked_collections().await;
        if collections.is_empty() {
            return;
        }
        let weak = std::sync::Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            let Some(inner) = weak.upgrade() else { return };
            let socket = Socket { inner };
            if let Err(error) = socket.resync_app_state(&collections, false).await {
                tracing::warn!(?error, "blocked app-state collection resync failed");
            }
        });
    }
    pub async fn blocked_app_state_collections(&self) -> Vec<String> {
        match self.inner.app_state_sync.get() {
            Some(sync) => sync.blocked_collections().await,
            None => vec![],
        }
    }
    /// Explicitly retry parked collections after keys arrive. This starts a fresh
    /// key cache and removes the parked set before retrying, as upstream does.
    pub async fn retry_blocked_app_state(&self) -> Result<app_state::SyncResult, Error> {
        let sync = self.app_state_synchronizer().await?;
        let collections = sync.take_blocked_collections().await;
        Ok(sync
            .resync(&collections, &self.inner.app_state_options)
            .await?)
    }
    /// Fetch collections and download their snapshots. Callers must verify and
    /// persist state and repeat requests while more patches are available.
    pub async fn query_app_state_collections(
        &self,
        collections: &[app_state::CollectionRequest],
        options: media::DownloadOptions,
    ) -> Result<IndexMap<String, app_state::CollectionResponse>, Error> {
        let response = self
            .query(app_state::collection_query(collections), None)
            .await?;
        let client = self
            .inner
            .media_http
            .get_or_try_init(|| async { media::HttpClient::new() })
            .await?
            .clone();
        let downloader = app_state::BlobDownloader::from_http(client, options);
        Ok(app_state::extract_collections(&response, &downloader).await?)
    }
}
