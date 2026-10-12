use super::{Error, Socket};
use crate::{app_state, media};
use indexmap::IndexMap;

impl Socket {
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
    /// Synchronize and atomically persist collection versions, returning actions
    /// for the caller to apply. Chat/contact/settings event application is separate.
    pub async fn sync_app_state(
        &self,
        collections: &[String],
        options: &app_state::SyncOptions,
    ) -> Result<app_state::SyncResult, Error> {
        *self
            .inner
            .app_state_options
            .lock()
            .map_err(|_| Error::Config)? = options.clone();
        Ok(self
            .app_state_synchronizer()
            .await?
            .resync(collections, options)
            .await?)
    }
    /// Synchronize collection state, then publish chat/contact/settings actions.
    pub async fn resync_app_state(
        &self,
        collections: &[String],
        initial_sync: bool,
        options: &app_state::SyncOptions,
    ) -> Result<app_state::SyncResult, Error> {
        self.with_buffered_events(self.resync_app_state_inner(collections, initial_sync, options))
            .await
    }
    async fn resync_app_state_inner(
        &self,
        collections: &[String],
        initial_sync: bool,
        options: &app_state::SyncOptions,
    ) -> Result<app_state::SyncResult, Error> {
        let _serial = self.inner.app_state_apply_lock.lock().await;
        let result = self.sync_app_state(collections, options).await?;
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
        let Ok(options) = self.inner.app_state_options.lock().map(|o| o.clone()) else {
            return;
        };
        let weak = std::sync::Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            let Some(inner) = weak.upgrade() else { return };
            let socket = Socket { inner };
            if let Err(error) = socket.resync_app_state(&collections, false, &options).await {
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
    pub async fn retry_blocked_app_state(
        &self,
        options: &app_state::SyncOptions,
    ) -> Result<app_state::SyncResult, Error> {
        let sync = self.app_state_synchronizer().await?;
        let collections = sync.take_blocked_collections().await;
        Ok(sync.resync(&collections, options).await?)
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
