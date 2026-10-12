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
        Ok(self
            .app_state_synchronizer()
            .await?
            .resync(collections, options)
            .await?)
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
