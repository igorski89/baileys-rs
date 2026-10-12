use super::{Error, Socket};
use crate::{app_state, media};
use indexmap::IndexMap;

impl Socket {
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
