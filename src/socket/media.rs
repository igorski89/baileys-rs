use super::{Error, Socket};
use crate::media::{self, MediaConnection};
use std::{path::Path, sync::Arc, time::SystemTime};

pub(super) struct MediaCache {
    connection: Option<Result<MediaConnection, Arc<Error>>>,
    host: String,
}
impl Default for MediaCache {
    fn default() -> Self {
        Self {
            connection: None,
            host: media::DEFAULT_MEDIA_HOST.into(),
        }
    }
}
impl Socket {
    /// Coalesce initial/expired queries. A rejected media-connection query remains
    /// cached until the connection ends, matching the upstream stored Promise.
    pub async fn refresh_media_connection(&self, force: bool) -> Result<MediaConnection, Error> {
        let mut cache = self.inner.media_cache.lock().await;
        if matches!(self.state(), super::ConnectionState::Closed { .. }) {
            cache.connection = None;
            return Err(self.closed_error());
        }
        match &cache.connection {
            Some(Err(error)) => return Err(Error::MediaConnection(error.clone())),
            Some(Ok(connection)) if !force && !connection.expired_at(SystemTime::now()) => {
                return Ok(connection.clone());
            }
            _ => {}
        }
        let result = match self.query(MediaConnection::query(), None).await {
            Ok(node) => MediaConnection::parse(&node, SystemTime::now()).map_err(Error::from),
            Err(error) => Err(error),
        };
        match result {
            Ok(connection) => {
                if let Some(host) = connection.hosts.first() {
                    cache.host.clone_from(&host.hostname);
                }
                cache.connection = Some(Ok(connection.clone()));
                Ok(connection)
            }
            Err(error) => {
                let error = Arc::new(error);
                cache.connection = Some(Err(error.clone()));
                Err(Error::MediaConnection(error))
            }
        }
    }
    pub async fn media_host(&self) -> String {
        self.inner.media_cache.lock().await.host.clone()
    }
    pub async fn upload_media(
        &self,
        path: &Path,
        kind: media::MediaType,
        encrypted_hash_base64: &str,
        options: &media::UploadOptions,
    ) -> Result<media::UploadResponse, Error> {
        let client = self
            .inner
            .media_http
            .get_or_try_init(|| async { media::HttpClient::new() })
            .await?;
        Ok(media::upload_with(
            client,
            path,
            kind,
            encrypted_hash_base64,
            options,
            |force| async move {
                self.refresh_media_connection(force)
                    .await
                    .map_err(|e| media::Error::Refresh {
                        status: e.status_code(),
                        message: e.to_string(),
                    })
            },
        )
        .await?)
    }
}
impl crate::media::MediaUploader for Socket {
    fn upload_media<'a>(
        &'a self,
        path: std::path::PathBuf,
        kind: crate::media::MediaType,
        hash: String,
        options: crate::media::UploadOptions,
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<crate::media::UploadResponse, crate::media::Error>,
    > {
        Box::pin(async move {
            Socket::upload_media(self, &path, kind, &hash, &options)
                .await
                .map_err(|e| crate::media::Error::Refresh {
                    status: e.status_code(),
                    message: e.to_string(),
                })
        })
    }
}
impl Socket {
    pub async fn prepare_media_message(
        &self,
        request: &crate::media::Request,
        options: &crate::media::PrepareOptions,
        hooks: &dyn crate::media::PrepareHooks,
    ) -> Result<crate::proto::Message, Error> {
        let http = self
            .inner
            .media_http
            .get_or_try_init(|| async { crate::media::HttpClient::new() })
            .await?;
        let uploader: &dyn crate::media::MediaUploader =
            options.uploader.as_ref().map(|u| u.get()).unwrap_or(self);
        Ok(crate::media::prepare(request, options, http, uploader, hooks).await?)
    }
}
