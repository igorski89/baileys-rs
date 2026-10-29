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
        // Configured hosts come first, as upstream's customUploadHosts do.
        let mut options = options.clone();
        options
            .custom_hosts
            .splice(0..0, self.inner.custom_upload_hosts.iter().cloned());
        Ok(media::upload_with(
            client,
            path,
            kind,
            encrypted_hash_base64,
            &options,
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
        self.prepare_media(request, options, hooks, true).await
    }
    /// `cached` is off for link-preview thumbnails, which upstream prepares
    /// without its media cache.
    pub(super) async fn prepare_media(
        &self,
        request: &crate::media::Request,
        options: &crate::media::PrepareOptions,
        hooks: &dyn crate::media::PrepareHooks,
        cached: bool,
    ) -> Result<crate::proto::Message, Error> {
        let http = self
            .inner
            .media_http
            .get_or_try_init(|| async { crate::media::HttpClient::new() })
            .await?;
        let uploader: &dyn crate::media::MediaUploader =
            options.uploader.as_ref().map(|u| u.get()).unwrap_or(self);
        // A configured media cache applies to every preparation, as upstream's mediaCache.
        let configured;
        let options = match (&options.media_cache, &self.inner.media_store) {
            (None, Some(store)) if cached => {
                configured = crate::media::PrepareOptions {
                    media_cache: Some(store.clone()),
                    ..options.clone()
                };
                &configured
            }
            _ => options,
        };
        Ok(crate::media::prepare(request, options, http, uploader, hooks).await?)
    }
}
impl Socket {
    /// Handle a mediaretry notification explicitly when automatic processing is disabled.
    pub async fn handle_media_notification(
        &self,
        node: &crate::binary::BinaryNode,
    ) -> Result<bool, Error> {
        if node.tag != "notification" || node.attrs.get("type").is_none_or(|s| s != "mediaretry") {
            return Ok(false);
        }
        let result = crate::media::decode_retry(node);
        if let Ok(event) = &result {
            let _ = self
                .inner
                .events
                .send(super::Event::MediaUpdated(vec![event.clone()]));
        }
        self.send_node(crate::receipts::acknowledgement(node, None, None))
            .await?;
        result?;
        Ok(true)
    }
    /// Request a phone reupload and mutate the selected original media payload.
    /// None/zero timeout waits until an ID-matched notification or connection close.
    pub async fn update_media_message(
        &self,
        message: &mut crate::proto::WebMessageInfo,
        limit: Option<std::time::Duration>,
    ) -> Result<(), Error> {
        let media_key = crate::media::retry_media_key(
            message
                .message
                .as_mut()
                .ok_or(crate::media::Error::NotMedia)?,
        )?;
        let key = message
            .key
            .clone()
            .ok_or(crate::media::Error::Input("missing message key"))?;
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        let request = crate::media::retry_request(&key, &media_key, credentials.me_id()?)?;
        let mut updates = self.subscribe();
        let mut state = self.inner.state.clone();
        let wait = async {
            loop {
                // A notification published before close must remain observable after close.
                match updates.try_recv() {
                    Ok(super::Event::MediaUpdated(events)) => {
                        if let Some(event) = events.into_iter().find(|e| e.key.id == key.id) {
                            return Ok(event);
                        }
                        continue;
                    }
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                        return Err(Error::EventLagged(n));
                    }
                    Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                        return Err(Error::Closed(428));
                    }
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
                }
                if let super::ConnectionState::Closed { code, .. } = state.borrow().clone() {
                    return Err(Error::Closed(code));
                }
                tokio::select! {
                    biased;
                    update=updates.recv()=>match update {
                        Ok(super::Event::MediaUpdated(events))=>if let Some(event)=events.into_iter().find(|e|e.key.id==key.id){return Ok(event);},
                        Ok(_)=>{},
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n))=>return Err(Error::EventLagged(n)),
                        Err(_)=>return Err(Error::Closed(428)),
                    },
                    result=state.changed()=>{result.map_err(|_|Error::Closed(428))?;}
                }
            }
        };
        let wait = async {
            match limit.filter(|d| !d.is_zero()) {
                Some(limit) => tokio::time::timeout(limit, wait)
                    .await
                    .map_err(|_| Error::Timeout)?,
                None => wait.await,
            }
        };
        let (_, event) = tokio::try_join!(self.send_node(request), wait)?;
        crate::media::apply_retry(message, &event, &self.media_host().await)?;
        let _ = self
            .inner
            .events
            .send(super::Event::MessageContentsUpdated(vec![
                crate::media::ContentUpdate {
                    key,
                    message: message.message.clone().map(Box::new),
                },
            ]));
        Ok(())
    }
}
impl Socket {
    /// Download an encrypted message, optionally requesting one phone reupload on HTTP 404/410
    /// (and 403 with `DownloadOptions::reupload_on_forbidden`).
    /// The writer is consumed only after a successful HTTP response, so the retry retains it.
    pub async fn download_media_message_to<W>(
        &self,
        message: &mut crate::proto::WebMessageInfo,
        options: &crate::media::DownloadOptions,
        output: W,
        reupload: bool,
        limit: Option<std::time::Duration>,
    ) -> Result<u64, Error>
    where
        W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let http = self
            .inner
            .media_http
            .get_or_try_init(|| async { crate::media::HttpClient::new() })
            .await?
            .clone();
        let fetch = |message: &crate::proto::WebMessageInfo| {
            let media = crate::media::download_content(message.message.as_ref())?;
            let url = crate::media::download_url(
                media.direct_path.as_deref(),
                media.url.as_deref(),
                options.host.as_deref(),
            )?;
            let keys = crate::media::Keys::derive(&media.key, media.kind)?;
            Ok::<_, crate::media::Error>((url, keys))
        };
        let (url, mut keys) = fetch(message)?;
        let response = match http
            .download_response(&url, options.range, options.headers.clone())
            .await
        {
            Err(crate::media::Error::HttpStatus(status))
                if reupload
                    && (matches!(status, 404 | 410)
                        || status == 403 && options.reupload_on_forbidden) =>
            {
                tracing::info!(
                    id = ?message.key.as_ref().and_then(|k| k.id.as_deref()),
                    "sending reupload media request..."
                );
                self.update_media_message(message, limit).await?;
                let (url, updated_keys) = fetch(message)?;
                keys = updated_keys;
                http.download_response(&url, options.range, options.headers.clone())
                    .await?
            }
            result => result?,
        };
        Ok(http
            .decrypt_response_to(response, keys, options.range, output)
            .await?)
    }
}
