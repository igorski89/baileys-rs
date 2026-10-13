use super::{
    DownloadPlan, EncryptedFile, Error, Keys, MediaType, Range, decrypt_download, upload_url,
};
use crate::{
    binary::{BinaryNode, NodeContent},
    defaults,
};
use futures_util::{StreamExt, future::BoxFuture};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, LOCATION, ORIGIN, RANGE};
use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tokio::io::AsyncWrite;
use tokio_util::io::{ReaderStream, StreamReader, SyncIoBridge};

#[derive(Debug, Clone, PartialEq)]
pub struct UploadHost {
    pub hostname: String,
    pub max_content_length_bytes: f64,
}
#[derive(Clone, PartialEq)]
pub struct MediaConnection {
    pub hosts: Vec<UploadHost>,
    pub auth: String,
    pub ttl_seconds: f64,
    pub fetched_at: SystemTime,
}
impl std::fmt::Debug for MediaConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaConnection")
            .field("hosts", &self.hosts)
            .field("ttl_seconds", &self.ttl_seconds)
            .finish_non_exhaustive()
    }
}
impl MediaConnection {
    /// The source expires only when elapsed time is strictly greater than TTL.
    pub fn expired_at(&self, now: SystemTime) -> bool {
        let millis = |time: SystemTime| match time.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_millis() as f64,
            Err(e) => -(e.duration().as_millis() as f64),
        };
        let elapsed = (millis(now) - millis(self.fetched_at)) / 1000.0;
        elapsed > self.ttl_seconds
    }
    pub fn parse(response: &BinaryNode, fetched_at: SystemTime) -> Result<Self, Error> {
        let node = response
            .child("media_conn")
            .ok_or(Error::Node("missing media_conn"))?;
        let number = |s: Option<&String>| s.map(|s| crate::privacy::number(s)).unwrap_or(f64::NAN);
        Ok(Self {
            hosts: node
                .children("host")
                .map(|h| {
                    Ok(UploadHost {
                        hostname: h
                            .attrs
                            .get("hostname")
                            .cloned()
                            .ok_or(Error::Node("missing media hostname"))?,
                        max_content_length_bytes: number(h.attrs.get("maxContentLengthBytes")),
                    })
                })
                .collect::<Result<_, Error>>()?,
            auth: node
                .attrs
                .get("auth")
                .cloned()
                .ok_or(Error::Node("missing media auth"))?,
            ttl_seconds: number(node.attrs.get("ttl")),
            fetched_at,
        })
    }
    pub fn query() -> BinaryNode {
        BinaryNode::new("iq")
            .attr("type", "set")
            .attr("xmlns", "w:m")
            .attr("to", "@s.whatsapp.net")
            .with_content(NodeContent::Children(vec![BinaryNode::new("media_conn")]))
    }
}
#[derive(Debug, Clone, Default, PartialEq, serde::Deserialize)]
pub struct UploadResponse {
    pub url: Option<String>,
    pub direct_path: Option<String>,
    pub meta_hmac: Option<String>,
    pub ts: Option<f64>,
    pub fbid: Option<f64>,
}
#[derive(Debug, Clone, Default)]
pub struct UploadOptions {
    pub custom_hosts: Vec<UploadHost>,
    pub headers: HeaderMap,
    pub timeout: Option<Duration>,
}
#[derive(Debug, Clone)]
pub struct UploadRequest {
    pub url: String,
    pub path: PathBuf,
    pub headers: HeaderMap,
    pub timeout: Option<Duration>,
}
pub trait UploadTransport: Send + Sync {
    fn upload<'a>(
        &'a self,
        request: UploadRequest,
    ) -> BoxFuture<'a, Result<Option<UploadResponse>, Error>>;
}
/// Original host order is retained after an unsuccessful JSON response refreshes auth.
/// Transport failures advance hosts without forcing a media-connection refresh;
/// invalid JSON is an absent response and triggers a refresh.
pub async fn upload_with<T, F, Fut>(
    transport: &T,
    path: &Path,
    kind: MediaType,
    hash: &str,
    options: &UploadOptions,
    mut refresh: F,
) -> Result<UploadResponse, Error>
where
    T: UploadTransport + ?Sized,
    F: FnMut(bool) -> Fut,
    Fut: std::future::Future<Output = Result<MediaConnection, Error>>,
{
    let mut connection = refresh(false).await?;
    let hosts: Vec<_> = options
        .custom_hosts
        .iter()
        .chain(&connection.hosts)
        .cloned()
        .collect();
    let mut headers = options.headers.clone();
    headers.insert(CONTENT_TYPE, "application/octet-stream".parse().unwrap());
    headers.insert(ORIGIN, defaults::DEFAULT_ORIGIN.parse().unwrap());
    for host in hosts {
        let result = transport
            .upload(UploadRequest {
                url: upload_url(&host.hostname, kind, hash, &connection.auth),
                path: path.to_owned(),
                headers: headers.clone(),
                timeout: options.timeout,
            })
            .await;
        match result {
            Ok(Some(result))
                if result.url.as_ref().is_some_and(|s| !s.is_empty())
                    || result.direct_path.as_ref().is_some_and(|s| !s.is_empty()) =>
            {
                return Ok(result);
            }
            Ok(_) => match refresh(true).await {
                Ok(updated) => connection = updated,
                Err(error) => tracing::debug!(?error, "media connection refresh failed"),
            },
            Err(error) => tracing::debug!(?error, "media upload host failed"),
        }
    }
    Err(Error::UploadFailed)
}

/// Reusable native HTTP clients; the upload client must disable automatic redirects
/// so every redirect reopens and replays the file as a POST, including 301/302/303.
#[derive(Debug, Clone)]
pub struct HttpClient {
    download: reqwest::Client,
    upload: reqwest::Client,
}
impl HttpClient {
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            // Node's fetch follows up to twenty redirects.
            download: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::limited(20))
                .build()?,
            upload: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
    /// Supply clients to configure proxy, TLS roots, DNS overrides and connection limits.
    pub fn with_clients(
        download: reqwest::Client,
        upload_without_redirects: reqwest::Client,
    ) -> Self {
        Self {
            download,
            upload: upload_without_redirects,
        }
    }
    /// Fetch a page for a link preview: its final URL, content type and body as UTF-8.
    /// The timeout covers the request up to the response headers, as upstream's does.
    pub async fn get_page(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<(String, Option<String>, String), Error> {
        let response = tokio::time::timeout(timeout, self.download.get(url).send())
            .await
            .map_err(|_| Error::Input("link preview request timed out"))??;
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let url = response.url().to_string();
        let body = response.bytes().await?;
        Ok((
            url,
            content_type,
            String::from_utf8_lossy(&body).into_owned(),
        ))
    }
    pub async fn get(&self, url: &str, headers: HeaderMap) -> Result<reqwest::Response, Error> {
        let response = self.download.get(url).headers(headers).send().await?;
        if !response.status().is_success() {
            return Err(Error::HttpStatus(response.status().as_u16()));
        }
        Ok(response)
    }
    /// Decrypt into an owned async writer with bounded buffers on a blocking worker.
    /// As in upstream, the legacy CBC downloader does not authenticate the MAC.
    pub async fn download_to<W>(
        &self,
        url: &str,
        keys: Keys,
        range: Range,
        headers: HeaderMap,
        output: W,
    ) -> Result<u64, Error>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let response = self.download_response(url, range, headers).await?;
        self.decrypt_response_to(response, keys, range, output)
            .await
    }
    pub async fn download_response(
        &self,
        url: &str,
        range: Range,
        mut headers: HeaderMap,
    ) -> Result<reqwest::Response, Error> {
        let plan = DownloadPlan::new(range)?;
        headers.insert(ORIGIN, defaults::DEFAULT_ORIGIN.parse().unwrap());
        if let Some(range) = plan.range_header() {
            headers.insert(RANGE, range.parse().unwrap());
        }
        self.get(url, headers).await
    }
    pub async fn decrypt_response_to<W>(
        &self,
        response: reqwest::Response,
        keys: Keys,
        range: Range,
        output: W,
    ) -> Result<u64, Error>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let plan = DownloadPlan::new(range)?;
        let input = StreamReader::new(
            response
                .bytes_stream()
                .map(|r| r.map_err(std::io::Error::other)),
        );
        let input = SyncIoBridge::new(input);
        let mut output = SyncIoBridge::new(output);
        tokio::task::spawn_blocking(move || {
            let mut buffered = std::io::BufWriter::with_capacity(64 * 1024, &mut output);
            let bytes = decrypt_download(input, &mut buffered, &keys, &plan)?;
            std::io::Write::flush(&mut buffered)?;
            Ok(bytes)
        })
        .await
        .map_err(|_| Error::Task)?
    }
    pub async fn download_message_to<W>(
        &self,
        message: &crate::proto::WebMessageInfo,
        options: &super::DownloadOptions,
        output: W,
    ) -> Result<u64, Error>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let media = super::download_content(message.message.as_ref())?;
        let url = super::download_url(
            media.direct_path.as_deref(),
            media.url.as_deref(),
            options.host.as_deref(),
        )?;
        self.download_to(
            &url,
            Keys::derive(&media.key, media.kind)?,
            options.range,
            options.headers.clone(),
            output,
        )
        .await
    }
    /// Download remote plaintext and encrypt directly to owned temporary files.
    pub async fn encrypt_remote(
        &self,
        url: &str,
        headers: HeaderMap,
        kind: MediaType,
        save_original: bool,
    ) -> Result<EncryptedFile, Error> {
        let response = self.get(url, headers).await?;
        let input = StreamReader::new(
            response
                .bytes_stream()
                .map(|r| r.map_err(std::io::Error::other)),
        );
        let input = SyncIoBridge::new(input);
        tokio::task::spawn_blocking(move || EncryptedFile::encrypt(input, kind, save_original))
            .await
            .map_err(|_| Error::Task)?
    }
    pub async fn post_file(
        &self,
        mut request: UploadRequest,
    ) -> Result<Option<UploadResponse>, Error> {
        for redirect_count in 0..=5 {
            let file = tokio::fs::File::open(&request.path).await?;
            let length = file.metadata().await?.len();
            let mut headers = request.headers.clone();
            headers.insert(CONTENT_LENGTH, length.into());
            let mut builder =
                self.upload
                    .post(&request.url)
                    .headers(headers)
                    .body(reqwest::Body::wrap_stream(ReaderStream::with_capacity(
                        file,
                        64 * 1024,
                    )));
            if let Some(timeout) = request.timeout.filter(|d| !d.is_zero()) {
                builder = builder.timeout(timeout);
            }
            let response = builder.send().await?;
            if response.status().is_redirection()
                && let Some(location) = response.headers().get(LOCATION)
            {
                if redirect_count == 5 {
                    return Err(Error::Redirects);
                }
                request.url = url::Url::parse(&request.url)
                    .map_err(|_| Error::Redirect)?
                    .join(location.to_str().map_err(|_| Error::Redirect)?)
                    .map_err(|_| Error::Redirect)?
                    .into();
                continue;
            }
            // The Node uploader parses JSON even on a non-2xx status, and treats invalid JSON as absent.
            let bytes = response.bytes().await?;
            return Ok(serde_json::from_slice(&bytes).ok());
        }
        Err(Error::Redirects)
    }
}
impl UploadTransport for HttpClient {
    fn upload<'a>(
        &'a self,
        request: UploadRequest,
    ) -> BoxFuture<'a, Result<Option<UploadResponse>, Error>> {
        Box::pin(self.post_file(request))
    }
}
