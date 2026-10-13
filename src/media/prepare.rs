use super::{
    EncryptedFile, Error, HttpClient, MediaField, MediaType, UploadOptions, UploadResponse,
};
use crate::{
    messages::generation::{Color, color_argb},
    proto,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{StreamExt, future::BoxFuture};
use prost::Message as _;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_util::io::{StreamReader, SyncIoBridge};

#[derive(Clone)]
pub struct MediaReader(Arc<Mutex<Option<Box<dyn Read + Send>>>>);
impl MediaReader {
    pub fn new(reader: impl Read + Send + 'static) -> Self {
        Self(Arc::new(Mutex::new(Some(Box::new(reader)))))
    }
}
impl std::fmt::Debug for MediaReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaReader").finish_non_exhaustive()
    }
}
#[derive(Debug, Clone)]
pub enum Input {
    Bytes(Arc<[u8]>),
    File(PathBuf),
    Url(String),
    Reader(MediaReader),
}
impl Input {
    pub fn bytes(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self::Bytes(bytes.into())
    }
    pub(super) async fn open(
        &self,
        http: &HttpClient,
        options: &PrepareOptions,
    ) -> Result<Box<dyn Read + Send>, Error> {
        Ok(match self {
            Self::Bytes(b) => Box::new(std::io::Cursor::new(b.clone())),
            Self::File(path) => Box::new(tokio::fs::File::open(path).await?.into_std().await),
            Self::Reader(reader) => reader
                .0
                .lock()
                .map_err(|_| Error::Input("media reader poisoned"))?
                .take()
                .ok_or(Error::Input("media reader already consumed"))?,
            Self::Url(url) if url.starts_with("data:") => {
                Box::new(std::io::Cursor::new(super::decode_base64(
                    url.split(',')
                        .nth(1)
                        .ok_or(Error::Input("invalid data URI"))?,
                )?))
            }
            Self::Url(url) if url.starts_with("http://") || url.starts_with("https://") => {
                let response = http.get(url, options.upload.headers.clone()).await?;
                Box::new(SyncIoBridge::new(StreamReader::new(
                    response
                        .bytes_stream()
                        .map(|r| r.map_err(std::io::Error::other)),
                )))
            }
            Self::Url(path) => Box::new(tokio::fs::File::open(path).await?.into_std().await),
        })
    }
}
#[derive(Debug, Clone)]
pub enum Payload {
    Image(Box<proto::message::ImageMessage>),
    Video(Box<proto::message::VideoMessage>),
    Audio(Box<proto::message::AudioMessage>),
    Document(Box<proto::message::DocumentMessage>),
    Sticker(Box<proto::message::StickerMessage>),
}
macro_rules! each_payload {
    ($self:expr,$value:ident,$body:expr) => {
        match $self {
            Payload::Image($value) => $body,
            Payload::Video($value) => $body,
            Payload::Audio($value) => $body,
            Payload::Document($value) => $body,
            Payload::Sticker($value) => $body,
        }
    };
}
impl Payload {
    pub fn kind(&self) -> MediaType {
        match self {
            Self::Image(_) => MediaType::Image,
            Self::Video(_) => MediaType::Video,
            Self::Audio(_) => MediaType::Audio,
            Self::Document(_) => MediaType::Document,
            Self::Sticker(_) => MediaType::Sticker,
        }
    }
    fn defaults(&mut self) {
        let mime = match self.kind() {
            MediaType::Image => "image/jpeg",
            MediaType::Video => "video/mp4",
            MediaType::Audio => "audio/ogg; codecs=opus",
            MediaType::Document => "application/pdf",
            _ => "image/webp",
        };
        each_payload!(self, p, {
            if p.mimetype.as_ref().is_none_or(|s| s.is_empty()) {
                p.mimetype = Some(mime.into());
            }
        });
        if let Self::Document(d) = self
            && d.file_name.as_ref().is_none_or(|s| s.is_empty())
        {
            d.file_name = Some("file".into());
        }
    }
    fn message(self, video_note: bool) -> proto::Message {
        let mut message = proto::Message::default();
        match self {
            Self::Image(p) => message.image_message = Some(p),
            Self::Video(p) => {
                if video_note {
                    message.ptv_message = Some(p)
                } else {
                    message.video_message = Some(p)
                }
            }
            Self::Audio(p) => message.audio_message = Some(p),
            Self::Document(p) => message.document_message = Some(p),
            Self::Sticker(p) => message.sticker_message = Some(p),
        }
        message
    }
}
#[derive(Debug, Clone)]
pub struct Request {
    pub input: Input,
    pub payload: Payload,
    pub video_note: bool,
}
#[derive(Debug, Clone, Default)]
pub struct PrepareOptions {
    pub uploader: Option<UploadFunction>,
    pub destination: Option<String>,
    pub media_type_override: Option<MediaType>,
    pub upload: UploadOptions,
    pub cache: bool,
    /// Largest body accepted when the input is fetched over HTTP; larger
    /// downloads fail while encrypting.
    pub max_content_length: Option<u64>,
    /// Explicit null/empty-list fields, overriding template values.
    /// Invalid fields fail before input, upload or cache hooks run.
    pub clear_fields: Vec<MediaField>,
    pub background_color: Option<Color>,
}
pub trait MediaUploader: Send + Sync {
    fn upload_media<'a>(
        &'a self,
        path: PathBuf,
        kind: MediaType,
        hash: String,
        options: UploadOptions,
    ) -> BoxFuture<'a, Result<UploadResponse, Error>>;
}
#[derive(Clone)]
pub struct UploadFunction(Arc<dyn MediaUploader>);
impl UploadFunction {
    pub fn new(uploader: impl MediaUploader + 'static) -> Self {
        Self(Arc::new(uploader))
    }
    pub fn get(&self) -> &dyn MediaUploader {
        self.0.as_ref()
    }
}
impl std::fmt::Debug for UploadFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadFunction").finish_non_exhaustive()
    }
}
#[derive(Debug, Default, Clone)]
pub struct Thumbnail {
    pub jpeg: Option<Vec<u8>>,
    pub dimensions: Option<(u32, u32)>,
}
pub trait PrepareHooks: Send + Sync {
    fn thumbnail(&self, path: PathBuf, kind: MediaType) -> BoxFuture<'_, Result<Thumbnail, Error>> {
        Box::pin(async move {
            super::MediaProcessors::default()
                .thumbnail(path, kind)
                .await
        })
    }
    fn duration(&self, path: PathBuf) -> BoxFuture<'_, Result<Option<u32>, Error>> {
        Box::pin(async move { super::MediaProcessors::default().duration(path).await })
    }
    fn waveform(&self, path: Option<PathBuf>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move { super::MediaProcessors::default().waveform(path).await })
    }
    fn cache_get(&self, _key: String) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async { Ok(None) })
    }
    fn cache_set(&self, _key: String, _value: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
    fn now_ms(&self) -> Result<i64, Error> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Range)?
            .as_millis()
            .try_into()
            .map_err(|_| Error::Range)
    }
    fn media_key(&self) -> Result<[u8; 32], Error> {
        let mut key = [0; 32];
        getrandom::fill(&mut key).map_err(|_| crate::crypto::Error::Random)?;
        Ok(key)
    }
}
#[derive(Debug, Default)]
pub struct DefaultPrepareHooks;
impl PrepareHooks for DefaultPrepareHooks {}
#[derive(Clone)]
pub struct PreparationHooks(Arc<dyn PrepareHooks>);
impl PreparationHooks {
    pub fn new(hooks: impl PrepareHooks + 'static) -> Self {
        Self(Arc::new(hooks))
    }
    pub fn get(&self) -> &dyn PrepareHooks {
        self.0.as_ref()
    }
}
impl Default for PreparationHooks {
    fn default() -> Self {
        Self::new(DefaultPrepareHooks)
    }
}
impl std::fmt::Debug for PreparationHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparationHooks").finish_non_exhaustive()
    }
}

// Fails a remote download once it exceeds the configured size.
struct Limited {
    input: Box<dyn Read + Send>,
    remaining: u64,
}
impl Read for Limited {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.input.read(buffer)?;
        self.remaining = self.remaining.checked_sub(read as u64).ok_or_else(|| {
            std::io::Error::other("content length exceeded when encrypting \"remote\"")
        })?;
        Ok(read)
    }
}
struct RawFile {
    file: tempfile::NamedTempFile,
    hash: [u8; 32],
    length: u64,
}
impl RawFile {
    fn copy(mut input: Box<dyn Read + Send>) -> Result<Self, Error> {
        let mut file = tempfile::NamedTempFile::new()?;
        let mut hash = Sha256::new();
        let mut length = 0u64;
        let mut buffer = zeroize::Zeroizing::new(vec![0; 64 * 1024]);
        loop {
            let n = match input.read(&mut buffer) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                r => r?,
            };
            if n == 0 {
                break;
            }
            length = length.checked_add(n as u64).ok_or(Error::Range)?;
            hash.update(&buffer[..n]);
            file.write_all(&buffer[..n])?;
        }
        file.flush()?;
        Ok(Self {
            file,
            hash: hash.finalize().into(),
            length,
        })
    }
}
/// Hash an input and upload it unencrypted, as catalog images and cover photos are.
/// The upload is addressed by the base64 SHA-256 of the content.
pub async fn upload_raw(
    input: &Input,
    kind: MediaType,
    http: &HttpClient,
    uploader: &dyn MediaUploader,
    options: &PrepareOptions,
) -> Result<UploadResponse, Error> {
    let reader = input.open(http, options).await?;
    let file = tokio::task::spawn_blocking(move || RawFile::copy(reader))
        .await
        .map_err(|_| Error::Task)??;
    uploader
        .upload_media(
            file.file.path().to_owned(),
            kind,
            STANDARD.encode(file.hash),
            options.upload.clone(),
        )
        .await
}
enum File {
    Raw(RawFile),
    Encrypted(EncryptedFile),
}
impl File {
    fn path(&self) -> PathBuf {
        match self {
            Self::Raw(f) => f.file.path(),
            Self::Encrypted(f) => f.encrypted.path(),
        }
        .to_owned()
    }
    fn original(&self) -> Option<PathBuf> {
        match self {
            Self::Raw(_) => None,
            Self::Encrypted(f) => f.original.as_ref().map(|f| f.path().to_owned()),
        }
    }
    fn plain_hash(&self) -> [u8; 32] {
        match self {
            Self::Raw(f) => f.hash,
            Self::Encrypted(f) => f.metadata.file_sha256,
        }
    }
    fn upload_hash(&self) -> [u8; 32] {
        match self {
            Self::Raw(f) => f.hash,
            Self::Encrypted(f) => f.metadata.file_enc_sha256,
        }
    }
    fn length(&self) -> u64 {
        match self {
            Self::Raw(f) => f.length,
            Self::Encrypted(f) => f.metadata.file_length,
        }
    }
}
async fn enrich(
    payload: &mut Payload,
    path: Option<PathBuf>,
    options: &PrepareOptions,
    hooks: &dyn PrepareHooks,
) -> Result<(), Error> {
    match payload {
        Payload::Image(p)
            if p.jpeg_thumbnail.is_none()
                && !options.clear_fields.contains(&MediaField::JpegThumbnail) =>
        {
            let t = hooks
                .thumbnail(
                    path.ok_or(Error::Input("missing original"))?,
                    MediaType::Image,
                )
                .await?;
            p.jpeg_thumbnail = t.jpeg;
            if p.width.is_none_or(|w| w == 0)
                && let Some((w, h)) = t.dimensions
            {
                p.width = Some(w);
                p.height = Some(h);
            }
        }
        Payload::Video(p)
            if p.jpeg_thumbnail.is_none()
                && !options.clear_fields.contains(&MediaField::JpegThumbnail) =>
        {
            let t = hooks
                .thumbnail(
                    path.ok_or(Error::Input("missing original"))?,
                    MediaType::Video,
                )
                .await?;
            p.jpeg_thumbnail = t.jpeg;
            if p.width.is_none_or(|w| w == 0)
                && let Some((w, h)) = t.dimensions
            {
                p.width = Some(w);
                p.height = Some(h);
            }
        }
        Payload::Audio(p) => {
            if p.seconds.is_none() && !options.clear_fields.contains(&MediaField::Seconds) {
                p.seconds = hooks
                    .duration(path.clone().ok_or(Error::Input("missing original"))?)
                    .await?;
            }
            if p.ptt == Some(true)
                && p.waveform.is_none()
                && !options.clear_fields.contains(&MediaField::Waveform)
            {
                p.waveform = hooks.waveform(path).await?;
            }
            if p.ptt == Some(true)
                && let Some(color) = options.background_color.as_ref().filter(|c| match c {
                    Color::Hex(s) => !s.is_empty(),
                    Color::Number(n) => *n != 0.0 && !n.is_nan(),
                })
            {
                p.background_argb = color_argb(color);
            }
        }
        _ => {}
    }
    Ok(())
}
/// Prepare an uploaded payload, including newsletter plaintext and URL-cache behavior.
pub async fn prepare(
    request: &Request,
    options: &PrepareOptions,
    http: &HttpClient,
    uploader: &dyn MediaUploader,
    hooks: &dyn PrepareHooks,
) -> Result<proto::Message, Error> {
    let mut payload = request.payload.clone();
    super::fields::clear(&mut payload, &options.clear_fields)?;
    payload.defaults();
    let kind = payload.kind();
    let cache_key = if options.cache {
        match &request.input {
            Input::Url(url) if !url.is_empty() => Some(format!("{}:{url}", kind.as_str())),
            _ => None,
        }
    } else {
        None
    };
    if let Some(key) = &cache_key
        && let Some(bytes) = hooks.cache_get(key.clone()).await?
    {
        let mut message = proto::Message::decode(bytes.as_slice())?;
        super::overlay::apply(&mut message, &payload, &options.clear_fields)?;
        return Ok(message);
    }
    let raw = options
        .destination
        .as_deref()
        .is_some_and(|jid| jid.ends_with("@newsletter"));
    let save_original = match &payload {
        Payload::Image(p) => {
            p.jpeg_thumbnail.is_none() && !options.clear_fields.contains(&MediaField::JpegThumbnail)
        }
        Payload::Video(p) => {
            p.jpeg_thumbnail.is_none() && !options.clear_fields.contains(&MediaField::JpegThumbnail)
        }
        Payload::Audio(p) => {
            p.seconds.is_none() && !options.clear_fields.contains(&MediaField::Seconds)
        }
        _ => false,
    };
    let input = request.input.open(http, options).await?;
    let remote = matches!(&request.input, Input::Url(url) if url.starts_with("http://") || url.starts_with("https://"));
    let input: Box<dyn Read + Send> = match options.max_content_length {
        Some(limit) if remote && limit != 0 => Box::new(Limited {
            input,
            remaining: limit,
        }),
        _ => input,
    };
    let cipher_kind = options.media_type_override.unwrap_or(kind);
    let file = if raw {
        tokio::task::spawn_blocking(move || RawFile::copy(input).map(File::Raw))
            .await
            .map_err(|_| Error::Task)??
    } else {
        let key = hooks.media_key()?;
        tokio::task::spawn_blocking(move || {
            EncryptedFile::encrypt_with(input, cipher_kind, key, save_original).map(File::Encrypted)
        })
        .await
        .map_err(|_| Error::Task)??
    };
    let upload = uploader.upload_media(
        file.path(),
        kind,
        STANDARD.encode(file.upload_hash()),
        options.upload.clone(),
    );
    let response = if raw {
        upload.await?
    } else {
        let (upload, extra) = tokio::join!(
            upload,
            enrich(&mut payload, file.original(), options, hooks)
        );
        if let Err(error) = extra {
            tracing::debug!(?error, "media metadata unavailable");
        }
        upload?
    };
    let timestamp = if matches!(&file, File::Encrypted(_)) {
        Some(hooks.now_ms()?.div_euclid(1000))
    } else {
        None
    };
    each_payload!(&mut payload, p, {
        if p.url.is_none() && !options.clear_fields.contains(&MediaField::Url) {
            p.url = response.url.clone();
        }
        if p.direct_path.is_none() && !options.clear_fields.contains(&MediaField::DirectPath) {
            p.direct_path = response.direct_path.clone();
        }
        if p.file_sha256.is_none() && !options.clear_fields.contains(&MediaField::FileSha256) {
            p.file_sha256 = Some(file.plain_hash().to_vec());
        }
        if p.file_length.is_none() && !options.clear_fields.contains(&MediaField::FileLength) {
            p.file_length = Some(file.length());
        }
        if let File::Encrypted(f) = &file {
            if p.media_key.is_none() && !options.clear_fields.contains(&MediaField::MediaKey) {
                p.media_key = Some(f.media_key.to_vec());
            }
            if p.file_enc_sha256.is_none()
                && !options.clear_fields.contains(&MediaField::FileEncSha256)
            {
                p.file_enc_sha256 = Some(f.metadata.file_enc_sha256.to_vec());
            }
            if p.media_key_timestamp.is_none()
                && !options
                    .clear_fields
                    .contains(&MediaField::MediaKeyTimestamp)
            {
                p.media_key_timestamp = timestamp;
            }
        }
    });
    if raw && let Payload::Sticker(p) = &mut payload {
        p.sticker_sent_ts = Some(hooks.now_ms()?);
    }
    let message = payload.message(request.video_note);
    if let Some(key) = cache_key {
        hooks.cache_set(key, message.encode_to_vec()).await?;
    }
    Ok(message)
}
