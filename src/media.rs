//! Media encryption, streaming CBC downloads and media-retry wire helpers.
mod http;
mod retry;
mod stream;
use crate::crypto;
pub use http::*;
pub use retry::*;
pub use stream::*;
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error("invalid media range or byte count overflow")]
    Range,
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("media HTTP status {0}")]
    HttpStatus(u16),
    #[error("media upload failed on all hosts")]
    UploadFailed,
    #[error("too many media upload redirects")]
    Redirects,
    #[error("invalid media upload redirect")]
    Redirect,
    #[error("media worker failed")]
    Task,
    #[error("media connection refresh failed: {message}")]
    Refresh {
        message: String,
        status: Option<u16>,
    },
    #[error("invalid media retry node: {0}")]
    Node(&'static str),
    #[error("No valid media URL or directPath present in message")]
    Url,
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Url => Some(400),
            Self::HttpStatus(code) => Some(*code),
            Self::UploadFailed => Some(500),
            Self::Refresh { status, .. } => *status,
            _ => None,
        }
    }
}
macro_rules! media_types {
    ($($variant:ident,$name:literal,$label:literal,$path:expr;)*)=>{
        #[derive(Debug,Clone,Copy,PartialEq,Eq)]
        pub enum MediaType {$($variant,)*}
        impl MediaType {
            pub const ALL: &'static [Self]=&[$(Self::$variant,)*];
            pub fn as_str(self)->&'static str {match self {$(Self::$variant=>$name,)*}}
            pub fn hkdf_info(self)->String {format!("WhatsApp {} Keys",match self {$(Self::$variant=>$label,)*})}
            pub fn upload_path(self)->Option<&'static str> {match self {$(Self::$variant=>$path,)*}}
        }
        impl std::str::FromStr for MediaType {
            type Err=Error;
            fn from_str(value:&str)->Result<Self,Error>{match value {$($name=>Ok(Self::$variant),)*_=>Err(Error::Node("unknown media type"))}}
        }
    }
}
media_types! {
    Audio,"audio","Audio",Some("/mms/audio");
    Document,"document","Document",Some("/mms/document");
    Gif,"gif","Video",None;
    Image,"image","Image",Some("/mms/image");
    ProfilePicture,"ppic","",None;
    Product,"product","Image",None;
    Ptt,"ptt","Audio",None;
    Sticker,"sticker","Image",Some("/mms/image");
    Video,"video","Video",Some("/mms/video");
    DocumentThumbnail,"thumbnail-document","Document Thumbnail",None;
    ImageThumbnail,"thumbnail-image","Image Thumbnail",None;
    VideoThumbnail,"thumbnail-video","Video Thumbnail",None;
    LinkThumbnail,"thumbnail-link","Link Thumbnail",Some("/mms/image");
    History,"md-msg-hist","History",Some("/mms/md-app-state");
    AppState,"md-app-state","App State",Some("");
    CatalogImage,"product-catalog-image","",Some("/product/image");
    PaymentBackground,"payment-bg-image","Payment Background",None;
    VideoNote,"ptv","Video",None;
    BusinessCover,"biz-cover-photo","Image",Some("/pps/biz-cover-photo");
}
#[derive(Clone)]
pub struct Keys {
    pub iv: [u8; 16],
    cipher: Zeroizing<[u8; 32]>,
    mac: Zeroizing<[u8; 32]>,
}
impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys").finish_non_exhaustive()
    }
}
impl Keys {
    pub fn derive_base64(value: &str, kind: MediaType) -> Result<Self, Error> {
        use base64::{
            Engine, alphabet,
            engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
        };
        if value.is_empty() {
            return Err(Error::Node("Cannot derive from empty media key"));
        }
        let text = value.replacen("data:;base64,", "", 1);
        let mut text: String = text
            .split('=')
            .next()
            .unwrap_or_default()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '-' | '_'))
            .collect();
        text = text.replace('-', "+").replace('_', "/");
        if text.len() % 4 == 1 {
            text.pop();
        }
        let engine = GeneralPurpose::new(
            &alphabet::STANDARD,
            GeneralPurposeConfig::new()
                .with_decode_padding_mode(DecodePaddingMode::Indifferent)
                .with_decode_allow_trailing_bits(true),
        );
        let key = Zeroizing::new(
            engine
                .decode(text)
                .map_err(|_| Error::Node("invalid media key"))?,
        );
        Self::derive(&key, kind)
    }
    /// Empty byte keys are accepted, as with upstream Buffer.alloc(0).
    pub fn derive(media_key: &[u8], kind: MediaType) -> Result<Self, Error> {
        let expanded = Zeroizing::new(crypto::hkdf(
            media_key,
            112,
            None,
            kind.hkdf_info().as_bytes(),
        )?);
        Ok(Self {
            iv: expanded[..16].try_into().unwrap(),
            cipher: Zeroizing::new(expanded[16..48].try_into().unwrap()),
            mac: Zeroizing::new(expanded[48..80].try_into().unwrap()),
        })
    }
    pub fn cipher_key(&self) -> &[u8; 32] {
        &self.cipher
    }
    pub fn mac_key(&self) -> &[u8; 32] {
        &self.mac
    }
}
/// encodeURIComponent, preserving only JavaScript's unescaped ASCII set.
pub fn encode_component(value: &str) -> String {
    let mut output = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            output.push(char::from(b));
        } else {
            output.push_str(&format!("%{b:02X}"));
        }
    }
    output
}
pub fn upload_token(base64: &str) -> String {
    encode_component(
        base64
            .replace('+', "-")
            .replace('/', "_")
            .trim_end_matches('='),
    )
}
pub const DEFAULT_MEDIA_HOST: &str = "mmg.whatsapp.net";
pub fn direct_path_url(path: &str, host: Option<&str>) -> String {
    format!("https://{}{}", host.unwrap_or(DEFAULT_MEDIA_HOST), path)
}
/// Prefer a direct path with explicit/original host, then the original URL.
pub fn download_url(
    direct_path: Option<&str>,
    original_url: Option<&str>,
    host: Option<&str>,
) -> Result<String, Error> {
    if let Some(path) = direct_path.filter(|s| !s.is_empty()) {
        let parsed = original_url
            .filter(|u| !u.is_empty())
            .and_then(|u| url::Url::parse(u).ok());
        let original_host = parsed.as_ref().map(|url| {
            let h = url.host_str().unwrap_or("");
            match url.port() {
                Some(port) => format!("{h}:{port}"),
                None => h.to_owned(),
            }
        });
        return Ok(direct_path_url(path, host.or(original_host.as_deref())));
    }
    original_url
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(Error::Url)
}
