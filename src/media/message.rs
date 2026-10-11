use super::{Error, MediaType, RetryEvent};
use crate::{messages::generation, proto};
use reqwest::header::HeaderMap;

#[derive(Debug, Clone, Default)]
pub struct DownloadOptions {
    pub range: super::Range,
    pub headers: HeaderMap,
    pub host: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Downloadable {
    pub kind: MediaType,
    pub key: Vec<u8>,
    pub url: Option<String>,
    pub direct_path: Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ContentUpdate {
    pub key: proto::MessageKey,
    pub message: Option<Box<proto::Message>>,
}
macro_rules! descriptor {
    ($kind:expr,$payload:expr) => {
        Downloadable {
            kind: $kind,
            key: $payload.media_key.clone().ok_or(Error::MissingMediaKey)?,
            url: $payload.url.clone(),
            direct_path: $payload.direct_path.clone(),
        }
    };
}
pub fn download_content(message: Option<&proto::Message>) -> Result<Downloadable, Error> {
    let content = generation::extract(message).ok_or(Error::MissingMessage)?;
    let field = generation::content_type(Some(&content)).ok_or(Error::DownloadNotMedia)?;
    Ok(match field {
        "imageMessage" => descriptor!(MediaType::Image, content.image_message.as_ref().unwrap()),
        "videoMessage" => descriptor!(MediaType::Video, content.video_message.as_ref().unwrap()),
        "ptvMessage" => descriptor!(MediaType::VideoNote, content.ptv_message.as_ref().unwrap()),
        "audioMessage" => descriptor!(MediaType::Audio, content.audio_message.as_ref().unwrap()),
        "documentMessage" => descriptor!(
            MediaType::Document,
            content.document_message.as_ref().unwrap()
        ),
        "stickerMessage" => descriptor!(
            MediaType::Sticker,
            content.sticker_message.as_ref().unwrap()
        ),
        "extendedTextMessage" => {
            let p = content.extended_text_message.as_ref().unwrap();
            Downloadable {
                kind: MediaType::LinkThumbnail,
                key: p.media_key.clone().ok_or(Error::MissingMediaKey)?,
                url: None,
                direct_path: p.thumbnail_direct_path.clone(),
            }
        }
        _ => return Err(Error::DownloadNotMedia),
    })
}
enum MediaMut<'a> {
    Image(&'a mut proto::message::ImageMessage),
    Video(&'a mut proto::message::VideoMessage),
    Audio(&'a mut proto::message::AudioMessage),
    Document(&'a mut proto::message::DocumentMessage),
    Sticker(&'a mut proto::message::StickerMessage),
}
macro_rules! with_media {
    ($media:expr,$p:ident,$body:expr) => {
        match $media {
            MediaMut::Image($p) => $body,
            MediaMut::Video($p) => $body,
            MediaMut::Audio($p) => $body,
            MediaMut::Document($p) => $body,
            MediaMut::Sticker($p) => $body,
        }
    };
}
fn media_mut(message: &mut proto::Message) -> Result<MediaMut<'_>, Error> {
    let content = generation::normalize_mut(message).ok_or(Error::NotMedia)?;
    macro_rules! header {
        ($value:expr,$ty:path) => {{
            use $ty as Header;
            return match $value {
                Some(Header::ImageMessage(p)) => Ok(MediaMut::Image(p)),
                Some(Header::VideoMessage(p)) => Ok(MediaMut::Video(p)),
                Some(Header::DocumentMessage(p)) => Ok(MediaMut::Document(p)),
                _ => Err(Error::NotMedia),
            };
        }};
    }
    if let Some(buttons) = &mut content.buttons_message {
        header!(
            buttons.header.as_mut(),
            proto::message::buttons_message::Header
        );
    }
    if let Some(template) = &mut content.template_message {
        use proto::message::template_message::Format;
        if matches!(&template.format, Some(Format::HydratedFourRowTemplate(_))) {
            if let Some(Format::HydratedFourRowTemplate(p)) = &mut template.format {
                header!(
                    p.title.as_mut(),
                    proto::message::template_message::hydrated_four_row_template::Title
                );
            }
        } else if let Some(p) = &mut template.hydrated_template {
            header!(
                p.title.as_mut(),
                proto::message::template_message::hydrated_four_row_template::Title
            );
        } else if let Some(Format::FourRowTemplate(p)) = &mut template.format {
            header!(
                p.title.as_mut(),
                proto::message::template_message::four_row_template::Title
            );
        }
    }
    if let Some(p) = &mut content.document_message {
        return Ok(MediaMut::Document(p));
    }
    if let Some(p) = &mut content.image_message {
        return Ok(MediaMut::Image(p));
    }
    if let Some(p) = &mut content.video_message {
        return Ok(MediaMut::Video(p));
    }
    if let Some(p) = &mut content.audio_message {
        return Ok(MediaMut::Audio(p));
    }
    if let Some(p) = &mut content.sticker_message {
        return Ok(MediaMut::Sticker(p));
    }
    Err(Error::NotMedia)
}
/// Source assertMediaContent order differs from generic content selection; PTV is excluded.
pub fn retry_media_key(message: &mut proto::Message) -> Result<Vec<u8>, Error> {
    let media = media_mut(message)?;
    with_media!(media, p, p.media_key.clone().ok_or(Error::MissingMediaKey))
}
/// Apply an ID-matched retry event while preserving the original wrapper/template objects.
pub fn apply_retry(
    message: &mut proto::WebMessageInfo,
    event: &RetryEvent,
    host: &str,
) -> Result<(), Error> {
    let id = message
        .key
        .as_ref()
        .and_then(|k| k.id.as_deref())
        .ok_or(Error::Input("missing message ID"))?;
    if event.key.id.as_deref() != Some(id) {
        return Err(Error::Input("media retry ID mismatch"));
    }
    let content = message.message.as_mut().ok_or(Error::NotMedia)?;
    let media = media_mut(content)?;
    let key = with_media!(&media, p, p.media_key.clone().ok_or(Error::MissingMediaKey))?;
    if let Some(error) = &event.error {
        return Err(Error::RetryFailure(error.clone()));
    }
    let data = super::decrypt_retry(
        event.media.as_ref().ok_or(Error::MissingMediaKey)?,
        &key,
        id,
    )?;
    if data.result != Some(proto::media_retry_notification::ResultType::Success as i32) {
        return Err(Error::RetryResult {
            result: data.result,
            status: data
                .result
                .and_then(|r| super::retry_status(f64::from(r)))
                .unwrap_or(404),
        });
    }
    let path = data.direct_path;
    // Upstream interpolates the protobuf null default when a successful notification omits directPath.
    let url = super::direct_path_url(path.as_deref().unwrap_or("null"), Some(host));
    with_media!(media, p, {
        p.direct_path = path;
        p.url = Some(url);
    });
    Ok(())
}
