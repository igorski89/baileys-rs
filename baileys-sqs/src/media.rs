//! Incoming media: downloaded, then uploaded to S3 or inlined as base64.
use crate::{config::S3Settings, whatsapp::WhatsApp};
use aws_sdk_s3::{Client, presigning::PresigningConfig, primitives::ByteStream};
use baileys_rs::proto;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::time::Duration;

/// The media kinds the connector extracts; other messages carry no `_media`.
/// Wrappers (disappearing, view once, document with caption) are looked
/// through, as normalizeMessageContent does.
pub fn media_type(message: &proto::WebMessageInfo) -> Option<&'static str> {
    let m = content(message)?;
    if m.image_message.is_some() {
        Some("image")
    } else if m.video_message.is_some() {
        Some("video")
    } else if m.audio_message.is_some() {
        Some("audio")
    } else if m.document_message.is_some() {
        Some("document")
    } else {
        None
    }
}

fn content(message: &proto::WebMessageInfo) -> Option<&proto::Message> {
    baileys_rs::relay::normalize(message.message.as_ref())
}

fn mimetype(message: &proto::WebMessageInfo, kind: &str) -> Option<String> {
    let m = content(message)?;
    match kind {
        "image" => m.image_message.as_ref()?.mimetype.clone(),
        "video" => m.video_message.as_ref()?.mimetype.clone(),
        "audio" => m.audio_message.as_ref()?.mimetype.clone(),
        _ => m.document_message.as_ref()?.mimetype.clone(),
    }
}

pub fn extension_from_mimetype(mimetype: Option<&str>) -> String {
    let Some(mimetype) = mimetype.filter(|m| !m.is_empty()) else {
        return "bin".into();
    };
    let known = match mimetype {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "video/mp4" => "mp4",
        "video/ogg" => "ogv",
        "audio/ogg" => "ogg",
        "audio/mpeg" => "mp3",
        "audio/mp4" => "m4a",
        "audio/aac" => "aac",
        "application/pdf" => "pdf",
        _ => "",
    };
    if !known.is_empty() {
        return known.into();
    }
    match mimetype.rsplit('/').next() {
        Some(last) if !last.is_empty() => last.into(),
        _ => "bin".into(),
    }
}

/// Uploads to S3 and presigns a GET with a second client, so the public URL
/// can differ from the endpoint the connector itself uses.
#[derive(Clone)]
pub struct S3Storage {
    settings: S3Settings,
    ops: Client,
    sign: Client,
}

impl S3Storage {
    pub async fn new(settings: S3Settings) -> Self {
        let client = |endpoint: Option<String>| {
            let settings = settings.clone();
            async move {
                let mut service = settings.service.clone();
                service.endpoint_url = endpoint;
                let shared = service.sdk_config().await;
                let config = aws_sdk_s3::config::Builder::from(&shared)
                    .force_path_style(settings.force_path_style)
                    .build();
                Client::from_conf(config)
            }
        };
        let ops = client(settings.service.endpoint_url.clone()).await;
        let sign = client(
            settings
                .public_url
                .clone()
                .or(settings.service.endpoint_url.clone()),
        )
        .await;
        Self {
            settings,
            ops,
            sign,
        }
    }

    pub async fn upload(
        &self,
        kind: &str,
        mimetype: Option<&str>,
        bytes: Vec<u8>,
        message_id: Option<&str>,
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
        let extension = extension_from_mimetype(mimetype);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        let id = match message_id.filter(|id| !id.is_empty()) {
            Some(id) => id.to_owned(),
            None => random_suffix(),
        };
        let key = format!("{}/{kind}/{now}-{id}.{extension}", self.settings.prefix);
        self.ops
            .put_object()
            .bucket(&self.settings.bucket)
            .key(&key)
            .body(ByteStream::from(bytes))
            .content_type(mimetype.unwrap_or("application/octet-stream"))
            .send()
            .await?;
        let url = self
            .sign
            .get_object()
            .bucket(&self.settings.bucket)
            .key(&key)
            .presigned(PresigningConfig::expires_in(Duration::from_secs(
                self.settings.url_expiration_seconds,
            ))?)
            .await?
            .uri()
            .to_owned();
        tracing::debug!(bucket = %self.settings.bucket, key, kind, "Uploaded media to S3");
        Ok((url, key))
    }
}

/// `Math.random().toString(36).slice(2)`: about eleven base-36 digits.
fn random_suffix() -> String {
    let mut n: u64 = rand::random::<u64>() >> 8;
    let mut digits = vec![];
    while n > 0 {
        let d = (n % 36) as u32;
        digits.push(char::from_digit(d, 36).expect("base 36 digit"));
        n /= 36;
    }
    digits.into_iter().rev().take(11).collect()
}

/// The `_media` object for a message, or `None` when it has no media or
/// the download or upload fails (the failure is logged).
pub async fn extract(
    whatsapp: &dyn WhatsApp,
    storage: Option<&S3Storage>,
    message: &proto::WebMessageInfo,
) -> Option<Value> {
    let kind = media_type(message)?;
    let message_id = message.key.as_ref().and_then(|k| k.id.as_deref());
    let result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
        let bytes = whatsapp.download_media(message).await?;
        let mimetype = mimetype(message, kind);
        let mut media = json!({ "type": kind });
        if let Some(mimetype) = &mimetype {
            media["mimetype"] = json!(mimetype);
        }
        if let Some(storage) = storage {
            let (url, key) = storage
                .upload(kind, mimetype.as_deref(), bytes, message_id)
                .await?;
            media["url"] = json!(url);
            media["s3_key"] = json!(key);
            media["storage"] = json!("s3");
        } else {
            media["data_base64"] = json!(STANDARD.encode(bytes));
            media["storage"] = json!("base64");
        }
        Ok(media)
    }
    .await;
    match result {
        Ok(media) => Some(media),
        Err(err) => {
            tracing::error!(%err, msg_id = message_id, "Failed to extract media");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extensions_follow_the_table_then_the_subtype() {
        assert_eq!(extension_from_mimetype(Some("image/jpeg")), "jpg");
        assert_eq!(
            extension_from_mimetype(Some("audio/ogg; codecs=opus")),
            "ogg; codecs=opus"
        );
        assert_eq!(extension_from_mimetype(Some("text/plain")), "plain");
        assert_eq!(extension_from_mimetype(None), "bin");
        assert_eq!(extension_from_mimetype(Some("weird/")), "bin");
        assert!(random_suffix().len() <= 11);
    }

    #[test]
    fn media_inside_a_disappearing_message_is_found() {
        let document = proto::Message {
            document_message: Some(Box::new(proto::message::DocumentMessage {
                mimetype: Some("application/pdf".into()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let wrapped = proto::WebMessageInfo {
            message: Some(proto::Message {
                ephemeral_message: Some(Box::new(proto::message::FutureProofMessage {
                    message: Some(Box::new(document)),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(media_type(&wrapped), Some("document"));
        assert_eq!(
            mimetype(&wrapped, "document").as_deref(),
            Some("application/pdf")
        );
    }
}
