//! Upstream's `AnyMessageContent` object, as JSON, converted to typed content.
use super::{
    CallKind, Content, ContextField, ContextPatch, Disappearing, Error, Event, LinkPreview,
    Modifiers, Poll, Preview, Text,
};
use crate::{
    media::{Input, Payload, Request},
    proto,
    proto_json::{self, JsonField},
};
use serde_json::{Map, Value};

/// Media keys in upstream's `MEDIA_KEYS` order; the last one present wins.
const MEDIA_KEYS: [&str; 5] = ["image", "video", "document", "audio", "sticker"];

fn invalid(what: impl std::fmt::Display) -> Error {
    Error::Json(what.to_string())
}

fn parse<T: JsonField>(value: &Value, field: &str) -> Result<T, Error> {
    T::from_value(value).map_err(|e| invalid(format!("{field}: {e}")))
}

fn present<'a>(object: &'a Map<String, Value>, name: &str) -> Option<&'a Value> {
    object.get(name).filter(|v| !v.is_null())
}

fn string(value: &Value, field: &str) -> Result<String, Error> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("{field} must be a string")))
}

fn optional_string(object: &Map<String, Value>, name: &str) -> Option<String> {
    present(object, name)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn bytes(value: &Value, field: &str) -> Result<Vec<u8>, Error> {
    proto_json::bytes_from_value(value).map_err(|e| invalid(format!("{field}: {e}")))
}

fn number(value: &Value, field: &str) -> Result<f64, Error> {
    value
        .as_f64()
        .ok_or_else(|| invalid(format!("{field} must be a number")))
}

fn object<'a>(value: &'a Value, field: &str) -> Result<&'a Map<String, Value>, Error> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{field} must be an object")))
}

/// A media upload: `{ url }` (an HTTP(S) URL or a file path) or a buffer in
/// any BufferJSON form, matching `WAMediaUpload` after JSON decoding.
fn media_input(value: &Value, field: &str) -> Result<Input, Error> {
    if let Some(url) = value.get("url") {
        return Ok(Input::Url(string(url, field)?));
    }
    Ok(Input::bytes(bytes(value, field)?))
}

/// Dates arrive as millisecond numbers.
fn date_ms(value: &Value, field: &str) -> Result<f64, Error> {
    number(value, field)
}

fn without(object: &Map<String, Value>, name: &str) -> Value {
    let mut object = object.clone();
    object.remove(name);
    Value::Object(object)
}

fn media(object: &Map<String, Value>) -> Result<Content, Error> {
    let Some(kind) = MEDIA_KEYS
        .iter()
        .rev()
        .find(|key| object.contains_key(**key))
    else {
        return Err(Error::InvalidMediaType);
    };
    let input = media_input(&object[*kind], kind)?;
    // Every other content field is copied into the media message, as upstream's
    // fromObject does; fields outside the schema are ignored there too.
    let template = without(object, kind);
    let payload = match *kind {
        "image" => Payload::Image(Box::new(parse(&template, kind)?)),
        "video" => Payload::Video(Box::new(parse(&template, kind)?)),
        "document" => Payload::Document(Box::new(parse(&template, kind)?)),
        "audio" => Payload::Audio(Box::new(parse(&template, kind)?)),
        _ => Payload::Sticker(Box::new(parse(&template, kind)?)),
    };
    Ok(Content::Media(Box::new(Request {
        input,
        payload,
        video_note: false,
    })))
}

fn text(object: &Map<String, Value>, text: &Value) -> Result<Content, Error> {
    let link_preview = match object.get("linkPreview") {
        None => Preview::Automatic,
        Some(Value::Null) => Preview::Disabled,
        Some(value) => {
            let preview = self::object(value, "linkPreview")?;
            Preview::Provided(Box::new(LinkPreview {
                matched_text: optional_string(preview, "matched-text"),
                jpeg_thumbnail: present(preview, "jpegThumbnail")
                    .map(|v| bytes(v, "jpegThumbnail"))
                    .transpose()?,
                description: optional_string(preview, "description"),
                title: optional_string(preview, "title"),
                high_quality_thumbnail: present(preview, "highQualityThumbnail")
                    .map(|v| parse(v, "highQualityThumbnail"))
                    .transpose()?,
            }))
        }
    };
    Ok(Content::Text(Box::new(Text {
        text: match text {
            Value::String(text) => text.clone(),
            other => String::from_value(other).map_err(|e| invalid(format!("text: {e}")))?,
        },
        link_preview,
    })))
}

fn event(value: &Value) -> Result<Content, Error> {
    let event = object(value, "event")?;
    let start = date_ms(event.get("startDate").unwrap_or(&Value::Null), "startDate")?;
    Ok(Content::Event(Box::new(Event {
        name: optional_string(event, "name").unwrap_or_default(),
        description: optional_string(event, "description"),
        start_time: (start / 1000.0).floor() as i64,
        end_time: present(event, "endDate")
            .map(|v| date_ms(v, "endDate").map(|ms| (ms / 1000.0).trunc() as i64))
            .transpose()?,
        location: present(event, "location")
            .map(|v| parse(v, "location").map(Box::new))
            .transpose()?,
        call: match present(event, "call").and_then(Value::as_str) {
            Some("audio") => Some(CallKind::Audio),
            Some("video") => Some(CallKind::Video),
            Some(other) => return Err(invalid(format!("unknown call type {other}"))),
            None => None,
        },
        is_canceled: present(event, "isCancelled").and_then(Value::as_bool),
        is_schedule_call: present(event, "isScheduleCall").and_then(Value::as_bool),
        extra_guests_allowed: present(event, "extraGuestsAllowed").and_then(Value::as_bool),
        message_secret: present(event, "messageSecret")
            .map(|v| bytes(v, "messageSecret"))
            .transpose()?,
    })))
}

fn poll(value: &Value) -> Result<Content, Error> {
    let poll = object(value, "poll")?;
    let values = poll
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("poll.values must be an array"))?;
    Ok(Content::Poll(Box::new(Poll {
        name: optional_string(poll, "name").unwrap_or_default(),
        values: values
            .iter()
            .map(|v| String::from_value(v).map_err(|e| invalid(format!("poll.values: {e}"))))
            .collect::<Result<_, _>>()?,
        selectable_count: present(poll, "selectableCount")
            .map(|v| number(v, "selectableCount").map(|n| n as i64))
            .transpose()?,
        to_announcement_group: present(poll, "toAnnouncementGroup")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        message_secret: present(poll, "messageSecret")
            .map(|v| bytes(v, "messageSecret"))
            .transpose()?,
    })))
}

fn content(object: &Map<String, Value>) -> Result<Content, Error> {
    if let Some(value) = present(object, "text") {
        return text(object, value);
    }
    if let Some(value) = present(object, "contacts") {
        return Ok(Content::Contacts(Box::new(parse(value, "contacts")?)));
    }
    if let Some(value) = present(object, "location") {
        return Ok(Content::Location(Box::new(parse(value, "location")?)));
    }
    if let Some(value) = present(object, "react") {
        return Ok(Content::Reaction(Box::new(parse(value, "react")?)));
    }
    if let Some(value) = present(object, "delete") {
        return Ok(Content::Delete(parse(value, "delete")?));
    }
    if let Some(value) = present(object, "forward") {
        return Ok(Content::Forward {
            message: Box::new(parse(value, "forward")?),
            force: present(object, "force").and_then(Value::as_bool) == Some(true),
        });
    }
    if let Some(value) = present(object, "disappearingMessagesInChat") {
        return Ok(Content::Disappearing(match value {
            Value::Bool(true) => Disappearing::Enabled,
            Value::Bool(false) => Disappearing::Disabled,
            other => Disappearing::Seconds(number(other, "disappearingMessagesInChat")? as u32),
        }));
    }
    if let Some(value) = present(object, "groupInvite") {
        let invite = self::object(value, "groupInvite")?;
        return Ok(Content::GroupInvite(Box::new(
            proto::message::GroupInviteMessage {
                invite_code: optional_string(invite, "inviteCode"),
                invite_expiration: present(invite, "inviteExpiration")
                    .map(|v| parse(v, "inviteExpiration"))
                    .transpose()?,
                caption: optional_string(invite, "text"),
                group_jid: optional_string(invite, "jid"),
                group_name: optional_string(invite, "subject"),
                ..Default::default()
            },
        )));
    }
    if let Some(value) = present(object, "pin") {
        return Ok(Content::Pin {
            key: parse(value, "pin")?,
            kind: present(object, "type")
                .map(|v| number(v, "type").map(|n| n as i32))
                .transpose()?
                .unwrap_or(0),
            duration_seconds: present(object, "time")
                .map(|v| number(v, "time").map(|n| n as u32))
                .transpose()?,
        });
    }
    if let Some(value) = present(object, "buttonReply") {
        let reply = self::object(value, "buttonReply")?;
        let id = optional_string(reply, "id").unwrap_or_default();
        let display_text = optional_string(reply, "displayText").unwrap_or_default();
        return Ok(match present(object, "type").and_then(Value::as_str) {
            Some("template") => Content::TemplateButtonReply {
                id,
                display_text,
                index: present(reply, "index")
                    .map(|v| number(v, "index").map(|n| n as u32))
                    .transpose()?
                    .unwrap_or(0),
            },
            Some("plain") => Content::ButtonReply { id, display_text },
            _ => return Err(invalid("buttonReply needs type \"template\" or \"plain\"")),
        });
    }
    if object.get("ptv").is_some_and(|v| v.as_bool() == Some(true)) {
        let video = object.get("video").unwrap_or(&Value::Null);
        return Ok(Content::VideoNote(media_input(video, "video")?));
    }
    if let Some(value) = present(object, "product") {
        let product = self::object(value, "product")?;
        let image = media_input(
            product.get("productImage").unwrap_or(&Value::Null),
            "productImage",
        )?;
        let mut template = object.clone();
        template.insert("product".into(), without(product, "productImage"));
        return Ok(Content::Product {
            image,
            template: Box::new(parse(&Value::Object(template), "product")?),
        });
    }
    if let Some(value) = present(object, "listReply") {
        return Ok(Content::ListReply(Box::new(parse(value, "listReply")?)));
    }
    if let Some(value) = present(object, "event") {
        return event(value);
    }
    if let Some(value) = present(object, "poll") {
        return poll(value);
    }
    if let Some(value) = present(object, "album") {
        let album = self::object(value, "album")?;
        let count = |name| {
            present(album, name)
                .map(|v| number(v, name).map(|n| n as u32))
                .transpose()
        };
        return Ok(Content::Album {
            expected_image_count: count("expectedImageCount")?,
            expected_video_count: count("expectedVideoCount")?,
        });
    }
    if present(object, "sharePhoneNumber").is_some() {
        return Ok(Content::SharePhoneNumber);
    }
    if present(object, "requestPhoneNumber").is_some() {
        return Ok(Content::RequestPhoneNumber);
    }
    if let Some(value) = present(object, "limitSharing") {
        return Ok(Content::LimitSharing(value.as_bool() == Some(true)));
    }
    media(object)
}

fn context(value: &Value) -> Result<ContextPatch, Error> {
    let fields = object(value, "contextInfo")?;
    let mut clear_fields = vec![];
    for (name, field) in fields {
        let cleared = field.is_null() || field.as_array().is_some_and(Vec::is_empty);
        if !cleared {
            continue;
        }
        let snake: String = name
            .chars()
            .flat_map(|c| {
                if c.is_ascii_uppercase() {
                    vec!['_', c.to_ascii_lowercase()]
                } else {
                    vec![c]
                }
            })
            .collect();
        if let Some((field, _)) = ContextField::ALL.iter().find(|(_, n)| *n == snake) {
            clear_fields.push(*field);
        }
    }
    Ok(ContextPatch {
        value: parse(value, "contextInfo")?,
        clear_fields,
    })
}

fn modifiers(object: &Map<String, Value>) -> Result<Modifiers, Error> {
    Ok(Modifiers {
        view_once: object.get("viewOnce").is_some_and(truthy),
        mentions: match present(object, "mentions") {
            Some(Value::Array(values)) => values
                .iter()
                .map(|v| string(v, "mentions"))
                .collect::<Result<_, _>>()?,
            Some(_) => return Err(invalid("mentions must be an array")),
            None => vec![],
        },
        mention_all: object.get("mentionAll").is_some_and(truthy),
        edit: present(object, "edit")
            .map(|v| parse(v, "edit"))
            .transpose()?,
        context: object
            .get("contextInfo")
            .filter(|v| truthy(v))
            .map(context)
            .transpose()?,
        album_parent_key: object
            .get("albumParentKey")
            .filter(|v| truthy(v))
            .map(|v| parse(v, "albumParentKey"))
            .transpose()?,
    })
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

/// Convert upstream's `AnyMessageContent` object, as `sendMessage` receives
/// it after `JSON.parse`, into content and modifiers.
///
/// Content keys are checked in upstream's order, so `{text, image}` sends
/// text. Media is `{ url }` or a buffer in BufferJSON form, and dates are
/// millisecond numbers. Without a known content key the object is treated
/// as media and fails with upstream's "Invalid media type".
pub fn content_from_json(value: &Value) -> Result<(Content, Modifiers), Error> {
    let object = object(value, "message content")?;
    Ok((content(object)?, modifiers(object)?))
}
