use baileys_rs::{
    messages::generation::{self, *},
    proto,
};
use prost::Message as _;
use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicI64, AtomicU8, Ordering},
};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/message_content.json")).unwrap()
}
fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn string(v: &Value) -> Option<String> {
    v.as_str().map(str::to_owned)
}
fn bytes(v: &Value) -> Option<Vec<u8>> {
    v.as_array()
        .map(|a| a.iter().map(|v| v.as_u64().unwrap() as u8).collect())
}
fn key(v: &Value) -> proto::MessageKey {
    serde_json::from_value(v.clone()).unwrap()
}
struct Hooks {
    config: Value,
    calls: Mutex<Vec<Value>>,
    clock: AtomicI64,
    random: AtomicU8,
}
impl Hooks {
    fn new(config: Value) -> Self {
        Self {
            config,
            calls: Mutex::default(),
            clock: AtomicI64::new(1_700_000_000_123),
            random: AtomicU8::new(0),
        }
    }
}
impl ContentHooks for Hooks {
    fn now_ms(&self) -> Result<i64, Error> {
        Ok(self.clock.fetch_add(1, Ordering::SeqCst) + 1)
    }
    fn message_secret(&self) -> Result<Vec<u8>, Error> {
        Ok(vec![self.random.fetch_add(1, Ordering::SeqCst) + 1; 32])
    }
    fn link_preview(&self, url: String) -> HookFuture<'_, Option<LinkPreview>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(json!(["preview", url]));
            if self.config["previewError"] == true {
                return Err(Error::Hook("preview failed".into()));
            }
            Ok((self.config["preview"] == true).then_some(LinkPreview {
                matched_text: Some(url),
                title: Some("hook preview".into()),
                ..Default::default()
            }))
        })
    }
    fn invite_thumbnail(&self, jid: String) -> HookFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(json!(["thumbnail", jid]));
            if self.config["thumbnail"] != true {
                return Ok(None);
            }
            if self.config["thumbnailError"] == true {
                return Err(Error::Hook("thumbnail failed".into()));
            }
            Ok((self.config["thumbnailOk"] != false).then_some(vec![9, 8, 7]))
        })
    }
    fn call_link(&self, kind: CallKind, start: i64) -> HookFuture<'_, Option<String>> {
        Box::pin(async move {
            if self.config["noCall"] == true {
                return Ok(None);
            }
            self.calls.lock().unwrap().push(json!([
                "call",
                if kind == CallKind::Audio {
                    "audio"
                } else {
                    "video"
                },
                start
            ]));
            if self.config["callError"] == true {
                return Err(Error::Hook("call failed".into()));
            }
            Ok(Some("TOKEN".into()))
        })
    }
}
fn input(case: &Value) -> Content {
    let payload =
        proto::Message::decode(hex(case["payload"].as_str().unwrap()).as_slice()).unwrap();
    let r = &case["request"];
    match case["kind"].as_str().unwrap() {
        "text" => Content::Text(Box::new(Text {
            text: r["text"].as_str().unwrap().into(),
            link_preview: match r.get("linkPreview") {
                None => Preview::Automatic,
                Some(Value::Null) => Preview::Disabled,
                Some(p) => Preview::Provided(Box::new(LinkPreview {
                    matched_text: string(&p["matched-text"]),
                    jpeg_thumbnail: bytes(&p["jpegThumbnail"]),
                    description: string(&p["description"]),
                    title: string(&p["title"]),
                    high_quality_thumbnail: payload.image_message.map(|m| *m),
                })),
            },
        })),
        "contacts" => Content::Contacts(payload.contacts_array_message.unwrap()),
        "location" => Content::Location(payload.location_message.unwrap()),
        "react" => Content::Reaction(Box::new(payload.reaction_message.unwrap())),
        "delete" => Content::Delete(
            proto::MessageKey::decode(hex(case["keyWire"].as_str().unwrap()).as_slice()).unwrap(),
        ),
        "forward" => Content::Forward {
            message: Box::new(
                proto::WebMessageInfo::decode(hex(case["forward"].as_str().unwrap()).as_slice())
                    .unwrap(),
            ),
            force: r["force"] == true,
        },
        "disappearingMessagesInChat" => {
            Content::Disappearing(match &r["disappearingMessagesInChat"] {
                Value::Bool(true) => Disappearing::Enabled,
                Value::Bool(false) => Disappearing::Disabled,
                v => Disappearing::Seconds(v.as_u64().unwrap() as u32),
            })
        }
        "groupInvite" => Content::GroupInvite(payload.group_invite_message.unwrap()),
        "pin" => Content::Pin {
            key: proto::MessageKey::decode(hex(case["keyWire"].as_str().unwrap()).as_slice())
                .unwrap(),
            kind: r["type"].as_i64().unwrap() as i32,
            duration_seconds: r["time"].as_u64().map(|n| n as u32),
        },
        "buttonReply" => {
            if r["type"] == "template" {
                Content::TemplateButtonReply {
                    id: r["buttonReply"]["id"].as_str().unwrap().into(),
                    display_text: r["buttonReply"]["displayText"].as_str().unwrap().into(),
                    index: r["buttonReply"]["index"].as_u64().unwrap() as u32,
                }
            } else {
                Content::ButtonReply {
                    id: r["buttonReply"]["id"].as_str().unwrap().into(),
                    display_text: r["buttonReply"]["displayText"].as_str().unwrap().into(),
                }
            }
        }
        "listReply" => Content::ListReply(payload.list_response_message.unwrap()),
        "event" => {
            let event = payload.event_message.unwrap();
            let e = &r["event"];
            Content::Event(Box::new(Event {
                name: event.name.unwrap(),
                description: event.description,
                start_time: event.start_time.unwrap(),
                end_time: event.end_time,
                location: event.location,
                call: e["call"].as_str().map(|s| {
                    if s == "audio" {
                        CallKind::Audio
                    } else {
                        CallKind::Video
                    }
                }),
                is_canceled: e["isCancelled"].as_bool(),
                is_schedule_call: e["isScheduleCall"].as_bool(),
                extra_guests_allowed: e["extraGuestsAllowed"].as_bool(),
                message_secret: bytes(&e["messageSecret"]),
            }))
        }
        "poll" => {
            let p = &r["poll"];
            Content::Poll(Box::new(Poll {
                name: p["name"].as_str().unwrap().into(),
                values: p["values"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().into())
                    .collect(),
                selectable_count: p["selectableCount"].as_i64(),
                to_announcement_group: p["toAnnouncementGroup"] == true,
                message_secret: bytes(&p["messageSecret"]),
            }))
        }
        "album" => Content::Album {
            expected_image_count: r["album"]["expectedImageCount"].as_u64().map(|v| v as u32),
            expected_video_count: r["album"]["expectedVideoCount"].as_u64().map(|v| v as u32),
        },
        "sharePhoneNumber" => Content::SharePhoneNumber,
        "requestPhoneNumber" => Content::RequestPhoneNumber,
        "limitSharing" => Content::LimitSharing(r["limitSharing"] == true),
        "prepared" => Content::PreparedMedia(Box::new(payload)),
        kind => panic!("unknown kind {kind}"),
    }
}
fn modifiers(case: &Value) -> Modifiers {
    let m = &case["modifier"];
    Modifiers {
        view_once: m["viewOnce"] == true,
        mention_all: m["mentionAll"] == true,
        mentions: m["mentions"]
            .as_array()
            .map(|v| v.iter().map(|s| s.as_str().unwrap().into()).collect())
            .unwrap_or_default(),
        edit: m.get("edit").map(key),
        album_parent_key: m.get("albumParentKey").map(key),
        context: case["context"].as_str().map(|s| ContextPatch {
            value: proto::ContextInfo::decode(hex(s).as_slice()).unwrap(),
            clear_fields: {
                let mut fields = vec![];
                if m["contextInfo"]["mentionedJid"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
                {
                    fields.push(ContextField::MentionedJid);
                }
                if m["contextInfo"]
                    .get("participant")
                    .is_some_and(Value::is_null)
                {
                    fields.push(ContextField::Participant);
                }
                fields
            },
        }),
    }
}
#[tokio::test]
async fn high_level_content_and_hook_order_match_original_upstream() {
    for (i, case) in fixture()["cases"].as_array().unwrap().iter().enumerate() {
        let hooks = Hooks::new(case["hooks"].clone());
        let o = &case["options"];
        let options = ContentOptions {
            background_color: o.get("backgroundColor").map(|v| match v {
                Value::String(s) => Color::Hex(s.clone()),
                _ => Color::Number(v.as_f64().unwrap()),
            }),
            font: o["font"].as_i64().map(|v| v as i32),
        };
        let actual = generation::content(&input(case), &modifiers(case), &options, &hooks).await;
        if !case["error"].is_null() {
            let error = actual.expect_err(&format!("case {i}: {case}"));
            assert_eq!(
                error.status_code(),
                case["error"]["status"].as_u64().map(|v| v as u16),
                "case {i}: {case}"
            );
        } else {
            assert_eq!(
                actual.unwrap().encode_to_vec(),
                hex(case["output"].as_str().unwrap()),
                "case {i}: {case}"
            );
        }
        assert_eq!(
            *hooks.calls.lock().unwrap(),
            case["calls"].as_array().unwrap().clone(),
            "calls in case {i}"
        );
    }
}
#[test]
fn url_extraction_matches_source_regex_and_whitespace() {
    for case in fixture()["urls"].as_array().unwrap() {
        assert_eq!(
            extract_url(case["text"].as_str().unwrap()),
            case["url"].as_str(),
            "{case}"
        );
    }
}
#[tokio::test]
async fn default_entropy_and_combined_generation_produce_relay_ready_messages() {
    let hooks = DefaultContentHooks;
    let content = Content::text("hello");
    let first = generation::content(
        &content,
        &Modifiers::default(),
        &ContentOptions::default(),
        &hooks,
    )
    .await
    .unwrap();
    let second = generation::content(
        &content,
        &Modifiers::default(),
        &ContentOptions::default(),
        &hooks,
    )
    .await
    .unwrap();
    let a = first.message_context_info.unwrap().message_secret.unwrap();
    assert_eq!(a.len(), 32);
    assert_ne!(
        a,
        second.message_context_info.unwrap().message_secret.unwrap()
    );
    let full = generate(
        "group@g.us",
        &content,
        &Modifiers::default(),
        &ContentOptions::default(),
        &Options {
            user_jid: "100:7@s.whatsapp.net".into(),
            message_id: Some("GENERATED".into()),
            ephemeral_expiration: Some(60),
            ..Default::default()
        },
        &hooks,
    )
    .await
    .unwrap();
    assert_eq!(full.participant.as_deref(), Some("100:7@s.whatsapp.net"));
    assert_eq!(full.key.unwrap().id.as_deref(), Some("GENERATED"));
    assert_eq!(
        full.message
            .unwrap()
            .extended_text_message
            .unwrap()
            .context_info
            .unwrap()
            .expiration,
        Some(60)
    );
    let reaction = Content::Reaction(Box::new(proto::message::ReactionMessage {
        text: Some("💚".into()),
        ..Default::default()
    }));
    let generated = generation::content(
        &reaction,
        &Modifiers::default(),
        &ContentOptions::default(),
        &hooks,
    )
    .await
    .unwrap();
    assert!(generated.message_context_info.is_none());
    assert!(
        generated
            .reaction_message
            .unwrap()
            .sender_timestamp_ms
            .unwrap()
            > 0
    );
}
