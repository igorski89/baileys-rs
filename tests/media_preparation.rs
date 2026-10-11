use baileys_rs::{media::*, messages::generation, proto};
use futures_util::future::BoxFuture;
use prost::Message as _;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/media_preparation.json")).unwrap()
}
fn hex(v: &Value) -> Vec<u8> {
    v.as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|s| u8::from_str_radix(std::str::from_utf8(s).unwrap(), 16).unwrap())
        .collect()
}
fn payload(kind: &str, message: proto::Message) -> Payload {
    match kind {
        "image" => Payload::Image(message.image_message.unwrap_or_default()),
        "video" => Payload::Video(message.video_message.unwrap_or_default()),
        "audio" => Payload::Audio(message.audio_message.unwrap_or_default()),
        "document" => Payload::Document(message.document_message.unwrap_or_default()),
        "sticker" => Payload::Sticker(message.sticker_message.unwrap_or_default()),
        _ => panic!(),
    }
}
#[derive(Clone)]
struct Hooks {
    mode: String,
    calls: Arc<Mutex<Vec<Value>>>,
    cache: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}
impl Hooks {
    fn new(mode: &str) -> Self {
        Self {
            mode: mode.into(),
            calls: Default::default(),
            cache: Default::default(),
        }
    }
}
impl PrepareHooks for Hooks {
    fn thumbnail(&self, path: PathBuf, kind: MediaType) -> BoxFuture<'_, Result<Thumbnail, Error>> {
        self.calls
            .lock()
            .unwrap()
            .push(serde_json::json!({"kind":"thumbnail","type":kind.as_str(),"original":true}));
        Box::pin(async move {
            assert_eq!(std::fs::read(path).unwrap(), (0..17u8).collect::<Vec<_>>());
            if self.mode == "fail" {
                return Err(Error::Input("metadata failure"));
            }
            Ok(Thumbnail {
                jpeg: Some(vec![9, 8]),
                dimensions: Some((320, 200)),
            })
        })
    }
    fn duration(&self, path: PathBuf) -> BoxFuture<'_, Result<Option<u32>, Error>> {
        self.calls
            .lock()
            .unwrap()
            .push(serde_json::json!({"kind":"duration","original":true}));
        Box::pin(async move {
            assert!(path.exists());
            if self.mode == "fail" {
                return Err(Error::Input("metadata failure"));
            }
            Ok(Some(3))
        })
    }
    fn waveform(&self, path: Option<PathBuf>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        self.calls
            .lock()
            .unwrap()
            .push(serde_json::json!({"kind":"waveform","original":path.is_some()}));
        Box::pin(async { Ok(Some(vec![1, 2, 3])) })
    }
    fn now_ms(&self) -> Result<i64, Error> {
        Ok(1700000000123)
    }
    fn media_key(&self) -> Result<[u8; 32], Error> {
        Ok(std::array::from_fn(|i| i as u8))
    }
    fn cache_get(&self, key: String) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move { Ok(self.cache.lock().unwrap().get(&key).cloned()) })
    }
    fn cache_set(&self, key: String, value: Vec<u8>) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.cache.lock().unwrap().insert(key, value);
            Ok(())
        })
    }
}
#[derive(Clone, Default)]
struct Uploader {
    calls: Arc<Mutex<Vec<Value>>>,
    paths: Arc<Mutex<Vec<PathBuf>>>,
    fail: bool,
}
impl MediaUploader for Uploader {
    fn upload_media<'a>(
        &'a self,
        path: PathBuf,
        kind: MediaType,
        hash: String,
        options: UploadOptions,
    ) -> BoxFuture<'a, Result<UploadResponse, Error>> {
        Box::pin(async move {
            self.paths.lock().unwrap().push(path.clone());
            let bytes = std::fs::read(&path).unwrap();
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            let mut call =
                serde_json::json!({"mediaType":kind.as_str(),"fileEncSha256B64":hash,"bytes":hex});
            if let Some(timeout) = options.timeout {
                call["timeoutMs"] = serde_json::json!(timeout.as_millis());
            }
            self.calls.lock().unwrap().push(call);
            if self.fail {
                return Err(Error::UploadFailed);
            }
            Ok(UploadResponse {
                url: Some("https://media.example/file".into()),
                direct_path: Some("/media/file".into()),
                ..Default::default()
            })
        })
    }
}
#[tokio::test]
async fn five_media_types_newsletters_defaults_overrides_and_metadata_match_upstream() {
    let http = HttpClient::new().unwrap();
    for case in fixture()
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c.get("cache").is_none())
    {
        let hooks = Hooks::new(case["mode"].as_str().unwrap());
        let uploader = Uploader::default();
        let request = Request {
            input: Input::bytes((0..17u8).collect::<Vec<_>>()),
            payload: payload(
                case["type"].as_str().unwrap(),
                proto::Message::decode(hex(&case["template"]).as_slice()).unwrap(),
            ),
            video_note: case["fields"]["ptv"].as_bool() == Some(true),
        };
        let options = PrepareOptions {
            destination: Some(
                if case["newsletter"].as_bool().unwrap() {
                    "channel@newsletter"
                } else {
                    "200@s.whatsapp.net"
                }
                .into(),
            ),
            background_color: Some(match case.get("backgroundColor") {
                Some(Value::String(s)) => generation::Color::Hex(s.clone()),
                Some(value) => generation::Color::Number(value.as_f64().unwrap()),
                None => generation::Color::Hex("#123456".into()),
            }),
            upload: UploadOptions {
                timeout: Some(std::time::Duration::from_millis(500)),
                ..Default::default()
            },
            ..Default::default()
        };
        let actual = prepare(&request, &options, &http, &uploader, &hooks)
            .await
            .unwrap();
        assert_eq!(actual.encode_to_vec(), hex(&case["output"]), "{case}");
        assert_eq!(
            *hooks.calls.lock().unwrap(),
            case["calls"].as_array().unwrap().clone(),
            "{case}"
        );
        assert_eq!(
            *uploader.calls.lock().unwrap(),
            case["uploads"].as_array().unwrap().clone(),
            "{case}"
        );
        for path in uploader.paths.lock().unwrap().iter() {
            assert!(!path.exists());
        }
    }
}
#[tokio::test]
async fn url_cache_reuses_upload_and_overlays_content_in_source_order() {
    let http = HttpClient::new().unwrap();
    for case in fixture()
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["cache"] == true)
    {
        let hooks = Hooks::new("ok");
        let uploader = Uploader::default();
        // Seed exactly the first generated payload: the input URL is the source cache identity.
        let first = hex(&case["outputs"][0]);
        hooks.cache.lock().unwrap().insert(
            format!(
                "{}:https://input.example/file",
                case["type"].as_str().unwrap()
            ),
            first,
        );
        let mut template = proto::Message::default();
        let mut p = payload(case["type"].as_str().unwrap(), template.clone());
        let context = Some(Box::new(proto::ContextInfo {
            expiration: Some(7),
            ..Default::default()
        }));
        match &mut p {
            Payload::Image(m) => {
                m.caption = Some("second".into());
                m.mimetype = Some("custom/type".into());
                m.context_info = context;
            }
            Payload::Video(m) => {
                m.caption = Some("second".into());
                m.mimetype = Some("custom/type".into());
                m.seconds = Some(0);
                m.context_info = context;
            }
            Payload::Audio(m) => {
                m.mimetype = Some("custom/type".into());
                m.seconds = Some(0);
                m.context_info = context;
            }
            Payload::Document(m) => {
                m.caption = Some("second".into());
                m.mimetype = Some("custom/type".into());
                m.context_info = context;
            }
            Payload::Sticker(m) => {
                m.mimetype = Some("custom/type".into());
                m.context_info = context;
            }
        }
        let request = Request {
            input: Input::Url("https://input.example/file".into()),
            payload: p,
            video_note: true,
        };
        template = prepare(
            &request,
            &PrepareOptions {
                cache: true,
                ..Default::default()
            },
            &http,
            &uploader,
            &hooks,
        )
        .await
        .unwrap();
        assert_eq!(template.encode_to_vec(), hex(&case["outputs"][1]), "{case}");
        assert!(uploader.calls.lock().unwrap().is_empty());
        assert!(hooks.calls.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn input_files_data_uris_streams_upload_failures_and_key_override_work() {
    let http = HttpClient::new().unwrap();
    let hooks = Hooks::new("ok");
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("plain");
    std::fs::write(&path, (0..17u8).collect::<Vec<_>>()).unwrap();
    let payload = Payload::Document(Box::default());
    for input in [
        Input::File(path),
        Input::Url("data:application/test;base64,AAECAwQFBgcICQoLDA0ODxA=".into()),
        Input::Reader(MediaReader::new(std::io::Cursor::new(
            (0..17u8).collect::<Vec<_>>(),
        ))),
    ] {
        let uploader = Uploader::default();
        let request = Request {
            input,
            payload: payload.clone(),
            video_note: false,
        };
        let options = PrepareOptions {
            media_type_override: Some(MediaType::History),
            ..Default::default()
        };
        let m = prepare(&request, &options, &http, &uploader, &hooks)
            .await
            .unwrap();
        let upload = hex(&uploader.calls.lock().unwrap()[0]["bytes"]);
        assert_eq!(
            decrypt_verified(
                &upload,
                &Keys::derive(
                    &std::array::from_fn::<_, 32, _>(|i| i as u8),
                    MediaType::History
                )
                .unwrap()
            )
            .unwrap(),
            (0..17u8).collect::<Vec<_>>()
        );
        assert_eq!(m.document_message.unwrap().file_length, Some(17));
    }
    let uploader = Uploader {
        fail: true,
        ..Default::default()
    };
    let request = Request {
        input: Input::bytes((0..17u8).collect::<Vec<_>>()),
        payload,
        video_note: false,
    };
    assert!(
        prepare(&request, &Default::default(), &http, &uploader, &hooks)
            .await
            .is_err()
    );
    assert!(uploader.paths.lock().unwrap().iter().all(|p| !p.exists()));
    let reader = MediaReader::new(std::io::Cursor::new(vec![1]));
    let request = Request {
        input: Input::Reader(reader),
        ..request
    };
    let uploader = Uploader::default();
    prepare(&request, &Default::default(), &http, &uploader, &hooks)
        .await
        .unwrap();
    assert!(
        prepare(&request, &Default::default(), &http, &uploader, &hooks)
            .await
            .is_err()
    );
}
struct Generation {
    http: HttpClient,
    hooks: Hooks,
    uploader: Uploader,
    newsletter: bool,
}
impl generation::ContentHooks for Generation {
    fn message_secret(&self) -> Result<Vec<u8>, generation::Error> {
        Ok(vec![7; 32])
    }
    fn now_ms(&self) -> Result<i64, generation::Error> {
        Ok(1700000000123)
    }
    fn prepare_media<'a>(
        &'a self,
        request: &'a Request,
        options: &'a PrepareOptions,
    ) -> generation::HookFuture<'a, proto::Message> {
        Box::pin(async move {
            let mut options = options.clone();
            options.destination = Some(
                if self.newsletter {
                    "channel@newsletter"
                } else {
                    "200@s.whatsapp.net"
                }
                .into(),
            );
            Ok(prepare(request, &options, &self.http, &self.uploader, &self.hooks).await?)
        })
    }
}
#[tokio::test]
async fn video_notes_products_and_modifiers_match_original_high_level_generation() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/media_content.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let hooks = Generation {
            http: HttpClient::new().unwrap(),
            hooks: Hooks::new("ok"),
            uploader: Uploader::default(),
            newsletter: case["newsletter"].as_bool().unwrap(),
        };
        let input = Input::bytes((0..17u8).collect::<Vec<_>>());
        let content = if case["kind"] == "ptv" {
            generation::Content::VideoNote(input)
        } else {
            generation::Content::Product {
                image: input,
                template: proto::Message::decode(hex(&case["template"]).as_slice())
                    .unwrap()
                    .product_message
                    .unwrap(),
            }
        };
        let modifier = &case["modifier"];
        let modifiers = generation::Modifiers {
            view_once: modifier["viewOnce"] == true,
            mentions: modifier["mentions"]
                .as_array()
                .map(|a| a.iter().map(|s| s.as_str().unwrap().to_owned()).collect())
                .unwrap_or_default(),
            mention_all: modifier["mentionAll"] == true,
            edit: modifier.get("edit").map(|k| proto::MessageKey {
                id: k["id"].as_str().map(str::to_owned),
                remote_jid: k["remoteJid"].as_str().map(str::to_owned),
                from_me: k["fromMe"].as_bool(),
                participant: None,
            }),
            ..Default::default()
        };
        let message = generation::content(&content, &modifiers, &Default::default(), &hooks)
            .await
            .unwrap();
        assert_eq!(message.encode_to_vec(), hex(&case["output"]), "{case}");
        assert_eq!(
            *hooks.hooks.calls.lock().unwrap(),
            case["calls"].as_array().unwrap().clone()
        );
        assert_eq!(
            *hooks.uploader.calls.lock().unwrap(),
            case["uploads"].as_array().unwrap().clone()
        );
    }
}
#[tokio::test]
async fn cache_population_corrupt_entries_and_video_note_type_mismatch_are_handled() {
    let hooks = Hooks::new("ok");
    let http = HttpClient::new().unwrap();
    let uploader = Uploader::default();
    let url = "data:;base64,AAECAwQFBgcICQoLDA0ODxA=";
    let request = Request {
        input: Input::Url(url.into()),
        payload: Payload::Document(Box::default()),
        video_note: false,
    };
    let options = PrepareOptions {
        cache: true,
        ..Default::default()
    };
    let first = prepare(&request, &options, &http, &uploader, &hooks)
        .await
        .unwrap();
    let failed = Uploader {
        fail: true,
        ..Default::default()
    };
    let mut request = request;
    request.payload = Payload::Document(Box::new(proto::message::DocumentMessage {
        caption: Some("cached caption".into()),
        ..Default::default()
    }));
    let second = prepare(&request, &options, &http, &failed, &hooks)
        .await
        .unwrap();
    assert_eq!(
        first.document_message.as_ref().unwrap().media_key,
        second.document_message.as_ref().unwrap().media_key
    );
    assert_eq!(
        second.document_message.unwrap().caption.as_deref(),
        Some("cached caption")
    );
    assert!(failed.calls.lock().unwrap().is_empty());
    assert_eq!(uploader.calls.lock().unwrap().len(), 1);
    hooks
        .cache
        .lock()
        .unwrap()
        .insert(format!("document:{url}"), vec![0xff]);
    assert!(
        prepare(&request, &options, &http, &uploader, &hooks)
            .await
            .is_err()
    );
    let video = Request {
        input: Input::Url(url.into()),
        payload: Payload::Video(Box::default()),
        video_note: true,
    };
    prepare(&video, &options, &http, &uploader, &hooks)
        .await
        .unwrap();
    assert!(
        prepare(&video, &options, &http, &uploader, &hooks)
            .await
            .is_err()
    );
}
