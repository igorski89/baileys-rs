use baileys_rs::{media::*, proto};
use futures_util::future::BoxFuture;
use image::GenericImageView;
use serde_json::Value;
use std::{
    io::{Cursor, Write},
    path::PathBuf,
};
fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/media_processors.json")).unwrap()
}
fn hex(value: &Value) -> Vec<u8> {
    value
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn pcm_durations_and_waveforms_match_original_audio_libraries() {
    for case in fixtures()["audios"].as_array().unwrap() {
        let bytes = hex(&case["input"]);
        let duration = audio_duration(Cursor::new(&bytes)).unwrap().unwrap();
        assert!(
            (duration - case["duration"].as_f64().unwrap()).abs() < 1e-12,
            "{case}"
        );
        let waveform = audio_waveform(Cursor::new(bytes)).unwrap();
        assert_eq!(waveform.to_vec(), hex(&case["waveform"]), "{case}");
    }
    assert_eq!(waveform_from_samples(&[]), [0; 64]);
    assert_eq!(waveform_from_samples(&[f32::NAN; 64]), [0; 64]);
    assert_eq!(waveform_from_samples(&[f32::INFINITY; 64]), [0; 64]);
    assert!(audio_duration(Cursor::new(b"bad audio")).is_err());
    assert!(audio_waveform(Cursor::new(b"bad audio")).is_err());
}
#[tokio::test]
async fn image_thumbnails_and_profile_cover_dimensions_match_original() {
    let http = HttpClient::new().unwrap();
    for case in fixtures()["images"].as_array().unwrap() {
        let bytes = hex(&case["input"]);
        let thumb = extract_image_thumb(Cursor::new(&bytes), 32).unwrap();
        assert_eq!(thumb.original_width, case["width"].as_u64().unwrap() as u32);
        assert_eq!(
            thumb.original_height,
            case["height"].as_u64().unwrap() as u32
        );
        let decoded = image::load_from_memory(&thumb.jpeg).unwrap();
        assert_eq!(
            decoded.dimensions(),
            (
                case["thumbnail"]["width"].as_u64().unwrap() as u32,
                case["thumbnail"]["height"].as_u64().unwrap() as u32
            )
        );
        let profile = generate_profile_picture(
            &Input::bytes(bytes),
            Some((40, 20)),
            &http,
            &PrepareOptions::default(),
        )
        .await
        .unwrap();
        let decoded = image::load_from_memory(&profile).unwrap();
        assert_eq!(decoded.dimensions(), (40, 20));
        if case["width"].as_u64().unwrap() >= 16 {
            let pixel = decoded.to_rgb8().get_pixel(20, 10).0;
            assert!(
                pixel[1] > 200 && pixel[0] < 55 && pixel[2] < 55,
                "center crop {pixel:?}"
            );
        }
    }
    assert!(extract_image_thumb(Cursor::new(b"bad image"), 32).is_err());
    assert!(extract_image_thumb(Cursor::new(b"bad image"), 0).is_err());
    let jpeg = generate_profile_picture(
        &Input::bytes(hex(&fixtures()["images"][0]["input"])),
        None,
        &http,
        &PrepareOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        image::load_from_memory(&jpeg).unwrap().dimensions(),
        (640, 640)
    );
}
#[tokio::test]
async fn compressed_audio_and_video_hooks_process_real_files() {
    let processor = MediaProcessors::default();
    let ffmpeg = std::process::Command::new(&processor.ffmpeg)
        .arg("-version")
        .output()
        .is_ok();
    let ffprobe = std::process::Command::new(&processor.ffprobe)
        .arg("-version")
        .output()
        .is_ok();
    for case in fixtures()["compressed"].as_array().unwrap() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&hex(&case["input"])).unwrap();
        if case["format"] == "opus" && (!ffmpeg || !ffprobe) {
            continue;
        }
        let duration = processor
            .duration(file.path().into())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            duration,
            case["duration"].as_f64().unwrap().trunc() as u32,
            "{} duration",
            case["format"]
        );
        let wave = processor.waveform(Some(file.path().into())).await.unwrap();
        // Upstream's decoder yields no waveform for AAC and non-16-bit WAV; the
        // native decoder may still produce one.
        if case["waveform"].is_null() {
            println!(
                "{} ({}): native waveform {}",
                case["format"],
                case["codec"],
                if wave.is_some() { "present" } else { "absent" }
            );
            continue;
        }
        let wave = wave.unwrap();
        let expected = hex(&case["waveform"]);
        // Lossy decoder implementations can round normalized bin levels differently.
        let tolerance = if case["format"] == "flac" { 0 } else { 1 };
        for (a, b) in wave.iter().zip(expected) {
            assert!(
                a.abs_diff(b) <= tolerance,
                "{} waveform: {a} vs {b}",
                case["format"]
            );
        }
    }
    if ffmpeg {
        // Spaces and shell punctuation are ordinary filename characters.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("video with spaces; literal $.mp4");
        std::fs::write(&path, hex(&fixtures()["video"]["input"])).unwrap();
        let thumb = processor.thumbnail(path, MediaType::Video).await.unwrap();
        assert_eq!(thumb.dimensions, None);
        let image = image::load_from_memory(&thumb.jpeg.unwrap()).unwrap();
        let expected = image::load_from_memory(&hex(&fixtures()["video"]["jpeg"])).unwrap();
        assert_eq!(image.dimensions(), expected.dimensions());
        assert_eq!(image.dimensions(), (32, 16));
    }
    let unavailable = MediaProcessors {
        ffmpeg: "/nonexistent/media-ffmpeg".into(),
        ffprobe: "/nonexistent/media-ffprobe".into(),
    };
    assert!(
        unavailable
            .thumbnail("/nonexistent/video.mp4".into(), MediaType::Video)
            .await
            .unwrap()
            .jpeg
            .is_none()
    );
    assert!(unavailable.waveform(None).await.unwrap().is_none());
    assert!(
        unavailable
            .waveform(Some("/nonexistent/audio".into()))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        unavailable
            .duration("/nonexistent/audio".into())
            .await
            .is_err()
    );
    let mut opus = tempfile::NamedTempFile::new().unwrap();
    let case = fixtures()["compressed"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["format"] == "opus")
        .unwrap()
        .clone();
    opus.write_all(&hex(&case["input"])).unwrap();
    // Opus duration is parsed natively; only decoding needs the fallback.
    assert_eq!(
        unavailable.duration(opus.path().into()).await.unwrap(),
        Some(1)
    );
    assert!(
        unavailable
            .waveform(Some(opus.path().into()))
            .await
            .unwrap()
            .is_none()
    );
}
struct Upload;
impl MediaUploader for Upload {
    fn upload_media<'a>(
        &'a self,
        path: PathBuf,
        _kind: MediaType,
        _hash: String,
        _options: UploadOptions,
    ) -> BoxFuture<'a, Result<UploadResponse, Error>> {
        Box::pin(async move {
            assert!(path.exists());
            Ok(UploadResponse {
                url: Some("https://media.example/file".into()),
                ..Default::default()
            })
        })
    }
}
#[tokio::test]
async fn default_preparation_generates_real_image_and_audio_metadata() {
    let http = HttpClient::new().unwrap();
    let request = Request {
        input: Input::bytes(hex(&fixtures()["images"][1]["input"])),
        payload: Payload::Image(Default::default()),
        video_note: false,
    };
    let image = prepare(
        &request,
        &PrepareOptions::default(),
        &http,
        &Upload,
        &DefaultPrepareHooks,
    )
    .await
    .unwrap()
    .image_message
    .unwrap();
    assert_eq!((image.width, image.height), (Some(64), Some(32)));
    assert_eq!(
        image::load_from_memory(&image.jpeg_thumbnail.unwrap())
            .unwrap()
            .dimensions(),
        (32, 16)
    );
    let fixture = fixtures();
    let case = fixture["audios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["count"] == 8011 && c["float"] == true && c["mode"] == "varying")
        .unwrap();
    let request = Request {
        input: Input::bytes(hex(&case["input"])),
        payload: Payload::Audio(Box::new(proto::message::AudioMessage {
            ptt: Some(true),
            ..Default::default()
        })),
        video_note: false,
    };
    let audio = prepare(
        &request,
        &PrepareOptions::default(),
        &http,
        &Upload,
        &DefaultPrepareHooks,
    )
    .await
    .unwrap()
    .audio_message
    .unwrap();
    assert_eq!(audio.seconds, Some(1));
    assert_eq!(audio.waveform, Some(hex(&case["waveform"])));
    // Source only retains an original when duration/thumbnail work requires it.
    let request = Request {
        payload: Payload::Audio(Box::new(proto::message::AudioMessage {
            ptt: Some(true),
            seconds: Some(1),
            ..Default::default()
        })),
        ..request
    };
    let audio = prepare(
        &request,
        &PrepareOptions::default(),
        &http,
        &Upload,
        &DefaultPrepareHooks,
    )
    .await
    .unwrap()
    .audio_message
    .unwrap();
    assert!(audio.waveform.is_none());
}
// Mean absolute channel difference between two RGB images of equal size.
fn difference(actual: &image::RgbImage, expected: &[u8]) -> f64 {
    assert_eq!(actual.as_raw().len(), expected.len());
    let total: u64 = actual
        .as_raw()
        .iter()
        .zip(expected)
        .map(|(a, b)| u64::from(a.abs_diff(*b)))
        .sum();
    total as f64 / expected.len() as f64
}
#[tokio::test]
async fn image_format_audit_against_original_thumbnails() {
    let http = HttpClient::new().unwrap();
    let cases = fixtures()["formats"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 17);
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let bytes = hex(&case["input"]);
        let thumb = extract_image_thumb(Cursor::new(&bytes), 32);
        // Encodings the native decoder lacks: AVIF, and JPEG-compressed (YCbCr) TIFF.
        if matches!(name, "avif" | "tiff") {
            assert!(thumb.is_err(), "{name} is now supported; compare it");
            continue;
        }
        let thumb = thumb.unwrap_or_else(|error| panic!("{name}: {error}"));
        let original = &case["original"];
        assert_eq!(u64::from(thumb.original_width), original["width"], "{name}");
        assert_eq!(
            u64::from(thumb.original_height),
            original["height"],
            "{name}"
        );
        let expected = &case["thumbnail"];
        assert_eq!(expected["channels"], 3);
        let decoded = image::load_from_memory(&thumb.jpeg).unwrap().to_rgb8();
        assert_eq!(u64::from(decoded.width()), expected["width"], "{name}");
        assert_eq!(u64::from(decoded.height()), expected["height"], "{name}");
        // Resampling and JPEG encoding differ from sharp; the pictures must still agree.
        let thumbnail = difference(&decoded, &hex(&expected["pixels"]));
        assert!(
            thumbnail < 8.0,
            "{name} thumbnail differs by {thumbnail:.2}"
        );
        let profile = generate_profile_picture(
            &Input::bytes(bytes),
            Some((24, 24)),
            &http,
            &PrepareOptions::default(),
        )
        .await
        .unwrap();
        let profile = image::load_from_memory(&profile).unwrap().to_rgb8();
        assert_eq!(profile.dimensions(), (24, 24));
        let profile = difference(&profile, &hex(&case["profile"]["pixels"]));
        assert!(
            profile < 10.0,
            "{name} profile picture differs by {profile:.2}"
        );
    }
}
