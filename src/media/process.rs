//! Built-in metadata processors. Image/audio decoding runs on blocking workers.
use super::{Error, HttpClient, Input, MediaType, PrepareHooks, PrepareOptions, Thumbnail};
use futures_util::future::BoxFuture;
use image::{GenericImageView, ImageReader, codecs::jpeg::JpegEncoder, imageops::FilterType};
use std::{
    io::{Cursor, Read},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use symphonia::core::{
    audio::SampleBuffer, codecs::DecoderOptions, errors::Error as AudioError,
    formats::FormatOptions, io::MediaSourceStream, meta::MetadataOptions, probe::Hint,
};

#[derive(Debug, Clone)]
pub struct ImageThumbnail {
    pub jpeg: Vec<u8>,
    pub original_width: u32,
    pub original_height: u32,
}

/// Decode an image and resize to the requested width, preserving aspect ratio.
/// Uses quality-50 JPEG; codec/resampler bytes may differ from Sharp/Jimp.
// JPEG has no alpha: like sharp, transparent pixels are flattened onto black.
fn flatten(image: image::DynamicImage) -> image::DynamicImage {
    if !image.color().has_alpha() {
        return image;
    }
    let mut rgba = image.to_rgba8();
    for pixel in rgba.pixels_mut() {
        let alpha = u32::from(pixel[3]);
        for channel in &mut pixel.0[..3] {
            *channel = ((u32::from(*channel) * alpha + 127) / 255) as u8;
        }
        pixel[3] = 255;
    }
    image::DynamicImage::ImageRgb8(image::DynamicImage::ImageRgba8(rgba).to_rgb8())
}
pub fn extract_image_thumb(mut input: impl Read, width: u32) -> Result<ImageThumbnail, Error> {
    if width == 0 {
        return Err(Error::Input("zero image width"));
    }
    let mut bytes = Vec::new();
    input.read_to_end(&mut bytes)?;
    let image = ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()?
        .decode()?;
    let (original_width, original_height) = image.dimensions();
    let image = flatten(image);
    let height =
        ((original_height as f64 * width as f64 / original_width as f64).round() as u32).max(1);
    let resized = image
        .resize_exact(width, height, FilterType::Lanczos3)
        .to_rgb8();
    let mut jpeg = Vec::new();
    JpegEncoder::new_with_quality(&mut jpeg, 50).encode_image(&resized)?;
    Ok(ImageThumbnail {
        jpeg,
        original_width,
        original_height,
    })
}

/// Generate a quality-50 JPEG profile image with Sharp-style centered cover crop.
pub async fn generate_profile_picture(
    input: &Input,
    dimensions: Option<(u32, u32)>,
    http: &HttpClient,
    options: &PrepareOptions,
) -> Result<Vec<u8>, Error> {
    let (width, height) = dimensions.unwrap_or((640, 640));
    if width == 0 || height == 0 {
        return Err(Error::Input("zero profile dimensions"));
    }
    let mut reader = input.open(http, options).await?;
    tokio::task::spawn_blocking(move || {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let image = ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()?
            .decode()?;
        let resized = flatten(image)
            .resize_to_fill(width, height, FilterType::Lanczos3)
            .to_rgb8();
        let mut jpeg = Vec::new();
        JpegEncoder::new_with_quality(&mut jpeg, 50).encode_image(&resized)?;
        Ok(jpeg)
    })
    .await
    .map_err(|_| Error::Task)?
}

/// Source-compatible 64-bin first-channel waveform, including short/silent input.
pub fn waveform_from_samples(samples: &[f32]) -> [u8; 64] {
    let block = samples.len() / 64;
    if block == 0 {
        return [0; 64];
    }
    let mut averages = [0.0f64; 64];
    for (i, value) in averages.iter_mut().enumerate() {
        *value = samples[i * block..(i + 1) * block]
            .iter()
            .map(|x| f64::from(*x).abs())
            .sum::<f64>()
            / block as f64;
    }
    if averages.iter().any(|x| x.is_nan()) {
        return [0; 64];
    }
    let max = averages.iter().copied().fold(0.0f64, f64::max);
    let multiplier = max.powf(-1.0);
    averages.map(|x| (100.0 * (x * multiplier)).floor() as u8)
}

fn probe(bytes: Vec<u8>) -> Result<Box<dyn symphonia::core::formats::FormatReader>, Error> {
    let options = FormatOptions {
        enable_gapless: bytes.starts_with(b"OggS"),
        ..Default::default()
    };
    let stream = MediaSourceStream::new(Box::new(Cursor::new(bytes)), Default::default());
    Ok(symphonia::default::get_probe()
        .format(&Hint::new(), stream, &options, &MetadataOptions::default())?
        .format)
}
/// Parse duration without decoding audio samples. Returns fractional seconds.
pub fn audio_duration(mut input: impl Read) -> Result<Option<f64>, Error> {
    let mut bytes = Vec::new();
    input.read_to_end(&mut bytes)?;
    let mut format = probe(bytes)?;
    let track = format
        .default_track()
        .ok_or(Error::Input("audio track missing"))?;
    let id = track.id;
    let params = track.codec_params.clone();
    let time_base = params.time_base.or_else(|| {
        params
            .sample_rate
            .map(|rate| symphonia::core::units::TimeBase::new(1, rate))
    });
    let Some(time_base) = time_base else {
        return Ok(None);
    };
    let frames = if let Some(frames) = params.n_frames {
        frames
    } else {
        let mut end = 0;
        loop {
            match format.next_packet() {
                Ok(packet) if packet.track_id() == id => {
                    end = end.max(packet.ts().saturating_add(packet.dur()))
                }
                Ok(_) => {}
                Err(AudioError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        end.saturating_sub(params.start_ts)
    };
    let time = time_base.calc_time(frames);
    Ok(Some(time.seconds as f64 + time.frac))
}
/// Decode the first audio channel and calculate the upstream waveform.
pub fn audio_waveform(mut input: impl Read) -> Result<[u8; 64], Error> {
    let mut bytes = Vec::new();
    input.read_to_end(&mut bytes)?;
    let wav = bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WAVE");
    let mut format = probe(bytes)?;
    let track = format
        .default_track()
        .ok_or(Error::Input("audio track missing"))?;
    let id = track.id;
    // node-wav uses different positive and negative full-scale divisors.
    let positive_scale = if wav {
        use symphonia::core::codecs::*;
        match track.codec_params.codec {
            CODEC_TYPE_PCM_U8 => Some(128.0),
            CODEC_TYPE_PCM_S16LE => Some(32768.0),
            CODEC_TYPE_PCM_S24LE => Some(8388608.0),
            CODEC_TYPE_PCM_S32LE => Some(2147483648.0),
            _ => None,
        }
    } else if track.codec_params.codec == symphonia::core::codecs::CODEC_TYPE_FLAC {
        track
            .codec_params
            .bits_per_sample
            .map(|bits| 2.0f64.powi(bits as i32 - 1))
    } else {
        None
    };
    let mut decoder =
        symphonia::default::get_codecs().make(&track.codec_params, &DecoderOptions::default())?;
    let wav_min24 =
        wav && track.codec_params.codec == symphonia::core::codecs::CODEC_TYPE_PCM_S24LE;
    let flac = track.codec_params.codec == symphonia::core::codecs::CODEC_TYPE_FLAC;
    let mut samples = Vec::new();
    let mut last_frame_start = 0;
    loop {
        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(AudioError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };
        if packet.track_id() != id {
            continue;
        }
        let decoded = decoder.decode(&packet)?;
        let channels = decoded.spec().channels.count();
        if channels == 0 {
            return Err(Error::Input("audio channels missing"));
        }
        let mut buffer = SampleBuffer::<f32>::new(decoded.capacity() as u64, *decoded.spec());
        buffer.copy_interleaved_ref(decoded);
        last_frame_start = samples.len();
        samples.extend(
            buffer
                .samples()
                .iter()
                .step_by(channels)
                .map(|x| match positive_scale {
                    Some(scale) if *x > 0.0 => (f64::from(*x) * scale / (scale - 1.0)) as f32,
                    // node-wav treats 0x800000 as positive due to its strict
                    // sign threshold, so the minimum 24-bit sample exceeds 1.
                    _ if wav_min24 && *x == -1.0 => (8388608.0 / 8388607.0) as f32,
                    _ => *x,
                }),
        );
    }
    if flac {
        // audio-decode 2.1.3 uses the incremental FLAC decoder without an EOF
        // flush. Its waveform excludes the final frame; preserve that boundary.
        samples.truncate(last_frame_start);
        if samples.is_empty() {
            return Err(Error::Input("FLAC waveform has no flushed frames"));
        }
    }
    Ok(waveform_from_samples(&samples))
}

/// Built-in hooks with configurable FFmpeg/FFprobe executable paths.
/// FFmpeg handles video thumbnails and audio codecs unsupported by Symphonia.
#[derive(Debug, Clone)]
pub struct MediaProcessors {
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
}
impl Default for MediaProcessors {
    fn default() -> Self {
        Self {
            ffmpeg: "ffmpeg".into(),
            ffprobe: "ffprobe".into(),
        }
    }
}
impl MediaProcessors {
    fn duration_file(&self, path: &Path) -> Result<Option<f64>, Error> {
        match audio_duration(std::fs::File::open(path)?) {
            Ok(duration) => Ok(duration),
            Err(Error::Audio(AudioError::Unsupported(_))) => {
                let out = Command::new(&self.ffprobe)
                    .args([
                        "-v",
                        "error",
                        "-show_entries",
                        "format=duration",
                        "-of",
                        "json",
                    ])
                    .arg(path)
                    .stdin(Stdio::null())
                    .output()?;
                if !out.status.success() {
                    return Err(Error::Input("audio duration processor failed"));
                }
                let value: serde_json::Value = serde_json::from_slice(&out.stdout)
                    .map_err(|_| Error::Input("invalid duration output"))?;
                Ok(value["format"]["duration"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok()))
            }
            Err(e) => Err(e),
        }
    }
    fn waveform_file(&self, path: &Path) -> Result<[u8; 64], Error> {
        match audio_waveform(std::fs::File::open(path)?) {
            Ok(wave) => Ok(wave),
            Err(Error::Audio(AudioError::Unsupported(_))) => {
                // pan selects channel zero; -ac 1 would mix stereo channels.
                let out = Command::new(&self.ffmpeg)
                    .args(["-v", "error", "-i"])
                    .arg(path)
                    .args([
                        "-map",
                        "0:a:0",
                        "-af",
                        "pan=mono|c0=c0",
                        "-f",
                        "f32le",
                        "-acodec",
                        "pcm_f32le",
                        "pipe:1",
                    ])
                    .stdin(Stdio::null())
                    .output()?;
                if !out.status.success() {
                    return Err(Error::Input("audio waveform processor failed"));
                }
                let (frames, trailing) = out.stdout.as_chunks::<4>();
                if !trailing.is_empty() {
                    return Err(Error::Input("invalid audio processor PCM output"));
                }
                let samples: Vec<f32> = frames.iter().map(|b| f32::from_le_bytes(*b)).collect();
                Ok(waveform_from_samples(&samples))
            }
            Err(e) => Err(e),
        }
    }
}
impl PrepareHooks for MediaProcessors {
    fn thumbnail(&self, path: PathBuf, kind: MediaType) -> BoxFuture<'_, Result<Thumbnail, Error>> {
        let ffmpeg = self.ffmpeg.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                if kind == MediaType::Image {
                    let thumb = extract_image_thumb(std::fs::File::open(path)?, 32)?;
                    return Ok(Thumbnail {
                        jpeg: Some(thumb.jpeg),
                        dimensions: Some((thumb.original_width, thumb.original_height)),
                    });
                }
                if kind != MediaType::Video {
                    return Err(Error::Input("thumbnail type unsupported"));
                }
                let result = (|| -> Result<Vec<u8>, Error> {
                    let mut file = tempfile::Builder::new().suffix(".jpg").tempfile()?;
                    let out = Command::new(ffmpeg)
                        .args(["-v", "error", "-ss", "00:00:00", "-i"])
                        .arg(path)
                        .args(["-y", "-vf", "scale=32:-1", "-vframes", "1", "-f", "image2"])
                        .arg(file.path())
                        .stdin(Stdio::null())
                        .output()?;
                    if !out.status.success() {
                        return Err(Error::Input("video thumbnail processor failed"));
                    }
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes)?;
                    Ok(bytes)
                })();
                match result {
                    Ok(jpeg) => Ok(Thumbnail {
                        jpeg: Some(jpeg),
                        dimensions: None,
                    }),
                    Err(error) => {
                        tracing::debug!(?error, "video thumbnail unavailable");
                        Ok(Thumbnail::default())
                    }
                }
            })
            .await
            .map_err(|_| Error::Task)?
        })
    }
    fn duration(&self, path: PathBuf) -> BoxFuture<'_, Result<Option<u32>, Error>> {
        let processor = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                processor
                    .duration_file(&path)
                    .map(|v| v.map(|n| n.trunc().rem_euclid(4294967296.0) as u32))
            })
            .await
            .map_err(|_| Error::Task)?
        })
    }
    fn waveform(&self, path: Option<PathBuf>) -> BoxFuture<'_, Result<Option<Vec<u8>>, Error>> {
        let processor = self.clone();
        Box::pin(async move {
            let Some(path) = path else {
                return Ok(None);
            };
            tokio::task::spawn_blocking(move || match processor.waveform_file(&path) {
                Ok(wave) => Ok(Some(wave.to_vec())),
                Err(error) => {
                    tracing::debug!(?error, "audio waveform unavailable");
                    Ok(None)
                }
            })
            .await
            .map_err(|_| Error::Task)?
        })
    }
}
