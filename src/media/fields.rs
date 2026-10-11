//! Explicit null/empty-list overrides for media preparation.
use super::{Error, Payload};

/// A payload field explicitly supplied as null (or an empty repeated list).
/// Fields must belong to the selected payload type. Clears take precedence over
/// template values; upstream defaults and metadata processing can then replace them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MediaField {
    #[serde(rename = "accessibilityLabel")]
    AccessibilityLabel,
    #[serde(rename = "annotations")]
    Annotations,
    #[serde(rename = "backgroundArgb")]
    BackgroundArgb,
    #[serde(rename = "caption")]
    Caption,
    #[serde(rename = "contactVcard")]
    ContactVcard,
    #[serde(rename = "contextInfo")]
    ContextInfo,
    #[serde(rename = "directPath")]
    DirectPath,
    #[serde(rename = "experimentGroupId")]
    ExperimentGroupId,
    #[serde(rename = "externalShareFullVideoDurationInSeconds")]
    ExternalShareFullVideoDurationInSeconds,
    #[serde(rename = "fileEncSha256")]
    FileEncSha256,
    #[serde(rename = "fileLength")]
    FileLength,
    #[serde(rename = "fileName")]
    FileName,
    #[serde(rename = "fileSha256")]
    FileSha256,
    #[serde(rename = "firstFrameLength")]
    FirstFrameLength,
    #[serde(rename = "firstFrameSidecar")]
    FirstFrameSidecar,
    #[serde(rename = "firstScanLength")]
    FirstScanLength,
    #[serde(rename = "firstScanSidecar")]
    FirstScanSidecar,
    #[serde(rename = "gifAttribution")]
    GifAttribution,
    #[serde(rename = "gifPlayback")]
    GifPlayback,
    #[serde(rename = "height")]
    Height,
    #[serde(rename = "imageSourceType")]
    ImageSourceType,
    #[serde(rename = "interactiveAnnotations")]
    InteractiveAnnotations,
    #[serde(rename = "isAiSticker")]
    IsAiSticker,
    #[serde(rename = "isAnimated")]
    IsAnimated,
    #[serde(rename = "isAvatar")]
    IsAvatar,
    #[serde(rename = "isLottie")]
    IsLottie,
    #[serde(rename = "jpegThumbnail")]
    JpegThumbnail,
    #[serde(rename = "mediaKey")]
    MediaKey,
    #[serde(rename = "mediaKeyDomain")]
    MediaKeyDomain,
    #[serde(rename = "mediaKeyTimestamp")]
    MediaKeyTimestamp,
    #[serde(rename = "metadataUrl")]
    MetadataUrl,
    #[serde(rename = "midQualityFileEncSha256")]
    MidQualityFileEncSha256,
    #[serde(rename = "midQualityFileSha256")]
    MidQualityFileSha256,
    #[serde(rename = "mimetype")]
    Mimetype,
    #[serde(rename = "motionPhotoPresentationOffsetMs")]
    MotionPhotoPresentationOffsetMs,
    #[serde(rename = "pageCount")]
    PageCount,
    #[serde(rename = "pngThumbnail")]
    PngThumbnail,
    #[serde(rename = "processedVideos")]
    ProcessedVideos,
    #[serde(rename = "ptt")]
    Ptt,
    #[serde(rename = "qrUrl")]
    QrUrl,
    #[serde(rename = "scanLengths")]
    ScanLengths,
    #[serde(rename = "scansSidecar")]
    ScansSidecar,
    #[serde(rename = "seconds")]
    Seconds,
    #[serde(rename = "staticUrl")]
    StaticUrl,
    #[serde(rename = "stickerSentTs")]
    StickerSentTs,
    #[serde(rename = "streamingSidecar")]
    StreamingSidecar,
    #[serde(rename = "thumbnailDirectPath")]
    ThumbnailDirectPath,
    #[serde(rename = "thumbnailEncSha256")]
    ThumbnailEncSha256,
    #[serde(rename = "thumbnailHeight")]
    ThumbnailHeight,
    #[serde(rename = "thumbnailSha256")]
    ThumbnailSha256,
    #[serde(rename = "thumbnailWidth")]
    ThumbnailWidth,
    #[serde(rename = "title")]
    Title,
    #[serde(rename = "url")]
    Url,
    #[serde(rename = "videoSourceType")]
    VideoSourceType,
    #[serde(rename = "viewOnce")]
    ViewOnce,
    #[serde(rename = "waveform")]
    Waveform,
    #[serde(rename = "width")]
    Width,
}

pub(super) fn clear(payload: &mut Payload, fields: &[MediaField]) -> Result<(), Error> {
    for field in fields {
        match payload {
            Payload::Image(p) => match field {
                MediaField::Url => p.url = Default::default(),
                MediaField::Mimetype => p.mimetype = Default::default(),
                MediaField::Caption => p.caption = Default::default(),
                MediaField::FileSha256 => p.file_sha256 = Default::default(),
                MediaField::FileLength => p.file_length = Default::default(),
                MediaField::Height => p.height = Default::default(),
                MediaField::Width => p.width = Default::default(),
                MediaField::MediaKey => p.media_key = Default::default(),
                MediaField::FileEncSha256 => p.file_enc_sha256 = Default::default(),
                MediaField::InteractiveAnnotations => {
                    p.interactive_annotations = Default::default()
                }
                MediaField::DirectPath => p.direct_path = Default::default(),
                MediaField::MediaKeyTimestamp => p.media_key_timestamp = Default::default(),
                MediaField::JpegThumbnail => p.jpeg_thumbnail = Default::default(),
                MediaField::ContextInfo => p.context_info = Default::default(),
                MediaField::FirstScanSidecar => p.first_scan_sidecar = Default::default(),
                MediaField::FirstScanLength => p.first_scan_length = Default::default(),
                MediaField::ExperimentGroupId => p.experiment_group_id = Default::default(),
                MediaField::ScansSidecar => p.scans_sidecar = Default::default(),
                MediaField::ScanLengths => p.scan_lengths = Default::default(),
                MediaField::MidQualityFileSha256 => p.mid_quality_file_sha256 = Default::default(),
                MediaField::MidQualityFileEncSha256 => {
                    p.mid_quality_file_enc_sha256 = Default::default()
                }
                MediaField::ViewOnce => p.view_once = Default::default(),
                MediaField::ThumbnailDirectPath => p.thumbnail_direct_path = Default::default(),
                MediaField::ThumbnailSha256 => p.thumbnail_sha256 = Default::default(),
                MediaField::ThumbnailEncSha256 => p.thumbnail_enc_sha256 = Default::default(),
                MediaField::StaticUrl => p.static_url = Default::default(),
                MediaField::Annotations => p.annotations = Default::default(),
                MediaField::ImageSourceType => p.image_source_type = Default::default(),
                MediaField::AccessibilityLabel => p.accessibility_label = Default::default(),
                MediaField::MediaKeyDomain => p.media_key_domain = Default::default(),
                MediaField::QrUrl => p.qr_url = Default::default(),
                _ => return Err(Error::Input("clear field does not belong to media type")),
            },
            Payload::Video(p) => match field {
                MediaField::ExternalShareFullVideoDurationInSeconds => {
                    p.external_share_full_video_duration_in_seconds = None
                }
                MediaField::Url => p.url = Default::default(),
                MediaField::Mimetype => p.mimetype = Default::default(),
                MediaField::FileSha256 => p.file_sha256 = Default::default(),
                MediaField::FileLength => p.file_length = Default::default(),
                MediaField::Seconds => p.seconds = Default::default(),
                MediaField::MediaKey => p.media_key = Default::default(),
                MediaField::Caption => p.caption = Default::default(),
                MediaField::GifPlayback => p.gif_playback = Default::default(),
                MediaField::Height => p.height = Default::default(),
                MediaField::Width => p.width = Default::default(),
                MediaField::FileEncSha256 => p.file_enc_sha256 = Default::default(),
                MediaField::InteractiveAnnotations => {
                    p.interactive_annotations = Default::default()
                }
                MediaField::DirectPath => p.direct_path = Default::default(),
                MediaField::MediaKeyTimestamp => p.media_key_timestamp = Default::default(),
                MediaField::JpegThumbnail => p.jpeg_thumbnail = Default::default(),
                MediaField::ContextInfo => p.context_info = Default::default(),
                MediaField::StreamingSidecar => p.streaming_sidecar = Default::default(),
                MediaField::GifAttribution => p.gif_attribution = Default::default(),
                MediaField::ViewOnce => p.view_once = Default::default(),
                MediaField::ThumbnailDirectPath => p.thumbnail_direct_path = Default::default(),
                MediaField::ThumbnailSha256 => p.thumbnail_sha256 = Default::default(),
                MediaField::ThumbnailEncSha256 => p.thumbnail_enc_sha256 = Default::default(),
                MediaField::StaticUrl => p.static_url = Default::default(),
                MediaField::Annotations => p.annotations = Default::default(),
                MediaField::AccessibilityLabel => p.accessibility_label = Default::default(),
                MediaField::ProcessedVideos => p.processed_videos = Default::default(),
                MediaField::MotionPhotoPresentationOffsetMs => {
                    p.motion_photo_presentation_offset_ms = Default::default()
                }
                MediaField::MetadataUrl => p.metadata_url = Default::default(),
                MediaField::VideoSourceType => p.video_source_type = Default::default(),
                MediaField::MediaKeyDomain => p.media_key_domain = Default::default(),
                _ => return Err(Error::Input("clear field does not belong to media type")),
            },
            Payload::Audio(p) => match field {
                MediaField::Url => p.url = Default::default(),
                MediaField::Mimetype => p.mimetype = Default::default(),
                MediaField::FileSha256 => p.file_sha256 = Default::default(),
                MediaField::FileLength => p.file_length = Default::default(),
                MediaField::Seconds => p.seconds = Default::default(),
                MediaField::Ptt => p.ptt = Default::default(),
                MediaField::MediaKey => p.media_key = Default::default(),
                MediaField::FileEncSha256 => p.file_enc_sha256 = Default::default(),
                MediaField::DirectPath => p.direct_path = Default::default(),
                MediaField::MediaKeyTimestamp => p.media_key_timestamp = Default::default(),
                MediaField::ContextInfo => p.context_info = Default::default(),
                MediaField::StreamingSidecar => p.streaming_sidecar = Default::default(),
                MediaField::Waveform => p.waveform = Default::default(),
                MediaField::BackgroundArgb => p.background_argb = Default::default(),
                MediaField::ViewOnce => p.view_once = Default::default(),
                MediaField::AccessibilityLabel => p.accessibility_label = Default::default(),
                MediaField::MediaKeyDomain => p.media_key_domain = Default::default(),
                _ => return Err(Error::Input("clear field does not belong to media type")),
            },
            Payload::Document(p) => match field {
                MediaField::Url => p.url = Default::default(),
                MediaField::Mimetype => p.mimetype = Default::default(),
                MediaField::Title => p.title = Default::default(),
                MediaField::FileSha256 => p.file_sha256 = Default::default(),
                MediaField::FileLength => p.file_length = Default::default(),
                MediaField::PageCount => p.page_count = Default::default(),
                MediaField::MediaKey => p.media_key = Default::default(),
                MediaField::FileName => p.file_name = Default::default(),
                MediaField::FileEncSha256 => p.file_enc_sha256 = Default::default(),
                MediaField::DirectPath => p.direct_path = Default::default(),
                MediaField::MediaKeyTimestamp => p.media_key_timestamp = Default::default(),
                MediaField::ContactVcard => p.contact_vcard = Default::default(),
                MediaField::ThumbnailDirectPath => p.thumbnail_direct_path = Default::default(),
                MediaField::ThumbnailSha256 => p.thumbnail_sha256 = Default::default(),
                MediaField::ThumbnailEncSha256 => p.thumbnail_enc_sha256 = Default::default(),
                MediaField::JpegThumbnail => p.jpeg_thumbnail = Default::default(),
                MediaField::ContextInfo => p.context_info = Default::default(),
                MediaField::ThumbnailHeight => p.thumbnail_height = Default::default(),
                MediaField::ThumbnailWidth => p.thumbnail_width = Default::default(),
                MediaField::Caption => p.caption = Default::default(),
                MediaField::AccessibilityLabel => p.accessibility_label = Default::default(),
                MediaField::MediaKeyDomain => p.media_key_domain = Default::default(),
                _ => return Err(Error::Input("clear field does not belong to media type")),
            },
            Payload::Sticker(p) => match field {
                MediaField::Url => p.url = Default::default(),
                MediaField::FileSha256 => p.file_sha256 = Default::default(),
                MediaField::FileEncSha256 => p.file_enc_sha256 = Default::default(),
                MediaField::MediaKey => p.media_key = Default::default(),
                MediaField::Mimetype => p.mimetype = Default::default(),
                MediaField::Height => p.height = Default::default(),
                MediaField::Width => p.width = Default::default(),
                MediaField::DirectPath => p.direct_path = Default::default(),
                MediaField::FileLength => p.file_length = Default::default(),
                MediaField::MediaKeyTimestamp => p.media_key_timestamp = Default::default(),
                MediaField::FirstFrameLength => p.first_frame_length = Default::default(),
                MediaField::FirstFrameSidecar => p.first_frame_sidecar = Default::default(),
                MediaField::IsAnimated => p.is_animated = Default::default(),
                MediaField::PngThumbnail => p.png_thumbnail = Default::default(),
                MediaField::ContextInfo => p.context_info = Default::default(),
                MediaField::StickerSentTs => p.sticker_sent_ts = Default::default(),
                MediaField::IsAvatar => p.is_avatar = Default::default(),
                MediaField::IsAiSticker => p.is_ai_sticker = Default::default(),
                MediaField::IsLottie => p.is_lottie = Default::default(),
                MediaField::AccessibilityLabel => p.accessibility_label = Default::default(),
                MediaField::MediaKeyDomain => p.media_key_domain = Default::default(),
                _ => return Err(Error::Input("clear field does not belong to media type")),
            },
        }
    }
    Ok(())
}
