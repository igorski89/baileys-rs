// Field-wise cache overlay; optional nested messages replace rather than merge.
use super::{Error, MediaField, Payload};
use crate::proto;
pub(super) fn apply(
    message: &mut proto::Message,
    payload: &Payload,
    clears: &[MediaField],
) -> Result<(), Error> {
    match payload {
        Payload::Image(patch) => {
            let target = message
                .image_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() || clears.contains(&MediaField::Url) {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() || clears.contains(&MediaField::Mimetype) {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.caption.is_some() || clears.contains(&MediaField::Caption) {
                target.caption = patch.caption.clone();
            }
            if patch.file_sha256.is_some() || clears.contains(&MediaField::FileSha256) {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() || clears.contains(&MediaField::FileLength) {
                target.file_length = patch.file_length;
            }
            if patch.height.is_some() || clears.contains(&MediaField::Height) {
                target.height = patch.height;
            }
            if patch.width.is_some() || clears.contains(&MediaField::Width) {
                target.width = patch.width;
            }
            if patch.media_key.is_some() || clears.contains(&MediaField::MediaKey) {
                target.media_key = patch.media_key.clone();
            }
            if patch.file_enc_sha256.is_some() || clears.contains(&MediaField::FileEncSha256) {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if !patch.interactive_annotations.is_empty()
                || clears.contains(&MediaField::InteractiveAnnotations)
            {
                target.interactive_annotations = patch.interactive_annotations.clone();
            }
            if patch.direct_path.is_some() || clears.contains(&MediaField::DirectPath) {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some()
                || clears.contains(&MediaField::MediaKeyTimestamp)
            {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.jpeg_thumbnail.is_some() || clears.contains(&MediaField::JpegThumbnail) {
                target.jpeg_thumbnail = patch.jpeg_thumbnail.clone();
            }
            if patch.context_info.is_some() || clears.contains(&MediaField::ContextInfo) {
                target.context_info = patch.context_info.clone();
            }
            if patch.first_scan_sidecar.is_some() || clears.contains(&MediaField::FirstScanSidecar)
            {
                target.first_scan_sidecar = patch.first_scan_sidecar.clone();
            }
            if patch.first_scan_length.is_some() || clears.contains(&MediaField::FirstScanLength) {
                target.first_scan_length = patch.first_scan_length;
            }
            if patch.experiment_group_id.is_some()
                || clears.contains(&MediaField::ExperimentGroupId)
            {
                target.experiment_group_id = patch.experiment_group_id;
            }
            if patch.scans_sidecar.is_some() || clears.contains(&MediaField::ScansSidecar) {
                target.scans_sidecar = patch.scans_sidecar.clone();
            }
            if !patch.scan_lengths.is_empty() || clears.contains(&MediaField::ScanLengths) {
                target.scan_lengths = patch.scan_lengths.clone();
            }
            if patch.mid_quality_file_sha256.is_some()
                || clears.contains(&MediaField::MidQualityFileSha256)
            {
                target.mid_quality_file_sha256 = patch.mid_quality_file_sha256.clone();
            }
            if patch.mid_quality_file_enc_sha256.is_some()
                || clears.contains(&MediaField::MidQualityFileEncSha256)
            {
                target.mid_quality_file_enc_sha256 = patch.mid_quality_file_enc_sha256.clone();
            }
            if patch.view_once.is_some() || clears.contains(&MediaField::ViewOnce) {
                target.view_once = patch.view_once;
            }
            if patch.thumbnail_direct_path.is_some()
                || clears.contains(&MediaField::ThumbnailDirectPath)
            {
                target.thumbnail_direct_path = patch.thumbnail_direct_path.clone();
            }
            if patch.thumbnail_sha256.is_some() || clears.contains(&MediaField::ThumbnailSha256) {
                target.thumbnail_sha256 = patch.thumbnail_sha256.clone();
            }
            if patch.thumbnail_enc_sha256.is_some()
                || clears.contains(&MediaField::ThumbnailEncSha256)
            {
                target.thumbnail_enc_sha256 = patch.thumbnail_enc_sha256.clone();
            }
            if patch.static_url.is_some() || clears.contains(&MediaField::StaticUrl) {
                target.static_url = patch.static_url.clone();
            }
            if !patch.annotations.is_empty() || clears.contains(&MediaField::Annotations) {
                target.annotations = patch.annotations.clone();
            }
            if patch.image_source_type.is_some() || clears.contains(&MediaField::ImageSourceType) {
                target.image_source_type = patch.image_source_type;
            }
            if patch.accessibility_label.is_some()
                || clears.contains(&MediaField::AccessibilityLabel)
            {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() || clears.contains(&MediaField::MediaKeyDomain) {
                target.media_key_domain = patch.media_key_domain;
            }
            if patch.qr_url.is_some() || clears.contains(&MediaField::QrUrl) {
                target.qr_url = patch.qr_url.clone();
            }
        }
        Payload::Video(patch) => {
            let target = message
                .video_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() || clears.contains(&MediaField::Url) {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() || clears.contains(&MediaField::Mimetype) {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.file_sha256.is_some() || clears.contains(&MediaField::FileSha256) {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() || clears.contains(&MediaField::FileLength) {
                target.file_length = patch.file_length;
            }
            if patch.seconds.is_some() || clears.contains(&MediaField::Seconds) {
                target.seconds = patch.seconds;
            }
            if patch.media_key.is_some() || clears.contains(&MediaField::MediaKey) {
                target.media_key = patch.media_key.clone();
            }
            if patch.caption.is_some() || clears.contains(&MediaField::Caption) {
                target.caption = patch.caption.clone();
            }
            if patch.gif_playback.is_some() || clears.contains(&MediaField::GifPlayback) {
                target.gif_playback = patch.gif_playback;
            }
            if patch.height.is_some() || clears.contains(&MediaField::Height) {
                target.height = patch.height;
            }
            if patch.width.is_some() || clears.contains(&MediaField::Width) {
                target.width = patch.width;
            }
            if patch.file_enc_sha256.is_some() || clears.contains(&MediaField::FileEncSha256) {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if !patch.interactive_annotations.is_empty()
                || clears.contains(&MediaField::InteractiveAnnotations)
            {
                target.interactive_annotations = patch.interactive_annotations.clone();
            }
            if patch.direct_path.is_some() || clears.contains(&MediaField::DirectPath) {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some()
                || clears.contains(&MediaField::MediaKeyTimestamp)
            {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.jpeg_thumbnail.is_some() || clears.contains(&MediaField::JpegThumbnail) {
                target.jpeg_thumbnail = patch.jpeg_thumbnail.clone();
            }
            if patch.context_info.is_some() || clears.contains(&MediaField::ContextInfo) {
                target.context_info = patch.context_info.clone();
            }
            if patch.streaming_sidecar.is_some() || clears.contains(&MediaField::StreamingSidecar) {
                target.streaming_sidecar = patch.streaming_sidecar.clone();
            }
            if patch.gif_attribution.is_some() || clears.contains(&MediaField::GifAttribution) {
                target.gif_attribution = patch.gif_attribution;
            }
            if patch.view_once.is_some() || clears.contains(&MediaField::ViewOnce) {
                target.view_once = patch.view_once;
            }
            if patch.thumbnail_direct_path.is_some()
                || clears.contains(&MediaField::ThumbnailDirectPath)
            {
                target.thumbnail_direct_path = patch.thumbnail_direct_path.clone();
            }
            if patch.thumbnail_sha256.is_some() || clears.contains(&MediaField::ThumbnailSha256) {
                target.thumbnail_sha256 = patch.thumbnail_sha256.clone();
            }
            if patch.thumbnail_enc_sha256.is_some()
                || clears.contains(&MediaField::ThumbnailEncSha256)
            {
                target.thumbnail_enc_sha256 = patch.thumbnail_enc_sha256.clone();
            }
            if patch.static_url.is_some() || clears.contains(&MediaField::StaticUrl) {
                target.static_url = patch.static_url.clone();
            }
            if !patch.annotations.is_empty() || clears.contains(&MediaField::Annotations) {
                target.annotations = patch.annotations.clone();
            }
            if patch.accessibility_label.is_some()
                || clears.contains(&MediaField::AccessibilityLabel)
            {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if !patch.processed_videos.is_empty() || clears.contains(&MediaField::ProcessedVideos) {
                target.processed_videos = patch.processed_videos.clone();
            }
            if patch
                .external_share_full_video_duration_in_seconds
                .is_some()
                || clears.contains(&MediaField::ExternalShareFullVideoDurationInSeconds)
            {
                target.external_share_full_video_duration_in_seconds =
                    patch.external_share_full_video_duration_in_seconds;
            }
            if patch.motion_photo_presentation_offset_ms.is_some()
                || clears.contains(&MediaField::MotionPhotoPresentationOffsetMs)
            {
                target.motion_photo_presentation_offset_ms =
                    patch.motion_photo_presentation_offset_ms;
            }
            if patch.metadata_url.is_some() || clears.contains(&MediaField::MetadataUrl) {
                target.metadata_url = patch.metadata_url.clone();
            }
            if patch.video_source_type.is_some() || clears.contains(&MediaField::VideoSourceType) {
                target.video_source_type = patch.video_source_type;
            }
            if patch.media_key_domain.is_some() || clears.contains(&MediaField::MediaKeyDomain) {
                target.media_key_domain = patch.media_key_domain;
            }
        }
        Payload::Audio(patch) => {
            let target = message
                .audio_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() || clears.contains(&MediaField::Url) {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() || clears.contains(&MediaField::Mimetype) {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.file_sha256.is_some() || clears.contains(&MediaField::FileSha256) {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() || clears.contains(&MediaField::FileLength) {
                target.file_length = patch.file_length;
            }
            if patch.seconds.is_some() || clears.contains(&MediaField::Seconds) {
                target.seconds = patch.seconds;
            }
            if patch.ptt.is_some() || clears.contains(&MediaField::Ptt) {
                target.ptt = patch.ptt;
            }
            if patch.media_key.is_some() || clears.contains(&MediaField::MediaKey) {
                target.media_key = patch.media_key.clone();
            }
            if patch.file_enc_sha256.is_some() || clears.contains(&MediaField::FileEncSha256) {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if patch.direct_path.is_some() || clears.contains(&MediaField::DirectPath) {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some()
                || clears.contains(&MediaField::MediaKeyTimestamp)
            {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.context_info.is_some() || clears.contains(&MediaField::ContextInfo) {
                target.context_info = patch.context_info.clone();
            }
            if patch.streaming_sidecar.is_some() || clears.contains(&MediaField::StreamingSidecar) {
                target.streaming_sidecar = patch.streaming_sidecar.clone();
            }
            if patch.waveform.is_some() || clears.contains(&MediaField::Waveform) {
                target.waveform = patch.waveform.clone();
            }
            if patch.background_argb.is_some() || clears.contains(&MediaField::BackgroundArgb) {
                target.background_argb = patch.background_argb;
            }
            if patch.view_once.is_some() || clears.contains(&MediaField::ViewOnce) {
                target.view_once = patch.view_once;
            }
            if patch.accessibility_label.is_some()
                || clears.contains(&MediaField::AccessibilityLabel)
            {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() || clears.contains(&MediaField::MediaKeyDomain) {
                target.media_key_domain = patch.media_key_domain;
            }
        }
        Payload::Document(patch) => {
            let target = message
                .document_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() || clears.contains(&MediaField::Url) {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() || clears.contains(&MediaField::Mimetype) {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.title.is_some() || clears.contains(&MediaField::Title) {
                target.title = patch.title.clone();
            }
            if patch.file_sha256.is_some() || clears.contains(&MediaField::FileSha256) {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() || clears.contains(&MediaField::FileLength) {
                target.file_length = patch.file_length;
            }
            if patch.page_count.is_some() || clears.contains(&MediaField::PageCount) {
                target.page_count = patch.page_count;
            }
            if patch.media_key.is_some() || clears.contains(&MediaField::MediaKey) {
                target.media_key = patch.media_key.clone();
            }
            if patch.file_name.is_some() || clears.contains(&MediaField::FileName) {
                target.file_name = patch.file_name.clone();
            }
            if patch.file_enc_sha256.is_some() || clears.contains(&MediaField::FileEncSha256) {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if patch.direct_path.is_some() || clears.contains(&MediaField::DirectPath) {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some()
                || clears.contains(&MediaField::MediaKeyTimestamp)
            {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.contact_vcard.is_some() || clears.contains(&MediaField::ContactVcard) {
                target.contact_vcard = patch.contact_vcard;
            }
            if patch.thumbnail_direct_path.is_some()
                || clears.contains(&MediaField::ThumbnailDirectPath)
            {
                target.thumbnail_direct_path = patch.thumbnail_direct_path.clone();
            }
            if patch.thumbnail_sha256.is_some() || clears.contains(&MediaField::ThumbnailSha256) {
                target.thumbnail_sha256 = patch.thumbnail_sha256.clone();
            }
            if patch.thumbnail_enc_sha256.is_some()
                || clears.contains(&MediaField::ThumbnailEncSha256)
            {
                target.thumbnail_enc_sha256 = patch.thumbnail_enc_sha256.clone();
            }
            if patch.jpeg_thumbnail.is_some() || clears.contains(&MediaField::JpegThumbnail) {
                target.jpeg_thumbnail = patch.jpeg_thumbnail.clone();
            }
            if patch.context_info.is_some() || clears.contains(&MediaField::ContextInfo) {
                target.context_info = patch.context_info.clone();
            }
            if patch.thumbnail_height.is_some() || clears.contains(&MediaField::ThumbnailHeight) {
                target.thumbnail_height = patch.thumbnail_height;
            }
            if patch.thumbnail_width.is_some() || clears.contains(&MediaField::ThumbnailWidth) {
                target.thumbnail_width = patch.thumbnail_width;
            }
            if patch.caption.is_some() || clears.contains(&MediaField::Caption) {
                target.caption = patch.caption.clone();
            }
            if patch.accessibility_label.is_some()
                || clears.contains(&MediaField::AccessibilityLabel)
            {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() || clears.contains(&MediaField::MediaKeyDomain) {
                target.media_key_domain = patch.media_key_domain;
            }
        }
        Payload::Sticker(patch) => {
            let target = message
                .sticker_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() || clears.contains(&MediaField::Url) {
                target.url = patch.url.clone();
            }
            if patch.file_sha256.is_some() || clears.contains(&MediaField::FileSha256) {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_enc_sha256.is_some() || clears.contains(&MediaField::FileEncSha256) {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if patch.media_key.is_some() || clears.contains(&MediaField::MediaKey) {
                target.media_key = patch.media_key.clone();
            }
            if patch.mimetype.is_some() || clears.contains(&MediaField::Mimetype) {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.height.is_some() || clears.contains(&MediaField::Height) {
                target.height = patch.height;
            }
            if patch.width.is_some() || clears.contains(&MediaField::Width) {
                target.width = patch.width;
            }
            if patch.direct_path.is_some() || clears.contains(&MediaField::DirectPath) {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.file_length.is_some() || clears.contains(&MediaField::FileLength) {
                target.file_length = patch.file_length;
            }
            if patch.media_key_timestamp.is_some()
                || clears.contains(&MediaField::MediaKeyTimestamp)
            {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.first_frame_length.is_some() || clears.contains(&MediaField::FirstFrameLength)
            {
                target.first_frame_length = patch.first_frame_length;
            }
            if patch.first_frame_sidecar.is_some()
                || clears.contains(&MediaField::FirstFrameSidecar)
            {
                target.first_frame_sidecar = patch.first_frame_sidecar.clone();
            }
            if patch.is_animated.is_some() || clears.contains(&MediaField::IsAnimated) {
                target.is_animated = patch.is_animated;
            }
            if patch.png_thumbnail.is_some() || clears.contains(&MediaField::PngThumbnail) {
                target.png_thumbnail = patch.png_thumbnail.clone();
            }
            if patch.context_info.is_some() || clears.contains(&MediaField::ContextInfo) {
                target.context_info = patch.context_info.clone();
            }
            if patch.sticker_sent_ts.is_some() || clears.contains(&MediaField::StickerSentTs) {
                target.sticker_sent_ts = patch.sticker_sent_ts;
            }
            if patch.is_avatar.is_some() || clears.contains(&MediaField::IsAvatar) {
                target.is_avatar = patch.is_avatar;
            }
            if patch.is_ai_sticker.is_some() || clears.contains(&MediaField::IsAiSticker) {
                target.is_ai_sticker = patch.is_ai_sticker;
            }
            if patch.is_lottie.is_some() || clears.contains(&MediaField::IsLottie) {
                target.is_lottie = patch.is_lottie;
            }
            if patch.accessibility_label.is_some()
                || clears.contains(&MediaField::AccessibilityLabel)
            {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() || clears.contains(&MediaField::MediaKeyDomain) {
                target.media_key_domain = patch.media_key_domain;
            }
        }
    }
    Ok(())
}
