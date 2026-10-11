// Field-wise cache overlay; optional nested messages replace rather than merge.
use super::{Error, Payload};
use crate::proto;
pub(super) fn apply(message: &mut proto::Message, payload: &Payload) -> Result<(), Error> {
    match payload {
        Payload::Image(patch) => {
            let target = message
                .image_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.caption.is_some() {
                target.caption = patch.caption.clone();
            }
            if patch.file_sha256.is_some() {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() {
                target.file_length = patch.file_length;
            }
            if patch.height.is_some() {
                target.height = patch.height;
            }
            if patch.width.is_some() {
                target.width = patch.width;
            }
            if patch.media_key.is_some() {
                target.media_key = patch.media_key.clone();
            }
            if patch.file_enc_sha256.is_some() {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if !patch.interactive_annotations.is_empty() {
                target.interactive_annotations = patch.interactive_annotations.clone();
            }
            if patch.direct_path.is_some() {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some() {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.jpeg_thumbnail.is_some() {
                target.jpeg_thumbnail = patch.jpeg_thumbnail.clone();
            }
            if patch.context_info.is_some() {
                target.context_info = patch.context_info.clone();
            }
            if patch.first_scan_sidecar.is_some() {
                target.first_scan_sidecar = patch.first_scan_sidecar.clone();
            }
            if patch.first_scan_length.is_some() {
                target.first_scan_length = patch.first_scan_length;
            }
            if patch.experiment_group_id.is_some() {
                target.experiment_group_id = patch.experiment_group_id;
            }
            if patch.scans_sidecar.is_some() {
                target.scans_sidecar = patch.scans_sidecar.clone();
            }
            if !patch.scan_lengths.is_empty() {
                target.scan_lengths = patch.scan_lengths.clone();
            }
            if patch.mid_quality_file_sha256.is_some() {
                target.mid_quality_file_sha256 = patch.mid_quality_file_sha256.clone();
            }
            if patch.mid_quality_file_enc_sha256.is_some() {
                target.mid_quality_file_enc_sha256 = patch.mid_quality_file_enc_sha256.clone();
            }
            if patch.view_once.is_some() {
                target.view_once = patch.view_once;
            }
            if patch.thumbnail_direct_path.is_some() {
                target.thumbnail_direct_path = patch.thumbnail_direct_path.clone();
            }
            if patch.thumbnail_sha256.is_some() {
                target.thumbnail_sha256 = patch.thumbnail_sha256.clone();
            }
            if patch.thumbnail_enc_sha256.is_some() {
                target.thumbnail_enc_sha256 = patch.thumbnail_enc_sha256.clone();
            }
            if patch.static_url.is_some() {
                target.static_url = patch.static_url.clone();
            }
            if !patch.annotations.is_empty() {
                target.annotations = patch.annotations.clone();
            }
            if patch.image_source_type.is_some() {
                target.image_source_type = patch.image_source_type;
            }
            if patch.accessibility_label.is_some() {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() {
                target.media_key_domain = patch.media_key_domain;
            }
            if patch.qr_url.is_some() {
                target.qr_url = patch.qr_url.clone();
            }
        }
        Payload::Video(patch) => {
            let target = message
                .video_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.file_sha256.is_some() {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() {
                target.file_length = patch.file_length;
            }
            if patch.seconds.is_some() {
                target.seconds = patch.seconds;
            }
            if patch.media_key.is_some() {
                target.media_key = patch.media_key.clone();
            }
            if patch.caption.is_some() {
                target.caption = patch.caption.clone();
            }
            if patch.gif_playback.is_some() {
                target.gif_playback = patch.gif_playback;
            }
            if patch.height.is_some() {
                target.height = patch.height;
            }
            if patch.width.is_some() {
                target.width = patch.width;
            }
            if patch.file_enc_sha256.is_some() {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if !patch.interactive_annotations.is_empty() {
                target.interactive_annotations = patch.interactive_annotations.clone();
            }
            if patch.direct_path.is_some() {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some() {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.jpeg_thumbnail.is_some() {
                target.jpeg_thumbnail = patch.jpeg_thumbnail.clone();
            }
            if patch.context_info.is_some() {
                target.context_info = patch.context_info.clone();
            }
            if patch.streaming_sidecar.is_some() {
                target.streaming_sidecar = patch.streaming_sidecar.clone();
            }
            if patch.gif_attribution.is_some() {
                target.gif_attribution = patch.gif_attribution;
            }
            if patch.view_once.is_some() {
                target.view_once = patch.view_once;
            }
            if patch.thumbnail_direct_path.is_some() {
                target.thumbnail_direct_path = patch.thumbnail_direct_path.clone();
            }
            if patch.thumbnail_sha256.is_some() {
                target.thumbnail_sha256 = patch.thumbnail_sha256.clone();
            }
            if patch.thumbnail_enc_sha256.is_some() {
                target.thumbnail_enc_sha256 = patch.thumbnail_enc_sha256.clone();
            }
            if patch.static_url.is_some() {
                target.static_url = patch.static_url.clone();
            }
            if !patch.annotations.is_empty() {
                target.annotations = patch.annotations.clone();
            }
            if patch.accessibility_label.is_some() {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if !patch.processed_videos.is_empty() {
                target.processed_videos = patch.processed_videos.clone();
            }
            if patch
                .external_share_full_video_duration_in_seconds
                .is_some()
            {
                target.external_share_full_video_duration_in_seconds =
                    patch.external_share_full_video_duration_in_seconds;
            }
            if patch.motion_photo_presentation_offset_ms.is_some() {
                target.motion_photo_presentation_offset_ms =
                    patch.motion_photo_presentation_offset_ms;
            }
            if patch.metadata_url.is_some() {
                target.metadata_url = patch.metadata_url.clone();
            }
            if patch.video_source_type.is_some() {
                target.video_source_type = patch.video_source_type;
            }
            if patch.media_key_domain.is_some() {
                target.media_key_domain = patch.media_key_domain;
            }
        }
        Payload::Audio(patch) => {
            let target = message
                .audio_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.file_sha256.is_some() {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() {
                target.file_length = patch.file_length;
            }
            if patch.seconds.is_some() {
                target.seconds = patch.seconds;
            }
            if patch.ptt.is_some() {
                target.ptt = patch.ptt;
            }
            if patch.media_key.is_some() {
                target.media_key = patch.media_key.clone();
            }
            if patch.file_enc_sha256.is_some() {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if patch.direct_path.is_some() {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some() {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.context_info.is_some() {
                target.context_info = patch.context_info.clone();
            }
            if patch.streaming_sidecar.is_some() {
                target.streaming_sidecar = patch.streaming_sidecar.clone();
            }
            if patch.waveform.is_some() {
                target.waveform = patch.waveform.clone();
            }
            if patch.background_argb.is_some() {
                target.background_argb = patch.background_argb;
            }
            if patch.view_once.is_some() {
                target.view_once = patch.view_once;
            }
            if patch.accessibility_label.is_some() {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() {
                target.media_key_domain = patch.media_key_domain;
            }
        }
        Payload::Document(patch) => {
            let target = message
                .document_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() {
                target.url = patch.url.clone();
            }
            if patch.mimetype.is_some() {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.title.is_some() {
                target.title = patch.title.clone();
            }
            if patch.file_sha256.is_some() {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_length.is_some() {
                target.file_length = patch.file_length;
            }
            if patch.page_count.is_some() {
                target.page_count = patch.page_count;
            }
            if patch.media_key.is_some() {
                target.media_key = patch.media_key.clone();
            }
            if patch.file_name.is_some() {
                target.file_name = patch.file_name.clone();
            }
            if patch.file_enc_sha256.is_some() {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if patch.direct_path.is_some() {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.media_key_timestamp.is_some() {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.contact_vcard.is_some() {
                target.contact_vcard = patch.contact_vcard;
            }
            if patch.thumbnail_direct_path.is_some() {
                target.thumbnail_direct_path = patch.thumbnail_direct_path.clone();
            }
            if patch.thumbnail_sha256.is_some() {
                target.thumbnail_sha256 = patch.thumbnail_sha256.clone();
            }
            if patch.thumbnail_enc_sha256.is_some() {
                target.thumbnail_enc_sha256 = patch.thumbnail_enc_sha256.clone();
            }
            if patch.jpeg_thumbnail.is_some() {
                target.jpeg_thumbnail = patch.jpeg_thumbnail.clone();
            }
            if patch.context_info.is_some() {
                target.context_info = patch.context_info.clone();
            }
            if patch.thumbnail_height.is_some() {
                target.thumbnail_height = patch.thumbnail_height;
            }
            if patch.thumbnail_width.is_some() {
                target.thumbnail_width = patch.thumbnail_width;
            }
            if patch.caption.is_some() {
                target.caption = patch.caption.clone();
            }
            if patch.accessibility_label.is_some() {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() {
                target.media_key_domain = patch.media_key_domain;
            }
        }
        Payload::Sticker(patch) => {
            let target = message
                .sticker_message
                .as_mut()
                .ok_or(Error::Input("cached media type missing"))?;
            if patch.url.is_some() {
                target.url = patch.url.clone();
            }
            if patch.file_sha256.is_some() {
                target.file_sha256 = patch.file_sha256.clone();
            }
            if patch.file_enc_sha256.is_some() {
                target.file_enc_sha256 = patch.file_enc_sha256.clone();
            }
            if patch.media_key.is_some() {
                target.media_key = patch.media_key.clone();
            }
            if patch.mimetype.is_some() {
                target.mimetype = patch.mimetype.clone();
            }
            if patch.height.is_some() {
                target.height = patch.height;
            }
            if patch.width.is_some() {
                target.width = patch.width;
            }
            if patch.direct_path.is_some() {
                target.direct_path = patch.direct_path.clone();
            }
            if patch.file_length.is_some() {
                target.file_length = patch.file_length;
            }
            if patch.media_key_timestamp.is_some() {
                target.media_key_timestamp = patch.media_key_timestamp;
            }
            if patch.first_frame_length.is_some() {
                target.first_frame_length = patch.first_frame_length;
            }
            if patch.first_frame_sidecar.is_some() {
                target.first_frame_sidecar = patch.first_frame_sidecar.clone();
            }
            if patch.is_animated.is_some() {
                target.is_animated = patch.is_animated;
            }
            if patch.png_thumbnail.is_some() {
                target.png_thumbnail = patch.png_thumbnail.clone();
            }
            if patch.context_info.is_some() {
                target.context_info = patch.context_info.clone();
            }
            if patch.sticker_sent_ts.is_some() {
                target.sticker_sent_ts = patch.sticker_sent_ts;
            }
            if patch.is_avatar.is_some() {
                target.is_avatar = patch.is_avatar;
            }
            if patch.is_ai_sticker.is_some() {
                target.is_ai_sticker = patch.is_ai_sticker;
            }
            if patch.is_lottie.is_some() {
                target.is_lottie = patch.is_lottie;
            }
            if patch.accessibility_label.is_some() {
                target.accessibility_label = patch.accessibility_label.clone();
            }
            if patch.media_key_domain.is_some() {
                target.media_key_domain = patch.media_key_domain;
            }
        }
    }
    Ok(())
}
