use baileys_rs::{
    binary::{BinaryNode, NodeContent},
    media::*,
    proto,
};
use prost::Message as _;
use serde_json::Value;
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
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    for (k, v) in v["attrs"].as_object().unwrap() {
        n = n.attr(k, v.as_str().unwrap());
    }
    if let Some(c) = v.get("content") {
        n = n.with_content(if let Some(a) = c.as_array() {
            NodeContent::Children(a.iter().map(node).collect())
        } else {
            NodeContent::Bytes(hex(&c["hex"]))
        });
    }
    n
}
#[test]
fn retry_receipts_and_original_nested_payload_updates_match_upstream() {
    let f: Value = serde_json::from_str(include_str!("fixtures/media_message_retry.json")).unwrap();
    for case in f.as_array().unwrap() {
        let mut message = proto::WebMessageInfo::decode(hex(&case["input"]).as_slice()).unwrap();
        let key = message.key.clone().unwrap();
        let result = message
            .message
            .as_mut()
            .ok_or(Error::NotMedia)
            .and_then(retry_media_key);
        if let Some(expected) = case["sent"].as_array().unwrap().first() {
            let request = retry_request_with_iv(
                &key,
                &result.as_ref().unwrap().clone(),
                "100:7@s.whatsapp.net",
                &std::array::from_fn(|i| i as u8),
            )
            .unwrap();
            assert_eq!(request, node(expected), "{case}");
        }
        let event = RetryEvent {
            key: proto::MessageKey {
                id: Some("ID".into()),
                remote_jid: Some("different@lid".into()),
                from_me: Some(true),
                participant: None,
            },
            media: case["event"].get("media").map(|d| RetryCiphertext {
                ciphertext: hex(&d["ciphertext"]["hex"]),
                iv: hex(&d["iv"]["hex"]),
            }),
            error: case["event"].get("error").map(|_| RetryFailure {
                code: None,
                status: 503,
                attrs: Default::default(),
            }),
        };
        let result = apply_retry(&mut message, &event, "custom.example:8443");
        // Not upstream: a video note can be reuploaded here; upstream rejects it.
        if message
            .message
            .as_ref()
            .is_some_and(|m| m.ptv_message.is_some())
        {
            assert_eq!(case["error"], "given message is not a media message");
            assert!(!matches!(result, Err(Error::NotMedia)), "{case}");
            continue;
        }
        if case.get("error").is_some() {
            assert!(result.is_err(), "{case}");
            if let Some(status) = case["status"].as_u64() {
                assert_eq!(
                    result.unwrap_err().status_code(),
                    Some(status as u16),
                    "{case}"
                );
            }
        } else {
            result.unwrap();
        }
        assert_eq!(message.encode_to_vec(), hex(&case["output"]), "{case}");
    }
}
#[test]
fn download_payload_selection_and_thumbnail_descriptors_match_upstream() {
    let f: Value =
        serde_json::from_str(include_str!("fixtures/media_message_download.json")).unwrap();
    for case in f.as_array().unwrap() {
        let message = proto::WebMessageInfo::decode(hex(&case["input"]).as_slice()).unwrap();
        let result = download_content(message.message.as_ref());
        if case.get("error").is_some() {
            assert!(result.is_err(), "{case}");
            assert_eq!(
                result.unwrap_err().status_code().map(u64::from),
                case["status"].as_u64()
            );
        } else {
            let d = result.unwrap();
            let expected = &case["downloads"][0];
            assert_eq!(d.kind.as_str(), expected["type"].as_str().unwrap());
            assert_eq!(d.url.as_deref(), expected["download"]["url"].as_str());
            assert_eq!(
                d.direct_path.as_deref(),
                expected["download"]["directPath"].as_str()
            );
            assert_eq!(d.key, hex(&expected["download"]["mediaKey"]["hex"]));
        }
    }
}
/// Not upstream: a video note's media key is found for a reupload request.
#[test]
fn a_video_note_can_be_reuploaded() {
    let mut message = proto::Message {
        ptv_message: Some(Box::new(proto::message::VideoMessage {
            media_key: Some(vec![7; 32]),
            ..Default::default()
        })),
        ..Default::default()
    };
    assert_eq!(retry_media_key(&mut message).unwrap(), vec![7; 32]);
}
