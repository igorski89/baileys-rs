use baileys_rs::{proto, reporting};
use prost::Message as _;
use serde_json::Value;
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn filtered_content_and_authenticated_tokens_match_upstream() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/reporting.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let wire = hex(case["wire"].as_str().unwrap());
        let message =
            proto::Message::decode(hex(case["message"].as_str().unwrap()).as_slice()).unwrap();
        let key: proto::MessageKey = serde_json::from_value(case["key"].clone()).unwrap();
        assert_eq!(reporting::should_include_token(&message), case["include"]);
        assert_eq!(
            reporting::extract_content(&wire),
            case["content"].as_str().map(hex)
        );
        let token = reporting::message_token(&wire, &message, &key).unwrap();
        assert_eq!(
            token
                .as_ref()
                .and_then(|n| n.child_buffer("reporting_token"))
                .map(Vec::from),
            case["token"].as_str().map(hex)
        );
        if let Some(node) = token {
            assert_eq!(node.tag, "reporting");
            assert_eq!(node.child("reporting_token").unwrap().attrs["v"], "2");
        }
    }
    for case in fixture["raw"].as_array().unwrap() {
        assert_eq!(
            reporting::extract_content(&hex(case["wire"].as_str().unwrap())),
            case["content"].as_str().map(hex)
        );
    }
}
#[test]
fn invalid_lengths_resource_limits_and_absent_metadata_are_bounded() {
    assert!(reporting::extract_content(&hex("0a8080808008")).is_none());
    assert!(reporting::extract_content(&vec![0; 16 * 1024 * 1024 + 1]).is_none());
    let message = proto::Message::default();
    assert!(
        reporting::message_token(&[], &message, &proto::MessageKey::default())
            .unwrap()
            .is_none()
    );
    let mut message = message;
    message.message_context_info = Some(Default::default());
    message
        .message_context_info
        .as_mut()
        .unwrap()
        .message_secret = Some(vec![]);
    let key = proto::MessageKey {
        id: Some("ABC".into()),
        ..Default::default()
    };
    assert!(matches!(
        reporting::message_token(&[8, 1], &message, &key),
        Err(reporting::Error::Address)
    ));
}
