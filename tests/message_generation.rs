use baileys_rs::{messages::generation, proto};
use prost::Message as _;
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/message_generation.json")).unwrap()
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
fn message(value: &Value) -> proto::Message {
    proto::Message::decode(hex(value.as_str().unwrap()).as_slice()).unwrap()
}
fn envelope(value: &Value) -> proto::WebMessageInfo {
    proto::WebMessageInfo::decode(hex(value.as_str().unwrap()).as_slice()).unwrap()
}
#[test]
fn content_selection_and_disappearing_settings_match_upstream() {
    let fixture = fixture();
    for case in fixture["classifications"].as_array().unwrap() {
        let message = message(&case["input"]);
        assert_eq!(
            generation::content_type(Some(&message)),
            case["type"].as_str(),
            "{case}"
        );
    }
    assert_eq!(generation::content_type(None), None);
    for case in fixture["disappearing"].as_array().unwrap() {
        let actual =
            generation::disappearing_setting(case["expiration"].as_u64().map(|v| v as u32));
        assert_eq!(
            actual.encode_to_vec(),
            hex(case["output"].as_str().unwrap())
        );
    }
}
#[test]
fn forwarding_matches_upstream_without_mutating_original() {
    for case in fixture()["forwards"].as_array().unwrap() {
        let original = envelope(&case["input"]);
        let before = original.encode_to_vec();
        let result = generation::forward(&original, case["force"].as_bool().unwrap());
        if case.get("error").is_some() {
            assert!(result.is_err(), "{case}");
        } else {
            assert_eq!(
                result.unwrap().encode_to_vec(),
                hex(case["output"].as_str().unwrap()),
                "{case}"
            );
        }
        assert_eq!(original.encode_to_vec(), before);
    }
}
#[test]
fn outgoing_envelopes_quote_context_and_expiration_match_upstream() {
    let fixture = fixture();
    for case in fixture["generated"].as_array().unwrap() {
        let mode = case["mode"].as_str().unwrap();
        let options = generation::Options {
            user_jid: "100:7@s.whatsapp.net".into(),
            message_id: Some("GENERATED".into()),
            timestamp_seconds: Some(1_700_000_000),
            quoted: matches!(mode, "quote" | "both")
                .then(|| envelope(case.get("quote").unwrap_or(&fixture["quote"]))),
            ephemeral_expiration: matches!(mode, "ephemeral" | "both").then_some(3600),
        };
        let actual = generation::from_content(
            case["jid"].as_str().unwrap(),
            message(&case["input"]),
            &options,
        );
        if case.get("error").is_some() {
            assert!(actual.is_err(), "{case}");
        } else {
            assert_eq!(
                actual.unwrap().encode_to_vec(),
                hex(case["output"].as_str().unwrap()),
                "{case}"
            );
        }
    }
}
#[test]
fn template_and_button_extraction_match_upstream() {
    for case in fixture()["extracted"].as_array().unwrap() {
        let input = (!case["input"].is_null()).then(|| message(&case["input"]));
        let actual = generation::extract(input.as_ref());
        if case["output"].is_null() {
            assert!(actual.is_none(), "{case}");
        } else {
            assert_eq!(
                actual.unwrap().encode_to_vec(),
                hex(case["output"].as_str().unwrap()),
                "{case}"
            );
        }
    }
}
#[test]
fn defaults_generate_unique_ids_and_reject_invalid_quote_content() {
    let options = generation::Options::default();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let first = generation::from_content("200@s.whatsapp.net", proto::Message::default(), &options)
        .unwrap();
    let second =
        generation::from_content("200@s.whatsapp.net", proto::Message::default(), &options)
            .unwrap();
    assert_ne!(
        first.key.as_ref().unwrap().id,
        second.key.as_ref().unwrap().id
    );
    assert!(
        first
            .key
            .as_ref()
            .unwrap()
            .id
            .as_ref()
            .unwrap()
            .starts_with("3EB0")
    );
    assert!(first.message_timestamp.unwrap() >= before);
    let bad_quote = generation::Options {
        quoted: Some(proto::WebMessageInfo::default()),
        ..options
    };
    let result = generation::from_content(
        "200@s.whatsapp.net",
        proto::Message {
            extended_text_message: Some(Box::default()),
            ..Default::default()
        },
        &bad_quote,
    );
    assert_eq!(result.unwrap_err().status_code(), Some(400));
}
