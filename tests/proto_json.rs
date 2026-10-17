use baileys_rs::{
    proto,
    proto_json::{self, JsonField},
};
use prost::Message as _;
use serde_json::{Value, json};
#[path = "support/json_numbers.rs"]
mod numbers;
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn every_message_and_schema_field_matches_original_protobufjs_json() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/proto_json.json")).unwrap();
    assert_eq!(json!(proto_json::MESSAGE_NAMES), fixture["names"]);
    for (i, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        let name = case["name"].as_str().unwrap();
        if case["error"].is_string() {
            assert!(
                proto_json::encode_named(name, &case["input"]).is_err(),
                "case{i} {name}"
            );
            continue;
        }
        let original = unhex(case["wire"].as_str().unwrap());
        let decoded = proto_json::decode_named(name, &original).unwrap();
        assert_eq!(
            numbers::canonical(&decoded),
            numbers::canonical(&case["expected"]),
            "decode case{i} {name}:{}",
            case["label"]
        );
        let native = proto_json::encode_named(name, &case["input"])
            .unwrap_or_else(|e| panic!("encode case{i} {name}:{} {e}", case["label"]));
        let from_json = proto_json::decode_named(name, &native).unwrap();
        assert_eq!(
            numbers::canonical(&from_json),
            numbers::canonical(&case["expected"]),
            "encode case{i} {name}:{}",
            case["label"]
        );
        assert_eq!(
            proto_json::encode_named(name, &decoded).unwrap(),
            native,
            "canonical wire case{i} {name}:{}",
            case["label"]
        );
        // protobufjs' toJSON: the same object with bytes as base64 strings.
        let expected = case.get("base64").unwrap_or(&case["expected"]);
        let base64 =
            proto_json::with_base64_bytes(|| proto_json::decode_named(name, &original)).unwrap();
        assert_eq!(
            numbers::canonical(&base64),
            numbers::canonical(expected),
            "base64 case{i} {name}:{}",
            case["label"]
        );
        assert_eq!(
            proto_json::encode_named(name, &base64).unwrap(),
            native,
            "base64 wire case{i} {name}:{}",
            case["label"]
        );
    }
    assert_eq!(
        proto_json::bytes_to_value(&[1]),
        json!({"type":"Buffer","data":[1]})
    );
}
#[test]
fn typed_messages_roundtrip_bytes_large_integers_and_flattened_oneofs() {
    assert_eq!(String::from_value(&json!(4.0)).unwrap(), "4");
    assert_eq!(String::from_value(&json!(1e21)).unwrap(), "1e+21");
    assert_eq!(String::from_value(&json!(-0.0)).unwrap(), "0");
    let value = json!({"imageMessage":{"caption":"caption","mediaKey":{"type":"Buffer","data":[1,2,3]},"fileLength":"18446744073709551615"}});
    let message = proto::Message::from_value(&value).unwrap();
    assert_eq!(message.to_value(), value);
    assert_eq!(
        proto::Message::decode(message.encode_to_vec().as_slice()).unwrap(),
        message
    );
    let button = proto::message::ButtonsMessage::from_value(
        &json!({"imageMessage":{"caption":"photo"},"contentText":"body"}),
    )
    .unwrap();
    assert!(matches!(
        button.header,
        Some(proto::message::buttons_message::Header::ImageMessage(_))
    ));
    assert!(button.to_value().get("header").is_none());
    assert!(button.to_value().get("imageMessage").is_some());
    assert!(matches!(
        proto_json::decode_named("missing", &[]),
        Err(proto_json::Error::UnknownMessage(_))
    ));
    assert!(matches!(
        proto_json::decode_named("proto.Message", &[255]),
        Err(proto_json::Error::Decode(_))
    ));
    let error = proto::Message::from_value(&json!({"imageMessage":{"mediaKey":"invalid@base64"}}))
        .unwrap_err()
        .to_string();
    assert!(error.contains(".proto.Message.imageMessage"));
    assert!(error.contains(".proto.Message.ImageMessage.mediaKey"));
    assert!(proto::Message::from_value(&json!({"buttonsMessage":{"buttons":false}})).is_err());
    assert!(proto::Message::from_value(&Value::Null).is_err());
    let original = proto::WebMessageInfo {
        key: Some(proto::MessageKey {
            id: Some("BUFFERED".into()),
            remote_jid: Some("200@s.whatsapp.net".into()),
            from_me: Some(false),
            ..Default::default()
        }),
        message: Some(message),
        message_timestamp: Some(u64::MAX),
        ..Default::default()
    };
    let mut buffer = baileys_rs::event_buffer::Buffer::new();
    buffer.buffer(std::time::Duration::ZERO);
    buffer
        .emit(
            "messages.upsert",
            json!({"type":"notify","messages":[original.to_value()]}),
        )
        .unwrap();
    buffer
        .emit(
            "messages.update",
            json!([{"key":original.key.as_ref().unwrap().to_value(),"update":{"status":4}}]),
        )
        .unwrap();
    let (_, batches) = buffer.flush();
    let restored =
        proto::WebMessageInfo::from_value(&batches[0].events["messages.upsert"]["messages"][0])
            .unwrap();
    assert_eq!(restored.message, original.message);
    assert_eq!(restored.message_timestamp, Some(u64::MAX));
    assert_eq!(restored.key, original.key);
    assert_eq!(restored.status, Some(4));
}
