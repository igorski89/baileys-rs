use baileys_rs::proto::{Message, MessageKey, WebMessageInfo};
use prost::Message as _;

#[test]
fn encodes_conversation_and_key_with_upstream_field_numbers() {
    let message = Message {
        conversation: Some("hello".into()),
        ..Default::default()
    };
    assert_eq!(message.encode_to_vec(), b"\x0a\x05hello");
    let key = MessageKey {
        remote_jid: Some("123@lid".into()),
        from_me: Some(true),
        id: Some("ABC".into()),
        ..Default::default()
    };
    assert_eq!(key.encode_to_vec(), b"\x0a\x07123@lid\x10\x01\x1a\x03ABC");
    let info = WebMessageInfo {
        key: Some(key),
        message: Some(message),
        message_timestamp: Some(u64::MAX),
        ..Default::default()
    };
    assert_eq!(
        WebMessageInfo::decode(info.encode_to_vec().as_slice()).unwrap(),
        info
    );
}
