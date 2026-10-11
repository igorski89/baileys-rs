use baileys_rs::receipts;
// Shared parsing helpers also contain receipt-only fixtures.
#[allow(dead_code)]
#[path = "support/receipt_helpers.rs"]
mod support;
#[test]
fn incoming_receipt_events_match_original_handler() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/incoming_receipts.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let incoming = receipts::incoming(
            &support::node(&case["node"]),
            fixture["me"].as_str().unwrap(),
            fixture["lid"].as_str(),
        )
        .unwrap();
        let events = case["events"].as_array().unwrap();
        let messages = events
            .iter()
            .find(|e| e["name"] == "messages.update")
            .map(|e| e["data"].as_array().unwrap().clone())
            .unwrap_or_default();
        let users = events
            .iter()
            .find(|e| e["name"] == "message-receipt.update")
            .map(|e| e["data"].as_array().unwrap().clone())
            .unwrap_or_default();
        assert_eq!(incoming.messages.len(), messages.len(), "{case}");
        assert_eq!(incoming.users.len(), users.len(), "{case}");
        for (actual, expected) in incoming.messages.iter().zip(messages) {
            check_key(&actual.key, &expected["key"]);
            assert_eq!(
                actual.status as i64,
                expected["update"]["status"].as_i64().unwrap()
            );
            assert_eq!(
                actual.message_timestamp,
                expected["update"]["messageTimestamp"].as_f64().unwrap()
            );
        }
        for (actual, expected) in incoming.users.iter().zip(users) {
            check_key(&actual.key, &expected["key"]);
            assert_eq!(
                actual.user_jid,
                expected["receipt"]["userJid"].as_str().unwrap()
            );
            for (field, time) in [
                ("receiptTimestamp", actual.receipt_timestamp),
                ("readTimestamp", actual.read_timestamp),
            ] {
                if let Some(time) = time {
                    if expected["receipt"][field].is_null() {
                        assert!(!time.is_finite());
                    } else {
                        assert_eq!(time, expected["receipt"][field].as_f64().unwrap());
                    }
                } else {
                    assert!(expected["receipt"].get(field).is_none());
                }
            }
        }
    }
}
fn check_key(key: &baileys_rs::proto::MessageKey, expected: &serde_json::Value) {
    assert_eq!(key.remote_jid.as_deref(), expected["remoteJid"].as_str());
    assert_eq!(key.participant.as_deref(), expected["participant"].as_str());
    assert_eq!(key.id.as_deref(), expected["id"].as_str());
    assert_eq!(key.from_me, expected["fromMe"].as_bool());
}
