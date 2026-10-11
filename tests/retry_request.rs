use baileys_rs::{auth::Credentials, binary, retry::request};
// This shared fixture module also includes receipt-only helpers.
#[allow(dead_code)]
#[path = "support/receipt_helpers.rs"]
mod support;
#[test]
fn retry_receipt_keys_and_attributes_match_original_sequences() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/retry_requests.json")).unwrap();
    let credentials: Credentials = serde_json::from_value(fixture["credentials"].clone()).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let node = support::node(&case["node"]);
        for step in case["steps"].as_array().unwrap() {
            let Some(wire) = step["wire"].as_str() else {
                continue;
            };
            let expected = binary::decode(&support::hex(wire)).unwrap();
            let count = expected.child("retry").unwrap().attrs["count"]
                .parse()
                .unwrap();
            let prekey = expected.child("keys").map(|k| {
                let id = k.child("key").unwrap().child_buffer("id").unwrap();
                (
                    u32::from_be_bytes([0, id[0], id[1], id[2]]),
                    &credentials.noise_key,
                )
            });
            assert_eq!(
                binary::encode(
                    &request::receipt(&node, &node.attrs["id"], count, &credentials, prekey)
                        .unwrap()
                )
                .unwrap(),
                support::hex(wire)
            );
        }
    }
}
