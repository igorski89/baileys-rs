use baileys_rs::{binary, groups};
use serde_json::Value;
#[path = "support/json_numbers.rs"]
mod json_numbers;
fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn metadata_matches_original_parser_including_errors_and_missing_numbers() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/groups.json")).unwrap();
    for case in fixture["metadata"].as_array().unwrap() {
        let node = binary::decode(&hex(case["wire"].as_str().unwrap())).unwrap();
        match groups::extract_metadata(&node) {
            Ok(metadata) => {
                assert!(case["error"].is_null());
                let serialized = serde_json::to_value(&metadata).unwrap();
                assert_eq!(
                    json_numbers::canonical(&serialized),
                    json_numbers::canonical(&case["expected"])
                );
                assert_eq!(
                    serde_json::to_value(
                        serde_json::from_value::<groups::Metadata>(serialized.clone()).unwrap()
                    )
                    .unwrap(),
                    serialized
                );
            }
            Err(error) => {
                assert_eq!(
                    u64::from(error.status_code()),
                    case["error"]["status"].as_u64().unwrap()
                );
                assert_eq!(
                    error.to_string(),
                    case["error"]["message"].as_str().unwrap()
                );
            }
        }
    }
}
