//! The pure helpers against the TypeScript connector's own output
//! (tests/fixtures/helpers.json, from scripts/generate-helper-fixtures.mjs).
use baileys_sqs::{commands::normalize_jid, media::extension_from_mimetype, output};
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/helpers.json")).unwrap()
}

#[test]
fn group_ids_dedup_ids_and_canonical_json_match_the_typescript_connector() {
    for case in fixture()["cases"].as_array().unwrap() {
        let body = &case["body"];
        assert_eq!(output::fifo_group_id(body), case["groupId"], "{body}");
        assert_eq!(output::dedup_prefix(body), case["prefix"], "{body}");
        assert_eq!(
            output::canonical_stringify(body),
            case["canonical"],
            "{body}"
        );
        assert_eq!(output::compute_dedup_id(body), case["dedupId"], "{body}");
    }
}

#[test]
fn jids_and_extensions_match_the_typescript_connector() {
    let fixture = fixture();
    for case in fixture["jids"].as_array().unwrap() {
        assert_eq!(normalize_jid(case["to"].as_str().unwrap()), case["jid"]);
    }
    for case in fixture["mimetypes"].as_array().unwrap() {
        assert_eq!(
            extension_from_mimetype(case["mimetype"].as_str()),
            case["extension"],
            "{case}"
        );
    }
}
