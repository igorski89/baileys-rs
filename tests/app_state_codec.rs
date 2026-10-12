use baileys_rs::{
    app_state::*,
    auth::{Credentials, KeyMutation, KeyStore, KeyValue, LtHashState, SqliteAuthStore, ValueMac},
    proto,
};
use prost::Message as _;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/app_state_codec.json")).unwrap()
}
fn hex(v: &Value) -> Vec<u8> {
    v.as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
fn wire(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
fn state(v: &Value) -> LtHashState {
    let mut s = new_lthash_state();
    s.version = v["version"].as_u64().unwrap();
    s.hash = hex(&v["hash"]);
    for (k, v) in v["map"].as_object().unwrap() {
        s.index_value_map
            .insert(k.clone(), ValueMac { value_mac: hex(v) });
    }
    s
}
fn output(s: &LtHashState) -> Value {
    json!({"version":s.version,"hash":wire(&s.hash),"map":s.index_value_map.iter().map(|(k,v)|(k.clone(),wire(&v.value_mac))).collect::<std::collections::BTreeMap<_,_>>()})
}
fn provider(mode: &str, secret: Vec<u8>) -> (KeyProvider, Arc<Mutex<Vec<String>>>) {
    let calls = Arc::new(Mutex::new(vec![]));
    let record = calls.clone();
    let mode = mode.to_owned();
    let p = KeyProvider::new(move |id| {
        record.lock().unwrap().push(id.clone());
        let secret = secret.clone();
        let mode = mode.clone();
        async move {
            if mode == "missing" || id == "CQ==" {
                return Ok(None);
            }
            if mode == "error" {
                return Err(Error::Lookup("temporary failure".into()));
            }
            Ok(Some(proto::message::AppStateSyncKeyData {
                key_data: if mode == "malformed" {
                    None
                } else {
                    Some(secret)
                },
                ..Default::default()
            }))
        }
    });
    (p, calls)
}
#[tokio::test]
async fn record_encoding_matches_original_ciphertext_patch_and_state() {
    let f = fixture();
    for c in f["encodings"].as_array().unwrap() {
        let (p, calls) = provider("good", hex(&f["secret"]));
        let initial = state(&c["input"]);
        let req = PatchCreate {
            name: "regular".into(),
            index: serde_json::from_value(c["index"].clone()).unwrap(),
            sync_action: proto::SyncActionValue::decode(hex(&c["action"]).as_slice()).unwrap(),
            api_version: 3,
            operation: c["operation"].as_i64().unwrap() as i32,
        };
        let result = encode_patch_with_iv(
            &req,
            f["id"].as_str().unwrap(),
            &initial,
            &p,
            &std::array::from_fn(|i| i as u8),
        )
        .await;
        if c.get("error").is_some() {
            assert!(result.is_err(), "{c}");
        } else {
            let result = result.unwrap();
            assert_eq!(result.patch.encode_to_vec(), hex(&c["patch"]), "{c}");
            assert_eq!(output(&result.state), c["state"], "{c}");
        }
        assert_eq!(output(&initial), c["original"]);
        assert_eq!(json!(*calls.lock().unwrap()), c["calls"]);
    }
}
#[tokio::test]
async fn mutation_stream_skip_abort_missing_key_and_callback_boundaries_match_original() {
    let f = fixture();
    for c in f["decodings"].as_array().unwrap() {
        let (p, calls) = provider(c["mode"].as_str().unwrap(), hex(&f["secret"]));
        let records = c["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| proto::SyncdRecord::decode(hex(v).as_slice()).unwrap())
            .collect::<Vec<_>>();
        let mut mutations = vec![];
        let result = decode_mutations(
            &records.iter().map(Record::Snapshot).collect::<Vec<_>>(),
            &new_lthash_state(),
            &p,
            &mut |m| {
                mutations
                    .push(json!({"action":wire(&m.sync_action.encode_to_vec()),"index":m.index}));
                if c["variant"] == "callbackError" && mutations.len() == 2 {
                    return Err(Error::Lookup("callback failed".into()));
                }
                Ok(())
            },
            c["validate"].as_bool().unwrap(),
        )
        .await;
        assert_eq!(result.is_err(), c.get("error").is_some(), "{c}");
        if let Ok(s) = &result {
            assert_eq!(output(s), c["state"], "{c}");
        }
        if let Err(e) = result {
            assert_eq!(e.is_missing_key(), c["missing"].as_bool().unwrap(), "{c}");
        }
        assert_eq!(json!(mutations), c["mutations"], "{c}");
        assert_eq!(json!(*calls.lock().unwrap()), c["calls"], "{c}");
    }
}
#[tokio::test]
async fn snapshots_and_patches_preserve_source_resilience_filters_and_cleanup() {
    let f = fixture();
    for c in f["aggregate"].as_array().unwrap() {
        let (p, calls) = provider(
            if c["variant"] == "missingKey" {
                "missing"
            } else {
                "good"
            },
            hex(&f["secret"]),
        );
        let validate = c["validate"].as_bool().unwrap();
        let minimum = c["minimum"].as_u64();
        let mut patches = vec![];
        let result = if c["kind"] == "snapshot" {
            decode_snapshot(
                &proto::SyncdSnapshot::decode(hex(&c["input"]).as_slice()).unwrap(),
                "regular",
                &p,
                minimum,
                validate,
            )
            .await
        } else {
            patches.push(proto::SyncdPatch::decode(hex(&c["input"]).as_slice()).unwrap());
            decode_patches(
                &mut patches,
                "regular",
                &new_lthash_state(),
                &p,
                minimum,
                validate,
            )
            .await
        };
        assert_eq!(result.is_err(), c.get("error").is_some(), "{c}");
        if let Ok(result) = &result {
            let warnings = c["warnings"].as_array().unwrap();
            let mismatch = warnings
                .iter()
                .any(|v| v.as_str().unwrap().contains("LTHash"));
            let verified = if !validate {
                None
            } else if c["kind"] == "snapshot" || c["cleared"] == true {
                Some(!mismatch)
            } else if mismatch {
                Some(false)
            } else {
                None
            };
            assert_eq!(result.hash_verified, verified, "{c}");
            assert_eq!(output(&result.state), c["state"], "{c}");
            assert_eq!(
                json!(
                    result
                        .mutations
                        .iter()
                        .map(|(k, m)| (
                            k.clone(),
                            json!({"action":wire(&m.sync_action.encode_to_vec()),"index":m.index})
                        ))
                        .collect::<std::collections::BTreeMap<_, _>>()
                ),
                c["mutations"],
                "{c}"
            );
            if c["kind"] == "patch" {
                assert_eq!(
                    patches[0].mutations.is_empty(),
                    c["cleared"].as_bool().unwrap(),
                    "{c}"
                );
            }
        }
        if let Err(e) = result {
            assert_eq!(e.is_missing_key(), c["missing"].as_bool().unwrap());
        }
        assert_eq!(json!(*calls.lock().unwrap()), c["calls"], "{c}");
    }
}
#[tokio::test]
async fn stored_key_lookup_entropy_and_missing_encode_keys_work() {
    let temp = tempfile::tempdir().unwrap();
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    store
        .save_credentials(&Credentials::generate().unwrap())
        .unwrap();
    store
        .set(&[KeyMutation::put(
            "AQID",
            KeyValue::AppStateSyncKey(proto::message::AppStateSyncKeyData {
                key_data: Some(vec![7; 32]),
                ..Default::default()
            }),
        )])
        .unwrap();
    let p = KeyProvider::from_store(store);
    let req = PatchCreate {
        name: "regular".into(),
        index: vec!["mute".into()],
        sync_action: Default::default(),
        api_version: 0,
        operation: 0,
    };
    let a = encode_patch(&req, "AQID", &new_lthash_state(), &p)
        .await
        .unwrap();
    let b = encode_patch(&req, "AQID", &new_lthash_state(), &p)
        .await
        .unwrap();
    assert_ne!(
        a.patch.mutations[0].record.as_ref().unwrap().value,
        b.patch.mutations[0].record.as_ref().unwrap().value
    );
    for id in ["", "missing"] {
        assert!(
            encode_patch(&req, id, &new_lthash_state(), &p)
                .await
                .is_err_and(|e| e.is_missing_key())
        );
    }
}
#[tokio::test]
async fn patch_sequences_skip_failures_stop_on_hash_mismatches_and_preserve_partial_mutations() {
    let f = fixture();
    for c in f["batches"].as_array().unwrap() {
        let (p, calls) = provider(
            if c["variant"] == "missingKey" {
                "missing"
            } else {
                "good"
            },
            hex(&f["secret"]),
        );
        let mut patches = c["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| proto::SyncdPatch::decode(hex(v).as_slice()).unwrap())
            .collect::<Vec<_>>();
        let result = decode_patches(
            &mut patches,
            "regular",
            &new_lthash_state(),
            &p,
            c["minimum"].as_u64(),
            c["validate"].as_bool().unwrap(),
        )
        .await;
        assert_eq!(result.is_err(), c.get("error").is_some(), "{c}");
        if let Ok(result) = &result {
            assert_eq!(output(&result.state), c["state"], "{c}");
            assert_eq!(
                json!(
                    result
                        .mutations
                        .iter()
                        .map(|(k, m)| (
                            k.clone(),
                            json!({"action":wire(&m.sync_action.encode_to_vec()),"index":m.index})
                        ))
                        .collect::<std::collections::BTreeMap<_, _>>()
                ),
                c["mutations"],
                "{c}"
            );
        }
        if let Err(e) = result {
            assert_eq!(e.is_missing_key(), c["missing"].as_bool().unwrap());
        }
        assert_eq!(
            json!(
                patches
                    .iter()
                    .map(|p| p.mutations.len())
                    .collect::<Vec<_>>()
            ),
            c["remaining"],
            "{c}"
        );
        assert_eq!(json!(*calls.lock().unwrap()), c["calls"], "{c}");
    }
}
