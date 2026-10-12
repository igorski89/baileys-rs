use baileys_rs::{
    app_state::*,
    auth::{Credentials, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore, ValueMac},
    crypto,
};
use serde_json::{Value, json};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/app_state_foundation.json")).unwrap()
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
#[test]
fn app_state_keys_and_all_mac_domains_match_original() {
    for case in fixture()["keys"].as_array().unwrap() {
        let keys = MutationKeys::derive(&hex(&case["secret"])).unwrap();
        for (name, key) in [
            ("indexKey", keys.index_key.as_ref()),
            ("valueEncryptionKey", keys.value_encryption_key.as_ref()),
            ("valueMacKey", keys.value_mac_key.as_ref()),
            ("snapshotMacKey", keys.snapshot_mac_key.as_ref()),
            ("patchMacKey", keys.patch_mac_key.as_ref()),
        ] {
            assert_eq!(key, hex(&case["keys"][name]), "{case}");
        }
        assert_eq!(format!("{keys:?}"), "MutationKeys { .. }");
    }
    for case in fixture()["macs"].as_array().unwrap() {
        let key = hex(&case["key"]);
        let value = value_mac(
            case["operation"].as_i64().unwrap() as i32,
            &hex(&case["data"]),
            &hex(&case["id"]),
            &key,
        );
        assert_eq!(value.to_vec(), hex(&case["value"]), "{case}");
        let result = snapshot_mac(
            &(0..128u8)
                .map(|i| i.wrapping_mul(17).wrapping_add(4))
                .collect::<Vec<_>>(),
            case["version"].as_u64().unwrap(),
            case["name"].as_str().unwrap(),
            &key,
        );
        if case.get("error").is_some() {
            assert!(matches!(result, Err(Error::Version)));
            continue;
        }
        let snapshot = result.unwrap();
        assert_eq!(snapshot.to_vec(), hex(&case["snapshot"]));
        assert_eq!(
            patch_mac(
                &snapshot,
                &[
                    value.to_vec(),
                    (0..32u8)
                        .map(|i| i.wrapping_mul(17).wrapping_add(5))
                        .collect()
                ],
                case["version"].as_u64().unwrap(),
                case["name"].as_str().unwrap(),
                &key
            )
            .unwrap()
            .to_vec(),
            hex(&case["patch"]),
            "{case}"
        );
    }
}
#[test]
fn wrapping_lt_hash_arithmetic_and_mutation_replacements_match_upstream() {
    for case in fixture()["arithmetic"].as_array().unwrap() {
        let sub = case["subtract"]
            .as_array()
            .unwrap()
            .iter()
            .map(hex)
            .collect::<Vec<_>>();
        let add = case["add"]
            .as_array()
            .unwrap()
            .iter()
            .map(hex)
            .collect::<Vec<_>>();
        assert_eq!(
            subtract_then_add(&hex(&case["base"]), &sub, &add)
                .unwrap()
                .to_vec(),
            hex(&case["output"]),
            "{case}"
        );
    }
    for case in fixture()["generators"].as_array().unwrap() {
        let mut initial = new_lthash_state();
        initial.version = 17;
        initial.hash = hex(&case["input"]["hash"]);
        for (k, v) in case["input"]["indexValueMap"].as_object().unwrap() {
            initial
                .index_value_map
                .insert(k.clone(), ValueMac { value_mac: hex(v) });
        }
        let mut generator = LtHashGenerator::new(&initial).unwrap();
        for m in case["mutations"].as_array().unwrap() {
            generator.mix(
                &hex(&m["index"]),
                &hex(&m["value"]),
                m["operation"].as_i64().unwrap() as i32,
            );
        }
        let result = generator.finish().unwrap();
        assert_eq!(result.hash, hex(&case["output"]["hash"]));
        assert_eq!(result.version, 17);
        let map = json!(
            result
                .index_value_map
                .iter()
                .map(|(k, v)| (k.clone(), wire(&v.value_mac)))
                .collect::<std::collections::BTreeMap<_, _>>()
        );
        assert_eq!(map, case["output"]["indexValueMap"]);
        assert_eq!(generator.finish().unwrap().hash, result.hash);
        assert_eq!(initial.hash, hex(&case["originalAfter"]["hash"]));
    }
}
#[test]
fn lt_updates_cancel_commute_and_resume_from_the_key_store() {
    let a = crypto::sha256(b"a").to_vec();
    let b = crypto::sha256(b"b").to_vec();
    let base = [0; 128];
    let ab = subtract_then_add(&base, &[], &[a.clone(), b.clone()]).unwrap();
    assert_eq!(
        ab,
        subtract_then_add(&base, &[], &[b.clone(), a.clone()]).unwrap()
    );
    assert_eq!(subtract_then_add(&ab, &[a.clone(), b], &[]).unwrap(), base);
    for size in [0, 1, 127, 129, 256] {
        assert!(matches!(
            subtract_then_add(&vec![0; size], &[], &[]),
            Err(Error::HashLength)
        ));
    }
    let temp = tempfile::tempdir().unwrap();
    {
        let store = SqliteAuthStore::open(temp.path()).unwrap();
        store
            .save_credentials(&Credentials::generate().unwrap())
            .unwrap();
        let mut gen1 = LtHashGenerator::new(&new_lthash_state()).unwrap();
        gen1.mix(b"INDEX", &a, 0);
        store
            .set(&[KeyMutation::put(
                "regular",
                KeyValue::AppStateSyncVersion(gen1.finish().unwrap()),
            )])
            .unwrap();
    }
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    let loaded = store
        .get(KeyCategory::AppStateSyncVersion, &["regular".into()])
        .unwrap();
    let Some(KeyValue::AppStateSyncVersion(state)) = loaded.get("regular") else {
        panic!()
    };
    let mut generator = LtHashGenerator::new(state).unwrap();
    generator.mix(b"missing", b"ignored", 1);
    assert_eq!(generator.finish().unwrap().hash, state.hash);
    generator.mix(b"INDEX", b"unused", 1);
    assert_eq!(generator.finish().unwrap().hash, vec![0; 128]);
    assert!(generator.finish().unwrap().index_value_map.is_empty());
}
