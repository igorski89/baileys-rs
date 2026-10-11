use baileys_rs::{
    auth::*,
    crypto::{KeyPair, signal_public_key, verify_signature},
    proto,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/auth.json")).unwrap()
}
fn ids(id: &str) -> Vec<String> {
    vec![id.to_owned()]
}
fn eq_key(a: &KeyValue, b: &KeyValue) {
    assert_eq!(
        serde_json::to_value(a).unwrap(),
        serde_json::to_value(b).unwrap()
    );
}

#[test]
fn loads_upstream_credential_defaults_and_preserves_populated_auth() {
    let f = fixture();
    let empty: Credentials = serde_json::from_value(f["empty"].clone()).unwrap();
    empty.validate().unwrap();
    assert_eq!(serde_json::to_value(&empty).unwrap(), f["empty"]);
    assert_eq!(empty.me_id().unwrap_err().status_code(), Some(401));
    let populated: Credentials = serde_json::from_value(f["populated"].clone()).unwrap();
    populated.validate().unwrap();
    assert_eq!(populated.me_id().unwrap(), "test:7@s.whatsapp.net");
    assert_eq!(populated.me.as_ref().unwrap().img_url, Some(None));
    assert_eq!(
        populated.processed_history_messages[0].message_timestamp,
        Some(u64::MAX)
    );
    assert_eq!(populated.extra["customFutureField"], "preserved");
    let value = serde_json::to_value(&populated).unwrap();
    assert_eq!(value["me"]["imgUrl"], Value::Null);
    let restored: Credentials = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), value);
}

#[test]
fn generates_valid_credentials_and_fails_fast_without_me_id() {
    let mut credentials = Credentials::generate().unwrap();
    credentials.validate().unwrap();
    assert_eq!(credentials.next_pre_key_id, 1);
    assert_eq!(credentials.first_unuploaded_pre_key_id, 1);
    assert!(credentials.registration_id <= 255);
    assert_eq!(credentials.signed_pre_key.key_id, 1);
    assert!(verify_signature(
        &credentials.signed_identity_key.public,
        &signal_public_key(&credentials.signed_pre_key.key_pair.public).unwrap(),
        &credentials.signed_pre_key.signature
    ));
    credentials.me = Some(Contact::default());
    assert!(matches!(credentials.me_id(), Err(Error::Unauthenticated)));
    let debug = format!("{credentials:?}");
    assert!(!debug.contains(&credentials.adv_secret_key));
    assert!(!debug.contains("private"));
    credentials.signed_pre_key.signature[0] ^= 1;
    assert!(credentials.validate().is_err());
}

#[test]
fn persists_all_ten_upstream_key_categories_and_credentials_across_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let f = fixture();
    let credentials: Credentials = serde_json::from_value(f["populated"].clone()).unwrap();
    let store = SqliteAuthStore::open(temp.path().join("auth")).unwrap();
    let mut mutations = Vec::new();
    for category in KeyCategory::ALL {
        mutations.push(KeyMutation::put(
            "synthetic",
            KeyValue::from_baileys_json(category, f["keys"][category.as_str()].clone()).unwrap(),
        ));
    }
    store
        .transaction(|tx| {
            tx.save_credentials(&credentials)?;
            tx.set(&mutations)
        })
        .unwrap();
    drop(store);
    let reopened = SqliteAuthStore::open(temp.path().join("auth")).unwrap();
    assert_eq!(
        serde_json::to_value(reopened.load_credentials().unwrap()).unwrap(),
        serde_json::to_value(credentials).unwrap()
    );
    for m in &mutations {
        let values = reopened.get(m.category, &ids("synthetic")).unwrap();
        eq_key(values.get("synthetic").unwrap(), m.value.as_ref().unwrap());
    }
    assert!(
        reopened
            .get(KeyCategory::IdentityKey, &ids("missing"))
            .unwrap()
            .is_empty()
    );
    reopened.clear().unwrap();
    for category in KeyCategory::ALL {
        assert!(
            reopened
                .get(category, &ids("synthetic"))
                .unwrap()
                .is_empty()
        );
    }
    assert!(reopened.load_credentials().unwrap().registered);
}

#[test]
fn atomically_rolls_back_failed_key_and_credential_mutations() {
    let store = SqliteAuthStore::in_memory().unwrap();
    let before = serde_json::to_value(store.load_credentials().unwrap()).unwrap();
    let result = store.transaction(|tx| {
        let mut credentials = tx.load_credentials()?;
        credentials.registered = true;
        tx.save_credentials(&credentials)?;
        tx.set(&[KeyMutation::put("one", KeyValue::IdentityKey(vec![1; 33]))])?;
        tx.transaction(|tx| {
            tx.set(&[KeyMutation {
                category: KeyCategory::PreKey,
                id: "bad".into(),
                value: Some(KeyValue::LidMapping("wrong".into())),
            }])
        })
    });
    assert!(matches!(result, Err(Error::CategoryMismatch)));
    assert_eq!(
        serde_json::to_value(store.load_credentials().unwrap()).unwrap(),
        before
    );
    assert!(
        store
            .get(KeyCategory::IdentityKey, &ids("one"))
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .set(&[
                KeyMutation::put("one", KeyValue::IdentityKey(vec![1; 33])),
                KeyMutation {
                    category: KeyCategory::DeviceList,
                    id: "bad".into(),
                    value: Some(KeyValue::LidMapping("bad".into()))
                }
            ])
            .is_err()
    );
    assert!(
        store
            .get(KeyCategory::IdentityKey, &ids("one"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn only_deletes_prekeys_loaded_in_the_transaction() {
    let store = SqliteAuthStore::in_memory().unwrap();
    store
        .set(&[KeyMutation::put(
            "1",
            KeyValue::PreKey(KeyPair::from_private([1; 32])),
        )])
        .unwrap();
    store
        .transaction(|tx| tx.set(&[KeyMutation::delete(KeyCategory::PreKey, "1")]))
        .unwrap();
    assert!(
        store
            .get(KeyCategory::PreKey, &ids("1"))
            .unwrap()
            .contains_key("1")
    );
    store
        .transaction(|tx| {
            tx.get(KeyCategory::PreKey, &ids("1"))?;
            tx.transaction(|tx| tx.set(&[KeyMutation::delete(KeyCategory::PreKey, "1")]))
        })
        .unwrap();
    assert!(
        store
            .get(KeyCategory::PreKey, &ids("1"))
            .unwrap()
            .is_empty()
    );
    store
        .set(&[KeyMutation::delete(KeyCategory::PreKey, "missing")])
        .unwrap();
}

#[test]
fn allocates_and_acknowledges_prekey_batches_atomically() {
    let store = SqliteAuthStore::in_memory().unwrap();
    let batch = store.transaction(|tx| tx.allocate_prekeys(3)).unwrap();
    assert_eq!(batch.new_pre_keys.len(), 3);
    assert_eq!(batch.last_pre_key_id, 3);
    let reused = store.transaction(|tx| tx.allocate_prekeys(2)).unwrap();
    assert!(reused.new_pre_keys.is_empty());
    let credentials = store.load_credentials().unwrap();
    assert_eq!(credentials.next_pre_key_id, 4);
    assert_eq!(credentials.first_unuploaded_pre_key_id, 1);
    store.transaction(|tx| tx.mark_prekeys_uploaded(2)).unwrap();
    let batch = store.transaction(|tx| tx.allocate_prekeys(3)).unwrap();
    assert_eq!(batch.new_pre_keys.len(), 2);
    assert_eq!(batch.first_pre_key_id, 3);
    assert_eq!(batch.last_pre_key_id, 5);
    assert!(store.transaction(|tx| tx.mark_prekeys_uploaded(6)).is_err());
    let before = store.load_credentials().unwrap().next_pre_key_id;
    let result: Result<(), Error> = store.transaction(|tx| {
        tx.allocate_prekeys(10)?;
        Err(Error::PreKeyRange)
    });
    assert!(result.is_err());
    assert_eq!(store.load_credentials().unwrap().next_pre_key_id, before);
    assert!(store.transaction(|tx| tx.allocate_prekeys(0)).is_err());
}

#[test]
fn independent_database_handles_serialize_concurrent_transactions() {
    let temp = tempfile::tempdir().unwrap();
    let folder = temp.path().join("auth");
    let store = SqliteAuthStore::open(&folder).unwrap();
    store
        .set(&[KeyMutation::put("counter", KeyValue::Session(json!(0)))])
        .unwrap();
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let store = SqliteAuthStore::open(&folder).unwrap();
            std::thread::spawn(move || {
                for _ in 0..20 {
                    store
                        .transaction(|tx| {
                            let current = tx.get(KeyCategory::Session, &ids("counter"))?;
                            let KeyValue::Session(n) = current.get("counter").unwrap() else {
                                panic!()
                            };
                            tx.set(&[KeyMutation::put(
                                "counter",
                                KeyValue::Session(json!(n.as_u64().unwrap() + 1)),
                            )])
                        })
                        .unwrap();
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let keys = store.get(KeyCategory::Session, &ids("counter")).unwrap();
    let KeyValue::Session(count) = &keys["counter"] else {
        panic!()
    };
    assert_eq!(count, 80);
}

#[test]
fn exports_and_imports_baileys_folder_snapshots_without_losing_ids() {
    let temp = tempfile::tempdir().unwrap();
    let f = fixture();
    let source = SqliteAuthStore::in_memory().unwrap();
    source
        .save_credentials(&serde_json::from_value(f["populated"].clone()).unwrap())
        .unwrap();
    let mutations: Vec<_> = KeyCategory::ALL
        .into_iter()
        .map(|c| {
            KeyMutation::put(
                "a/b:7",
                KeyValue::from_baileys_json(c, f["keys"][c.as_str()].clone()).unwrap(),
            )
        })
        .collect();
    source.set(&mutations).unwrap();
    let folder = temp.path().join("export");
    let map = source.export_baileys_folder(&folder).unwrap();
    assert!(source.export_baileys_folder(&folder).is_err());
    let destination = SqliteAuthStore::in_memory().unwrap();
    assert!(
        destination
            .import_baileys_folder(&folder, &BTreeMap::new())
            .is_err()
    );
    destination.import_baileys_folder(&folder, &map).unwrap();
    for m in mutations {
        eq_key(
            &destination.get(m.category, &ids("a/b:7")).unwrap()["a/b:7"],
            m.value.as_ref().unwrap(),
        );
    }
    assert_eq!(
        serde_json::to_value(source.load_credentials().unwrap()).unwrap(),
        serde_json::to_value(destination.load_credentials().unwrap()).unwrap()
    );
    source
        .set(&[KeyMutation::put(
            "a__b:7",
            KeyValue::IdentityKey(vec![1; 33]),
        )])
        .unwrap();
    assert!(matches!(
        source.export_baileys_folder(temp.path().join("collision")),
        Err(Error::FileName)
    ));
    assert!(!temp.path().join("collision").exists());
}

#[test]
fn corrupted_credentials_never_generate_a_replacement_identity() {
    let temp = tempfile::tempdir().unwrap();
    let folder = temp.path().join("auth");
    let store = SqliteAuthStore::open(&folder).unwrap();
    let path = store.path().unwrap().to_owned();
    drop(store);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("UPDATE credentials SET data=?1", [b"broken".as_slice()])
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteAuthStore::open(&folder),
        Err(Error::Json(_))
    ));
    let connection = rusqlite::Connection::open(path).unwrap();
    let data: Vec<u8> = connection
        .query_row("SELECT data FROM credentials WHERE id=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(data, b"broken");
}

#[test]
fn protobuf_auth_data_keeps_long_precision_and_numeric_buffer_objects() {
    let key:proto::message::AppStateSyncKeyData=serde_json::from_value(json!({"keyData":{"1":2,"0":1},"timestamp":{"low":-1,"high":2147483647,"unsigned":false},"fingerprint":{}})).unwrap();
    assert_eq!(key.key_data, Some(vec![1, 2]));
    assert_eq!(key.timestamp, Some(i64::MAX));
    assert!(key.fingerprint.unwrap().device_indexes.is_empty());
}

#[cfg(unix)]
#[test]
fn creates_private_database_and_export_files() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let folder = temp.path().join("auth");
    let store = SqliteAuthStore::open(&folder).unwrap();
    assert_eq!(
        std::fs::metadata(&folder).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(store.path().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let export = temp.path().join("export");
    store.export_baileys_folder(&export).unwrap();
    assert_eq!(
        std::fs::metadata(export.join("creds.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

struct CountingStore {
    inner: SqliteAuthStore,
    reads: Arc<AtomicUsize>,
    fail: Arc<AtomicUsize>,
}
impl KeyStore for CountingStore {
    fn get(&self, c: KeyCategory, ids: &[String]) -> Result<BTreeMap<String, KeyValue>, Error> {
        if !ids.is_empty() {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(c, ids)
    }
    fn set(&self, m: &[KeyMutation]) -> Result<(), Error> {
        if self.fail.load(Ordering::SeqCst) > 0 {
            return Err(Error::CategoryMismatch);
        }
        self.inner.set(m)
    }
    fn clear(&self) -> Result<(), Error> {
        self.inner.clear()
    }
}
#[test]
fn cache_reuses_reads_invalidates_and_avoids_publishing_failed_writes() {
    let reads = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicUsize::new(0));
    let store = CachedKeyStore::new(
        CountingStore {
            inner: SqliteAuthStore::in_memory().unwrap(),
            reads: reads.clone(),
            fail: fail.clone(),
        },
        Duration::from_secs(300),
        10,
    );
    store
        .get(KeyCategory::IdentityKey, &ids("missing"))
        .unwrap();
    store
        .get(KeyCategory::IdentityKey, &ids("missing"))
        .unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    store
        .set(&[KeyMutation::put("key", KeyValue::IdentityKey(vec![1; 33]))])
        .unwrap();
    fail.store(1, Ordering::SeqCst);
    assert!(
        store
            .set(&[KeyMutation::put("key", KeyValue::IdentityKey(vec![2; 33]))])
            .is_err()
    );
    eq_key(
        &store.get(KeyCategory::IdentityKey, &ids("key")).unwrap()["key"],
        &KeyValue::IdentityKey(vec![1; 33]),
    );
    fail.store(0, Ordering::SeqCst);
    store.invalidate().unwrap();
    store.get(KeyCategory::IdentityKey, &ids("key")).unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 2);
    store.clear().unwrap();
    assert!(
        store
            .get(KeyCategory::IdentityKey, &ids("key"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn crash_writer_child() {
    let Ok(folder) = std::env::var("BAILEYS_RS_SYNTHETIC_CRASH_TEST") else {
        return;
    };
    let store = SqliteAuthStore::open(folder).unwrap();
    let _: Result<(), Error> = store.transaction(|tx| {
        let mut credentials = tx.load_credentials()?;
        credentials.registered = true;
        tx.save_credentials(&credentials)?;
        for index in 0..32 {
            tx.set(&[KeyMutation::put(
                format!("uncommitted-{index}"),
                KeyValue::Session(json!("x".repeat(8192))),
            )])?;
        }
        // Exit without dropping the SQLite transaction or Rust connection.
        std::process::exit(73);
    });
    panic!("crash writer unexpectedly returned");
}

#[test]
fn recovers_uncommitted_credentials_and_keys_after_process_exit() {
    let temp = tempfile::tempdir().unwrap();
    let folder = temp.path().join("auth");
    let store = SqliteAuthStore::open(&folder).unwrap();
    let before = serde_json::to_value(store.load_credentials().unwrap()).unwrap();
    store
        .set(&[KeyMutation::put(
            "committed",
            KeyValue::IdentityKey(vec![5; 33]),
        )])
        .unwrap();
    drop(store);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_writer_child"])
        .env("BAILEYS_RS_SYNTHETIC_CRASH_TEST", &folder)
        .output()
        .unwrap()
        .status;
    assert_eq!(status.code(), Some(73));
    let recovered = SqliteAuthStore::open(&folder).unwrap();
    assert_eq!(
        serde_json::to_value(recovered.load_credentials().unwrap()).unwrap(),
        before
    );
    assert!(
        recovered
            .get(KeyCategory::Session, &ids("uncommitted-0"))
            .unwrap()
            .is_empty()
    );
    assert!(
        recovered
            .get(KeyCategory::IdentityKey, &ids("committed"))
            .unwrap()
            .contains_key("committed")
    );
}

#[test]
fn expired_and_zero_capacity_caches_fetch_again() {
    for (ttl, capacity) in [(Duration::ZERO, 10), (Duration::from_secs(300), 0)] {
        let reads = Arc::new(AtomicUsize::new(0));
        let store = CachedKeyStore::new(
            CountingStore {
                inner: SqliteAuthStore::in_memory().unwrap(),
                reads: reads.clone(),
                fail: Arc::new(AtomicUsize::new(0)),
            },
            ttl,
            capacity,
        );
        store
            .get(KeyCategory::IdentityKey, &ids("missing"))
            .unwrap();
        store
            .get(KeyCategory::IdentityKey, &ids("missing"))
            .unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }
}

#[test]
#[ignore = "requires Node and installed npm fixture tooling"]
fn upstream_adapter_resumes_and_updates_rust_export() {
    let temp = tempfile::tempdir().unwrap();
    let source = SqliteAuthStore::in_memory().unwrap();
    let f = fixture();
    source
        .save_credentials(&serde_json::from_value(f["populated"].clone()).unwrap())
        .unwrap();
    let mutations: Vec<_> = KeyCategory::ALL
        .into_iter()
        .map(|c| {
            KeyMutation::put(
                "synthetic",
                KeyValue::from_baileys_json(c, f["keys"][c.as_str()].clone()).unwrap(),
            )
        })
        .collect();
    source.set(&mutations).unwrap();
    let export = temp.path().join("export");
    source.export_baileys_folder(&export).unwrap();
    let output = std::process::Command::new("node")
        .arg("scripts/check-auth-export.mjs")
        .arg(&export)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let resumed = SqliteAuthStore::in_memory().unwrap();
    resumed
        .import_baileys_folder(export, &BTreeMap::new())
        .unwrap();
    let credentials = resumed.load_credentials().unwrap();
    assert!(!credentials.registered);
    assert_eq!(credentials.account_sync_counter, 1);
    eq_key(
        &resumed
            .get(KeyCategory::IdentityKey, &ids("synthetic"))
            .unwrap()["synthetic"],
        &KeyValue::IdentityKey(vec![31; 33]),
    );
    assert!(
        resumed
            .get(KeyCategory::DeviceList, &ids("synthetic"))
            .unwrap()
            .is_empty()
    );
}
