use baileys_rs::{
    auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    signal::{
        Address, Error,
        mapping::{Config, Mapping, Store},
    },
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/mapping.json")).unwrap()
}
fn strings(value: &Value) -> Vec<String> {
    serde_json::from_value(value.clone()).unwrap()
}
fn pair(pn: &str, lid: &str) -> Mapping {
    Mapping {
        pn: pn.into(),
        lid: lid.into(),
    }
}
fn nullable(pairs: Vec<Mapping>) -> Value {
    if pairs.is_empty() {
        Value::Null
    } else {
        serde_json::to_value(pairs).unwrap()
    }
}
#[test]
fn matches_upstream_storage_batch_resolution_remapping_and_hosted_quirks() {
    let fixture = fixture();
    let temp = tempfile::tempdir().unwrap();
    let database = SqliteAuthStore::open(temp.path()).unwrap();
    let calls = Arc::new(Mutex::new(vec![]));
    let capture = calls.clone();
    let store = Store::with_resolver(
        database.clone(),
        Config::default(),
        Some(Arc::new(move |pns: &[String]| {
            capture.lock().unwrap().push(pns.to_vec());
            Ok(pns
                .iter()
                .filter_map(|pn| match pn.as_str() {
                    "300@s.whatsapp.net" => Some(pair(pn, "700@lid")),
                    "400@s.whatsapp.net" => Some(pair(pn, "600@lid")),
                    _ => None,
                })
                .collect())
        })),
    );
    let all_keys: Vec<String> = fixture["steps"].as_array().unwrap().last().unwrap()["storage"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    for step in fixture["steps"].as_array().unwrap() {
        let input = &step["input"];
        let result = match step["method"].as_str().unwrap() {
            "storeLIDPNMappings" => {
                store
                    .store_mappings(&serde_json::from_value::<Vec<Mapping>>(input.clone()).unwrap())
                    .unwrap();
                Value::Null
            }
            "getLIDsForPNs" => nullable(store.get_lids_for_pns(&strings(input)).unwrap()),
            "getPNsForLIDs" => nullable(store.get_pns_for_lids(&strings(input)).unwrap()),
            "getLIDForPN" => json!(store.get_lid_for_pn(input.as_str().unwrap()).unwrap()),
            "getPNForLID" => json!(store.get_pn_for_lid(input.as_str().unwrap()).unwrap()),
            "close" => {
                store.close().unwrap();
                Value::Null
            }
            _ => panic!("unknown fixture operation"),
        };
        assert_eq!(result, step["result"], "{} {input}", step["method"]);
        assert_eq!(json!(*calls.lock().unwrap()), step["calls"]);
        let stored: serde_json::Map<String, Value> = database
            .get(KeyCategory::LidMapping, &all_keys)
            .unwrap()
            .into_iter()
            .map(|(key, value)| {
                let KeyValue::LidMapping(value) = value else {
                    panic!()
                };
                (key, json!(value))
            })
            .collect();
        assert_eq!(Value::Object(stored), step["storage"]);
    }
    let resumed = Store::new(SqliteAuthStore::open(temp.path()).unwrap());
    assert_eq!(
        resumed.get_lid_for_pn("100:7@s.whatsapp.net").unwrap(),
        Some("902:7@lid".into())
    );
    assert_eq!(
        resumed
            .resolve_address(&Address::from_jid("400:99@hosted").unwrap())
            .unwrap()
            .to_string(),
        "600_129.99"
    );
    assert_eq!(
        resumed
            .resolve_address(&Address::from_jid("700:7@lid").unwrap())
            .unwrap()
            .to_string(),
        "700_1.7"
    );
}
#[test]
fn bounded_disabled_expired_and_cleared_caches_reload_storage() {
    for config in [
        Config {
            ttl: Duration::from_secs(60),
            capacity: 0,
        },
        Config {
            ttl: Duration::ZERO,
            capacity: 8,
        },
        Config {
            ttl: Duration::from_millis(2),
            capacity: 8,
        },
        Config {
            ttl: Duration::from_secs(60),
            capacity: 1,
        },
    ] {
        let database = SqliteAuthStore::in_memory().unwrap();
        let store = Store::with_resolver(database.clone(), config.clone(), None);
        store
            .store_mappings(&[pair("100@s.whatsapp.net", "900@lid")])
            .unwrap();
        assert_eq!(
            store
                .get_lid_for_pn("100:7@s.whatsapp.net")
                .unwrap()
                .as_deref(),
            Some("900:7@lid")
        );
        database
            .set(&[KeyMutation::put("100", KeyValue::LidMapping("901".into()))])
            .unwrap();
        if config.ttl == Duration::from_millis(2) {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            store
                .get_lid_for_pn("100@s.whatsapp.net")
                .unwrap()
                .as_deref(),
            Some("901@lid")
        );
    }
    let database = SqliteAuthStore::in_memory().unwrap();
    let store = Store::new(database.clone());
    store
        .store_mappings(&[pair("100@s.whatsapp.net", "900@lid")])
        .unwrap();
    database
        .set(&[KeyMutation::put("100", KeyValue::LidMapping("901".into()))])
        .unwrap();
    assert_eq!(
        store
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .as_deref(),
        Some("900@lid")
    );
    store.close().unwrap();
    assert_eq!(
        store
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .as_deref(),
        Some("901@lid")
    );
}
#[test]
fn coalesces_concurrent_batches_including_empty_and_failed_results() {
    for mode in 0..3 {
        let database = SqliteAuthStore::in_memory().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let store = Store::with_resolver(
            database,
            Config::default(),
            Some(Arc::new(move |_: &[String]| {
                count.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(100));
                match mode {
                    0 => Ok(vec![pair("100@s.whatsapp.net", "900@lid")]),
                    1 => Ok(vec![]),
                    _ => Err(Error::Invalid),
                }
            })),
        );
        let barrier = Arc::new(Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|i| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let ids = if i % 2 == 0 {
                        vec!["100@s.whatsapp.net".into(), "100:7@s.whatsapp.net".into()]
                    } else {
                        vec![
                            "100:7@s.whatsapp.net".into(),
                            "100@s.whatsapp.net".into(),
                            "100@s.whatsapp.net".into(),
                        ]
                    };
                    store.get_lids_for_pns(&ids)
                })
            })
            .collect();
        for worker in workers {
            let result = worker.join().unwrap();
            match mode {
                0 => assert_eq!(result.unwrap().len(), 2),
                1 => assert!(result.unwrap().is_empty()),
                _ => assert!(matches!(result, Err(Error::Shared(_)))),
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if mode != 0 {
            let _ = store.get_lid_for_pn("100@s.whatsapp.net");
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
    }
}
#[test]
fn resolver_unwind_wakes_followers_and_allows_later_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let store = Store::with_resolver(
        SqliteAuthStore::in_memory().unwrap(),
        Config::default(),
        Some(Arc::new(move |_: &[String]| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                std::thread::sleep(Duration::from_millis(100));
                panic!("synthetic resolver failure")
            };
            Ok(vec![])
        })),
    );
    let barrier = Arc::new(Barrier::new(4));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.get_lid_for_pn("100@s.whatsapp.net")
            })
        })
        .collect();
    let mut panics = 0;
    for worker in workers {
        match worker.join() {
            Err(_) => panics += 1,
            Ok(result) => assert!(matches!(result, Err(Error::Shared(_)))),
        }
    }
    assert_eq!(panics, 1);
    assert!(
        store
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .is_none()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn failed_database_batch_never_publishes_new_cache_values() {
    let temp = tempfile::tempdir().unwrap();
    let database = SqliteAuthStore::open(temp.path()).unwrap();
    let store = Store::new(database.clone());
    store
        .store_mappings(&[pair("100@s.whatsapp.net", "900@lid")])
        .unwrap();
    let connection = rusqlite::Connection::open(database.path().unwrap()).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_mapping BEFORE INSERT ON signal_keys BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END;").unwrap();
    assert!(
        store
            .store_mappings(&[pair("100@s.whatsapp.net", "901@lid")])
            .is_err()
    );
    assert_eq!(
        store
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .as_deref(),
        Some("900@lid")
    );
    assert!(store.get_pn_for_lid("901@lid").unwrap().is_none());
    store.close().unwrap();
    assert_eq!(
        store
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .as_deref(),
        Some("900@lid")
    );
}
