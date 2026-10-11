use baileys_rs::{
    auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    signal::{direct::Repository, migration::Config},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/migration.json")).unwrap()
}
fn put_data(store: &SqliteAuthStore, data: &Value) {
    let mut updates = Vec::new();
    for (category, keys) in data.as_object().unwrap() {
        for (id, value) in keys.as_object().unwrap() {
            let value = match category.as_str() {
                "session" => KeyValue::Session(value.clone()),
                "device-list" => {
                    KeyValue::DeviceList(serde_json::from_value(value.clone()).unwrap())
                }
                "identity-key" => {
                    KeyValue::IdentityKey(serde_json::from_value(value.clone()).unwrap())
                }
                _ => panic!("unknown fixture category"),
            };
            updates.push(KeyMutation::put(id, value));
        }
    }
    store.set(&updates).unwrap();
}
fn snapshot(store: &SqliteAuthStore) -> Value {
    let sql = rusqlite::Connection::open(store.path().unwrap()).unwrap();
    let mut statement = sql
        .prepare("SELECT category,id FROM signal_keys ORDER BY category,id")
        .unwrap();
    let keys = statement
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut output: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for (category, id) in keys {
        let kind = match category.as_str() {
            "session" => KeyCategory::Session,
            "device-list" => KeyCategory::DeviceList,
            "identity-key" => KeyCategory::IdentityKey,
            _ => panic!("unexpected category"),
        };
        let values = store.get(kind, std::slice::from_ref(&id)).unwrap();
        let value = match &values[&id] {
            KeyValue::Session(value) => value.clone(),
            KeyValue::DeviceList(value) => json!(value),
            KeyValue::IdentityKey(value) => json!(value),
            _ => unreachable!(),
        };
        output.entry(category).or_default().insert(id, value);
    }
    json!(output)
}
fn without_empty_categories(mut value: Value) -> Value {
    value
        .as_object_mut()
        .unwrap()
        .retain(|_, value| !value.as_object().unwrap().is_empty());
    value
}
#[test]
fn matches_upstream_migrations_counters_cache_and_hosted_quirks() {
    let fixture = fixtures();
    for case in fixture["cases"].as_array().unwrap() {
        let folder = tempfile::tempdir().unwrap();
        let store = SqliteAuthStore::open(folder.path()).unwrap();
        put_data(&store, &case["data"]);
        let repo = Repository::new(store.clone());
        let sql = rusqlite::Connection::open(store.path().unwrap()).unwrap();
        let mut failed_write = case["failFirst"].as_bool().unwrap_or(false);
        if failed_write {
            sql.execute_batch("CREATE TRIGGER reject_migration BEFORE INSERT ON signal_keys BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END;").unwrap();
        }
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let args = step["args"].as_array().unwrap();
            if args[0] == "close" {
                repo.close().unwrap();
            } else {
                let result =
                    repo.migrate_session(args[0].as_str().unwrap(), args[1].as_str().unwrap());
                if !step["error"].is_null() {
                    assert!(result.is_err(), "{}", case["name"]);
                } else {
                    assert_eq!(json!(result.unwrap()), step["result"], "{}", case["name"]);
                }
            }
            assert_eq!(
                snapshot(&store),
                without_empty_categories(step["storage"].clone()),
                "{} step {index}",
                case["name"]
            );
            assert_eq!(
                snapshot(&SqliteAuthStore::open(folder.path()).unwrap()),
                snapshot(&store)
            );
            if failed_write {
                sql.execute_batch("DROP TRIGGER reject_migration;").unwrap();
                failed_write = false;
            }
            if index == 0
                && let Some(record) = case.get("recreate")
            {
                store
                    .set(&[KeyMutation::put("100.1", KeyValue::Session(record.clone()))])
                    .unwrap();
            }
        }
    }
}

fn source_record() -> Value {
    fixtures()["cases"][0]["data"]["session"]["100.0"].clone()
}
#[test]
fn concurrent_clones_move_sessions_once_and_roll_back_partial_writes() {
    let folder = tempfile::tempdir().unwrap();
    let store = SqliteAuthStore::open(folder.path()).unwrap();
    let data = json!({"device-list":{"100":["0","1"]},"session":{"100.0":source_record(),"100.1":source_record()}});
    put_data(&store, &data);
    let repo = Repository::new(store.clone());
    let sql = rusqlite::Connection::open(store.path().unwrap()).unwrap();
    sql.execute_batch("CREATE TRIGGER reject_second BEFORE INSERT ON signal_keys WHEN NEW.id='900_1.1' BEGIN SELECT RAISE(ABORT, 'second move failed'); END;").unwrap();
    assert!(
        repo.migrate_session("100@s.whatsapp.net", "900@lid")
            .is_err()
    );
    assert_eq!(snapshot(&store), data);
    sql.execute_batch("DROP TRIGGER reject_second;").unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let threads = (0..8)
        .map(|_| {
            let repo = repo.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                repo.migrate_session("100@s.whatsapp.net", "900@lid")
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().map(|r| r.migrated).sum::<usize>(), 2);
    assert_eq!(results.iter().map(|r| r.total).sum::<usize>(), 2);
    assert_eq!(
        store
            .get(KeyCategory::Session, &["100.0".into(), "100.1".into()])
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        store
            .get(KeyCategory::Session, &["900_1.0".into(), "900_1.1".into()])
            .unwrap()
            .len(),
        2
    );
    // Invalid second source never commits a previously planned first move.
    put_data(&store, &data);
    repo.close().unwrap();
    store
        .set(&[KeyMutation::put(
            "100.1",
            KeyValue::Session(json!({"version":"bad"})),
        )])
        .unwrap();
    let before = snapshot(&store);
    assert!(
        repo.migrate_session("100@s.whatsapp.net", "900@lid")
            .is_err()
    );
    assert_eq!(snapshot(&store), before);
}

#[test]
fn migration_cache_is_bounded_disabled_and_expires() {
    for (capacity, ttl) in [
        (0, Duration::from_secs(10)),
        (1, Duration::from_secs(10)),
        (4, Duration::from_millis(2)),
    ] {
        let folder = tempfile::tempdir().unwrap();
        let store = SqliteAuthStore::open(folder.path()).unwrap();
        let data = json!({"device-list":{"100":["0","1"]},"session":{"100.0":source_record(),"100.1":source_record()}});
        put_data(&store, &data);
        let repo = Repository::new(store.clone()).with_migration_config(Config { capacity, ttl });
        assert_eq!(
            repo.migrate_session("100@s.whatsapp.net", "900@lid")
                .unwrap()
                .migrated,
            2
        );
        put_data(&store, &data);
        if ttl < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(10));
        }
        let result = repo
            .migrate_session("100@s.whatsapp.net", "901@lid")
            .unwrap();
        assert_eq!(result.migrated, if capacity == 1 { 1 } else { 2 });
        let keys = store
            .get(KeyCategory::Session, &["901_1.0".into(), "901_1.1".into()])
            .unwrap()
            .into_keys()
            .collect::<BTreeSet<_>>();
        assert!(keys.contains("901_1.0"));
    }
}
