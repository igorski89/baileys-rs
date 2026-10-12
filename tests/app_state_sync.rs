use baileys_rs::{
    app_state::*,
    auth::{
        self, KeyCategory, KeyMutation, KeyStore, KeyValue, LtHashState, SqliteAuthStore, ValueMac,
    },
    proto,
};
use indexmap::IndexMap;
use prost::Message as _;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn hex(s: &[u8]) -> String {
    s.iter().map(|v| format!("{v:02x}")).collect()
}
fn state(v: &Value) -> LtHashState {
    LtHashState {
        version: v["version"].as_u64().unwrap(),
        hash: unhex(v["hash"].as_str().unwrap()),
        index_value_map: v["map"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    ValueMac {
                        value_mac: unhex(v.as_str().unwrap()),
                    },
                )
            })
            .collect(),
    }
}
fn state_json(s: &LtHashState) -> Value {
    json!({"version":s.version,"hash":hex(&s.hash),"map":s.index_value_map.iter().map(|(k,v)|(k,hex(&v.value_mac))).collect::<BTreeMap<_,_>>()})
}
fn page(v: &Value) -> IndexMap<String, CollectionResponse> {
    v.as_object()
        .unwrap()
        .iter()
        .map(|(name, c)| {
            (
                name.clone(),
                CollectionResponse {
                    patches: c["patches"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|p| {
                            proto::SyncdPatch::decode(unhex(p.as_str().unwrap()).as_slice())
                                .unwrap()
                        })
                        .collect(),
                    snapshot: c["snapshot"]
                        .as_str()
                        .map(|s| proto::SyncdSnapshot::decode(unhex(s).as_slice()).unwrap()),
                    has_more_patches: c["hasMorePatches"].as_bool().unwrap(),
                },
            )
        })
        .collect()
}
fn options(validate: bool) -> SyncOptions {
    SyncOptions {
        verify_patch: validate,
        verify_snapshot: validate,
        commit_retry_delay: Duration::ZERO,
        ..Default::default()
    }
}
#[tokio::test]
async fn synchronization_matches_original_loop_and_codecs() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/app_state_sync.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let store = SqliteAuthStore::in_memory().unwrap();
        for (name, v) in case["initial"].as_object().unwrap() {
            store
                .set(&[KeyMutation::put(
                    name,
                    KeyValue::AppStateSyncVersion(state(v)),
                )])
                .unwrap();
        }
        let key_calls = Arc::new(Mutex::new(Vec::new()));
        let logged = key_calls.clone();
        let missing = case["keyMode"] == "missing";
        let keys = KeyProvider::new(move |id| {
            logged.lock().unwrap().push(id.clone());
            async move {
                Ok(if missing || id == "CQ==" {
                    None
                } else {
                    Some(proto::message::AppStateSyncKeyData {
                        key_data: Some(vec![7; 32]),
                        ..Default::default()
                    })
                })
            }
        });
        let queries = Arc::new(Mutex::new(Vec::new()));
        let logged = queries.clone();
        let pages = Arc::new(Mutex::new(case["pages"].as_array().unwrap().clone()));
        let transport = CollectionTransport::new(move |requests| {
            let query = collection_query(&requests);
            logged.lock().unwrap().push(
                serde_json::to_value(
                    query
                        .child("sync")
                        .unwrap()
                        .children("collection")
                        .map(|n| &n.attrs)
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            );
            let data = pages.lock().unwrap().remove(0);
            async move {
                if data["error"] == true {
                    Err(Error::Transport {
                        message: "query failed".into(),
                        status_code: None,
                    })
                } else {
                    Ok(page(&data))
                }
            }
        });
        let downloads = Arc::new(Mutex::new(Vec::new()));
        let logged = downloads.clone();
        let downloader = BlobDownloader::new(move |reference| {
            logged.lock().unwrap().push(reference.direct_path);
            async {
                Err(Error::Transport {
                    message: "external download failed".into(),
                    status_code: None,
                })
            }
        });
        let sync = Synchronizer::new(Arc::new(store.clone()), keys, downloader, transport);
        let names: Vec<String> = serde_json::from_value(case["names"].clone()).unwrap();
        let result = sync
            .resync(&names, &options(case["validate"].as_bool().unwrap()))
            .await;
        let expected = &case["expected"];
        assert_eq!(
            result.is_err(),
            expected["error"].is_string(),
            "{} validate={}",
            case["name"],
            case["validate"]
        );
        assert_eq!(
            serde_json::to_value(&*queries.lock().unwrap()).unwrap(),
            expected["requests"],
            "{}",
            case["name"]
        );
        assert_eq!(
            serde_json::to_value(&*key_calls.lock().unwrap()).unwrap(),
            expected["calls"],
            "{}",
            case["name"]
        );
        assert_eq!(
            serde_json::to_value(&*downloads.lock().unwrap()).unwrap(),
            expected["downloads"],
            "{}",
            case["name"]
        );
        assert_eq!(
            serde_json::to_value(sync.blocked_collections().await).unwrap(),
            expected["blocked"],
            "{}",
            case["name"]
        );
        let values = store
            .get(
                KeyCategory::AppStateSyncVersion,
                &["regular".into(), "regular_high".into()],
            )
            .unwrap();
        let states: BTreeMap<_, _> = values
            .into_iter()
            .map(|(k, v)| match v {
                KeyValue::AppStateSyncVersion(s) => (k, state_json(&s)),
                _ => panic!(),
            })
            .collect();
        assert_eq!(
            serde_json::to_value(states).unwrap(),
            expected["states"],
            "{}",
            case["name"]
        );
        if let Ok(result) = result {
            let mutations: Vec<_> = result
                .mutations
                .values()
                .map(|m| json!({"action":hex(&m.sync_action.encode_to_vec()),"index":m.index}))
                .collect();
            assert_eq!(json!(mutations), expected["mutations"], "{}", case["name"]);
        }
    }
}

struct FlakyStore {
    store: SqliteAuthStore,
    failures: Mutex<usize>,
    attempts: Mutex<usize>,
}
impl KeyStore for FlakyStore {
    fn get(
        &self,
        category: KeyCategory,
        ids: &[String],
    ) -> Result<BTreeMap<String, KeyValue>, auth::Error> {
        self.store.get(category, ids)
    }
    fn set(&self, mutations: &[KeyMutation]) -> Result<(), auth::Error> {
        *self.attempts.lock().unwrap() += 1;
        let mut remaining = self.failures.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            return Err(auth::Error::Poisoned);
        }
        self.store.set(mutations)
    }
    fn clear(&self) -> Result<(), auth::Error> {
        self.store.clear()
    }
}
#[tokio::test]
async fn commit_retries_and_exhaustion_keep_transaction_atomic() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/app_state_sync.json")).unwrap();
    let input = fixture
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "snapshot" && c["validate"] == true)
        .unwrap()["pages"][0]
        .clone();
    for failures in [2, 5] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FlakyStore {
            store: SqliteAuthStore::open(dir.path()).unwrap(),
            failures: Mutex::new(failures),
            attempts: Mutex::new(0),
        });
        let input = input.clone();
        let transport = CollectionTransport::new(move |_| {
            let input = input.clone();
            async move { Ok(page(&input)) }
        });
        let keys = KeyProvider::new(|_| async {
            Ok(Some(proto::message::AppStateSyncKeyData {
                key_data: Some(vec![7; 32]),
                ..Default::default()
            }))
        });
        let sync = Synchronizer::new(
            store.clone(),
            keys,
            BlobDownloader::new(|_| async { panic!("no external data") }),
            transport,
        );
        let result = sync
            .resync(
                &["regular".into()],
                &SyncOptions {
                    max_commit_attempts: 3,
                    ..options(true)
                },
            )
            .await;
        assert_eq!(result.is_ok(), failures == 2);
        assert_eq!(*store.attempts.lock().unwrap(), 3);
        let resumed = SqliteAuthStore::open(dir.path()).unwrap();
        assert_eq!(
            resumed
                .get(KeyCategory::AppStateSyncVersion, &["regular".into()])
                .unwrap()
                .contains_key("regular"),
            failures == 2
        );
    }
}

#[tokio::test]
async fn parked_collections_retry_with_fresh_keys_and_concurrent_calls_serialize() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/app_state_sync.json")).unwrap();
    let input = fixture
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "snapshot" && c["validate"] == true)
        .unwrap()["pages"][0]
        .clone();
    let available = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let key_flag = available.clone();
    let keys = KeyProvider::new(move |_| {
        let flag = key_flag.clone();
        async move {
            Ok(flag.load(std::sync::atomic::Ordering::SeqCst).then(|| {
                proto::message::AppStateSyncKeyData {
                    key_data: Some(vec![7; 32]),
                    ..Default::default()
                }
            }))
        }
    });
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = active.clone();
    let transport = CollectionTransport::new(move |_| {
        let count = count.clone();
        let input = input.clone();
        async move {
            assert_eq!(count.fetch_add(1, std::sync::atomic::Ordering::SeqCst), 0);
            tokio::time::sleep(Duration::from_millis(10)).await;
            count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Ok(page(&input))
        }
    });
    let sync = Synchronizer::new(
        Arc::new(SqliteAuthStore::in_memory().unwrap()),
        keys,
        BlobDownloader::new(|_| async { panic!() }),
        transport,
    );
    let names = vec!["regular".into()];
    let opts = options(true);
    let result = sync.resync(&names, &opts).await.unwrap();
    assert_eq!(result.blocked, names);
    assert_eq!(result.attempts["regular"], 2);
    let retry = sync.take_blocked_collections().await;
    assert_eq!(retry, names);
    assert!(sync.blocked_collections().await.is_empty());
    available.store(true, std::sync::atomic::Ordering::SeqCst);
    let (a, b) = tokio::join!(sync.resync(&retry, &opts), sync.resync(&names, &opts));
    assert_eq!(a.unwrap().mutations.len(), 1);
    assert!(b.unwrap().mutations.is_empty());
    assert!(!SyncOptions::default().verify_patch && !SyncOptions::default().verify_snapshot);
}
