use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    privacy::{self, Repository},
    signal::mapping::{Mapping, Store},
};
use serde_json::{Value, json};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/privacy.json")).unwrap()
}
fn entries(store: &SqliteAuthStore, ids: &[String]) -> Value {
    let data = store.get(KeyCategory::Tctoken, ids).unwrap();
    let mut out = serde_json::Map::new();
    for (jid, entry) in data {
        if let KeyValue::Tctoken(entry) = entry {
            out.insert(jid, serde_json::to_value(entry).unwrap());
        }
    }
    Value::Object(out)
}
#[test]
fn upstream_expiry_and_issuance_boundaries() {
    let f = fixture();
    let now = f["now"].as_u64().unwrap();
    for case in f["expiry"].as_array().unwrap() {
        let t: Option<TokenTimestamp> = serde_json::from_value(case["timestamp"].clone()).unwrap();
        assert_eq!(
            privacy::is_expired(t.as_ref(), now),
            case["expired"].as_bool().unwrap(),
            "{case}"
        );
    }
    for case in f["issue"].as_array().unwrap() {
        assert_eq!(
            privacy::should_issue(case["sender"].as_u64(), now),
            case["issue"].as_bool().unwrap(),
            "{case}"
        );
    }
}
#[test]
fn upstream_storage_preserves_metadata_and_rejects_stale_tokens() {
    for case in fixture()["cases"].as_array().unwrap() {
        let store = SqliteAuthStore::in_memory().unwrap();
        let mappings = Store::new(store.clone());
        mappings
            .store_mappings(&[Mapping {
                pn: "100@s.whatsapp.net".into(),
                lid: "900@lid".into(),
            }])
            .unwrap();
        for (jid, entry) in case["initial"].as_object().unwrap() {
            store
                .set(&[KeyMutation::put(
                    jid,
                    KeyValue::Tctoken(serde_json::from_value(entry.clone()).unwrap()),
                )])
                .unwrap();
        }
        let tokens = case["incoming"]
            .as_array()
            .unwrap()
            .iter()
            .map(|attrs| {
                let mut node = BinaryNode::new("token")
                    .attr("type", "trusted_contact")
                    .with_content(NodeContent::Bytes(vec![1, 2, 3]));
                for (k, v) in attrs.as_object().unwrap() {
                    node = node.attr(k, v.as_str().unwrap());
                }
                node
            })
            .collect();
        let iq = BinaryNode::new("iq").with_content(NodeContent::Children(vec![
            BinaryNode::new("tokens").with_content(NodeContent::Children(tokens)),
        ]));
        let repo = Repository::with_mappings(store.clone(), mappings);
        assert_eq!(
            serde_json::to_value(
                repo.store_from_iq(&iq, case["fallback"].as_str().unwrap())
                    .unwrap()
            )
            .unwrap(),
            case["stored"],
            "{}",
            case["name"]
        );
        let ids = [
            "900@lid",
            "200@s.whatsapp.net",
            "0@s.whatsapp.net",
            "13135551234@s.whatsapp.net",
            "13165550012@s.whatsapp.net",
            "123@bot",
            "123@g.us",
        ]
        .map(str::to_owned);
        assert_eq!(entries(&store, &ids), case["expected"], "{}", case["name"]);
    }
}
#[test]
fn cleanup_and_index_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let now = 1700000000;
    let store = SqliteAuthStore::open(dir.path()).unwrap();
    let repo = Repository::new(store.clone());
    for (jid, timestamp, sender) in [
        ("old@lid", 0, Some(now)),
        ("dead@lid", 0, None),
        ("fresh@lid", now, Some(0)),
    ] {
        store
            .set(&[KeyMutation::put(
                jid,
                KeyValue::Tctoken(TcToken {
                    token: vec![1],
                    timestamp: Some(timestamp.into()),
                    sender_timestamp: sender,
                    extra: Default::default(),
                }),
            )])
            .unwrap();
        repo.mark_issued(jid, sender.unwrap_or(0)).unwrap();
    }
    let base = vec![BinaryNode::new("base")];
    assert_eq!(repo.build_content("old@lid", base.clone(), now), base);
    assert_eq!(
        entries(&store, &["old@lid".into()])["old@lid"],
        json!({"token":{"type":"Buffer","data":""},"senderTimestamp":now})
    );
    let content = repo.build_content("fresh@lid", vec![], now);
    assert_eq!(content[0].attrs["t"], now.to_string());
    let result = repo.prune(&["missing@lid".into()], now).unwrap();
    assert_eq!(
        result,
        privacy::Pruned {
            mutated: 2,
            remaining: 2
        }
    );
    drop(repo);
    drop(store);
    let repo = Repository::new(SqliteAuthStore::open(dir.path()).unwrap());
    assert_eq!(repo.read_index().unwrap(), ["old@lid", "fresh@lid"]);
    let query = privacy::issuance_query(&["100:7@c.us".into()], now);
    assert_eq!(
        query.child("tokens").unwrap().child("token").unwrap().attrs["jid"],
        "100@s.whatsapp.net"
    );
}

#[test]
fn failed_index_write_rolls_back_sender_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteAuthStore::open(dir.path()).unwrap();
    let connection = rusqlite::Connection::open(store.path().unwrap()).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_token_index BEFORE INSERT ON signal_keys WHEN NEW.category='tctoken' AND NEW.id='__index' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;").unwrap();
    let repo = Repository::new(store.clone());
    assert!(repo.mark_issued("100@lid", 1700000000).is_err());
    assert_eq!(
        entries(&store, &["100@lid".into(), "__index".into()]),
        json!({})
    );
    connection
        .execute_batch("DROP TRIGGER reject_token_index")
        .unwrap();
    repo.mark_issued("100@lid", 1700000000).unwrap();
    assert_eq!(repo.read_index().unwrap(), ["100@lid"]);
}
