use baileys_rs::{
    devices::{Cache, Config},
    jid::Jid,
    usync,
};
use serde_json::{Value, json};
use std::time::Duration;
#[test]
fn cache_planning_and_devices_match_upstream_operations() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/devices.json")).unwrap();
    let response: usync::ResultList = serde_json::from_value(fixture["response"].clone()).unwrap();
    let cache = Cache::new(Config::default());
    let mut storage = serde_json::Map::new();
    for step in fixture["steps"].as_array().unwrap() {
        let operation = &step["operation"];
        let inputs = serde_json::from_value::<Vec<String>>(operation["jids"].clone()).unwrap();
        let mut plan = cache
            .plan(&inputs, operation["useCache"].as_bool().unwrap())
            .unwrap();
        assert_eq!(json!(plan.to_fetch), step["toFetch"]);
        if !plan.to_fetch.is_empty() {
            let extracted = usync::extract_device_jids(
                &response.list,
                fixture["me"].as_str().unwrap(),
                fixture["lid"].as_str().unwrap(),
                operation["ignoreZero"].as_bool().unwrap(),
            )
            .unwrap();
            let groups = cache.update(extracted).unwrap();
            for (user, devices) in &groups {
                storage.insert(
                    user.clone(),
                    json!(
                        devices
                            .iter()
                            .map(|d| d.device.to_string())
                            .collect::<Vec<_>>()
                    ),
                );
            }
            plan.devices.extend(groups.into_values().flatten());
        }
        assert_eq!(json!(plan.devices), step["result"]);
        assert_eq!(json!(storage), step["storage"]);
    }
}
#[test]
fn cache_expiry_capacity_disable_and_invalidation_are_bounded() {
    for (capacity, ttl) in [
        (0, Duration::from_secs(300)),
        (1, Duration::from_secs(300)),
        (8, Duration::from_millis(2)),
    ] {
        let cache = Cache::new(Config {
            capacity,
            ttl,
            store: None,
        });
        cache
            .update(vec![Jid::parse("100:1@s.whatsapp.net").unwrap()])
            .unwrap();
        if ttl < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_millis(10));
        }
        let plan = cache.plan(&["100@s.whatsapp.net".into()], true).unwrap();
        assert_eq!(plan.to_fetch.is_empty(), capacity == 1);
        cache
            .update(vec![Jid::parse("200:1@lid").unwrap()])
            .unwrap();
        if capacity == 1 {
            assert_eq!(
                cache
                    .plan(&["100@s.whatsapp.net".into()], true)
                    .unwrap()
                    .to_fetch
                    .len(),
                1
            );
        }
        let clone = cache.clone();
        clone.invalidate_users(&["200".into()]).unwrap();
        assert_eq!(
            cache
                .plan(&["200@lid".into()], true)
                .unwrap()
                .to_fetch
                .len(),
            1
        );
        cache.clear().unwrap();
        assert!(cache.plan(&["invalid".into()], true).is_err());
        let plan = cache.plan(&["200:0@lid".into()], false).unwrap();
        assert_eq!(plan.devices[0].jid, "200:0@lid");
        assert!(plan.to_fetch.is_empty());
    }
}

#[test]
fn session_key_requests_match_upstream_assertion_fixtures() {
    fn js_node(node: &baileys_rs::binary::BinaryNode) -> Value {
        let mut result = json!({"tag":node.tag,"attrs":node.attrs});
        if let Some(baileys_rs::binary::NodeContent::Children(children)) = &node.content {
            result["content"] = json!(children.iter().map(js_node).collect::<Vec<_>>());
        }
        result
    }
    let fixture: Value = serde_json::from_str(include_str!("fixtures/devices.json")).unwrap();
    for case in fixture["assertions"].as_array().unwrap() {
        let queries = case["queries"].as_array().unwrap();
        assert_eq!(case["fetched"], !queries.is_empty());
        for request in queries {
            let jids = request["content"][0]["content"]
                .as_array()
                .unwrap()
                .iter()
                .map(|u| u["attrs"]["jid"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            let node = baileys_rs::signal::wire::session_query_with_reason(
                &jids,
                case["config"]["force"].as_bool().unwrap(),
            )
            .unwrap();
            assert_eq!(js_node(&node), *request);
        }
    }
}
#[test]
fn external_store_replaces_the_built_in_cache() {
    use baileys_rs::devices::{Change, Device, ExternalStore, Store};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    #[derive(Clone, Default)]
    struct Shared {
        entries: Arc<Mutex<HashMap<String, Vec<Device>>>>,
        log: Arc<Mutex<Vec<String>>>,
    }
    impl Store for Shared {
        fn get(&self, user: &str) -> Option<Vec<Device>> {
            self.log.lock().unwrap().push(format!("get {user}"));
            self.entries.lock().unwrap().get(user).cloned()
        }
        fn set(&self, user: &str, devices: Vec<Device>) {
            self.log.lock().unwrap().push(format!("set {user}"));
            self.entries.lock().unwrap().insert(user.into(), devices);
        }
        fn del(&self, user: &str) {
            self.log.lock().unwrap().push(format!("del {user}"));
            self.entries.lock().unwrap().remove(user);
        }
        fn flush_all(&self) {
            self.log.lock().unwrap().push("flush".into());
            self.entries.lock().unwrap().clear();
        }
    }
    let store = Shared::default();
    // The built-in limits do not apply to an external store.
    let cache = Cache::new(Config {
        capacity: 0,
        ttl: Duration::ZERO,
        store: Some(ExternalStore::new(store.clone())),
    });
    let device = |user: &str, device: u16| Jid {
        user: user.into(),
        server: "s.whatsapp.net".into(),
        device: Some(device),
        domain_type: 0,
    };
    cache
        .update(vec![device("200", 0), device("200", 1), device("300", 0)])
        .unwrap();
    assert_eq!(store.entries.lock().unwrap()["200"].len(), 2);
    let plan = cache
        .plan(
            &["200@s.whatsapp.net".into(), "400@s.whatsapp.net".into()],
            true,
        )
        .unwrap();
    assert_eq!(plan.devices.len(), 2);
    assert_eq!(plan.to_fetch, ["400@s.whatsapp.net"]);
    // Without the cache nothing is read.
    let plan = cache.plan(&["200@s.whatsapp.net".into()], false).unwrap();
    assert_eq!(plan.to_fetch, ["200@s.whatsapp.net"]);
    cache
        .apply_change("200", Change::Add, &[device("200", 5)])
        .unwrap();
    assert_eq!(cache.cached("200").unwrap().unwrap().len(), 3);
    cache
        .apply_change(
            "200",
            Change::Remove,
            &[device("200", 0), device("200", 1), device("200", 5)],
        )
        .unwrap();
    assert_eq!(cache.cached("200").unwrap(), None);
    // A change for a user the store does not hold writes nothing.
    cache
        .apply_change("500", Change::Add, &[device("500", 1)])
        .unwrap();
    assert!(!store.entries.lock().unwrap().contains_key("500"));
    cache.apply_change("300", Change::Update, &[]).unwrap();
    assert_eq!(cache.cached("300").unwrap(), None);
    cache.update(vec![device("600", 0)]).unwrap();
    cache.invalidate_users(&["600".into()]).unwrap();
    assert!(store.entries.lock().unwrap().is_empty());
    cache.update(vec![device("700", 0)]).unwrap();
    cache.clear().unwrap();
    assert!(store.entries.lock().unwrap().is_empty());
    let log = store.log.lock().unwrap();
    assert_eq!(log.last().unwrap(), "flush");
    assert!(log.contains(&"del 200".to_string()) && log.contains(&"del 300".to_string()));
}
