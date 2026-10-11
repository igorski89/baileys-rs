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
        let cache = Cache::new(Config { capacity, ttl });
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
