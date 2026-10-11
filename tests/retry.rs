use baileys_rs::{
    proto,
    retry::{self, Manager},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
fn text(step: &Value, field: &str) -> String {
    step[field].as_str().unwrap().into()
}
fn bytes(step: &Value) -> Vec<u8> {
    serde_json::from_value(step["key"].clone()).unwrap()
}
#[test]
fn retry_manager_matches_original_cache_counters_recreation_and_base_keys() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/retry.json")).unwrap();
    let clock = Arc::new(AtomicU64::new(fixture["start"].as_u64().unwrap()));
    let observed = clock.clone();
    let manager = Manager::with_clock(fixture["max"].as_u64().unwrap(), move || {
        observed.load(Ordering::SeqCst)
    });
    for (i, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        let step = &case["step"];
        let mut result = Value::Null;
        match step["op"].as_str().unwrap() {
            "parse" => result = Value::Array(step["values"].as_array().unwrap().iter().map(|v| {
                let reason = retry::parse_error_code(v.as_str());
                json!({"reason":reason.map(|r| r as u8), "mac":reason.is_some_and(|r| r.is_mac_error())})
            }).collect()),
            "clock" => {clock.store(step["ms"].as_u64().unwrap(), Ordering::SeqCst);},
            "advance" => {clock.fetch_add(step["ms"].as_u64().unwrap(), Ordering::SeqCst);},
            "recreate" => result = serde_json::to_value(manager.should_recreate_session(&text(step,"jid"), step["session"].as_bool().unwrap(), step["error"].as_u64().and_then(|n| retry::parse_error_code(Some(&n.to_string())))).unwrap()).unwrap(),
            "add" => manager.add_recent_message(&text(step,"to"), &text(step,"id"), proto::Message {conversation:Some(text(step,"text")), ..Default::default()}).unwrap(),
            "fillRecent" => for n in 0..step["count"].as_u64().unwrap() {manager.add_recent_message("fill", &format!("m{n}"), proto::Message {conversation:Some(n.to_string()), ..Default::default()}).unwrap();},
            "get" => {result = manager.get_recent_message(&text(step,"to"), &text(step,"id")).unwrap().map(|r| json!({"message":{"conversation":r.message.conversation},"timestamp":r.timestamp})).unwrap_or(Value::Null);},
            "increment" => result = json!(manager.increment_retry_count(&text(step,"id")).unwrap()),
            "count" => result = json!(manager.get_retry_count(&text(step,"id")).unwrap()),
            "exceeded" => result = json!(manager.has_exceeded_max_retries(&text(step,"id")).unwrap()),
            "success" => manager.mark_retry_success(&text(step,"id")).unwrap(),
            "failed" => manager.mark_retry_failed(&text(step,"id")).unwrap(),
            "base" => manager.save_base_key(&text(step,"address"), &text(step,"id"), &bytes(step)).unwrap(),
            "sameBase" => result = json!(manager.has_same_base_key(&text(step,"address"), &text(step,"id"), &bytes(step)).unwrap()),
            "deleteBase" => manager.delete_base_key(&text(step,"address"), &text(step,"id")).unwrap(),
            "fillBase" => for n in 0..step["count"].as_u64().unwrap() {manager.save_base_key("fill", &n.to_string(), &[(n % 256) as u8]).unwrap();},
            "stats" => result = serde_json::to_value(manager.statistics().unwrap()).unwrap(),
            "clear" => manager.clear().unwrap(),
            op => panic!("unknown operation {op}"),
        }
        assert_eq!(result, case["result"], "operation {i}: {step}");
    }
}

#[tokio::test]
async fn phone_requests_replace_cancel_clear_and_finish_without_holding_state_lock() {
    use tokio::time::{Duration, timeout};
    let manager = Manager::new(5);
    let calls = Arc::new(AtomicU64::new(0));
    let obsolete = calls.clone();
    manager
        .schedule_phone_request(
            "replace",
            move || {
                obsolete.fetch_add(100, Ordering::SeqCst);
            },
            Some(Duration::from_secs(60)),
        )
        .unwrap();
    let fired = calls.clone();
    let callback_manager = manager.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    manager
        .schedule_phone_request(
            "replace",
            move || {
                // Reentrant manager operations must not deadlock, and stats update before callback.
                assert_eq!(callback_manager.statistics().unwrap().phone_requests, 1);
                callback_manager
                    .increment_retry_count("from callback")
                    .unwrap();
                fired.fetch_add(1, Ordering::SeqCst);
                tx.send(()).unwrap();
            },
            Some(Duration::ZERO),
        )
        .unwrap();
    for (id, finish) in [("cancel", 0), ("success", 1), ("failed", 2)] {
        let obsolete = calls.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        manager
            .schedule_phone_request(
                id,
                move || {
                    obsolete.fetch_add(100, Ordering::SeqCst);
                    let _ = tx.send(());
                },
                Some(Duration::from_secs(60)),
            )
            .unwrap();
        match finish {
            0 => manager.cancel_pending_phone_request(id).unwrap(),
            1 => manager.mark_retry_success(id).unwrap(),
            _ => manager.mark_retry_failed(id).unwrap(),
        }
        assert!(timeout(Duration::from_secs(2), rx).await.unwrap().is_err());
    }
    timeout(Duration::from_secs(2), rx).await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(manager.statistics().unwrap().total_retries, 1);
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    manager
        .schedule_phone_request(
            "clear",
            move || {
                let _ = tx.send(());
            },
            Some(Duration::from_secs(60)),
        )
        .unwrap();
    manager.clear().unwrap();
    assert!(timeout(Duration::from_secs(2), rx).await.unwrap().is_err());
    assert_eq!(manager.statistics().unwrap(), Default::default());
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    manager
        .schedule_phone_request(
            "drop",
            move || {
                let _ = tx.send(());
            },
            Some(Duration::from_secs(60)),
        )
        .unwrap();
    drop(manager);
    assert!(timeout(Duration::from_secs(2), rx).await.unwrap().is_err());
}

#[test]
fn phone_request_requires_runtime_and_shared_clones_serialize_counters() {
    let manager = Manager::new(5);
    assert!(matches!(
        manager.schedule_phone_request("x", || {}, None),
        Err(retry::Error::NoRuntime)
    ));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let manager = manager.clone();
            scope.spawn(move || {
                for _ in 0..100 {
                    manager.increment_retry_count("shared").unwrap();
                }
            });
        }
    });
    assert_eq!(manager.get_retry_count("shared").unwrap(), 400);
    assert_eq!(manager.statistics().unwrap().total_retries, 400);
}
