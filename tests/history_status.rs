use baileys_rs::{history::StatusTracker, proto, proto_json::JsonField};
use serde_json::{Value, json};
use std::time::Duration;
#[test]
fn history_progress_and_lifecycle_deadlines_match_original_socket() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/history_status.json")).unwrap();
    for (case_index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        let mut tracker = StatusTracker::default();
        let mut now = Duration::ZERO;
        for (step, (operation, expected)) in case["operations"]
            .as_array()
            .unwrap()
            .iter()
            .zip(case["steps"].as_array().unwrap())
            .enumerate()
        {
            let events = match operation["op"].as_str().unwrap() {
                "observe" => tracker.observe(
                    &proto::message::HistorySyncNotification::from_value(
                        &operation["notification"],
                    )
                    .unwrap(),
                    operation["eligible"].as_bool().unwrap(),
                    now,
                ),
                "pending" => {
                    tracker.pending_notifications();
                    vec![]
                }
                "close" => {
                    tracker.close();
                    vec![]
                }
                "advance" => {
                    now = Duration::from_millis(operation["now"].as_u64().unwrap());
                    tracker.tick(now)
                }
                _ => panic!(),
            };
            assert_eq!(
                json!(events),
                expected["events"],
                "case{case_index} step{step}"
            );
            assert_eq!(
                json!({"initialBootstrapComplete":tracker.initial_bootstrap_complete,"recentSyncComplete":tracker.recent_sync_complete}),
                expected["state"],
                "case{case_index} step{step}"
            );
            assert_eq!(
                json!(tracker.next_deadline().map(|d| d.as_millis() as u64)),
                expected["deadline"],
                "case{case_index} step{step}"
            );
        }
    }
}
