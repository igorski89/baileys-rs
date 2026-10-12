use baileys_rs::socket::initial_sync::{Coordinator, Effects, Phase};
use serde_json::{Value, json};
use std::time::Duration;
fn actions(effects: Effects) -> Vec<&'static str> {
    let mut out = vec![];
    if effects.buffer {
        out.push("buffer");
    }
    if effects.flush {
        out.push("flush");
    }
    if effects.increment_counter {
        out.push("increment");
    }
    out
}
#[test]
fn complete_initial_sync_phase_streams_match_original_chat_socket() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/initial_sync.json")).unwrap();
    for (case_index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        let mut state = Coordinator::default();
        let mut now = Duration::ZERO;
        let mut counter = 0;
        for (step, (op, expected)) in case["operations"]
            .as_array()
            .unwrap()
            .iter()
            .zip(case["steps"].as_array().unwrap())
            .enumerate()
        {
            let mut events = vec![];
            let mut error = false;
            match op["op"].as_str().unwrap() {
                "pending" => {
                    counter = op["counter"].as_u64().unwrap() as u32;
                    events.extend(actions(state.pending_notifications(
                        op["policy"].as_bool().unwrap(),
                        counter,
                        now,
                    )));
                }
                "history" => {
                    if op["present"] == true {
                        events.extend(actions(
                            state.history_notification(op["eligible"].as_bool().unwrap()),
                        ));
                    }
                }
                "sync" => {
                    if state.phase() == Phase::Syncing {
                        if op["fail"] == true {
                            error = true;
                        } else {
                            events.extend(actions(state.sync_completed()));
                        }
                    }
                }
                "close" => state.close(),
                "advance" => {
                    now = Duration::from_millis(op["now"].as_u64().unwrap());
                    while let Some(at) = state.next_deadline().filter(|at| *at <= now) {
                        events.extend(actions(state.tick(at)));
                    }
                }
                _ => panic!(),
            }
            counter += events.iter().filter(|e| **e == "increment").count() as u32;
            assert_eq!(
                json!(events),
                expected["events"],
                "case{case_index} step{step}"
            );
            assert_eq!(
                state.phase() as u32,
                expected["phase"].as_u64().unwrap() as u32,
                "case{case_index} step{step}"
            );
            assert_eq!(
                counter,
                expected["counter"].as_u64().unwrap() as u32,
                "case{case_index} step{step}"
            );
            assert_eq!(
                json!(state.next_deadline().map(|t| t.as_millis() as u64)),
                expected["deadline"],
                "case{case_index} step{step}"
            );
            assert_eq!(error, expected["error"].as_bool().unwrap());
        }
    }
}
