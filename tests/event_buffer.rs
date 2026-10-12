use baileys_rs::{app_state, event_buffer::*};
use serde_json::{Value, json};
use std::time::Duration;
#[path = "support/json_numbers.rs"]
mod numbers;
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/event_buffer.json")).unwrap()
}
fn condition(spec: &Value, id: String) -> Condition {
    let kind = spec["kind"].as_str().unwrap();
    match kind {
        "false" => Condition::new(|_| Some(false)),
        "true" => Condition::new(|_| Some(true)),
        "missing" => Condition::new(|_| None),
        "throw" => Condition::try_new(|_| Err(Error::Condition("condition failed".into()))),
        "chat" => {
            let last = spec["last"].as_i64();
            Condition::chat(app_state::ChatCondition {
                id,
                message_range: last.map(|last| {
                    baileys_rs::proto::sync_action_value::SyncActionMessageRange {
                        last_message_timestamp: Some(last),
                        ..Default::default()
                    }
                }),
            })
        }
        _ => panic!(),
    }
}
#[test]
fn complete_event_streams_and_timers_match_original_buffer() {
    for (case_index, case) in fixture()["cases"].as_array().unwrap().iter().enumerate() {
        let mut buffer = Buffer::new();
        let mut now = Duration::ZERO;
        for (step, (op, expected)) in case["operations"]
            .as_array()
            .unwrap()
            .iter()
            .zip(case["steps"].as_array().unwrap())
            .enumerate()
        {
            let mut result = None;
            let mut output = vec![];
            let work: Result<(), Error> = (|| {
                match op["op"].as_str().unwrap() {
                    "buffer" | "begin" => buffer.buffer(now),
                    "flush" => {
                        let (flushed, batches) = buffer.flush();
                        result = Some(flushed);
                        output = batches;
                    }
                    "destroy" => buffer.destroy(),
                    "end" => buffer.finish_function(now, op["success"].as_bool().unwrap()),
                    "tick" => {
                        now = Duration::from_millis(op["now"].as_u64().unwrap());
                        output = buffer.tick(now);
                    }
                    "emit" => {
                        if let Some(conditions) = op["conditions"].as_array() {
                            let updates = op["data"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .enumerate()
                                .map(|(i, data)| ChatUpdate {
                                    data: data.clone(),
                                    condition: conditions
                                        .get(i)
                                        .filter(|s| !s.is_null())
                                        .map(|c| condition(c, data["id"].as_str().unwrap().into())),
                                })
                                .collect();
                            output = buffer.emit_chat_updates(updates)?;
                        } else {
                            output =
                                buffer.emit(op["event"].as_str().unwrap(), op["data"].clone())?;
                        }
                    }
                    _ => panic!(),
                }
                Ok(())
            })();
            assert_eq!(
                work.is_err(),
                expected["error"].is_string(),
                "case{case_index} step{step} op{op}"
            );
            output.extend(buffer.take_failed_emissions());
            assert_eq!(
                buffer.is_buffering(),
                expected["buffering"].as_bool().unwrap(),
                "case{case_index} step{step}"
            );
            assert_eq!(
                result,
                expected["result"].as_bool(),
                "case{case_index} step{step}"
            );
            assert_eq!(
                numbers::canonical(&json!(output.iter().map(|b| &b.events).collect::<Vec<_>>())),
                numbers::canonical(&expected["emitted"]),
                "case{case_index} step{step} op{op}"
            );
        }
    }
}
#[test]
fn history_cache_clears_only_when_over_source_limit() {
    for case in fixture()["cacheCases"].as_array().unwrap() {
        let mut buffer = Buffer::new();
        let size = case["size"].as_u64().unwrap() as usize;
        let history = |chats: Value| json!({"chats":chats,"messages":[],"contacts":[]});
        buffer.buffer(Duration::ZERO);
        buffer
            .emit(
                "messaging-history.set",
                history(json!(
                    (0..size)
                        .map(|i| json!({"id":format!("chat{i}")}))
                        .collect::<Vec<_>>()
                )),
            )
            .unwrap();
        let (_, a) = buffer.flush();
        buffer.buffer(Duration::ZERO);
        buffer
            .emit("messaging-history.set", history(json!([{"id":"chat0"}])))
            .unwrap();
        let (_, b) = buffer.flush();
        assert_eq!(
            json!([
                a[0].events["messaging-history.set"]["chats"]
                    .as_array()
                    .unwrap()
                    .len(),
                b[0].events["messaging-history.set"]["chats"]
                    .as_array()
                    .unwrap()
                    .len()
            ]),
            case["counts"]
        );
    }
}
#[test]
fn native_condition_sidecars_and_error_boundaries_are_retained() {
    let mut buffer = Buffer::new();
    let updates = vec![ChatUpdate {
        data: json!({"id":"chat","archived":true}),
        condition: Some(Condition::new(|_| None)),
    }];
    let output = buffer.emit_chat_updates(updates).unwrap();
    assert_eq!(output[0].chat_conditions.len(), 1);
    assert!(output[0].chat_conditions[0].is_some());
    assert_eq!(
        output[0].chat_conditions[0]
            .as_ref()
            .unwrap()
            .evaluate(&Data::default())
            .unwrap(),
        None
    );
    buffer.buffer(Duration::ZERO);
    assert!(matches!(
        buffer.emit(
            "messages.upsert",
            json!({"type":"notify","messages":[{"key":null}]})
        ),
        Err(Error::Input(_))
    ));
    buffer.destroy();
    assert!(!buffer.is_buffering());
    assert!(buffer.data().message_upserts.is_empty());
    assert_eq!(buffer.next_deadline(), None);
}
