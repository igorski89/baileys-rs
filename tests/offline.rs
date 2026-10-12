use baileys_rs::{
    binary::BinaryNode,
    offline::{self, Dependencies, Failure, Processor},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::time::{Duration, timeout};
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    n
}
#[tokio::test]
async fn offline_fifo_errors_batches_and_reopening_match_original_streams() {
    let future = async {
        let fixture: Value = serde_json::from_str(include_str!("fixtures/offline.json")).unwrap();
        for case in fixture["cases"].as_array().unwrap() {
            let trace = Arc::new(Mutex::new(Vec::<Value>::new()));
            let open = Arc::new(AtomicBool::new(true));
            let mut handlers: HashMap<String, offline::Handler<String>> = HashMap::new();
            for kind in ["message", "call", "receipt", "notification"] {
                let (trace, open) = (trace.clone(), open.clone());
                handlers.insert(
                    kind.into(),
                    Arc::new(move |n| {
                        let (trace, open) = (trace.clone(), open.clone());
                        Box::pin(async move {
                            trace
                                .lock()
                                .unwrap()
                                .push(json!(["handle", kind, n.attrs["id"]]));
                            if n.attrs.contains_key("close") {
                                open.store(false, Ordering::SeqCst);
                            }
                            if n.attrs.contains_key("crash") {
                                Err("handler failure".into())
                            } else {
                                Ok(())
                            }
                        })
                    }),
                );
            }
            let (trace_error, open_test) = (trace.clone(), open.clone());
            let mut deps = Dependencies::new(
                move || open_test.load(Ordering::SeqCst),
                move |failure, context| {
                    let message = match failure {
                        Failure::UnknownType(s) | Failure::Error(s) => s,
                    };
                    trace_error
                        .lock()
                        .unwrap()
                        .push(json!(["error", context, message]));
                },
            );
            let fail_yield = case["failYield"].as_bool().unwrap();
            let trace_yield = trace.clone();
            deps.yield_to_event_loop = Arc::new(move || {
                let trace = trace_yield.clone();
                Box::pin(async move {
                    trace.lock().unwrap().push(json!(["yield"]));
                    tokio::task::yield_now().await;
                    if fail_yield {
                        Err("yield failure".into())
                    } else {
                        Ok(())
                    }
                })
            });
            let processor = Processor::with_batch_size(
                handlers,
                deps,
                case["batch"].as_u64().unwrap() as usize,
            );
            for (phase, expected) in case["phases"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case["expected"].as_array().unwrap())
            {
                if let Some(is_open) = phase["open"].as_bool() {
                    open.store(is_open, Ordering::SeqCst);
                }
                for item in phase["items"].as_array().unwrap() {
                    processor.enqueue(item["type"].as_str().unwrap(), node(&item["node"]));
                }
                loop {
                    let stuck = trace
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|v| v[0] == "error" && v[1] == "processing offline nodes");
                    if !processor.is_processing() || stuck {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                assert_eq!(json!(*trace.lock().unwrap()), *expected, "{case}");
            }
        }
    };
    timeout(Duration::from_secs(10), future).await.unwrap();
}
#[tokio::test]
async fn enqueues_during_a_stalled_handler_share_one_processor() {
    let future = async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let ids = Arc::new(Mutex::new(vec![]));
        let handler: offline::Handler<String> = {
            let (entered, release, ids) = (entered.clone(), release.clone(), ids.clone());
            Arc::new(move |node| {
                let (entered, release, ids) = (entered.clone(), release.clone(), ids.clone());
                Box::pin(async move {
                    let id = node.attrs["id"].clone();
                    ids.lock().unwrap().push(id.clone());
                    if id == "first" {
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            })
        };
        let processor = Processor::with_batch_size(
            HashMap::from([("message".into(), handler)]),
            Dependencies::new(|| true, |error, context| panic!("{error:?}: {context}")),
            10,
        );
        processor.enqueue("message", BinaryNode::new("message").attr("id", "first"));
        entered.notified().await;
        for id in ["second", "third"] {
            processor.enqueue("message", BinaryNode::new("message").attr("id", id));
        }
        assert_eq!(processor.queued_len(), 2);
        assert_eq!(*ids.lock().unwrap(), vec!["first"]);
        release.notify_one();
        while processor.is_processing() {
            tokio::task::yield_now().await;
        }
        assert_eq!(*ids.lock().unwrap(), vec!["first", "second", "third"]);
    };
    timeout(Duration::from_secs(5), future).await.unwrap();
}
