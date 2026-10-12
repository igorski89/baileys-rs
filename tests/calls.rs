use baileys_rs::{
    binary::{BinaryNode, NodeContent},
    calls::{self, Cache, Error, Event, OfferCache},
};
use futures_util::future::BoxFuture;
use prost::Message;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    if let Some(c) = v["content"].as_array() {
        n.content = Some(NodeContent::Children(c.iter().map(node).collect()));
    }
    n
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn numbers(mut value: Value) -> Value {
    fn convert(v: &mut Value) {
        match v {
            Value::Number(n) => *v = json!(n.as_f64().unwrap()),
            Value::Array(vs) => vs.iter_mut().for_each(convert),
            Value::Object(vs) => vs.values_mut().for_each(convert),
            _ => (),
        }
    }
    convert(&mut value);
    value
}
struct ObservedCache {
    actual: Cache,
    trace: Arc<Mutex<Vec<Value>>>,
    failure: Option<String>,
}
impl OfferCache for ObservedCache {
    fn get<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<Option<Event>, Error>> {
        Box::pin(async move {
            self.trace.lock().unwrap().push(json!(["get", id]));
            if self.failure.as_deref() == Some("get") {
                return Err(Error::Cache("cache get failure".into()));
            }
            self.actual.get(id).await
        })
    }
    fn set<'a>(&'a self, id: &'a str, call: Event) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.trace.lock().unwrap().push(json!(["set", id]));
            if self.failure.as_deref() == Some("set") {
                return Err(Error::Cache("cache set failure".into()));
            }
            self.actual.set(id, call).await
        })
    }
    fn delete<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.trace.lock().unwrap().push(json!(["delete", id]));
            if self.failure.as_deref() == Some("delete") {
                return Err(Error::Cache("cache delete failure".into()));
            }
            self.actual.delete(id).await
        })
    }
}
#[tokio::test(start_paused = true)]
async fn call_events_offer_expiry_and_generated_messages_match_original_streams() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/calls.json")).unwrap();
    assert_eq!(fixture["cacheVersion"], "1.6.0");
    for case in fixture["cases"].as_array().unwrap() {
        let trace = Arc::new(Mutex::new(vec![]));
        let cache = Cache::new(ObservedCache {
            actual: Cache::default(),
            trace: trace.clone(),
            failure: case["failure"].as_str().map(str::to_owned),
        });
        let mut events = vec![];
        let mut messages = vec![];
        let mut errors = 0;
        for op in case["ops"].as_array().unwrap() {
            if let Some(ms) = op["advance"].as_u64() {
                tokio::time::advance(Duration::from_millis(ms)).await;
                continue;
            }
            let n = node(&op["node"]);
            match calls::process(&n, &cache).await {
                Ok(call) => {
                    events.push(json!([call.clone()]));
                    if let Some(message) = calls::message(&call).unwrap() {
                        messages.push((
                            if call.offline { "append" } else { "notify" },
                            hex(&message.encode_to_vec()),
                        ));
                    }
                }
                Err(_) => errors += 1,
            }
        }
        assert_eq!(
            numbers(json!(events)),
            numbers(case["expected"]["calls"].clone()),
            "{case}"
        );
        let expected = case["expected"]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| {
                (
                    m["kind"].as_str().unwrap(),
                    m["wire"].as_str().unwrap().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(messages, expected, "{case}");
        assert_eq!(
            errors,
            case["expected"]["errors"].as_array().unwrap().len(),
            "{case}"
        );
        // Missing IDs are rejected before calling the native cache, whose keys are typed strings.
        if case["ops"].as_array().unwrap().iter().all(|op| {
            op.get("node").is_none()
                || op["node"]["content"].as_array().is_none_or(|children| {
                    children
                        .first()
                        .is_none_or(|c| c["attrs"]["call-id"].is_string())
                })
        }) {
            assert_eq!(
                json!(*trace.lock().unwrap()),
                case["expected"]["cacheTrace"],
                "{case}"
            );
        }
    }
}
