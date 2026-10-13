#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, Credentials, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::{self, KeyPair},
    signal::{direct, mapping::Mapping},
    socket::{Config, Event, Socket},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, time::timeout};

// Every JID the cases store tokens under, or must not.
const JIDS: [&str; 12] = [
    "200@s.whatsapp.net",
    "2000@lid",
    "2999@lid",
    "3000@lid",
    "100@s.whatsapp.net",
    "900@lid",
    "0@s.whatsapp.net",
    "13135551234@s.whatsapp.net",
    "13165550012@s.whatsapp.net",
    "123@bot",
    "group@g.us",
    "200@c.us",
];
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    n.content = match &v["content"] {
        Value::Array(children) => Some(NodeContent::Children(children.iter().map(node).collect())),
        Value::String(text) => Some(NodeContent::Text(text.clone())),
        Value::Object(buffer) => Some(NodeContent::Bytes(
            STANDARD.decode(buffer["data"].as_str().unwrap()).unwrap(),
        )),
        _ => None,
    };
    n
}
/// The fixture ran at a fixed time; its timestamps move with the distance to now.
struct Clock {
    fixture: i64,
    shift: i64,
}
impl Clock {
    fn new(fixture: &Value) -> Self {
        let fixture = fixture["now"].as_i64().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        Self {
            fixture,
            shift: now - fixture,
        }
    }
    // Only values near the fixture's clock are timestamps of this run.
    fn recent(&self, value: i64) -> bool {
        (value - self.fixture).abs() < 400 * 86400
    }
    fn text(&self, text: &str) -> String {
        match text.parse::<i64>() {
            Ok(value) if self.recent(value) => (value + self.shift).to_string(),
            _ => text.into(),
        }
    }
    fn value(&self, value: &Value) -> Value {
        match value {
            Value::String(text) => json!(self.text(text)),
            Value::Number(number) => match number.as_i64() {
                Some(value) if self.recent(value) => json!(value + self.shift),
                _ => value.clone(),
            },
            _ => value.clone(),
        }
    }
    fn token(&self, entry: &Value) -> Value {
        let mut entry = entry.clone();
        for name in ["timestamp", "senderTimestamp"] {
            if let Some(value) = entry.get(name).cloned() {
                entry[name] = self.value(&value);
            }
        }
        entry
    }
    fn tokens(&self, tokens: &Value) -> Value {
        tokens
            .as_object()
            .map(|entries| {
                entries
                    .iter()
                    .map(|(jid, entry)| (jid.clone(), self.token(entry)))
                    .collect::<serde_json::Map<_, _>>()
            })
            .unwrap_or_default()
            .into()
    }
    fn node(&self, mut node: BinaryNode) -> BinaryNode {
        if node.tag == "token"
            && let Some(t) = node.attrs.get("t").cloned()
        {
            node.attrs.insert("t".into(), self.text(&t));
        }
        if let Some(NodeContent::Children(children)) = &mut node.content {
            *children = std::mem::take(children)
                .into_iter()
                .map(|child| self.node(child))
                .collect();
        }
        node
    }
}
fn bundle() -> direct::Bundle {
    let peer = Credentials::generate().unwrap();
    direct::Bundle {
        registration_id: peer.registration_id.into(),
        identity: crypto::signal_public_key(&peer.signed_identity_key.public)
            .unwrap()
            .to_vec(),
        signed: direct::PublicPreKey {
            id: peer.signed_pre_key.key_id,
            public: crypto::signal_public_key(&peer.signed_pre_key.key_pair.public)
                .unwrap()
                .to_vec(),
        },
        signature: peer.signed_pre_key.signature.clone(),
        pre_key: None,
    }
}
fn stored(store: &SqliteAuthStore) -> Value {
    let keys: Vec<String> = JIDS.iter().map(|jid| jid.to_string()).collect();
    let values = store.get(KeyCategory::Tctoken, &keys).unwrap();
    let mut tokens = serde_json::Map::new();
    for jid in keys {
        if let Some(KeyValue::Tctoken(entry)) = values.get(&jid) {
            tokens.insert(jid, serde_json::to_value(entry).unwrap());
        }
    }
    tokens.into()
}
/// What upstream's stubbed socket calls correspond to on the wire.
fn calls(case: &Value, clock: &Clock) -> Vec<Value> {
    let mut calls: Vec<_> = case["log"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["call"] != "track")
        .map(|entry| {
            let mut entry = entry.clone();
            if let Some(timestamp) = entry.get("timestamp").cloned() {
                entry["timestamp"] = clock.value(&timestamp);
            }
            entry.as_object_mut().unwrap().remove("force");
            // A session refresh asks for LIDs only: a mapped phone number goes out as
            // its LID and an unmapped one is left out, as upstream's assertSessions does
            // beneath the stub.
            if entry["call"] == "assertSessions" {
                let wire: Vec<_> = entry["jids"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|jid| {
                        let jid = jid.as_str().unwrap();
                        if jid.ends_with("@lid") {
                            Some(json!(jid))
                        } else {
                            case["seed"]["lids"].get(jid).cloned()
                        }
                    })
                    .collect();
                entry["jids"] = wire.into();
            }
            entry
        })
        .collect();
    // The token issuance runs beside the session refresh, in no fixed order.
    calls.sort_by_key(|entry| entry["call"].as_str().unwrap().to_owned());
    calls
}
fn same_calls(seen: &[Value], wanted: &[Value]) -> bool {
    seen.len() == wanted.len()
        && seen.iter().zip(wanted).all(|(seen, wanted)| {
            seen["call"] == wanted["call"]
                && seen["jids"] == wanted["jids"]
                && match (seen["timestamp"].as_i64(), wanted["timestamp"].as_i64()) {
                    // The clocks of the two runs are a few seconds apart at most.
                    (Some(seen), Some(wanted)) => (seen - wanted).abs() <= 10,
                    (seen, wanted) => seen == wanted,
                }
        })
}
enum Run {
    Notification,
    Ack,
}
async fn replay(section: &str, run: Run) {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/notification-handlers.json")).unwrap();
    let clock = Arc::new(Clock::new(&fixture));
    let me: Contact = serde_json::from_value(fixture["me"].clone()).unwrap();
    let cases = fixture[section].as_array().unwrap();
    assert!(cases.len() >= 14, "{section}");
    for case in cases {
        let temp = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            url: format!("ws://{}", listener.local_addr().unwrap()),
            initialize_session: false,
            fire_init_queries: false,
            announce_presence_on_connect: false,
            process_notifications: false,
            query_timeout: Duration::from_secs(3),
            noise: baileys_rs::noise::Config {
                certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = SqliteAuthStore::open(temp.path()).unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(me.clone());
        store.save_credentials(&creds).unwrap();
        let seed = &case["seed"];
        let repository = direct::Repository::new(store.clone());
        let lids = seed["lids"].as_object().cloned().unwrap_or_default();
        let mappings: Vec<_> = lids
            .iter()
            .map(|(pn, lid)| Mapping {
                pn: pn.clone(),
                lid: lid.as_str().unwrap().into(),
            })
            .collect();
        repository.lid_mapping().store_mappings(&mappings).unwrap();
        for jid in seed["sessions"].as_array().into_iter().flatten() {
            let jid = jid.as_str().unwrap();
            repository.inject_session(jid, &bundle()).unwrap();
            // Upstream's stub answers for the JID as given; here a mapped PN is
            // looked up under its LID.
            if let Some(lid) = lids.get(jid) {
                repository
                    .inject_session(lid.as_str().unwrap(), &bundle())
                    .unwrap();
            }
        }
        let seeded: Vec<_> = clock
            .tokens(&seed["tokens"])
            .as_object()
            .unwrap()
            .iter()
            .map(|(jid, entry)| {
                KeyMutation::put(
                    jid,
                    KeyValue::Tctoken(serde_json::from_value(entry.clone()).unwrap()),
                )
            })
            .collect();
        store.set(&seeded).unwrap();
        let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
        let (done, mut finish) = tokio::sync::oneshot::channel::<()>();
        let server = {
            let (seen, clock) = (seen.clone(), clock.clone());
            let (assert_fails, issue_fails) = (
                seed["assertFails"].as_bool().unwrap_or(false),
                seed["issueFails"].as_bool().unwrap_or(false),
            );
            tokio::spawn(async move {
                let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
                peer.send(BinaryNode::new("success").attr("lid", "900:7@lid"))
                    .await;
                assert!(peer.read().await.child("unified_session").is_some());
                let failure = |request: &BinaryNode| {
                    BinaryNode::new("iq")
                        .attr("id", &request.attrs["id"])
                        .attr("type", "error")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("error")
                                .attr("code", "500")
                                .attr("text", "unavailable"),
                        ]))
                };
                loop {
                    let request = tokio::select! {
                        request = peer.read() => request,
                        _ = &mut finish => break,
                    };
                    if request.tag != "iq" {
                        continue;
                    }
                    let xmlns = request.attrs.get("xmlns").map(String::as_str);
                    if xmlns == Some("encrypt") && request.attrs["type"] == "set" {
                        seen.lock().unwrap().push(json!({"call": "uploadPreKeys"}));
                        peer.result(&request).await;
                    } else if xmlns == Some("encrypt") {
                        let jids: Vec<_> = request
                            .child("key")
                            .unwrap()
                            .all_children()
                            .iter()
                            .map(|user| user.attrs["jid"].clone())
                            .collect();
                        seen.lock()
                            .unwrap()
                            .push(json!({"call": "assertSessions", "jids": jids}));
                        // No bundle is returned either way: the refreshed session is
                        // upstream's stub too, and only the request is compared.
                        let _ = assert_fails;
                        peer.send(failure(&request)).await;
                    } else if xmlns == Some("privacy") {
                        let tokens = request.child("tokens").unwrap();
                        let jids: Vec<_> = tokens
                            .children("token")
                            .map(|token| token.attrs["jid"].clone())
                            .collect();
                        let timestamp: i64 = tokens.children("token").next().unwrap().attrs["t"]
                            .parse()
                            .unwrap();
                        seen.lock().unwrap().push(json!({
                            "call": "issuePrivacyTokens",
                            "jids": jids,
                            "timestamp": timestamp,
                        }));
                        if issue_fails {
                            peer.send(failure(&request)).await;
                            continue;
                        }
                        // The reply upstream's stub gives.
                        let issued = jids
                            .iter()
                            .map(|jid| {
                                BinaryNode::new("token")
                                    .attr("jid", jid)
                                    .attr("t", (clock.fixture + clock.shift).to_string())
                                    .attr("type", "trusted_contact")
                                    .with_content(NodeContent::Bytes(
                                        format!("issued for {jid}").into_bytes(),
                                    ))
                            })
                            .collect();
                        peer.send(
                            BinaryNode::new("iq")
                                .attr("id", &request.attrs["id"])
                                .attr("type", "result")
                                .with_content(NodeContent::Children(vec![
                                    BinaryNode::new("tokens")
                                        .with_content(NodeContent::Children(issued)),
                                ])),
                        )
                        .await;
                    } else {
                        // LID lookups before a session refresh find nothing.
                        peer.result(&request).await;
                    }
                }
            })
        };
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        if seed["issueToLid"].as_bool().unwrap_or(false) {
            socket
                .apply_server_props(&[("14303".to_string(), "true".to_string())].into())
                .unwrap();
        }
        while events.try_recv().is_ok() {}
        let input = clock.node(node(&case["node"]));
        let repeats = if case["repeat"].as_bool().unwrap_or(false) {
            2
        } else {
            1
        };
        let mut failed = false;
        for _ in 0..repeats {
            failed |= match run {
                Run::Notification => socket.handle_notification(&input).await.is_err(),
                Run::Ack => socket.handle_bad_ack(&input).await.is_err(),
            };
        }
        assert_eq!(failed, case["error"].as_bool().unwrap(), "{case}");
        let wanted_calls = calls(case, &clock);
        let wanted_tokens = clock.tokens(&case["tokens"]);
        // Background work is given time to finish, or to show up where none is due.
        let settled = timeout(Duration::from_secs(3), async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            loop {
                let mut seen = seen.lock().unwrap().clone();
                seen.sort_by_key(|entry| entry["call"].as_str().unwrap().to_owned());
                if same_calls(&seen, &wanted_calls) && stored(&store) == wanted_tokens {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            settled.is_ok(),
            "calls {:?} wanted {wanted_calls:?}\ntokens {} wanted {wanted_tokens}\n{case}",
            seen.lock().unwrap(),
            stored(&store),
        );
        // Stored JIDs are tracked for pruning, as upstream's onNewJidStored reports.
        let mut tracked: Vec<_> = case["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["call"] == "track")
            .map(|entry| entry["jid"].as_str().unwrap().to_owned())
            .collect();
        tracked.sort();
        tracked.dedup();
        let mut index = baileys_rs::privacy::Repository::new(store.clone())
            .read_index()
            .unwrap();
        index.sort();
        assert_eq!(index, tracked, "{case}");
        let mut updates = vec![];
        while let Ok(event) = events.try_recv() {
            if let Event::MessageUpdates(list) = event {
                updates.extend(list.into_iter().map(|update| {
                    let mut key = serde_json::Map::new();
                    if let Some(jid) = update.key.remote_jid {
                        key.insert("remoteJid".into(), json!(jid));
                    }
                    key.insert("fromMe".into(), json!(update.key.from_me.unwrap()));
                    if let Some(id) = update.key.id {
                        key.insert("id".into(), json!(id));
                    }
                    json!({"key": key, "update": {
                        "status": update.info.status.unwrap(),
                        "messageStubParameters": update.info.message_stub_parameters,
                    }})
                }));
            }
        }
        let wanted: Vec<_> = case["events"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|event| {
                assert_eq!(event["name"], "messages.update");
                event["data"].as_array().unwrap().clone()
            })
            .collect();
        assert_eq!(updates, wanted, "{case}");
        done.send(()).unwrap();
        server.await.unwrap();
        socket.close().await.ok();
    }
}
#[tokio::test]
async fn low_pre_key_counts_match_the_original_upload_decisions() {
    timeout(
        Duration::from_secs(60),
        replay("prekeys", Run::Notification),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn identity_changes_match_the_original_session_and_token_calls() {
    timeout(
        Duration::from_secs(60),
        replay("identities", Run::Notification),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn privacy_token_notifications_match_the_original_stored_tokens() {
    timeout(Duration::from_secs(60), replay("tokens", Run::Notification))
        .await
        .unwrap();
}
#[tokio::test]
async fn errored_acks_match_the_original_updates_and_token_recovery() {
    timeout(Duration::from_secs(60), replay("acks", Run::Ack))
        .await
        .unwrap();
}
