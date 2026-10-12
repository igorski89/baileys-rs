#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    socket::{Config, Event, ServerProps, Socket, startup},
};
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
fn node(value: &Value) -> BinaryNode {
    let mut result = BinaryNode::new(value["tag"].as_str().unwrap());
    result.attrs = serde_json::from_value(value["attrs"].clone()).unwrap();
    if let Some(content) = value["content"].as_array() {
        result.content = Some(NodeContent::Children(content.iter().map(node).collect()));
    }
    result
}
fn value(node: &BinaryNode) -> Value {
    let mut result = json!({"tag":node.tag,"attrs":node.attrs});
    if let Some(NodeContent::Children(children)) = &node.content {
        result["content"] = json!(children.iter().map(value).collect::<Vec<_>>());
    }
    result
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/startup.json")).unwrap()
}
fn props_value(props: &startup::Properties) -> Value {
    json!(
        props
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
            .collect::<indexmap::IndexMap<_, _>>()
    )
}
fn flags(props: ServerProps) -> Value {
    json!({"privacyTokenOn1to1":props.privacy_token_on_1_to_1,"profilePicPrivacyToken":props.profile_pic_privacy_token,"lidTrustedTokenIssueToLid":props.lid_trusted_token_issue_to_lid})
}
#[test]
fn startup_iqs_dictionaries_and_flag_precedence_match_original_helpers() {
    for case in fixture()["cases"].as_array().unwrap() {
        let response = node(&case["response"]);
        if case["kind"] == "props" {
            assert_eq!(
                value(&startup::props_query(case["lastHash"].as_str())),
                case["expected"]["requests"][0]
            );
            let props = startup::extract_props(&response);
            assert_eq!(props_value(&props), case["expected"]["result"]);
            let mut state = ServerProps::default();
            state.apply(
                &props
                    .iter()
                    .filter_map(|(k, v)| v.as_ref().map(|v| (k.clone(), v.clone())))
                    .collect(),
            );
            assert_eq!(flags(state), case["expected"]["serverProps"]);
        } else {
            assert_eq!(
                value(&startup::blocklist_query()),
                case["expected"]["requests"][0]
            );
            assert_eq!(
                json!(startup::extract_blocklist(&response)),
                case["expected"]["result"]
            );
        }
    }
}
fn store() -> SqliteAuthStore {
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100@s.whatsapp.net".into(),
        ..Default::default()
    });
    creds.last_prop_hash = Some("old".into());
    store.save_credentials(&creds).unwrap();
    store
}
async fn setup(enabled: bool) -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        fire_init_queries: enabled,
        query_timeout: Duration::from_secs(2),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        ..Default::default()
    };
    (listener, config)
}
#[tokio::test]
async fn fetched_hashes_are_persisted_and_reused_without_dropping_optional_values() {
    timeout(Duration::from_secs(15), async {
        let fixture = fixture();
        let cases = fixture["cases"].as_array().unwrap().clone();
        let responses = cases.clone();
        let auth = store();
        let (listener, config) = setup(false).await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            for case in responses {
                let mut request = peer.read().await;
                let id = request.attrs.shift_remove("id").unwrap();
                assert_eq!(value(&request), case["expected"]["requests"][0]);
                peer.send(
                    node(&case["response"])
                        .attr("id", id)
                        .attr("type", "result"),
                )
                .await;
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, auth.clone()).await.unwrap();
        for case in cases {
            let mut creds = auth.load_credentials().unwrap();
            creds.last_prop_hash = case["lastHash"].as_str().map(str::to_owned);
            auth.save_credentials(&creds).unwrap();
            socket
                .apply_server_props(&indexmap::IndexMap::from([
                    ("10518".into(), "true".into()),
                    ("9666".into(), "true".into()),
                    ("14303".into(), "false".into()),
                ]))
                .unwrap();
            if case["kind"] == "props" {
                assert_eq!(
                    props_value(&socket.fetch_props().await.unwrap()),
                    case["expected"]["result"]
                );
                assert_eq!(
                    flags(socket.server_props().unwrap()),
                    case["expected"]["serverProps"]
                );
                assert_eq!(
                    json!(auth.load_credentials().unwrap().last_prop_hash),
                    case["expected"]["hash"]
                );
                let updates = std::iter::from_fn(|| events.try_recv().ok())
                    .filter(|e| matches!(e, Event::CredentialsUpdated))
                    .count();
                assert_eq!(
                    updates,
                    case["expected"]["events"].as_array().unwrap().len()
                );
            } else {
                assert_eq!(
                    json!(socket.fetch_blocklist().await.unwrap()),
                    case["expected"]["result"]
                );
            }
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn startup_queries_run_on_open_and_survive_one_query_failure() {
    timeout(Duration::from_secs(12),async {
        for automatic in [true,false] {
            let auth=store();let(listener,config)=setup(automatic).await;
            let(server_ready,ready)=tokio::sync::oneshot::channel();
            let(release,wait)=tokio::sync::oneshot::channel();
            let server=tokio::spawn(async move {
                let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;
                peer.send(BinaryNode::new("success")).await;assert_eq!(peer.read().await.tag,"ib");
                let mut requests=vec![];for _ in 0..3 {requests.push(peer.read().await);}
                let names:std::collections::BTreeSet<_>=requests.iter().map(|n|n.attrs["xmlns"].as_str()).collect();assert_eq!(names,std::collections::BTreeSet::from(["abt","blocklist","privacy"]));
                let props=requests.iter().find(|r|r.attrs["xmlns"]=="abt").unwrap();assert_eq!(props.child("props").unwrap().attrs["hash"],"old");
                server_ready.send(()).unwrap();
                if !automatic {peer.send(BinaryNode::new("iq").attr("type","error").attr("id",props.attrs["id"].clone()).with_content(NodeContent::Children(vec![BinaryNode::new("error").attr("code","500")]))).await;}
                wait.await.unwrap();
                for request in requests {
                    let content=match request.attrs["xmlns"].as_str() {
                        "abt" if automatic=>BinaryNode::new("props").attr("hash","new").with_content(NodeContent::Children(vec![BinaryNode::new("prop").attr("name","10518").attr("value","false")])),
                        "abt"=>continue,
                        "privacy"=>BinaryNode::new("privacy").with_content(NodeContent::Children(vec![BinaryNode::new("category").attr("name","readreceipts").attr("value","none")])),
                        _=>BinaryNode::new("list"),
                    };
                    peer.send(BinaryNode::new("iq").attr("type","result").attr("id",request.attrs["id"].clone()).with_content(NodeContent::Children(vec![content]))).await;
                }
                let probe=peer.read().await;assert_eq!(probe.attrs["xmlns"],"probe");peer.result(&probe).await;peer.closed().await;
            });
            let(socket,mut events)=Socket::connect(config,auth.clone()).await.unwrap();socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
            if automatic {ready.await.unwrap();release.send(()).unwrap();
                loop {if matches!(events.recv().await.unwrap(),Event::CredentialsUpdated)&&auth.load_credentials().unwrap().last_prop_hash.as_deref()==Some("new"){break;}}
                while socket.server_props().unwrap().privacy_token_on_1_to_1 { tokio::task::yield_now().await; }
            }else {let mut operation=Box::pin(socket.execute_init_queries());tokio::select!{biased;result=ready=>result.unwrap(),_=&mut operation=>panic!("queries completed before all requests arrived")};assert!(operation.await.is_err());release.send(()).unwrap();}
            socket.query(BinaryNode::new("iq").attr("type","get").attr("xmlns","probe"),None).await.unwrap();tokio::task::yield_now().await;
            assert_eq!(socket.fetch_privacy_settings(false).await.unwrap().get("readreceipts"),Some(&Some("none".into())));
            socket.close().await.unwrap();server.await.unwrap();
        }
    }).await.unwrap();
}
