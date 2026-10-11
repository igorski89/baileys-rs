use baileys_rs::{
    binary::{self, BinaryNode, NodeContent},
    signal::{direct::Bundle, wire},
    usync::{self, Protocol, Query, User},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/usync.json")).unwrap()
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn node(f: &Value, key: &str) -> BinaryNode {
    binary::decode(&hex(f[key].as_str().unwrap())).unwrap()
}
#[test]
fn query_bytes_and_all_protocol_results_match_upstream() {
    let f = fixture();
    let response = node(&f, "response");
    for case in f["queries"].as_array().unwrap() {
        let query: Query = serde_json::from_value(case["config"].clone()).unwrap();
        assert_eq!(
            binary::encode(&query.stanza("sid").unwrap()).unwrap(),
            hex(case["wire"].as_str().unwrap())
        );
        let parsed = query.parse(&response).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&parsed).unwrap(), case["parsed"]);
        let reloaded: usync::ResultList =
            serde_json::from_value(serde_json::to_value(parsed).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(reloaded).unwrap(), case["parsed"]);
    }
    let bot = node(&f, "bot");
    let parsed = Protocol::Bot.parse(&bot).unwrap().unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), f["botParsed"]);
    assert!(Protocol::Bot.parse(&BinaryNode::new("bot")).is_err());
}
#[test]
fn device_selection_matches_hosted_domain_propagation_and_own_device_filtering() {
    let f = fixture();
    let query = Query::default().with_protocol(Protocol::Devices);
    let parsed = query.parse(&node(&f, "response")).unwrap().unwrap();
    for case in f["devices"].as_array().unwrap() {
        let selected = usync::extract_device_jids(
            &parsed.list,
            "100:7@s.whatsapp.net",
            "900",
            case["exclude"].as_bool().unwrap(),
        )
        .unwrap();
        let result:Vec<_>=selected.into_iter().map(|j|json!({"user":j.user,"server":j.server,"device":j.device,"domainType":j.domain_type})).collect();
        assert_eq!(json!(result), case["result"]);
    }
}
fn bundle_json(bundle: &Bundle) -> Value {
    let bytes = |b: &[u8]| json!({"type":"Buffer","data":STANDARD.encode(b)});
    let mut result = json!({"registrationId":bundle.registration_id,"identityKey":bytes(&bundle.identity),"signedPreKey":{"keyId":bundle.signed.id,"publicKey":bytes(&bundle.signed.public),"signature":bytes(&bundle.signature)}});
    if let Some(pre) = &bundle.pre_key {
        result["preKey"] = json!({"keyId":pre.id,"publicKey":bytes(&pre.public)});
    }
    result
}
#[test]
fn encrypt_iq_and_retry_bundles_match_upstream() {
    let f = fixture();
    let signal = &f["signal"];
    let bundles = wire::parse_session_bundles(&node(signal, "bundles")).unwrap();
    assert_eq!(bundles.len(), 1);
    assert_eq!(
        json!({"jid":bundles[0].jid,"session":bundle_json(&bundles[0].bundle)}),
        signal["injected"][0]
    );
    let retry = wire::extract_retry_bundle(&node(signal, "receipt")).unwrap();
    assert_eq!(bundle_json(&retry), signal["retry"]);
    let stanza = wire::session_query(&["200:1@s.whatsapp.net".into()]).unwrap();
    assert_eq!(
        stanza.child("key").unwrap().all_children()[0].attrs["jid"],
        "200:1@s.whatsapp.net"
    );
    assert!(wire::extract_retry_bundle(&BinaryNode::new("receipt")).is_none());
}
#[test]
fn rejects_empty_protocols_stanza_errors_and_malformed_keys() {
    assert!(Query::default().stanza("sid").is_err());
    let query = Query::default().with_protocol(Protocol::Contact);
    assert!(
        query
            .parse(&BinaryNode::new("iq").attr("type", "error"))
            .unwrap()
            .is_none()
    );
    let error = BinaryNode::new("contact").with_content(NodeContent::Children(vec![
        BinaryNode::new("error").attr("code", "403"),
    ]));
    assert_eq!(
        Protocol::Contact.parse(&error).unwrap_err().status_code(),
        Some(403)
    );
    let error = BinaryNode::new("iq").with_content(NodeContent::Children(vec![
        BinaryNode::new("list").with_content(NodeContent::Children(vec![
            BinaryNode::new("user").with_content(NodeContent::Children(vec![
                BinaryNode::new("error").attr("code", "404"),
            ])),
        ])),
    ]));
    assert!(wire::parse_session_bundles(&error).is_err());
    let malformed = BinaryNode::new("iq").with_content(NodeContent::Children(vec![
        BinaryNode::new("list").with_content(NodeContent::Children(vec![
            BinaryNode::new("user").attr("jid", "200:1@s.whatsapp.net"),
        ])),
    ]));
    assert!(wire::parse_session_bundles(&malformed).is_err());
    let contact = usync::contact_query(&["900@lid".into(), "+100:7@s.whatsapp.net".into()]);
    assert_eq!(contact.users.len(), 1);
    assert_eq!(contact.users[0].phone.as_deref(), Some("+100"));
    let username = Protocol::Username
        .parse(&BinaryNode::new("username").with_content(NodeContent::Text("sample".into())))
        .unwrap();
    assert!(matches!(username,Some(usync::Value::Text(s)) if s=="sample"));
    assert!(Protocol::Username.user_element(&User::default()).is_none());
}
