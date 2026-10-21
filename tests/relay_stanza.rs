use baileys_rs::{
    auth::*,
    binary::{self, BinaryNode},
    devices::Device,
    messages, privacy, proto, relay,
    signal::{self, group},
};
use prost::Message as _;
use serde_json::Value;
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/relay-stanza.json")).unwrap()
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn decode(v: &Value) -> BinaryNode {
    binary::decode(&hex(v.as_str().unwrap())).unwrap()
}
struct RetryOracle;
impl relay::Encryptor for RetryOracle {
    fn encrypt(&self, _: &str, data: &[u8]) -> Result<signal::direct::Encrypted, relay::Error> {
        Ok(signal::direct::Encrypted {
            kind: signal::direct::MessageType::Signal,
            ciphertext: data.to_vec(),
        })
    }
}
#[test]
fn assemblies_match_original_routing_identity_reporting_privacy_and_node_order() {
    for case in fixture()["assembled"].as_array().unwrap() {
        let config = &case["config"];
        let destination = config["destination"]
            .as_str()
            .unwrap_or("200@s.whatsapp.net");
        let message =
            proto::Message::decode(hex(case["message"].as_str().unwrap()).as_slice()).unwrap();
        let account = proto::AdvSignedDeviceIdentity::decode(
            hex(case["account"].as_str().unwrap()).as_slice(),
        )
        .unwrap();
        let options = relay::StanzaOptions {
            retry: config.get("participant").map(|p| relay::Retry {
                jid: p["jid"].as_str().unwrap().into(),
                count: p["count"].as_u64().unwrap() as u32,
            }),
            additional_attributes: config
                .get("attrs")
                .map(|v| serde_json::from_value(v.clone()).unwrap())
                .unwrap_or_default(),
            additional_nodes: if config["additional"].as_bool() == Some(true) {
                vec![BinaryNode::new("extra")]
            } else {
                vec![]
            },
        };
        let store = SqliteAuthStore::in_memory().unwrap();
        let repo = privacy::Repository::new(store.clone());
        if let Some(token) = config.get("token") {
            store
                .set(&[KeyMutation::put(
                    destination,
                    KeyValue::Tctoken(TcToken {
                        token: serde_json::from_value(token.clone()).unwrap(),
                        timestamp: config["timestamp"].as_str().map(Into::into),
                        sender_timestamp: Some(1700000000),
                        extra: Default::default(),
                    }),
                )])
                .unwrap();
        }
        let token = if relay::is_one_to_one(destination, &options).unwrap() {
            repo.relay_token(destination, 1700000000).unwrap()
        } else {
            None
        };
        let cleared = store
            .get(KeyCategory::Tctoken, &[destination.into()])
            .unwrap()
            .get(destination)
            .is_some_and(|v| matches!(v,KeyValue::Tctoken(t) if t.token.is_empty()));
        assert_eq!(cleared, case["cleared"].as_bool().unwrap());
        let report_bytes = messages::pad_with_byte(&message.encode_to_vec(), 8);
        let result = relay::assemble(relay::Stanza {
            destination,
            id: "STANZA-ID",
            message: &message,
            me: "100:7@s.whatsapp.net",
            options: &options,
            content: case["content"]
                .as_array()
                .unwrap()
                .iter()
                .map(decode)
                .collect(),
            participants: case["participants"]
                .as_array()
                .unwrap()
                .iter()
                .map(decode)
                .collect(),
            include_device_identity: config["identity"].as_bool().unwrap_or(false),
            account: Some(&account),
            reporting: config["report"]
                .as_bool()
                .unwrap_or(false)
                .then_some((&message, report_bytes.as_slice())),
            privacy_token: token.as_deref(),
            privacy_token_on_1_to_1: config["privacyEnabled"].as_bool().unwrap_or(false),
        })
        .unwrap();
        assert!(result.reporting_error.is_none());
        assert_eq!(
            binary::encode(&result.node).unwrap(),
            hex(case["wire"].as_str().unwrap()),
            "{config}"
        );
    }
}
#[test]
fn retry_payload_and_newsletter_bytes_match_original() {
    let fixture = fixture();
    for case in fixture["retries"].as_array().unwrap() {
        let message =
            proto::Message::decode(hex(case["message"].as_str().unwrap()).as_slice()).unwrap();
        let destination = case["destination"].as_str().unwrap();
        let distribution = (destination != "200@s.whatsapp.net"
            && !case["available"].as_array().unwrap().is_empty())
        .then_some([3, 4]);
        let payload = relay::retry_payload(
            destination,
            &message,
            case["recipient"].as_str().unwrap(),
            "100:7@s.whatsapp.net",
            Some("900:7@lid"),
            distribution.as_ref().map(|b| b.as_slice()),
        )
        .unwrap();
        assert_eq!(
            messages::pad_with_byte(&payload.encode_to_vec(), 8),
            hex(case["wire"].as_str().unwrap())
        );
        let retry = relay::Retry {
            jid: case["recipient"].as_str().unwrap().into(),
            count: 2,
        };
        let node = relay::encrypt_retry_with_byte(&retry, &payload, &RetryOracle, 8).unwrap();
        assert_eq!(
            binary::encode(&node).unwrap(),
            hex(case["node"].as_str().unwrap())
        );
    }
    for case in fixture["newsletters"].as_array().unwrap() {
        let message =
            proto::Message::decode(hex(case["message"].as_str().unwrap()).as_slice()).unwrap();
        let options = relay::StanzaOptions {
            additional_attributes: serde_json::from_value(case["attrs"].clone()).unwrap(),
            additional_nodes: vec![BinaryNode::new("extra")],
            ..Default::default()
        };
        let node =
            relay::newsletter_stanza("123@newsletter", "STANZA-ID", &message, &message, &options)
                .unwrap();
        assert_eq!(
            binary::encode(&node).unwrap(),
            hex(case["wire"].as_str().unwrap())
        );
    }
    for case in fixture["splits"].as_array().unwrap() {
        let devices: Vec<Device> = serde_json::from_value(case["devices"].clone()).unwrap();
        let recipients =
            relay::direct_recipients(&devices, "100:7@s.whatsapp.net", Some("900:7@lid")).unwrap();
        assert_eq!(
            serde_json::json!({"all":recipients.all,"own":recipients.own,"other":recipients.other}),
            case["expected"]
        );
    }
}
#[test]
fn retry_prefers_lid_sender_key_and_retains_corrupt_record_on_warning() {
    let store = SqliteAuthStore::in_memory().unwrap();
    let group = group::Repository::new(store.clone());
    let pn = signal::SenderKeyName::new("group@g.us", "100:7@s.whatsapp.net").unwrap();
    let lid = signal::SenderKeyName::new("group@g.us", "900:7@lid").unwrap();
    assert!(
        relay::retry_distribution(
            &group,
            "group@g.us",
            "100:7@s.whatsapp.net",
            Some("900:7@lid")
        )
        .unwrap()
        .bytes
        .is_none()
    );
    let pn_bytes = group.distribution(&pn).unwrap();
    assert_eq!(
        relay::retry_distribution(&group, "group@g.us", "100:7@s.whatsapp.net", Some(""))
            .unwrap()
            .bytes,
        Some(pn_bytes.clone())
    );
    assert_eq!(
        relay::retry_distribution(
            &group,
            "group@g.us",
            "100:7@s.whatsapp.net",
            Some("900:7@lid")
        )
        .unwrap()
        .bytes,
        Some(pn_bytes)
    );
    let lid_bytes = group.distribution(&lid).unwrap();
    assert_eq!(
        relay::retry_distribution(
            &group,
            "group@g.us",
            "100:7@s.whatsapp.net",
            Some("900:7@lid")
        )
        .unwrap()
        .bytes,
        Some(lid_bytes)
    );
    store
        .set(&[KeyMutation::put(
            lid.to_string(),
            KeyValue::SenderKey(vec![0xff]),
        )])
        .unwrap();
    let result = relay::retry_distribution(
        &group,
        "group@g.us",
        "100:7@s.whatsapp.net",
        Some("900:7@lid"),
    )
    .unwrap();
    assert!(result.warning.is_some());
    assert!(result.bytes.is_none());
    assert!(
        relay::retry_distribution(
            &group,
            "200@s.whatsapp.net",
            "100:7@s.whatsapp.net",
            Some("900:7@lid")
        )
        .unwrap()
        .bytes
        .is_none()
    );
    let reaction = proto::Message {
        reaction_message: Some(Default::default()),
        ..Default::default()
    };
    assert_eq!(
        relay::encrypted_attributes(&reaction)["decrypt-fail"],
        "hide"
    );
    // Not upstream's: a vote is hidden on failure, as a phone's votes are.
    let vote = proto::Message {
        poll_update_message: Some(Default::default()),
        ..Default::default()
    };
    assert_eq!(relay::encrypted_attributes(&vote)["decrypt-fail"], "hide");
    let image = proto::Message {
        image_message: Some(Default::default()),
        ..Default::default()
    };
    assert_eq!(relay::encrypted_attributes(&image)["mediatype"], "image");
    let invalid_retry = relay::StanzaOptions {
        retry: Some(relay::Retry {
            jid: "invalid".into(),
            count: 1,
        }),
        ..Default::default()
    };
    assert!(
        relay::newsletter_stanza("123@newsletter", "id", &image, &image, &invalid_retry).is_err()
    );
    let options = relay::StanzaOptions::default();
    assert!(matches!(
        relay::assemble(relay::Stanza {
            destination: "200@s.whatsapp.net",
            id: "id",
            message: &image,
            me: "100@s.whatsapp.net",
            options: &options,
            content: vec![],
            participants: vec![],
            include_device_identity: true,
            account: None,
            reporting: None,
            privacy_token: None,
            privacy_token_on_1_to_1: false
        }),
        Err(relay::Error::Identity)
    ));
}
