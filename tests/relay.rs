use baileys_rs::{
    binary, crypto, proto,
    relay::{self, Encryptor, Error, Fanout, Patch, PatchedMessage},
    signal::direct,
};
use indexmap::IndexMap;
use prost::Message as _;
use serde_json::{Value, json};
fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/relay.json")).unwrap()
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn message(v: &Value) -> proto::Message {
    proto::Message::decode(hex(v.as_str().unwrap()).as_slice()).unwrap()
}
struct Oracle {
    failures: Vec<String>,
    calls: std::sync::Mutex<Vec<Value>>,
}
impl Encryptor for Oracle {
    fn encrypt(&self, jid: &str, data: &[u8]) -> Result<direct::Encrypted, Error> {
        let encoded = data.iter().map(|b| format!("{b:02x}")).collect::<String>();
        self.calls
            .lock()
            .unwrap()
            .push(json!({"jid":jid,"data":encoded}));
        if self.failures.iter().any(|failed| failed == jid) {
            return Err(Error::Patch("synthetic cipher failure".into()));
        }
        Ok(direct::Encrypted {
            kind: if jid.contains(":2") {
                direct::MessageType::PreKey
            } else {
                direct::MessageType::Signal
            },
            ciphertext: data.to_vec(),
        })
    }
}
#[test]
fn fanout_patches_dsm_overrides_nodes_and_failures_match_upstream() {
    for case in fixtures()["fanouts"].as_array().unwrap() {
        let config = &case["config"];
        let recipients =
            serde_json::from_value::<Vec<String>>(config["recipients"].clone()).unwrap();
        let attrs = config
            .get("extra")
            .map(|v| serde_json::from_value::<IndexMap<String, String>>(v.clone()).unwrap())
            .unwrap_or_default();
        let original = message(&case["message"]);
        let dsm = message(&case["dsm"]);
        let oracle = Oracle {
            failures: config
                .get("failures")
                .map(|v| serde_json::from_value(v.clone()).unwrap())
                .unwrap_or_default(),
            calls: Default::default(),
        };
        let calls = std::cell::Cell::new(0);
        let result = relay::create_participant_nodes_with(
            Fanout {
                recipients: &recipients,
                message: &original,
                me: "100:7@s.whatsapp.net",
                me_lid: Some("900:7@lid"),
                extra_attrs: &attrs,
                device_sent_message: config["dsm"].as_bool().unwrap_or(false).then_some(&dsm),
            },
            &oracle,
            |original, _| {
                calls.set(calls.get() + 1);
                if config["patchError"].as_bool().unwrap_or(false) {
                    return Err(Error::Patch("synthetic patch failure".into()));
                }
                if let Some(patches) = config.get("perRecipient") {
                    return Ok(Patch::PerRecipient(
                        patches
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|p| PatchedMessage {
                                recipient_jid: p["recipientJid"].as_str().map(str::to_owned),
                                message: message(&p["wire"]),
                            })
                            .collect(),
                    ));
                }
                Ok(Patch::Shared(Box::new(original.clone())))
            },
            || Ok(4),
        );
        assert_eq!(calls.get(), case["patches"].as_u64().unwrap());
        assert_eq!(json!(*oracle.calls.lock().unwrap()), case["calls"]);
        if !case["error"].is_null() {
            let error = result.unwrap_err();
            assert_eq!(json!(error.status_code()), case["error"]["status"]);
            if let Error::AllEncryptionsFailed { failures } = error {
                assert_eq!(
                    json!(failures.iter().map(|f| &f.jid).collect::<Vec<_>>()),
                    case["failures"]
                );
            }
        } else {
            let result = result.unwrap();
            assert_eq!(
                result.should_include_device_identity,
                case["shouldIncludeDeviceIdentity"]
            );
            let wires = result
                .nodes
                .iter()
                .map(|n| binary::encode(n).unwrap())
                .collect::<Vec<_>>();
            let expected = case["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| hex(v.as_str().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(wires, expected);
            assert_eq!(
                json!(result.failures.iter().map(|f| &f.jid).collect::<Vec<_>>()),
                case["failures"]
            );
        }
    }
}
#[test]
fn normalization_types_hashes_identity_and_newsletters_match_upstream() {
    let fixture = fixtures();
    for case in fixture["classifications"].as_array().unwrap() {
        let content = case["wire"].as_str().map(|_| message(&case["wire"]));
        assert_eq!(relay::message_type(content.as_ref()), case["messageType"]);
        assert_eq!(
            content.as_ref().map(relay::media_type).unwrap_or_default(),
            case["mediaType"]
        );
        let normalized = relay::normalize(content.as_ref()).map(|m| m.encode_to_vec());
        assert_eq!(normalized, case["normalized"].as_str().map(hex));
    }
    for case in fixture["hashes"].as_array().unwrap() {
        let mut participants: Vec<String> = serde_json::from_value(case["input"].clone()).unwrap();
        assert_eq!(relay::participant_hash_v2(&mut participants), case["hash"]);
        assert_eq!(json!(participants), case["sorted"]);
    }
    for case in fixture["identityCases"].as_array().unwrap() {
        let account = proto::AdvSignedDeviceIdentity::decode(
            hex(case["account"].as_str().unwrap()).as_slice(),
        )
        .unwrap();
        assert_eq!(
            relay::encode_device_identity(&account, case["include"].as_bool().unwrap()),
            hex(case["wire"].as_str().unwrap())
        );
    }
    let m = message(&fixture["fanouts"][0]["message"]);
    assert_eq!(
        relay::encode_newsletter_message(&m),
        hex(fixture["newsletter"].as_str().unwrap())
    );
    let wrapped = relay::device_sent_message("200@s.whatsapp.net", &m);
    let dsm = wrapped.device_sent_message.unwrap();
    assert_eq!(dsm.destination_jid.as_deref(), Some("200@s.whatsapp.net"));
    assert_eq!(dsm.message.as_deref(), Some(&m));
    assert_eq!(wrapped.message_context_info, m.message_context_info);
}

#[test]
fn native_fanout_decrypts_payloads_dsm_and_retains_successful_recipients() {
    use baileys_rs::{
        auth::{Credentials, SqliteAuthStore},
        messages,
    };
    fn repository() -> (SqliteAuthStore, direct::Repository, Credentials) {
        let store = SqliteAuthStore::in_memory().unwrap();
        let credentials = Credentials::generate().unwrap();
        store.save_credentials(&credentials).unwrap();
        (store.clone(), direct::Repository::new(store), credentials)
    }
    fn bundle(c: &Credentials) -> direct::Bundle {
        direct::Bundle {
            registration_id: c.registration_id.into(),
            identity: crypto::signal_public_key(&c.signed_identity_key.public)
                .unwrap()
                .to_vec(),
            signed: direct::PublicPreKey {
                id: c.signed_pre_key.key_id,
                public: crypto::signal_public_key(&c.signed_pre_key.key_pair.public)
                    .unwrap()
                    .to_vec(),
            },
            signature: c.signed_pre_key.signature.clone(),
            pre_key: None,
        }
    }
    let (_, alice, _) = repository();
    let (_, bob, bc) = repository();
    let (_, own, oc) = repository();
    alice
        .inject_session("200:1@s.whatsapp.net", &bundle(&bc))
        .unwrap();
    alice
        .inject_session("100:2@s.whatsapp.net", &bundle(&oc))
        .unwrap();
    let content = proto::Message {
        conversation: Some("fanout".into()),
        ..Default::default()
    };
    let dsm = relay::device_sent_message("200@s.whatsapp.net", &content);
    let recipients = vec![
        "200:1@s.whatsapp.net".into(),
        "300:1@s.whatsapp.net".into(),
        "100:2@s.whatsapp.net".into(),
    ];
    let nodes = relay::create_participant_nodes(
        Fanout {
            recipients: &recipients,
            message: &content,
            me: "100:7@s.whatsapp.net",
            me_lid: None,
            extra_attrs: &IndexMap::new(),
            device_sent_message: Some(&dsm),
        },
        &alice,
    )
    .unwrap();
    assert!(nodes.should_include_device_identity);
    assert_eq!(nodes.nodes.len(), 2);
    assert_eq!(nodes.failures.len(), 1);
    assert_eq!(nodes.failures[0].jid, "300:1@s.whatsapp.net");
    for node in nodes.nodes {
        let enc = node.child("enc").unwrap();
        let bytes = node.child_buffer("enc").unwrap();
        assert_eq!(enc.attrs["type"], "pkmsg");
        let (repo, expected) = if node.attrs["jid"] == "200:1@s.whatsapp.net" {
            (&bob, &content)
        } else {
            (&own, &dsm)
        };
        let plain = repo
            .decrypt("100:7@s.whatsapp.net", direct::MessageType::PreKey, bytes)
            .unwrap();
        assert_eq!(
            proto::Message::decode(messages::unpad(&plain).unwrap()).unwrap(),
            *expected
        );
    }
}
