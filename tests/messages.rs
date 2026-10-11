use baileys_rs::{
    auth::{Credentials, KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    binary::{self, BinaryNode, NodeContent},
    crypto,
    messages::{self, Decryptor, Error, SignalDecryptor},
    proto,
    signal::{self, SenderKeyName, direct},
};
use prost::Message as _;
use serde_json::Value;

fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/messages.json")).unwrap()
}
fn node(v: &Value) -> BinaryNode {
    binary::decode(&hex(v["wire"].as_str().unwrap())).unwrap()
}
#[test]
fn matches_upstream_envelopes_and_addressing_context() {
    let f = fixture();
    for case in f["envelopes"].as_array().unwrap() {
        let envelope = messages::decode_node(
            &node(case),
            f["me"].as_str().unwrap(),
            f["lid"].as_str().unwrap(),
        );
        if case.get("error").is_some() {
            assert!(envelope.is_err());
            continue;
        }
        let envelope = envelope.unwrap();
        assert_eq!(
            envelope.message.encode_to_vec(),
            hex(case["message"].as_str().unwrap())
        );
        assert_eq!(
            serde_json::to_value(envelope.key_metadata).unwrap(),
            case["metadata"]
        );
        assert_eq!(envelope.author, case["author"]);
        assert_eq!(envelope.sender, case["sender"]);
    }
    let missing_time = BinaryNode::new("message")
        .attr("id", "A")
        .attr("from", "200@s.whatsapp.net");
    assert!(messages::decode_node(&missing_time, "100@s.whatsapp.net", "900@lid").is_err());
}

struct Oracle {
    calls: std::sync::Mutex<Vec<Value>>,
}
impl Decryptor for Oracle {
    fn decrypt_direct(
        &self,
        author: &str,
        kind: direct::MessageType,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.calls.lock().unwrap().push(serde_json::json!({"kind":if kind == direct::MessageType::PreKey {"pkmsg"} else {"msg"},"author":author}));
        Ok(ciphertext.into())
    }
    fn decrypt_group(
        &self,
        sender: &str,
        author: &str,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        self.calls
            .lock()
            .unwrap()
            .push(serde_json::json!({"kind":"skmsg","group":sender,"author":author}));
        Ok(ciphertext.into())
    }
    fn process_distribution(
        &self,
        author: &str,
        distribution: &proto::message::SenderKeyDistributionMessage,
    ) -> Result<(), Error> {
        self.calls.lock().unwrap().push(serde_json::json!({"kind":"distribution","group":distribution.group_id,"author":author}));
        Ok(())
    }
}
#[test]
fn matches_upstream_payload_unwrapping_shallow_merge_and_stubs() {
    let f = fixture();
    for case in f["decryptions"].as_array().unwrap() {
        let oracle = Oracle {
            calls: Default::default(),
        };
        let decrypted = messages::decrypt_node(
            &node(case),
            f["me"].as_str().unwrap(),
            f["lid"].as_str().unwrap(),
            &oracle,
        )
        .unwrap();
        let mut expected =
            proto::WebMessageInfo::decode(hex(case["message"].as_str().unwrap()).as_slice())
                .unwrap();
        let mut actual = decrypted.envelope.message;
        if expected.message_stub_type.is_some() {
            assert!(!actual.message_stub_parameters.is_empty());
            // Prost and protobufjs use different diagnostic text for invalid protobufs/types.
            expected.message_stub_parameters.clear();
            actual.message_stub_parameters.clear();
        }
        assert_eq!(actual, expected);
        assert_eq!(
            serde_json::to_value(decrypted.envelope.key_metadata).unwrap(),
            case["metadata"]
        );
        assert_eq!(
            serde_json::to_value(decrypted.envelope.retry_count).unwrap(),
            case["retryCount"]
        );
        assert_eq!(
            *oracle.calls.lock().unwrap(),
            *case["calls"].as_array().unwrap()
        );
        assert!(decrypted.distribution_errors.is_empty());
    }
}

#[test]
fn matches_upstream_ids_and_padding_including_source_edge_behavior() {
    let f = fixture();
    for case in f["ids"].as_array().unwrap() {
        assert_eq!(
            messages::id_v2_with(case["user"].as_str(), 1700000000, &[7; 16]).unwrap(),
            case["id"]
        );
    }
    for case in f["pads"].as_array().unwrap() {
        let padded = messages::pad_with_byte(&[1, 2, 3], case["byte"].as_u64().unwrap() as u8);
        assert_eq!(padded, hex(case["wire"].as_str().unwrap()));
        assert_eq!(messages::unpad(&padded).unwrap(), [1, 2, 3]);
    }
    assert!(messages::unpad(&[]).is_err());
    assert!(messages::unpad(&[1, 4]).is_err());
    assert_eq!(messages::unpad(&[1, 0]).unwrap(), [1, 0]);
    assert_eq!(messages::unpad(&[1, 99, 2]).unwrap(), [1]);
    assert!(messages::unpad(&[17; 17]).unwrap().is_empty());
    assert!(messages::id_v2_with(Some(&format!("{}@lid", "a".repeat(40))), 0, &[0; 16]).is_err());
    let a = messages::generate_id().unwrap();
    let b = messages::generate_id().unwrap();
    assert!(a.starts_with("3EB0"));
    assert_eq!(a.len(), 40);
    assert_ne!(a, b);
    assert_eq!(messages::generate_id_v2(None).unwrap().len(), 22);
}

fn credentials(identity: u8, signed: u8, pre: u8) -> (SqliteAuthStore, Credentials) {
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = Credentials::generate().unwrap();
    creds.signed_identity_key = crypto::KeyPair::from_private([identity; 32]);
    creds.signed_pre_key = crypto::signed_key_pair(&creds.signed_identity_key, 5).unwrap();
    creds.signed_pre_key.key_pair = crypto::KeyPair::from_private([signed; 32]);
    creds.signed_pre_key.signature = creds
        .signed_identity_key
        .sign(&crypto::signal_public_key(&creds.signed_pre_key.key_pair.public).unwrap())
        .to_vec();
    store.save_credentials(&creds).unwrap();
    store
        .set(&[KeyMutation::put(
            "7",
            KeyValue::PreKey(crypto::KeyPair::from_private([pre; 32])),
        )])
        .unwrap();
    (store, creds)
}
fn message_node(from: &str, participant: Option<&str>, kind: &str, bytes: Vec<u8>) -> BinaryNode {
    let mut node = BinaryNode::new("message")
        .attr("id", "ABC")
        .attr("t", "1700000000")
        .attr("from", from);
    if let Some(participant) = participant {
        node = node.attr("participant", participant);
    }
    node.with_content(NodeContent::Children(vec![
        BinaryNode::new("enc")
            .attr("type", kind)
            .with_content(NodeContent::Bytes(bytes)),
    ]))
}

#[test]
fn native_signal_decrypts_direct_distributions_groups_and_rejects_replay() {
    let (a, _) = credentials(10, 11, 12);
    let (b, bc) = credentials(20, 21, 22);
    let alice = direct::Repository::new(a.clone());
    let bundle = direct::Bundle {
        registration_id: bc.registration_id.into(),
        identity: crypto::signal_public_key(&bc.signed_identity_key.public)
            .unwrap()
            .to_vec(),
        signed: direct::PublicPreKey {
            id: 5,
            public: crypto::signal_public_key(&bc.signed_pre_key.key_pair.public)
                .unwrap()
                .to_vec(),
        },
        signature: bc.signed_pre_key.signature.clone(),
        pre_key: Some(direct::PublicPreKey {
            id: 7,
            public: crypto::signal_public_key(&crypto::KeyPair::from_private([22; 32]).public)
                .unwrap()
                .to_vec(),
        }),
    };
    alice
        .inject_session("200:1@s.whatsapp.net", &bundle)
        .unwrap();
    let bob = SignalDecryptor::new(b.clone());
    let message = proto::Message {
        conversation: Some("hello encrypted peer".into()),
        ..Default::default()
    };
    let enc = alice
        .encrypt("200:1@s.whatsapp.net", &messages::encode(&message).unwrap())
        .unwrap();
    assert_eq!(enc.kind, direct::MessageType::PreKey);
    let node = message_node("100:1@s.whatsapp.net", None, "pkmsg", enc.ciphertext);
    let result = messages::decrypt_node(&node, "200@s.whatsapp.net", "800@lid", &bob).unwrap();
    assert_eq!(result.envelope.message.message, Some(message));
    assert!(result.envelope.message.message_stub_type.is_none());
    assert!(
        b.get(KeyCategory::PreKey, &["7".into()])
            .unwrap()
            .is_empty()
    );
    let replay = messages::decrypt_node(&node, "200@s.whatsapp.net", "800@lid", &bob).unwrap();
    assert_eq!(
        replay.envelope.message.message_stub_type,
        Some(proto::web_message_info::StubType::Ciphertext as i32)
    );

    let name = SenderKeyName::new("123@g.us", "100:1@s.whatsapp.net").unwrap();
    let group = signal::group::Repository::new(a);
    let group_message = proto::Message {
        conversation: Some("hello encrypted group".into()),
        ..Default::default()
    };
    let encrypted = group
        .encrypt(&name, &messages::encode(&group_message).unwrap())
        .unwrap();
    let distribution = proto::Message {
        sender_key_distribution_message: Some(proto::message::SenderKeyDistributionMessage {
            group_id: Some("123@g.us".into()),
            axolotl_sender_key_distribution_message: Some(encrypted.distribution),
        }),
        ..Default::default()
    };
    let enc = alice
        .encrypt(
            "200:1@s.whatsapp.net",
            &messages::encode(&distribution).unwrap(),
        )
        .unwrap();
    let mut node = message_node(
        "123@g.us",
        Some("100:1@s.whatsapp.net"),
        if enc.kind == direct::MessageType::PreKey {
            "pkmsg"
        } else {
            "msg"
        },
        enc.ciphertext,
    );
    let Some(NodeContent::Children(children)) = &mut node.content else {
        unreachable!()
    };
    children.push(
        BinaryNode::new("enc")
            .attr("type", "skmsg")
            .with_content(NodeContent::Bytes(encrypted.ciphertext)),
    );
    let result = messages::decrypt_node(&node, "200@s.whatsapp.net", "800@lid", &bob).unwrap();
    assert!(result.distribution_errors.is_empty());
    assert!(result.envelope.message.message_stub_type.is_none());
    let message = result.envelope.message.message.unwrap();
    assert_eq!(message.conversation, group_message.conversation);
    assert_eq!(
        message.sender_key_distribution_message,
        distribution.sender_key_distribution_message
    );
    assert!(bob.group.has_sender_key(&name).unwrap());
    // Clear Alice's pending prekey with a reply, so a recreated session could not
    // accidentally mask a lost session during envelope migration.
    let reply = bob
        .direct
        .encrypt("100:1@s.whatsapp.net", b"acknowledged")
        .unwrap();
    alice
        .decrypt("200:1@s.whatsapp.net", reply.kind, &reply.ciphertext)
        .unwrap();
    b.set(&[KeyMutation::put(
        "100",
        KeyValue::DeviceList(vec!["1".into()]),
    )])
    .unwrap();
    let message = proto::Message {
        conversation: Some("ordinary ratchet after LID migration".into()),
        ..Default::default()
    };
    let encrypted = alice
        .encrypt("200:1@s.whatsapp.net", &messages::encode(&message).unwrap())
        .unwrap();
    assert_eq!(encrypted.kind, direct::MessageType::Signal);
    let node = message_node("100:1@s.whatsapp.net", None, "msg", encrypted.ciphertext)
        .attr("sender_lid", "900@lid");
    let result = messages::decrypt_node(&node, "200@s.whatsapp.net", "800@lid", &bob).unwrap();
    assert!(result.mapping_errors.is_empty());
    assert!(result.envelope.message.message_stub_type.is_none());
    assert_eq!(result.envelope.message.message, Some(message));
    assert!(
        b.get(KeyCategory::Session, &["100.1".into()])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        b.get(KeyCategory::Session, &["900_1.1".into()])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        bob.direct
            .lid_mapping()
            .get_lid_for_pn("100:1@s.whatsapp.net")
            .unwrap()
            .as_deref(),
        Some("900:1@lid")
    );
    // Existing mappings win over an alternative address on a later envelope.
    let encrypted = alice
        .encrypt(
            "200:1@s.whatsapp.net",
            &messages::encode(&proto::Message {
                conversation: Some("follow-up".into()),
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();
    let node = message_node("100:1@s.whatsapp.net", None, "msg", encrypted.ciphertext)
        .attr("sender_lid", "901@lid");
    let result = messages::decrypt_node(&node, "200@s.whatsapp.net", "800@lid", &bob).unwrap();
    assert!(result.envelope.message.message_stub_type.is_none());
    assert_eq!(
        bob.direct
            .lid_mapping()
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .as_deref(),
        Some("900@lid")
    );
}

#[test]
fn failed_distribution_is_reported_without_discarding_decoded_content() {
    let (store, _) = credentials(30, 31, 32);
    let message = proto::Message {
        conversation: Some("visible content".into()),
        sender_key_distribution_message: Some(Default::default()),
        ..Default::default()
    };
    let mut node = message_node(
        "100:1@s.whatsapp.net",
        None,
        "plaintext",
        message.encode_to_vec(),
    );
    // An explicit plaintext tag also exercises the newsletter/plaintext path.
    if let Some(NodeContent::Children(children)) = &mut node.content {
        children[0].tag = "plaintext".into();
    }
    let result = messages::decrypt_node(
        &node,
        "200@s.whatsapp.net",
        "800@lid",
        &SignalDecryptor::new(store),
    )
    .unwrap();
    assert_eq!(result.envelope.message.message, Some(message));
    assert!(result.envelope.message.message_stub_type.is_none());
    assert_eq!(result.distribution_errors.len(), 1);
    assert!(
        format!("{:?}", result.envelope)
            .find("visible content")
            .is_none()
    );
}

#[test]
fn failed_envelope_migration_reports_warning_and_preserves_source_mapping_order() {
    let (store, _) = credentials(30, 31, 32);
    store
        .set(&[
            KeyMutation::put("100", KeyValue::DeviceList(vec!["1".into()])),
            KeyMutation::put(
                "100.1",
                KeyValue::Session(serde_json::json!({"version":"invalid"})),
            ),
        ])
        .unwrap();
    let repo = SignalDecryptor::new(store.clone());
    let plaintext = proto::Message {
        conversation: Some("plaintext does not learn mappings".into()),
        ..Default::default()
    };
    let mut plain = message_node(
        "100:1@s.whatsapp.net",
        None,
        "plaintext",
        plaintext.encode_to_vec(),
    )
    .attr("sender_lid", "900@lid");
    if let Some(NodeContent::Children(children)) = &mut plain.content {
        children[0].tag = "plaintext".into();
    }
    let result = messages::decrypt_node(&plain, "200@s.whatsapp.net", "800@lid", &repo).unwrap();
    assert!(result.mapping_errors.is_empty());
    assert!(
        repo.direct
            .lid_mapping()
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .is_none()
    );
    let node =
        message_node("100:1@s.whatsapp.net", None, "msg", vec![1]).attr("sender_lid", "900@lid");
    let result = messages::decrypt_node(&node, "200@s.whatsapp.net", "800@lid", &repo).unwrap();
    assert_eq!(result.mapping_errors.len(), 1);
    assert!(result.envelope.message.message_stub_type.is_some());
    assert_eq!(
        repo.direct
            .lid_mapping()
            .get_lid_for_pn("100@s.whatsapp.net")
            .unwrap()
            .as_deref(),
        Some("900@lid")
    );
    assert!(
        store
            .get(KeyCategory::Session, &["100.1".into()])
            .unwrap()
            .contains_key("100.1")
    );
    assert!(
        store
            .get(KeyCategory::Session, &["900_1.1".into()])
            .unwrap()
            .is_empty()
    );
}
