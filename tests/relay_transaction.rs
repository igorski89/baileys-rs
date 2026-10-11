use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto,
    devices::Device,
    jid, messages, proto,
    relay::{self, Encryptor},
    signal::{self, direct, group},
};
use prost::Message as _;
fn setup() -> (SqliteAuthStore, direct::Repository, Credentials) {
    let store = SqliteAuthStore::in_memory().unwrap();
    let creds = store.load_credentials().unwrap();
    (store.clone(), direct::Repository::new(store), creds)
}
fn bundle(creds: &Credentials) -> direct::Bundle {
    direct::Bundle {
        registration_id: creds.registration_id.into(),
        identity: crypto::signal_public_key(&creds.signed_identity_key.public)
            .unwrap()
            .to_vec(),
        signed: direct::PublicPreKey {
            id: creds.signed_pre_key.key_id,
            public: crypto::signal_public_key(&creds.signed_pre_key.key_pair.public)
                .unwrap()
                .to_vec(),
        },
        signature: creds.signed_pre_key.signature.clone(),
        pre_key: None,
    }
}
fn device(jid: &str) -> Device {
    jid::Jid::parse(jid).unwrap().into()
}
fn enc(node: &BinaryNode) -> (direct::MessageType, &[u8]) {
    let enc = node.child("enc").unwrap();
    let kind = if enc.attrs["type"] == "pkmsg" {
        direct::MessageType::PreKey
    } else {
        direct::MessageType::Signal
    };
    let Some(NodeContent::Bytes(bytes)) = &enc.content else {
        panic!("missing cipher")
    };
    (kind, bytes)
}
fn receive(repo: &direct::Repository, node: &BinaryNode) -> proto::Message {
    let (kind, bytes) = enc(node);
    let clear = repo.decrypt("100:7@s.whatsapp.net", kind, bytes).unwrap();
    proto::Message::decode(messages::unpad(&clear).unwrap()).unwrap()
}
#[test]
fn group_batch_distributes_keys_once_and_decrypts_with_persistent_receivers() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteAuthStore::open(dir.path()).unwrap();
    let alice = direct::Repository::new(store.clone());
    let (bs, bob, bc) = setup();
    let (cs, carol, cc) = setup();
    alice
        .inject_session("200:1@s.whatsapp.net", &bundle(&bc))
        .unwrap();
    alice
        .inject_session("300:1@s.whatsapp.net", &bundle(&cc))
        .unwrap();
    let devices = vec![
        device("200:1@s.whatsapp.net"),
        device("300:1@s.whatsapp.net"),
        device("200:99@s.whatsapp.net"),
        device("400:1@hosted"),
        device("500:1@hosted.lid"),
    ];
    let targets = vec!["200:1@s.whatsapp.net".into(), "300:1@s.whatsapp.net".into()];
    let attrs = indexmap::IndexMap::new();
    let repo = relay::Repository::new(store.clone());
    let sender = signal::SenderKeyName::new("group@g.us", "100:7@s.whatsapp.net").unwrap();
    let bg = group::Repository::new(bs);
    let cg = group::Repository::new(cs);
    for iteration in 0..2 {
        let message = proto::Message {
            conversation: Some(format!("group {iteration}")),
            ..Default::default()
        };
        let prepared = repo
            .transaction(&targets, |tx| {
                tx.group_fanout(relay::GroupFanout {
                    destination: "group@g.us",
                    sender: "100:7@s.whatsapp.net",
                    message: &message,
                    devices: &devices,
                    me: "100:7@s.whatsapp.net",
                    me_lid: None,
                    extra_attrs: &attrs,
                })
            })
            .unwrap();
        if iteration == 0 {
            assert_eq!(prepared.participants.nodes.len(), 2);
            assert!(prepared.participants.should_include_device_identity);
            for (receiver, group, node) in [
                (&bob, &bg, &prepared.participants.nodes[0]),
                (&carol, &cg, &prepared.participants.nodes[1]),
            ] {
                let payload = receive(receiver, node);
                let distribution = payload.sender_key_distribution_message.unwrap();
                assert_eq!(distribution.group_id.as_deref(), Some("group@g.us"));
                group
                    .process_distribution(
                        &sender,
                        distribution
                            .axolotl_sender_key_distribution_message
                            .as_deref()
                            .unwrap(),
                    )
                    .unwrap();
            }
        } else {
            assert!(prepared.participants.nodes.is_empty());
        }
        let Some(NodeContent::Bytes(cipher)) = prepared.encrypted.content else {
            panic!("missing group cipher")
        };
        for receiver in [&bg, &cg] {
            let data = receiver.decrypt(&sender, &cipher).unwrap();
            assert_eq!(
                proto::Message::decode(messages::unpad(&data).unwrap()).unwrap(),
                message
            );
        }
    }
    let memory = repo
        .transaction(&[], |tx| tx.sender_key_memory("group@g.us"))
        .unwrap();
    assert_eq!(memory.len(), 2);
    assert!(memory.values().all(|v| *v));
    drop(repo);
    drop(alice);
    drop(store);
    let reopened = SqliteAuthStore::open(dir.path()).unwrap();
    assert_eq!(
        relay::Repository::new(reopened.clone())
            .transaction(&[], |tx| tx.sender_key_memory("group@g.us"))
            .unwrap(),
        memory
    );
    assert!(
        group::Repository::new(reopened)
            .has_sender_key(&sender)
            .unwrap()
    );
}
#[test]
fn transaction_rolls_back_sessions_group_key_and_memory_on_late_failure() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteAuthStore::open(dir.path()).unwrap();
    let alice = direct::Repository::new(store.clone());
    let (_, _, bc) = setup();
    let target = "200:1@s.whatsapp.net".to_owned();
    alice.inject_session(&target, &bundle(&bc)).unwrap();
    let before = store.get(KeyCategory::Session, &["200.1".into()]).unwrap();
    let before = serde_json::to_value(&before).unwrap();
    let repo = relay::Repository::new(store.clone());
    let devices = vec![device(&target)];
    let attrs = Default::default();
    let message = proto::Message {
        conversation: Some("rollback".into()),
        ..Default::default()
    };
    let connection = rusqlite::Connection::open(store.path().unwrap()).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_memory BEFORE INSERT ON signal_keys WHEN NEW.category='sender-key-memory' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;").unwrap();
    let attempt = || {
        repo.transaction(std::slice::from_ref(&target), |tx| {
            tx.group_fanout(relay::GroupFanout {
                destination: "group@g.us",
                sender: "100:7@s.whatsapp.net",
                message: &message,
                devices: &devices,
                me: "100:7@s.whatsapp.net",
                me_lid: None,
                extra_attrs: &attrs,
            })
        })
    };
    assert!(attempt().is_err());
    assert_eq!(
        serde_json::to_value(store.get(KeyCategory::Session, &["200.1".into()]).unwrap()).unwrap(),
        before
    );
    let name = signal::SenderKeyName::new("group@g.us", "100:7@s.whatsapp.net").unwrap();
    assert!(
        !group::Repository::new(store.clone())
            .has_sender_key(&name)
            .unwrap()
    );
    assert!(
        repo.transaction(&[], |tx| tx.sender_key_memory("group@g.us"))
            .unwrap()
            .is_empty()
    );
    connection
        .execute_batch("DROP TRIGGER reject_memory")
        .unwrap();
    assert!(attempt().is_ok());
    assert_ne!(
        serde_json::to_value(store.get(KeyCategory::Session, &["200.1".into()]).unwrap()).unwrap(),
        before
    );
}
#[test]
fn duplicates_and_pn_lid_aliases_share_counters_and_concurrent_batches_serialize() {
    let (store, _, _) = setup();
    let (_, _, bc) = setup();
    let mappings = signal::mapping::Store::new(store.clone());
    mappings
        .store_mappings(&[signal::mapping::Mapping {
            pn: "200@s.whatsapp.net".into(),
            lid: "800@lid".into(),
        }])
        .unwrap();
    let alice = direct::Repository::with_mappings(store.clone(), mappings.clone());
    alice
        .inject_session("200:1@s.whatsapp.net", &bundle(&bc))
        .unwrap();
    let repo = relay::Repository::with_mappings(store, mappings);
    let targets = vec!["200:1@s.whatsapp.net".into(), "800:1@lid".into()];
    let output = repo
        .transaction(&targets, |tx| {
            Ok([
                tx.encrypt(&targets[0], b"one")?,
                tx.encrypt(&targets[1], b"two")?,
            ])
        })
        .unwrap();
    assert_ne!(output[0].ciphertext, output[1].ciphertext);
    let threads = (0..4)
        .map(|_| {
            let repo = repo.clone();
            let targets = targets.clone();
            std::thread::spawn(move || {
                repo.transaction(&targets, |tx| tx.encrypt(&targets[0], b"concurrent"))
                    .unwrap()
                    .ciphertext
            })
        })
        .collect::<Vec<_>>();
    let outputs = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(outputs.len(), 4);
    assert!(alice.validate_session("800:1@lid").unwrap());
}

#[test]
fn batch_retains_partial_success_and_rolls_back_all_failed_group_distribution() {
    let (store, alice, _) = setup();
    let (_, _, bc) = setup();
    alice
        .inject_session("200:1@s.whatsapp.net", &bundle(&bc))
        .unwrap();
    let repo = relay::Repository::new(store.clone());
    let targets = vec!["200:1@s.whatsapp.net".into(), "invalid".into()];
    let message = proto::Message {
        conversation: Some("partial".into()),
        ..Default::default()
    };
    let attrs = Default::default();
    let result = repo
        .transaction(&targets, |tx| {
            relay::create_participant_nodes(
                relay::Fanout {
                    recipients: &targets,
                    message: &message,
                    me: "100:7@s.whatsapp.net",
                    me_lid: None,
                    extra_attrs: &attrs,
                    device_sent_message: None,
                },
                tx,
            )
        })
        .unwrap();
    assert_eq!(result.nodes.len(), 1);
    assert_eq!(result.failures.len(), 1);
    assert_eq!(result.failures[0].jid, "invalid");
    let missing: Vec<String> = vec!["300:1@s.whatsapp.net".into()];
    let mixed = vec![targets[0].clone(), missing[0].clone()];
    let mixed_devices = mixed.iter().map(|jid| device(jid)).collect::<Vec<_>>();
    let partial = repo
        .transaction(&mixed, |tx| {
            tx.group_fanout(relay::GroupFanout {
                destination: "partial@g.us",
                sender: "100:7@s.whatsapp.net",
                message: &message,
                devices: &mixed_devices,
                me: "100:7@s.whatsapp.net",
                me_lid: None,
                extra_attrs: &attrs,
            })
        })
        .unwrap();
    assert_eq!(partial.participants.nodes.len(), 1);
    assert_eq!(partial.participants.failures.len(), 1);
    let memory = repo
        .transaction(&[], |tx| tx.sender_key_memory("partial@g.us"))
        .unwrap();
    assert!(mixed.iter().all(|jid| memory[jid]));
    let devices = vec![device(&missing[0])];
    let result = repo.transaction(&missing, |tx| {
        tx.group_fanout(relay::GroupFanout {
            destination: "failed@g.us",
            sender: "100:7@s.whatsapp.net",
            message: &message,
            devices: &devices,
            me: "100:7@s.whatsapp.net",
            me_lid: None,
            extra_attrs: &attrs,
        })
    });
    assert_eq!(result.unwrap_err().status_code(), Some(500));
    assert!(
        !group::Repository::new(store)
            .has_sender_key(
                &signal::SenderKeyName::new("failed@g.us", "100:7@s.whatsapp.net").unwrap()
            )
            .unwrap()
    );
    assert!(
        repo.transaction(&[], |tx| tx.sender_key_memory("failed@g.us"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn sender_key_selection_matches_upstream_and_status_redistributes_every_send() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/relay-stanza.json")).unwrap();
    for case in fixture["senderKeySelection"].as_array().unwrap() {
        let devices: Vec<Device> = serde_json::from_value(case["devices"].clone()).unwrap();
        let mut memory = serde_json::from_value(case["memory"].clone()).unwrap();
        let recipients =
            relay::sender_key_recipients(&devices, &mut memory, case["force"].as_bool().unwrap());
        assert_eq!(
            serde_json::json!({"recipients":recipients,"memory":memory}),
            case["expected"]
        );
    }
    let (store, alice, _) = setup();
    let (bs, bob, bc) = setup();
    alice
        .inject_session("200:1@s.whatsapp.net", &bundle(&bc))
        .unwrap();
    let repo = relay::Repository::new(store);
    let targets: Vec<String> = vec!["200:1@s.whatsapp.net".into()];
    let devices = vec![device(&targets[0])];
    let attrs = Default::default();
    let receiver = group::Repository::new(bs);
    let sender = signal::SenderKeyName::new(jid::STORIES_JID, "100:7@s.whatsapp.net").unwrap();
    for _ in 0..2 {
        let message = proto::Message {
            conversation: Some("status".into()),
            ..Default::default()
        };
        let result = repo
            .transaction(&targets, |tx| {
                let prepared = tx.group_fanout(relay::GroupFanout {
                    destination: jid::STORIES_JID,
                    sender: "100:7@s.whatsapp.net",
                    message: &message,
                    devices: &devices,
                    me: "100:7@s.whatsapp.net",
                    me_lid: None,
                    extra_attrs: &attrs,
                })?;
                let current =
                    relay::retry_distribution(tx, jid::STORIES_JID, "100:7@s.whatsapp.net", None)?;
                assert!(current.bytes.is_some());
                assert!(current.warning.is_none());
                Ok(prepared)
            })
            .unwrap();
        assert_eq!(result.participants.nodes.len(), 1);
        let distribution = receive(&bob, &result.participants.nodes[0])
            .sender_key_distribution_message
            .unwrap();
        receiver
            .process_distribution(
                &sender,
                distribution
                    .axolotl_sender_key_distribution_message
                    .as_deref()
                    .unwrap(),
            )
            .unwrap();
        let Some(NodeContent::Bytes(cipher)) = result.encrypted.content else {
            panic!("missing ciphertext")
        };
        let bytes = receiver.decrypt(&sender, &cipher).unwrap();
        assert_eq!(
            proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap(),
            message
        );
    }
}
