use baileys_rs::{
    auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
    crypto::KeyPair,
    proto,
    signal::{
        Address, Error, SenderKeyName,
        group::{Record, Repository},
    },
};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use serde_json::Value;
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/group.json")).unwrap()
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn bytes(f: &Value, key: &str) -> Vec<u8> {
    hex(f[key].as_str().unwrap())
}
fn record(f: &Value) -> Record {
    Record::from_bytes(f["initial"].as_str().unwrap().as_bytes()).unwrap()
}
fn canonical(value: Value) -> Value {
    match value {
        Value::Object(mut map) if map.get("type").and_then(Value::as_str) == Some("Buffer") => {
            let data = map.remove("data").unwrap();
            let data = if let Some(s) = data.as_str() {
                STANDARD.decode(s).unwrap()
            } else {
                data.as_array()
                    .unwrap()
                    .iter()
                    .map(|n| n.as_u64().unwrap() as u8)
                    .collect()
            };
            serde_json::json!(data)
        }
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, canonical(v))).collect())
        }
        Value::Array(array) => Value::Array(array.into_iter().map(canonical).collect()),
        other => other,
    }
}
fn assert_record(record: &Record, expected: &Value) {
    assert_eq!(
        canonical(serde_json::from_slice(&record.to_bytes().unwrap()).unwrap()),
        canonical(serde_json::from_str(expected.as_str().unwrap()).unwrap())
    );
}
#[test]
fn matches_upstream_ciphertext_distribution_and_persisted_chains() {
    let f = fixture();
    let mut record = record(&f);
    assert_eq!(record.distribution().unwrap(), bytes(&f, "distribution"));
    for (i, case) in f["messages"].as_array().unwrap().iter().enumerate() {
        assert_eq!(record.distribution().unwrap(), bytes(case, "distribution"));
        let ciphertext = record.encrypt(&bytes(case, "data")).unwrap();
        assert_eq!(ciphertext, bytes(case, "ciphertext"));
        let parsed =
            proto::SenderKeyMessage::decode(&ciphertext[1..ciphertext.len() - 64]).unwrap();
        assert_eq!(parsed.iteration, Some((i * 2) as u32));
        assert_record(&record, &case["record"]);
        record = Record::from_bytes(&record.to_bytes().unwrap()).unwrap();
    }
    let mut fresh = Record::default();
    fresh.reset(
        12345,
        0,
        bytes(&f, "seed").try_into().unwrap(),
        &KeyPair::from_private(bytes(&f, "private").try_into().unwrap()),
    );
    assert_record(&fresh, &f["initial"]);
}
#[test]
fn decrypts_upstream_messages_out_of_order_and_rejects_replay() {
    let f = fixture();
    let mut record = Record::default();
    record
        .process_distribution(&bytes(&f, "distribution"))
        .unwrap();
    for index in f["order"].as_array().unwrap() {
        let case = &f["messages"][index.as_u64().unwrap() as usize];
        let ciphertext = bytes(case, "ciphertext");
        assert_eq!(record.decrypt(&ciphertext).unwrap(), bytes(case, "data"));
        let before = record.to_bytes().unwrap();
        assert!(matches!(record.decrypt(&ciphertext), Err(Error::Replay)));
        assert_eq!(record.to_bytes().unwrap(), before);
        record = Record::from_bytes(&record.to_bytes().unwrap()).unwrap();
    }
    assert_record(&record, &f["receiver"]);
}
#[test]
fn invalid_signatures_padding_versions_and_future_counters_do_not_consume_keys() {
    let f = fixture();
    let original = bytes(&f["messages"][0], "ciphertext");
    let mut record = Record::default();
    record
        .process_distribution(&bytes(&f, "distribution"))
        .unwrap();
    let before = record.to_bytes().unwrap();
    for offset in 0..original.len() {
        let mut wire = original.clone();
        wire[offset] ^= 0x80;
        assert!(record.decrypt(&wire).is_err(), "tampered byte {offset}");
        assert_eq!(record.to_bytes().unwrap(), before);
    }
    let signing = KeyPair::from_private(bytes(&f, "private").try_into().unwrap());
    for (iteration, ciphertext) in [
        (2001, vec![0; 16]),
        (0, vec![0; 15]),
        (u32::MAX, vec![0; 16]),
    ] {
        let msg = proto::SenderKeyMessage {
            id: Some(12345),
            iteration: Some(iteration),
            ciphertext: Some(ciphertext),
        };
        let mut wire = vec![0x33];
        wire.extend(msg.encode_to_vec());
        wire.extend(signing.sign(&wire));
        assert!(record.decrypt(&wire).is_err());
        assert_eq!(record.to_bytes().unwrap(), before);
    }
    assert_eq!(
        record.decrypt(&original).unwrap(),
        bytes(&f["messages"][0], "data")
    );
}
#[test]
fn limits_states_skipped_keys_and_counter_wraparound() {
    let f = fixture();
    let mut record = Record::default();
    for id in 0..6 {
        let msg = proto::SenderKeyDistributionMessage {
            id: Some(id),
            iteration: Some(0),
            chain_key: Some(bytes(&f, "seed")),
            signing_key: Some(bytes(&f, "public")),
        };
        let mut wire = vec![0x33];
        wire.extend(msg.encode_to_vec());
        record.process_distribution(&wire).unwrap();
    }
    assert_eq!(record.state_count(), 5);
    let signing = KeyPair::from_private(bytes(&f, "private").try_into().unwrap());
    let make = |iteration, id| {
        let msg = proto::SenderKeyMessage {
            id: Some(id),
            iteration: Some(iteration),
            ciphertext: Some(vec![0; 16]),
        };
        let mut wire = vec![0x33];
        wire.extend(msg.encode_to_vec());
        wire.extend(signing.sign(&wire));
        wire
    };
    assert!(matches!(record.decrypt(&make(0, 0)), Err(Error::NoSession)));
    assert!(matches!(record.encrypt(b"test"), Err(Error::NoSigningKey)));
    record.reset(
        12345,
        u32::MAX,
        bytes(&f, "seed").try_into().unwrap(),
        &signing,
    );
    let before = record.to_bytes().unwrap();
    assert!(matches!(record.encrypt(b"test"), Err(Error::Counter)));
    assert_eq!(record.to_bytes().unwrap(), before);
    let mut receiver = Record::default();
    receiver
        .process_distribution(&bytes(&f, "distribution"))
        .unwrap();
    for case in f["gaps"].as_array().unwrap().iter().take(2) {
        assert_eq!(
            receiver.decrypt(&bytes(case, "ciphertext")).unwrap(),
            bytes(case, "data")
        );
        let data: Value = serde_json::from_slice(&receiver.to_bytes().unwrap()).unwrap();
        assert_eq!(data[0]["senderMessageKeys"].as_array().unwrap().len(), 2000);
    }
    assert!(matches!(
        receiver.decrypt(&bytes(&f["messages"][0], "ciphertext")),
        Err(Error::Replay)
    ));
    let before = receiver.to_bytes().unwrap();
    assert!(matches!(
        receiver.decrypt(&bytes(&f["gaps"][2], "ciphertext")),
        Err(Error::Future)
    ));
    assert_eq!(receiver.to_bytes().unwrap(), before);
}
#[test]
fn persistent_repository_resumes_and_rolls_back_failed_decryptions() {
    let f = fixture();
    let temp = tempfile::tempdir().unwrap();
    let sender_dir = temp.path().join("sender");
    let receiver_dir = temp.path().join("receiver");
    let name = SenderKeyName::new("synthetic@g.us", "123456789:7@lid").unwrap();
    let sender = SqliteAuthStore::open(&sender_dir).unwrap();
    sender
        .set(&[KeyMutation::put(
            name.to_string(),
            KeyValue::SenderKey(record(&f).to_bytes().unwrap()),
        )])
        .unwrap();
    let receiver = SqliteAuthStore::open(&receiver_dir).unwrap();
    let repo = Repository::new(receiver.clone());
    assert!(!repo.has_sender_key(&name).unwrap());
    repo.process_distribution(&name, &bytes(&f, "distribution"))
        .unwrap();
    for case in f["messages"].as_array().unwrap() {
        let sender = Repository::new(SqliteAuthStore::open(&sender_dir).unwrap());
        let encrypted = sender.encrypt(&name, &bytes(case, "data")).unwrap();
        assert_eq!(encrypted.ciphertext, bytes(case, "ciphertext"));
        assert_eq!(encrypted.distribution, bytes(case, "distribution"));
        let mut bad = encrypted.ciphertext.clone();
        *bad.last_mut().unwrap() ^= 1;
        let before = receiver
            .get(KeyCategory::SenderKey, &[name.to_string()])
            .unwrap();
        assert!(repo.decrypt(&name, &bad).is_err());
        let after = receiver
            .get(KeyCategory::SenderKey, &[name.to_string()])
            .unwrap();
        assert_eq!(
            serde_json::to_value(&before).unwrap(),
            serde_json::to_value(after).unwrap()
        );
        let repo = Repository::new(SqliteAuthStore::open(&receiver_dir).unwrap());
        assert_eq!(
            repo.decrypt(&name, &encrypted.ciphertext).unwrap(),
            bytes(case, "data")
        );
    }
    let empty_name = SenderKeyName::new("new@g.us", "123:1@s.whatsapp.net").unwrap();
    let distribution = repo.distribution(&empty_name).unwrap();
    assert_eq!(distribution[0], 0x33);
    assert!(repo.has_sender_key(&empty_name).unwrap());
}
#[test]
fn addresses_names_and_utf16_hashes_match_upstream() {
    let f = fixture();
    for case in f["names"].as_array().unwrap() {
        let name = SenderKeyName::new(case["group"].as_str().unwrap(), "123456789:7@lid").unwrap();
        assert_eq!(name.to_string(), case["name"].as_str().unwrap());
        assert_eq!(i64::from(name.hash_code()), case["hash"].as_i64().unwrap());
    }
    for (jid, expected) in [
        ("123@s.whatsapp.net", "123.0"),
        ("123:7@lid", "123_1.7"),
        ("123:99@hosted", "123_128.99"),
        ("123:99@hosted.lid", "123_129.99"),
    ] {
        assert_eq!(Address::from_jid(jid).unwrap().to_string(), expected);
    }
    for jid in [
        "invalid",
        "@s.whatsapp.net",
        "123:99@s.whatsapp.net",
        "123:x@lid",
    ] {
        assert!(Address::from_jid(jid).is_err());
    }
}

#[test]
fn concurrent_group_writes_never_reuse_iterations() {
    let f = fixture();
    let temp = tempfile::tempdir().unwrap();
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    let name = SenderKeyName::new("synthetic@g.us", "123456789:7@lid").unwrap();
    store
        .set(&[KeyMutation::put(
            name.to_string(),
            KeyValue::SenderKey(record(&f).to_bytes().unwrap()),
        )])
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|i| {
            let name = name.clone();
            let barrier = barrier.clone();
            let independent = SqliteAuthStore::open(temp.path()).unwrap();
            std::thread::spawn(move || {
                barrier.wait();
                let plaintext = format!("worker {i}").into_bytes();
                let encrypted = Repository::new(independent)
                    .encrypt(&name, &plaintext)
                    .unwrap();
                (plaintext, encrypted.ciphertext)
            })
        })
        .collect();
    let mut receiver = Record::default();
    receiver
        .process_distribution(&bytes(&f, "distribution"))
        .unwrap();
    let mut iterations = vec![];
    for worker in workers {
        let (plaintext, ciphertext) = worker.join().unwrap();
        let message =
            proto::SenderKeyMessage::decode(&ciphertext[1..ciphertext.len() - 64]).unwrap();
        iterations.push(message.iteration.unwrap());
        assert_eq!(receiver.decrypt(&ciphertext).unwrap(), plaintext);
    }
    iterations.sort();
    assert_eq!(iterations, (0..8).map(|i| i * 2).collect::<Vec<_>>());
    let result: Result<(), Error> = store.transaction_with_error(|tx| {
        tx.set(&[KeyMutation::put("rollback", KeyValue::SenderKey(vec![1]))])?;
        Err(Error::Invalid)
    });
    assert!(matches!(result, Err(Error::Invalid)));
    assert!(
        !store
            .get(KeyCategory::SenderKey, &["rollback".into()])
            .unwrap()
            .contains_key("rollback")
    );
}

#[test]
fn imported_record_validation_preserves_empty_states_and_rejects_bad_keys() {
    let f = fixture();
    assert!(Record::from_bytes(b"[]").unwrap().is_empty());
    let mut data: Value = serde_json::from_str(f["initial"].as_str().unwrap()).unwrap();
    data[0]["senderSigningKey"]["private"] = serde_json::json!({"type":"Buffer","data":[1,2]});
    assert!(Record::from_bytes(&serde_json::to_vec(&data).unwrap()).is_err());
    let mut data: Value = serde_json::from_str(f["initial"].as_str().unwrap()).unwrap();
    data[0]["senderSigningKey"]["public"] = serde_json::json!({"type":"Buffer","data":STANDARD.encode(KeyPair::from_private([99;32]).public)});
    assert!(Record::from_bytes(&serde_json::to_vec(&data).unwrap()).is_err());
    assert!(!format!("{:?}", record(&f)).contains("seed"));
    assert!(!format!("{:?}", record(&f)).contains("private"));
}
