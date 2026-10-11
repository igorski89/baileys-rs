use baileys_rs::{
    crypto::KeyPair,
    signal::{
        Error,
        direct::{Bundle, MessageType, PublicPreKey, Record},
    },
};
use prost::Message;
use serde_json::Value;
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/direct.json")).unwrap()
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn bytes(v: &Value, key: &str) -> Vec<u8> {
    hex(v[key].as_str().unwrap())
}
fn key(v: &Value, name: &str) -> KeyPair {
    KeyPair::from_private(bytes(v, name).try_into().unwrap())
}
fn bundle(f: &Value) -> Bundle {
    let b = &f["bundle"];
    Bundle {
        registration_id: b["registrationId"].as_u64().unwrap() as u32,
        identity: bytes(b, "identityKey"),
        signed: PublicPreKey {
            id: 5,
            public: bytes(&b["signedPreKey"], "publicKey"),
        },
        signature: bytes(&b["signedPreKey"], "signature"),
        pre_key: Some(PublicPreKey {
            id: 7,
            public: bytes(&b["preKey"], "publicKey"),
        }),
    }
}
fn without_times(mut v: Value) -> Value {
    fn remove(v: &mut Value) {
        match v {
            Value::Object(map) => {
                map.remove("created");
                map.remove("used");
                for value in map.values_mut() {
                    remove(value)
                }
            }
            Value::Array(values) => {
                for value in values {
                    remove(value)
                }
            }
            _ => (),
        }
    }
    remove(&mut v);
    v
}
fn state(record: &Record, expected: &Value) {
    assert_eq!(
        without_times(record.to_json().unwrap()),
        without_times(expected.clone())
    );
}
#[test]
fn matches_upstream_x3dh_ratchets_ciphertext_and_legacy_session_state() {
    let f = fixture();
    let ai = key(&f["alice"], "identity");
    let bi = key(&f["bob"], "identity");
    let asigned = key(&f["alice"], "signed");
    let bsigned = key(&f["bob"], "signed");
    let ap = key(&f["alice"], "pre");
    let bp = key(&f["bob"], "pre");
    let keys: Vec<_> = f["ephemeral"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| KeyPair::from_private(hex(v.as_str().unwrap()).try_into().unwrap()))
        .collect();
    let mut index = 2;
    let mut alice = Record::default();
    alice
        .initialize_with_keys(&ai, &bundle(&f), &keys[0], &keys[1])
        .unwrap();
    state(&alice, &f["initial"]["alice"]);
    let mut bob = Record::default();
    let mut delayed = None;
    for case in f["messages"].as_array().unwrap() {
        let sending_alice = case["sender"] == "alice";
        let kind = if case["type"] == "pkmsg" {
            MessageType::PreKey
        } else {
            MessageType::Signal
        };
        let data = bytes(case, "data");
        let (sender, receiver, identity, remote_identity, signed, pre, registration) =
            if sending_alice {
                (&mut alice, &mut bob, &ai, &bi, &bsigned, &bp, 1)
            } else {
                (&mut bob, &mut alice, &bi, &ai, &asigned, &ap, 11)
            };
        let encrypted = sender.encrypt(identity, registration, &data).unwrap();
        assert_eq!(encrypted.kind, kind);
        assert_eq!(encrypted.ciphertext, bytes(case, "ciphertext"));
        if case["delivered"].as_bool().unwrap() {
            assert_eq!(
                receiver
                    .decrypt_with_rng(
                        remote_identity,
                        signed,
                        Some(pre),
                        kind,
                        &encrypted.ciphertext,
                        || {
                            let key = keys[index].clone();
                            index += 1;
                            Ok(key)
                        }
                    )
                    .unwrap(),
                data
            );
        } else {
            delayed = Some(encrypted.ciphertext)
        }
        state(&alice, &case["alice"]);
        state(&bob, &case["bob"]);
        alice = Record::from_json(alice.to_json().unwrap()).unwrap();
        bob = Record::from_json(bob.to_json().unwrap()).unwrap();
    }
    assert_eq!(
        bob.decrypt_with_rng(
            &bi,
            &bsigned,
            None,
            MessageType::Signal,
            &delayed.unwrap(),
            || panic!("old chain should not ratchet")
        )
        .unwrap(),
        b"delayed"
    );
    state(&bob, &f["afterDelayed"]["bob"]);
}
#[test]
fn authentication_failure_replay_and_invalid_bundle_preserve_state() {
    let f = fixture();
    let ai = key(&f["alice"], "identity");
    let bi = key(&f["bob"], "identity");
    let bsigned = key(&f["bob"], "signed");
    let bp = key(&f["bob"], "pre");
    let mut record = Record::default();
    let mut bad = bundle(&f);
    bad.signature[0] ^= 1;
    assert!(matches!(
        record.initialize(&ai, &bad),
        Err(Error::Signature)
    ));
    assert!(!record.has_open_session());
    let wire = bytes(&f["messages"][0], "ciphertext");
    let mut outer = baileys_rs::proto::PreKeySignalMessage::decode(&wire[1..]).unwrap();
    *outer.message.as_mut().unwrap().last_mut().unwrap() ^= 1;
    let mut tampered = vec![0x33];
    tampered.extend(outer.encode_to_vec());
    let before = record.to_json().unwrap();
    assert!(
        record
            .decrypt(&bi, &bsigned, Some(&bp), MessageType::PreKey, &tampered)
            .is_err()
    );
    assert_eq!(record.to_json().unwrap(), before);
    assert!(matches!(
        record.decrypt(&bi, &bsigned, None, MessageType::PreKey, &wire),
        Err(Error::MissingPreKey)
    ));
    assert_eq!(
        record
            .decrypt(&bi, &bsigned, Some(&bp), MessageType::PreKey, &wire)
            .unwrap(),
        bytes(&f["messages"][0], "data")
    );
    let before = record.to_json().unwrap();
    assert!(matches!(
        record.decrypt(&bi, &bsigned, None, MessageType::PreKey, &wire),
        Err(Error::Replay)
    ));
    assert_eq!(record.to_json().unwrap(), before);
}

fn database(folder: &std::path::Path, local: &Value) -> baileys_rs::auth::SqliteAuthStore {
    use baileys_rs::{
        auth::{Credentials, KeyMutation, KeyStore, KeyValue, SqliteAuthStore},
        crypto::{self, SignedKeyPair},
    };
    let store = SqliteAuthStore::open(folder).unwrap();
    let mut creds = Credentials::generate().unwrap();
    creds.signed_identity_key = key(local, "identity");
    let signed = key(local, "signed");
    creds.signed_pre_key = SignedKeyPair {
        signature: creds
            .signed_identity_key
            .sign(&crypto::signal_public_key(&signed.public).unwrap())
            .to_vec(),
        key_pair: signed,
        key_id: 5,
        timestamp_s: None,
    };
    creds.registration_id = local["registration"].as_u64().unwrap() as u16;
    store.save_credentials(&creds).unwrap();
    store
        .set(&[KeyMutation::put("7", KeyValue::PreKey(key(local, "pre")))])
        .unwrap();
    store
}
#[test]
fn persistent_repository_commits_prekey_consumption_and_rolls_back_failures() {
    use baileys_rs::{
        auth::{KeyCategory, KeyStore, SqliteAuthStore},
        signal::{
            direct::Repository,
            mapping::{Mapping, Store},
        },
    };
    let f = fixture();
    let temp = tempfile::tempdir().unwrap();
    let adir = temp.path().join("alice");
    let bdir = temp.path().join("bob");
    let a = database(&adir, &f["alice"]);
    let b = database(&bdir, &f["bob"]);
    Store::new(a.clone())
        .store_mappings(&[Mapping {
            pn: "200@s.whatsapp.net".into(),
            lid: "900@lid".into(),
        }])
        .unwrap();
    let keys: Vec<_> = f["ephemeral"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| KeyPair::from_private(hex(v.as_str().unwrap()).try_into().unwrap()))
        .collect();
    let mut index = 2;
    Repository::new(a.clone())
        .inject_session_with_keys("200:1@s.whatsapp.net", &bundle(&f), &keys[0], &keys[1])
        .unwrap();
    assert!(
        Repository::new(a.clone())
            .validate_session("900:1@lid")
            .unwrap()
    );
    for case in f["messages"].as_array().unwrap() {
        let send_alice = case["sender"] == "alice";
        let (sender, receiver, jid, author) = if send_alice {
            (
                SqliteAuthStore::open(&adir).unwrap(),
                SqliteAuthStore::open(&bdir).unwrap(),
                "200:1@s.whatsapp.net",
                "100:1@s.whatsapp.net",
            )
        } else {
            (
                SqliteAuthStore::open(&bdir).unwrap(),
                SqliteAuthStore::open(&adir).unwrap(),
                "100:1@s.whatsapp.net",
                "200:1@s.whatsapp.net",
            )
        };
        let encrypted = Repository::new(sender)
            .encrypt(jid, &bytes(case, "data"))
            .unwrap();
        assert_eq!(encrypted.ciphertext, bytes(case, "ciphertext"));
        if !case["delivered"].as_bool().unwrap() {
            continue;
        }
        let repo = Repository::new(receiver.clone());
        let ids = [if send_alice { "100.1" } else { "900_1.1" }.into()];
        let before = receiver.get(KeyCategory::Session, &ids).unwrap();
        let mut damaged = encrypted.ciphertext.clone();
        if encrypted.kind == MessageType::PreKey {
            let mut message =
                baileys_rs::proto::PreKeySignalMessage::decode(&damaged[1..]).unwrap();
            *message.message.as_mut().unwrap().last_mut().unwrap() ^= 1;
            damaged = vec![0x33];
            damaged.extend(message.encode_to_vec());
        } else {
            *damaged.last_mut().unwrap() ^= 1;
        }
        assert!(repo.decrypt(author, encrypted.kind, &damaged).is_err());
        assert_eq!(
            serde_json::to_value(&before).unwrap(),
            serde_json::to_value(receiver.get(KeyCategory::Session, &ids).unwrap()).unwrap()
        );
        let plaintext = repo
            .decrypt_with_rng(author, encrypted.kind, &encrypted.ciphertext, || {
                let key = keys[index].clone();
                index += 1;
                Ok(key)
            })
            .unwrap();
        assert_eq!(plaintext, bytes(case, "data"));
    }
    assert!(
        b.get(KeyCategory::PreKey, &["7".into()])
            .unwrap()
            .is_empty()
    );
    assert!(
        !b.get(KeyCategory::IdentityKey, &["100.1".into()])
            .unwrap()
            .is_empty()
    );
    let info = Repository::new(a.clone())
        .session_info("900:1@lid")
        .unwrap()
        .unwrap();
    assert_eq!(info.1, 11);
    let repo = Repository::new(a);
    repo.delete_sessions(&["900:1@lid".into()]).unwrap();
    assert!(!repo.validate_session("200:1@s.whatsapp.net").unwrap());
}
#[test]
fn matches_upstream_three_dh_exchange_without_one_time_prekey() {
    let f = fixture();
    let case = &f["noPreKey"];
    let keys: Vec<_> = case["ephemeral"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| KeyPair::from_private(hex(v.as_str().unwrap()).try_into().unwrap()))
        .collect();
    let mut bundle = bundle(&f);
    bundle.pre_key = None;
    let mut alice = Record::default();
    let ai = key(&f["alice"], "identity");
    alice
        .initialize_with_keys(&ai, &bundle, &keys[0], &keys[1])
        .unwrap();
    state(&alice, &case["initial"]);
    let encrypted = alice.encrypt(&ai, 1, &bytes(case, "data")).unwrap();
    assert_eq!(encrypted.ciphertext, bytes(case, "ciphertext"));
    let mut bob = Record::default();
    assert_eq!(
        bob.decrypt_with_rng(
            &key(&f["bob"], "identity"),
            &key(&f["bob"], "signed"),
            None,
            MessageType::PreKey,
            &encrypted.ciphertext,
            || Ok(keys[2].clone())
        )
        .unwrap(),
        bytes(case, "data")
    );
    state(&bob, &case["bob"]);
    state(&alice, &case["alice"]);
}

#[test]
fn concurrent_repository_encryption_never_reuses_message_counters() {
    use baileys_rs::{auth::SqliteAuthStore, signal::direct::Repository};
    let f = fixture();
    let temp = tempfile::tempdir().unwrap();
    let store = database(temp.path(), &f["alice"]);
    Repository::new(store)
        .inject_session("200:1@s.whatsapp.net", &bundle(&f))
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|i| {
            let store = SqliteAuthStore::open(temp.path()).unwrap();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let data = format!("worker {i}").into_bytes();
                let encrypted = Repository::new(store)
                    .encrypt("200:1@s.whatsapp.net", &data)
                    .unwrap();
                (data, encrypted)
            })
        })
        .collect();
    let mut receiver = Record::default();
    let mut counters = vec![];
    for worker in workers {
        let (data, encrypted) = worker.join().unwrap();
        let outer =
            baileys_rs::proto::PreKeySignalMessage::decode(&encrypted.ciphertext[1..]).unwrap();
        let wire = outer.message.unwrap();
        let inner = baileys_rs::proto::SignalMessage::decode(&wire[1..wire.len() - 8]).unwrap();
        counters.push(inner.counter.unwrap());
        assert_eq!(
            receiver
                .decrypt(
                    &key(&f["bob"], "identity"),
                    &key(&f["bob"], "signed"),
                    Some(&key(&f["bob"], "pre")),
                    encrypted.kind,
                    &encrypted.ciphertext
                )
                .unwrap(),
            data
        );
    }
    counters.sort();
    assert_eq!(counters, (0..8).collect::<Vec<_>>());
}
