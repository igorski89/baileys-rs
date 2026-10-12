use baileys_rs::{
    messages::{self, EventContext, PollContext},
    proto,
};
use prost::Message as _;
use serde_json::Value;
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/encrypted_messages.json")).unwrap()
}
fn hex(v: &Value) -> Vec<u8> {
    v.as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn secret_derived_votes_and_responses_match_original_crypto_and_protobufs() {
    for case in fixture()["votes"].as_array().unwrap() {
        let secret = hex(&case["secret"]);
        let value = proto::message::PollEncValue {
            enc_payload: Some(hex(&case["ciphertext"])),
            enc_iv: Some(hex(&case["iv"])),
        };
        let id = case["id"].as_str().unwrap();
        let creator = case["creator"].as_str().unwrap();
        let actor = case["actor"].as_str().unwrap();
        let result = if case["kind"] == "poll" {
            messages::decrypt_poll_vote(
                &value,
                &PollContext {
                    poll_creator_jid: creator,
                    poll_msg_id: id,
                    poll_enc_key: &secret,
                    voter_jid: actor,
                },
            )
            .map(|v| v.encode_to_vec())
        } else {
            messages::decrypt_event_response(
                &value,
                &EventContext {
                    event_creator_jid: creator,
                    event_msg_id: id,
                    event_enc_key: &secret,
                    responder_jid: actor,
                },
            )
            .map(|v| v.encode_to_vec())
        };
        if case.get("error").is_some() {
            assert!(result.is_err(), "{case}");
        } else {
            assert_eq!(result.unwrap(), hex(&case["output"]), "{case}");
        }
    }
}
#[test]
fn author_precedence_matches_runtime_alt_keys_and_self_identity() {
    for case in fixture()["authors"].as_array().unwrap() {
        let key: Option<proto::MessageKey> = serde_json::from_value(case["key"].clone()).unwrap();
        let meta = messages::KeyMetadata {
            participant_alt: case["meta"]["participantAlt"].as_str().map(str::to_owned),
            remote_jid_alt: case["meta"]["remoteJidAlt"].as_str().map(str::to_owned),
            ..Default::default()
        };
        assert_eq!(
            messages::key_author(key.as_ref(), Some(&meta), case["me"].as_str().unwrap()),
            case["author"].as_str().unwrap(),
            "{case}"
        );
    }
}
#[test]
fn every_ciphertext_byte_nonce_context_and_secret_are_authenticated() {
    let fixture = fixture();
    let case = fixture["votes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| {
            c["kind"] == "poll"
                && c["variant"] == "normal"
                && c["secret"].as_str().unwrap().len() == 64
        })
        .unwrap();
    let secret = hex(&case["secret"]);
    let ctx = PollContext {
        poll_creator_jid: case["creator"].as_str().unwrap(),
        poll_msg_id: case["id"].as_str().unwrap(),
        poll_enc_key: &secret,
        voter_jid: case["actor"].as_str().unwrap(),
    };
    let original = proto::message::PollEncValue {
        enc_payload: Some(hex(&case["ciphertext"])),
        enc_iv: Some(hex(&case["iv"])),
    };
    for i in 0..original.enc_payload.as_ref().unwrap().len() {
        let mut changed = original.clone();
        changed.enc_payload.as_mut().unwrap()[i] ^= 1;
        assert!(messages::decrypt_poll_vote(&changed, &ctx).is_err());
    }
    for i in 0..12 {
        let mut changed = original.clone();
        changed.enc_iv.as_mut().unwrap()[i] ^= 1;
        assert!(messages::decrypt_poll_vote(&changed, &ctx).is_err());
    }
    let wrong = [7; 32];
    assert!(
        messages::decrypt_poll_vote(
            &original,
            &PollContext {
                poll_enc_key: &wrong,
                ..ctx
            }
        )
        .is_err()
    );
    assert!(
        messages::decrypt_event_response(
            &original,
            &EventContext {
                event_creator_jid: ctx.poll_creator_jid,
                event_msg_id: ctx.poll_msg_id,
                event_enc_key: &secret,
                responder_jid: ctx.voter_jid
            }
        )
        .is_err()
    );
    for invalid in [
        proto::message::PollEncValue::default(),
        proto::message::PollEncValue {
            enc_iv: Some(vec![]),
            ..original.clone()
        },
    ] {
        assert!(messages::decrypt_poll_vote(&invalid, &ctx).is_err());
    }
    assert!(!format!("{ctx:?}").contains("poll_enc_key"));
}
