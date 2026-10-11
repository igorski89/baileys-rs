use baileys_rs::{auth::Credentials, binary, connection::*, crypto, proto};
use prost::Message;
use serde_json::{Value, json};
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn credentials() -> Credentials {
    let f: Value = serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    serde_json::from_value(f["empty"].clone()).unwrap()
}
fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/connection.json")).unwrap()
}
#[test]
fn matches_upstream_login_registration_and_qr_data() {
    let f = fixtures();
    let credentials = credentials();
    for case in f["configurations"].as_array().unwrap() {
        let config: PayloadConfig = serde_json::from_value(case["config"].clone()).unwrap();
        assert_eq!(
            login_payload("123456789:7@s.whatsapp.net", &config)
                .unwrap()
                .encode_to_vec(),
            hex(case["login"].as_str().unwrap())
        );
        assert_eq!(
            registration_payload(&credentials, &config)
                .unwrap()
                .encode_to_vec(),
            hex(case["registration"].as_str().unwrap())
        );
        assert_eq!(
            pairing_qr("ref", &credentials, &config.browser),
            case["qr"].as_str().unwrap()
        );
    }
    for case in f["crockford"].as_array().unwrap() {
        assert_eq!(
            bytes_to_crockford(&hex(case["input"].as_str().unwrap())),
            case["output"].as_str().unwrap()
        );
    }
}
#[test]
fn validates_standard_and_hosted_pairing_and_matches_upstream_reply() {
    let f = fixtures();
    for case in f["pair"].as_array().unwrap() {
        let mut credentials = credentials();
        let stanza = binary::decode(&hex(case["stanza"].as_str().unwrap())).unwrap();
        let result = configure_successful_pairing(&stanza, &credentials).unwrap();
        assert_eq!(
            binary::encode(&result.reply).unwrap(),
            hex(case["reply"].as_str().unwrap())
        );
        assert_eq!(
            result.account.encode_to_vec(),
            hex(case["account"].as_str().unwrap())
        );
        result.apply(&mut credentials);
        assert_eq!(credentials.me_id().unwrap(), "123456789:7@s.whatsapp.net");
        assert_eq!(
            credentials.signal_identities.as_ref().unwrap()[0]
                .identifier
                .name,
            "987654:7@lid"
        );
        assert!(credentials.account.is_some());
    }
}
#[test]
fn rejects_tampered_pairing_hmac_and_signed_account() {
    let f = fixtures();
    let case = &f["pair"][0];
    let credentials = credentials();
    let original = binary::decode(&hex(case["stanza"].as_str().unwrap())).unwrap();
    for tamper_signature in [false, true] {
        let mut stanza = original.clone();
        let binary::NodeContent::Children(top) = stanza.content.as_mut().unwrap() else {
            panic!()
        };
        let binary::NodeContent::Children(children) = top[0].content.as_mut().unwrap() else {
            panic!()
        };
        let binary::NodeContent::Bytes(bytes) = children[0].content.as_mut().unwrap() else {
            panic!()
        };
        let mut signed = proto::AdvSignedDeviceIdentityHmac::decode(bytes.as_slice()).unwrap();
        if tamper_signature {
            let mut account =
                proto::AdvSignedDeviceIdentity::decode(signed.details.as_ref().unwrap().as_slice())
                    .unwrap();
            account.account_signature.as_mut().unwrap()[0] ^= 1;
            signed.details = Some(account.encode_to_vec());
            use base64::Engine;
            let secret = base64::engine::general_purpose::STANDARD
                .decode(&credentials.adv_secret_key)
                .unwrap();
            signed.hmac =
                Some(crypto::hmac_sha256(signed.details.as_ref().unwrap(), &secret).to_vec());
        } else {
            signed.hmac.as_mut().unwrap()[0] ^= 1;
        }
        *bytes = signed.encode_to_vec();
        assert!(matches!(
            configure_successful_pairing(&stanza, &credentials),
            Err(Error::Authentication(_))
        ));
    }
    assert!(credentials.me.is_none());
}
#[test]
fn wraps_and_unwraps_phone_pairing_keys_with_expected_derivation() {
    let mut credentials = credentials();
    let browser = Browser::macos("Chrome");
    let salt = [1; 32];
    let iv = [2; 16];
    let request = pairing_code_request_with_material(
        &mut credentials,
        "123456789",
        "ABCDEFGH",
        &browser,
        "code",
        &salt,
        &iv,
    )
    .unwrap();
    let reg = request.child("link_code_companion_reg").unwrap();
    let wrapped = reg
        .child_buffer("link_code_pairing_wrapped_companion_ephemeral_pub")
        .unwrap();
    let key = crypto::derive_pairing_code_key("ABCDEFGH", &salt);
    assert_eq!(
        crypto::aes_decrypt_ctr(&wrapped[48..], &key, &iv).unwrap(),
        credentials.pairing_ephemeral_key_pair.public
    );
    let primary = crypto::KeyPair::from_private([42; 32]);
    let mut wrapped = salt.to_vec();
    wrapped.extend(iv);
    wrapped.extend(crypto::aes_encrypt_ctr(&primary.public, &key, &iv).unwrap());
    let notification =
        binary::BinaryNode::new("notification").with_content(binary::NodeContent::Children(vec![
            binary::BinaryNode::new("link_code_companion_reg").with_content(
                binary::NodeContent::Children(vec![
                    binary::BinaryNode::new("link_code_pairing_ref")
                        .with_content(binary::NodeContent::Bytes(b"ref".to_vec())),
                    binary::BinaryNode::new("primary_identity_pub")
                        .with_content(binary::NodeContent::Bytes(primary.public.to_vec())),
                    binary::BinaryNode::new("link_code_pairing_wrapped_primary_ephemeral_pub")
                        .with_content(binary::NodeContent::Bytes(wrapped)),
                ]),
            ),
        ]));
    let finish = pairing_code_finish_with_material(
        &credentials,
        &notification,
        "finish",
        &[3; 32],
        &[4; 32],
        &[5; 12],
    )
    .unwrap();
    let bytes = finish
        .node
        .child("link_code_companion_reg")
        .unwrap()
        .child_buffer("link_code_pairing_wrapped_key_bundle")
        .unwrap();
    let shared = primary
        .shared_key(&credentials.pairing_ephemeral_key_pair.public)
        .unwrap();
    let expanded = crypto::hkdf(
        &shared,
        32,
        Some(&[4; 32]),
        b"link_code_pairing_key_bundle_encryption_key",
    )
    .unwrap();
    let plain = crypto::aes_decrypt_gcm(&bytes[44..], &expanded, &bytes[32..44], b"").unwrap();
    assert_eq!(&plain[..32], &credentials.signed_identity_key.public);
    assert_eq!(&plain[32..64], &primary.public);
    assert_eq!(&plain[64..], &[3; 32]);
    assert!(
        pairing_code_request_with_material(
            &mut credentials,
            "bad",
            "ABCDEFGH",
            &browser,
            "x",
            &salt,
            &iv
        )
        .is_err()
    );
    assert!(login_payload("invalid", &PayloadConfig::default()).is_err());
    assert_eq!(json!(finish.node.attrs["xmlns"]), "md");
}

#[test]
fn matches_upstream_phone_pairing_request_and_finish_exchange() {
    let f = fixtures();
    let phone = &f["phone"];
    let mut creds = credentials();
    let request = pairing_code_request_with_material(
        &mut creds,
        "123456789",
        "ABCDEFGH",
        &Browser::macos("Chrome"),
        "code",
        &[3; 32],
        &[2; 16],
    )
    .unwrap();
    assert_eq!(
        binary::encode(&request).unwrap(),
        hex(phone["request"].as_str().unwrap())
    );
    let notification = binary::decode(&hex(phone["notification"].as_str().unwrap())).unwrap();
    let finish = pairing_code_finish_with_material(
        &creds,
        &notification,
        "finish",
        &[3; 32],
        &[3; 32],
        &[5; 12],
    )
    .unwrap();
    assert_eq!(
        binary::encode(&finish.node).unwrap(),
        hex(phone["finish"].as_str().unwrap())
    );
    assert_eq!(
        finish.adv_secret_key,
        phone["advSecretKey"].as_str().unwrap()
    );
}
