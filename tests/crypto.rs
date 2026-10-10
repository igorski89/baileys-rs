use baileys_rs::crypto::*;
use serde_json::Value;
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn bytes(case: &Value, name: &str) -> Vec<u8> {
    hex(case[name].as_str().unwrap())
}

#[test]
fn matches_upstream_cipher_hash_and_key_derivation_fixtures() {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/crypto.json")).unwrap();
    for case in fixtures["cases"].as_array().unwrap() {
        let data = bytes(case, "data");
        let key = bytes(case, "key");
        let iv = bytes(case, "iv");
        let aad = bytes(case, "aad");
        let encrypted = aes_encrypt_gcm(&data, &key, &iv[..12], &aad).unwrap();
        assert_eq!(encrypted, bytes(case, "gcm"));
        assert_eq!(
            aes_decrypt_gcm(&encrypted, &key, &iv[..12], &aad).unwrap(),
            data
        );
        let encrypted = aes_encrypt_ctr(&data, &key, &iv).unwrap();
        assert_eq!(encrypted, bytes(case, "ctr"));
        assert_eq!(aes_decrypt_ctr(&encrypted, &key, &iv).unwrap(), data);
        let encrypted = aes_encrypt_cbc_with_iv(&data, &key, &iv).unwrap();
        assert_eq!(encrypted, bytes(case, "cbc"));
        assert_eq!(
            aes_decrypt_cbc_with_iv(&encrypted, &key, &iv).unwrap(),
            data
        );
        assert_eq!(sha256(&data).as_slice(), bytes(case, "sha256"));
        assert_eq!(md5(&data).as_slice(), bytes(case, "md5"));
        assert_eq!(hmac_sha256(&data, &key).as_slice(), bytes(case, "hmac256"));
        assert_eq!(hmac_sha512(&data, &key).as_slice(), bytes(case, "hmac512"));
        assert_eq!(
            hkdf(&data, 112, Some(&iv), b"WhatsApp Image Keys").unwrap(),
            bytes(case, "hkdf")
        );
    }
    for case in fixtures["pairing"].as_array().unwrap() {
        assert_eq!(
            derive_pairing_code_key(case["code"].as_str().unwrap(), &bytes(case, "salt"))
                .as_slice(),
            bytes(case, "key")
        );
    }
}

#[test]
fn matches_libsignal_signatures_and_shared_keys() {
    let fixtures: Value = serde_json::from_str(include_str!("fixtures/crypto.json")).unwrap();
    let mut sign_bits = [false; 2];
    for case in fixtures["signatures"].as_array().unwrap() {
        let pair = KeyPair::from_private(bytes(case, "private").try_into().unwrap());
        let message = bytes(case, "message");
        let signature = bytes(case, "signature");
        assert_eq!(pair.public.as_slice(), bytes(case, "public"));
        assert_eq!(pair.sign(&message).as_slice(), signature);
        assert!(verify_signature(&pair.public, &message, &signature));
        assert!(verify_signature(
            &signal_public_key(&pair.public).unwrap(),
            &message,
            &signature
        ));
        assert_eq!(
            pair.shared_key(&bytes(case, "peer")).unwrap().as_slice(),
            bytes(case, "shared")
        );
        sign_bits[(signature[63] >> 7) as usize] = true;
        for i in 0..64 {
            let mut corrupted = signature.clone();
            corrupted[i] ^= 1;
            assert!(
                !verify_signature(&pair.public, &message, &corrupted),
                "corruption at {i}"
            );
        }
    }
    assert_eq!(sign_bits, [true, true]);
}

#[test]
fn rejects_invalid_crypto_inputs_and_tampered_ciphertexts() {
    let key = [7; 32];
    let iv = [1; 12];
    let plain = b"authenticated payload";
    let ciphertext = aes_encrypt_gcm(plain, &key, &iv, b"aad").unwrap();
    for i in 0..ciphertext.len() {
        let mut corrupted = ciphertext.clone();
        corrupted[i] ^= 1;
        assert!(aes_decrypt_gcm(&corrupted, &key, &iv, b"aad").is_err());
    }
    assert!(aes_decrypt_gcm(&ciphertext, &key, &iv, b"changed").is_err());
    assert!(aes_encrypt_gcm(plain, &key, &iv[..11], b"").is_err());
    assert!(aes_encrypt_ctr(plain, &key[..31], &[0; 16]).is_err());
    assert!(aes_decrypt_cbc_with_iv(&[0; 15], &key, &[0; 16]).is_err());
    assert!(aes_decrypt(&[0; 15], &key).is_err());
    assert!(hkdf(b"", 8161, None, b"").is_err());
    let pair = KeyPair::from_private([42; 32]);
    assert!(matches!(
        pair.shared_key(&[0; 32]),
        Err(Error::WeakPublicKey)
    ));
    assert!(signal_public_key(&[4; 33]).is_err());
    assert!(!verify_signature(&pair.public, b"", &[0; 63]));
}

#[test]
fn generates_signed_prekeys_and_iv_prefixed_cbc() {
    let pair = KeyPair::generate().unwrap();
    let signed = signed_key_pair(&pair, 42).unwrap();
    assert_eq!(signed.key_id, 42);
    assert!(verify_signature(
        &pair.public,
        &signal_public_key(&signed.key_pair.public).unwrap(),
        &signed.signature
    ));
    let encrypted = aes_encrypt(b"stored content", &[1; 32]).unwrap();
    assert_eq!(
        aes_decrypt(&encrypted, &[1; 32]).unwrap(),
        b"stored content"
    );
    let debug = format!("{pair:?}");
    assert!(!debug.contains("private"));
}

#[test]
fn signs_imported_unclamped_private_keys_like_libsignal() {
    let normalized = KeyPair::from_private([255; 32]);
    let imported = KeyPair {
        public: normalized.public,
        private: [255; 32],
    };
    assert_eq!(
        imported.sign(b"imported key"),
        normalized.sign(b"imported key")
    );
    assert!(verify_signature(
        &imported.public,
        b"imported key",
        &imported.sign(b"imported key")
    ));
}
