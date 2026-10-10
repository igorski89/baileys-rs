//! Primitives matching Baileys' crypto helpers and libsignal's Curve25519 signatures.
use aes::cipher::{
    BlockDecryptMut, BlockEncryptMut, KeyIvInit, StreamCipher, block_padding::Pkcs7,
};
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use curve25519_dalek::{edwards::EdwardsPoint, montgomery::MontgomeryPoint, scalar::Scalar};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid cryptographic key or IV length")]
    InvalidLength,
    #[error("ciphertext authentication failed")]
    Authentication,
    #[error("invalid CBC ciphertext or padding")]
    Padding,
    #[error("HKDF output exceeds maximum length")]
    HkdfLength,
    #[error("random number generation failed")]
    Random,
    #[error("invalid Signal public key encoding")]
    PublicKey,
    #[error("non-contributory Curve25519 public key")]
    WeakPublicKey,
    #[error("CTR stream exhausted")]
    StreamExhausted,
}

#[derive(Clone, Zeroize, ZeroizeOnDrop, serde::Serialize, serde::Deserialize)]
pub struct KeyPair {
    pub public: [u8; 32],
    pub private: [u8; 32],
}
impl std::fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPair")
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}
impl KeyPair {
    pub fn generate() -> Result<Self, Error> {
        let mut bytes = Zeroizing::new([0; 32]);
        getrandom::fill(bytes.as_mut()).map_err(|_| Error::Random)?;
        Ok(Self::from_private(*bytes))
    }
    pub fn from_private(mut private: [u8; 32]) -> Self {
        private[0] &= 248;
        private[31] = (private[31] & 127) | 64;
        let secret = x25519_dalek::StaticSecret::from(private);
        Self {
            public: x25519_dalek::PublicKey::from(&secret).to_bytes(),
            private,
        }
    }
    pub fn shared_key(&self, public: &[u8]) -> Result<[u8; 32], Error> {
        let public = x25519_dalek::PublicKey::from(raw_public_key(public)?);
        let shared = x25519_dalek::StaticSecret::from(self.private).diffie_hellman(&public);
        if !shared.was_contributory() {
            return Err(Error::WeakPublicKey);
        }
        Ok(shared.to_bytes())
    }
    /// Deterministic signature used by libsignal@6.0.0's curve25519-js backend.
    /// Algorithm adapted from its MIT-licensed crypto_sign_direct/curve25519_sign;
    /// see licenses/curve25519-js.txt. This is not Ed25519 seed-key signing.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        // Node-generated and imported persisted private keys may be unclamped.
        let mut private = Zeroizing::new(self.private);
        private[0] &= 248;
        private[31] = (private[31] & 127) | 64;
        let secret = Zeroizing::new(Scalar::from_bytes_mod_order(*private));
        let public = EdwardsPoint::mul_base(&secret).compress();
        let mut nonce_hash = Sha512::new();
        nonce_hash.update(private.as_slice());
        nonce_hash.update(message);
        let mut wide_nonce: [u8; 64] = nonce_hash.finalize().into();
        let nonce = Zeroizing::new(Scalar::from_bytes_mod_order_wide(&wide_nonce));
        wide_nonce.zeroize();
        let r = EdwardsPoint::mul_base(&nonce).compress();
        let mut challenge_hash = Sha512::new();
        challenge_hash.update(r.as_bytes());
        challenge_hash.update(public.as_bytes());
        challenge_hash.update(message);
        let challenge = Scalar::from_bytes_mod_order_wide(&challenge_hash.finalize().into());
        let s = Zeroizing::new(*nonce + challenge * *secret);
        let mut signature = [0; 64];
        signature[..32].copy_from_slice(r.as_bytes());
        signature[32..].copy_from_slice(s.as_bytes());
        signature[63] |= public.as_bytes()[31] & 128;
        signature
    }
}

pub fn raw_public_key(public: &[u8]) -> Result<[u8; 32], Error> {
    let raw = match public.len() {
        32 => public,
        33 if public[0] == 5 => &public[1..],
        _ => return Err(Error::PublicKey),
    };
    raw.try_into().map_err(|_| Error::PublicKey)
}
pub fn signal_public_key(public: &[u8]) -> Result<[u8; 33], Error> {
    let raw = raw_public_key(public)?;
    let mut encoded = [5; 33];
    encoded[1..].copy_from_slice(&raw);
    Ok(encoded)
}

pub fn verify_signature(public: &[u8], message: &[u8], signature: &[u8]) -> bool {
    let Ok(raw) = raw_public_key(public) else {
        return false;
    };
    let Ok(mut sig) = <[u8; 64]>::try_from(signature) else {
        return false;
    };
    let Some(edwards) = MontgomeryPoint(raw).to_edwards(sig[63] >> 7) else {
        return false;
    };
    let Ok(verifier) = ed25519_dalek::VerifyingKey::from_bytes(edwards.compress().as_bytes())
    else {
        return false;
    };
    sig[63] &= 127;
    verifier
        .verify_strict(message, &ed25519_dalek::Signature::from_bytes(&sig))
        .is_ok()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SignedKeyPair {
    pub key_pair: KeyPair,
    pub signature: Vec<u8>,
    pub key_id: u32,
    pub timestamp_s: Option<u64>,
}
pub fn signed_key_pair(identity: &KeyPair, key_id: u32) -> Result<SignedKeyPair, Error> {
    let key_pair = KeyPair::generate()?;
    let signature = identity
        .sign(&signal_public_key(&key_pair.public)?)
        .to_vec();
    Ok(SignedKeyPair {
        key_pair,
        signature,
        key_id,
        timestamp_s: None,
    })
}

pub fn aes_encrypt_gcm(
    plaintext: &[u8],
    key: &[u8],
    iv: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, Error> {
    if iv.len() != 12 {
        return Err(Error::InvalidLength);
    }
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| Error::InvalidLength)?;
    cipher
        .encrypt(
            Nonce::from_slice(iv),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| Error::Authentication)
}
pub fn aes_decrypt_gcm(
    ciphertext: &[u8],
    key: &[u8],
    iv: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, Error> {
    if iv.len() != 12 {
        return Err(Error::InvalidLength);
    }
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|_| Error::InvalidLength)?;
    cipher
        .decrypt(
            Nonce::from_slice(iv),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| Error::Authentication)
}
pub fn aes_encrypt_ctr(plaintext: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, Error> {
    let mut cipher =
        ctr::Ctr128BE::<aes::Aes256>::new_from_slices(key, iv).map_err(|_| Error::InvalidLength)?;
    let mut output = plaintext.to_vec();
    cipher
        .try_apply_keystream(&mut output)
        .map_err(|_| Error::StreamExhausted)?;
    Ok(output)
}
pub fn aes_decrypt_ctr(ciphertext: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, Error> {
    aes_encrypt_ctr(ciphertext, key, iv)
}
pub fn aes_encrypt_cbc_with_iv(plaintext: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, Error> {
    let cipher = cbc::Encryptor::<aes::Aes256>::new_from_slices(key, iv)
        .map_err(|_| Error::InvalidLength)?;
    Ok(cipher.encrypt_padded_vec_mut::<Pkcs7>(plaintext))
}
pub fn aes_decrypt_cbc_with_iv(ciphertext: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>, Error> {
    let cipher = cbc::Decryptor::<aes::Aes256>::new_from_slices(key, iv)
        .map_err(|_| Error::InvalidLength)?;
    cipher
        .decrypt_padded_vec_mut::<Pkcs7>(ciphertext)
        .map_err(|_| Error::Padding)
}
pub fn aes_encrypt(plaintext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let mut iv = [0; 16];
    getrandom::fill(&mut iv).map_err(|_| Error::Random)?;
    let mut output = iv.to_vec();
    output.extend(aes_encrypt_cbc_with_iv(plaintext, key, &iv)?);
    Ok(output)
}
pub fn aes_decrypt(ciphertext: &[u8], key: &[u8]) -> Result<Vec<u8>, Error> {
    let iv = ciphertext.get(..16).ok_or(Error::InvalidLength)?;
    aes_decrypt_cbc_with_iv(&ciphertext[16..], key, iv)
}
pub fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}
pub fn md5(data: &[u8]) -> [u8; 16] {
    md5::Md5::digest(data).into()
}
pub fn hmac_sha256(data: &[u8], key: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}
pub fn hmac_sha512(data: &[u8], key: &[u8]) -> [u8; 64] {
    let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}
pub fn hkdf(
    data: &[u8],
    length: usize,
    salt: Option<&[u8]>,
    info: &[u8],
) -> Result<Vec<u8>, Error> {
    if length > 255 * 32 {
        return Err(Error::HkdfLength);
    }
    let mut result = vec![0; length];
    hkdf::Hkdf::<Sha256>::new(salt, data)
        .expand(info, &mut result)
        .map_err(|_| Error::HkdfLength)?;
    Ok(result)
}
pub fn derive_pairing_code_key(code: &str, salt: &[u8]) -> [u8; 32] {
    let mut result = [0; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(code.as_bytes(), salt, 131_072, &mut result);
    result
}
