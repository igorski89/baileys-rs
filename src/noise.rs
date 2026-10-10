//! WhatsApp Noise handshake, authenticated transport and incremental frame reassembly.
use crate::{
    binary::{self, BinaryNode, DecodeLimits},
    crypto::{self, KeyPair},
    defaults, proto,
};
use prost::Message;
use zeroize::Zeroizing;

const MAX_WIRE_FRAME: usize = 0xffffff;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Binary(#[from] binary::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error("invalid Noise handshake: {0}")]
    Handshake(&'static str),
    #[error("invalid Noise certificate: {0}")]
    Certificate(&'static str),
    #[error("Noise counter exhausted")]
    CounterExhausted,
    #[error("Noise frame exceeds configured size")]
    FrameLimit,
    #[error("Noise handler is in failed state")]
    Failed,
    #[error("invalid Noise state transition")]
    State,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub header: Vec<u8>,
    pub routing_info: Option<Vec<u8>>,
    pub certificate_public_key: [u8; 32],
    pub certificate_issuer_serial: u32,
    pub max_frame_size: usize,
    pub binary_limits: DecodeLimits,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            header: defaults::NOISE_WA_HEADER.to_vec(),
            routing_info: None,
            certificate_public_key: defaults::WA_CERT_PUBLIC_KEY,
            certificate_issuer_serial: defaults::WA_CERT_SERIAL,
            max_frame_size: MAX_WIRE_FRAME,
            binary_limits: DecodeLimits::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Handshake(Vec<u8>),
    Node(BinaryNode),
}

struct HandshakeState {
    hash: [u8; 32],
    salt: Zeroizing<[u8; 32]>,
    key: Zeroizing<[u8; 32]>,
    counter: u64,
    verified: bool,
}
struct TransportState {
    write_key: Zeroizing<[u8; 32]>,
    read_key: Zeroizing<[u8; 32]>,
    write_counter: u64,
    read_counter: u64,
}
enum State {
    Handshake(HandshakeState),
    Transport(TransportState),
    Failed,
}

/// All mutations take exclusive ownership through `&mut self`, including decoding
/// partial frames. Handshake and authentication errors permanently fail the handler.
pub struct NoiseHandler {
    ephemeral: KeyPair,
    config: Config,
    state: State,
    intro: Vec<u8>,
    sent_intro: bool,
    pending: Vec<u8>,
}
impl NoiseHandler {
    pub fn new(ephemeral: KeyPair, config: Config) -> Result<Self, Error> {
        if config.max_frame_size > MAX_WIRE_FRAME {
            return Err(Error::FrameLimit);
        }
        let mut intro = Vec::new();
        if let Some(routing) = &config.routing_info {
            if routing.len() > MAX_WIRE_FRAME {
                return Err(Error::FrameLimit);
            }
            intro.extend(b"ED\0\x01");
            write_length(routing.len(), &mut intro);
            intro.extend(routing);
        }
        intro.extend(&config.header);
        let hash = *defaults::NOISE_MODE;
        let mut handler = Self {
            ephemeral,
            config,
            state: State::Handshake(HandshakeState {
                hash,
                salt: Zeroizing::new(hash),
                key: Zeroizing::new(hash),
                counter: 0,
                verified: false,
            }),
            intro,
            sent_intro: false,
            pending: Vec::new(),
        };
        handler.authenticate(&handler.config.header.clone())?;
        let public = handler.ephemeral.public;
        handler.authenticate(&public)?;
        Ok(handler)
    }
    pub fn is_transport(&self) -> bool {
        matches!(self.state, State::Transport(_))
    }
    pub fn is_failed(&self) -> bool {
        matches!(self.state, State::Failed)
    }
    pub fn authenticate(&mut self, data: &[u8]) -> Result<(), Error> {
        match &mut self.state {
            State::Handshake(state) => {
                let mut bytes = Vec::with_capacity(32 + data.len());
                bytes.extend(state.hash);
                bytes.extend(data);
                state.hash = crypto::sha256(&bytes);
                Ok(())
            }
            State::Transport(_) => Ok(()),
            State::Failed => Err(Error::Failed),
        }
    }
    pub fn mix_into_key(&mut self, data: &[u8]) -> Result<(), Error> {
        let State::Handshake(state) = &mut self.state else {
            return Err(if self.is_failed() {
                Error::Failed
            } else {
                Error::State
            });
        };
        let keys = Zeroizing::new(crypto::hkdf(data, 64, Some(state.salt.as_slice()), b"")?);
        state.salt.copy_from_slice(&keys[..32]);
        state.key.copy_from_slice(&keys[32..]);
        state.counter = 0;
        Ok(())
    }
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let result = match &mut self.state {
            State::Handshake(state) => {
                let iv = nonce(&mut state.counter)?;
                crypto::aes_encrypt_gcm(plaintext, state.key.as_slice(), &iv, &state.hash)
            }
            State::Transport(state) => {
                let iv = nonce(&mut state.write_counter)?;
                crypto::aes_encrypt_gcm(plaintext, state.write_key.as_slice(), &iv, b"")
            }
            State::Failed => return Err(Error::Failed),
        };
        match result {
            Ok(ciphertext) => {
                self.authenticate(&ciphertext)?;
                Ok(ciphertext)
            }
            Err(error) => {
                self.state = State::Failed;
                Err(error.into())
            }
        }
    }
    pub fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, Error> {
        let result = match &mut self.state {
            State::Handshake(state) => {
                let iv = nonce(&mut state.counter)?;
                crypto::aes_decrypt_gcm(ciphertext, state.key.as_slice(), &iv, &state.hash)
            }
            State::Transport(state) => {
                let iv = nonce(&mut state.read_counter)?;
                crypto::aes_decrypt_gcm(ciphertext, state.read_key.as_slice(), &iv, b"")
            }
            State::Failed => return Err(Error::Failed),
        };
        match result {
            Ok(plaintext) => {
                self.authenticate(ciphertext)?;
                Ok(plaintext)
            }
            Err(error) => {
                self.state = State::Failed;
                Err(error.into())
            }
        }
    }
    /// Verify the server certificate chain and produce the encrypted client static key.
    pub fn process_handshake(
        &mut self,
        message: &proto::HandshakeMessage,
        noise_key: &KeyPair,
    ) -> Result<Vec<u8>, Error> {
        if !matches!(&self.state,State::Handshake(s) if !s.verified) {
            return Err(if self.is_failed() {
                Error::Failed
            } else {
                Error::State
            });
        }
        let result = self.handshake_inner(message, noise_key);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn handshake_inner(
        &mut self,
        message: &proto::HandshakeMessage,
        noise_key: &KeyPair,
    ) -> Result<Vec<u8>, Error> {
        let hello = message
            .server_hello
            .as_ref()
            .ok_or(Error::Handshake("missing server hello"))?;
        let ephemeral = hello
            .ephemeral
            .as_deref()
            .ok_or(Error::Handshake("missing ephemeral key"))?;
        self.authenticate(ephemeral)?;
        let shared = Zeroizing::new(self.ephemeral.shared_key(ephemeral)?);
        self.mix_into_key(shared.as_slice())?;
        let static_key = self.decrypt(
            hello
                .r#static
                .as_deref()
                .ok_or(Error::Handshake("missing static key"))?,
        )?;
        let shared = Zeroizing::new(self.ephemeral.shared_key(&static_key)?);
        self.mix_into_key(shared.as_slice())?;
        let certificate = self.decrypt(
            hello
                .payload
                .as_deref()
                .ok_or(Error::Handshake("missing certificate payload"))?,
        )?;
        self.verify_certificate(&certificate)?;
        let encrypted = self.encrypt(&noise_key.public)?;
        let shared = Zeroizing::new(noise_key.shared_key(ephemeral)?);
        self.mix_into_key(shared.as_slice())?;
        if let State::Handshake(state) = &mut self.state {
            state.verified = true;
        }
        Ok(encrypted)
    }
    fn verify_certificate(&self, bytes: &[u8]) -> Result<(), Error> {
        let chain = proto::CertChain::decode(bytes)?;
        let leaf = chain.leaf.ok_or(Error::Certificate("missing leaf"))?;
        let intermediate = chain
            .intermediate
            .ok_or(Error::Certificate("missing intermediate"))?;
        let details = intermediate
            .details
            .ok_or(Error::Certificate("missing intermediate details"))?;
        let signature = intermediate
            .signature
            .ok_or(Error::Certificate("missing intermediate signature"))?;
        let decoded = proto::cert_chain::noise_certificate::Details::decode(details.as_slice())?;
        let leaf_details = leaf
            .details
            .ok_or(Error::Certificate("missing leaf details"))?;
        let leaf_signature = leaf
            .signature
            .ok_or(Error::Certificate("missing leaf signature"))?;
        let key = decoded
            .key
            .ok_or(Error::Certificate("missing intermediate key"))?;
        if !crypto::verify_signature(&key, &leaf_details, &leaf_signature) {
            return Err(Error::Certificate("leaf signature"));
        }
        if !crypto::verify_signature(&self.config.certificate_public_key, &details, &signature) {
            return Err(Error::Certificate("intermediate signature"));
        }
        if decoded.issuer_serial != Some(self.config.certificate_issuer_serial) {
            return Err(Error::Certificate("issuer serial"));
        }
        Ok(())
    }
    /// Transition only after a verified handshake; any partial frame stays buffered.
    pub fn finish_init(&mut self) -> Result<(), Error> {
        let State::Handshake(state) = &self.state else {
            return Err(if self.is_failed() {
                Error::Failed
            } else {
                Error::State
            });
        };
        if !state.verified {
            return Err(Error::State);
        }
        let keys = Zeroizing::new(crypto::hkdf(b"", 64, Some(state.salt.as_slice()), b"")?);
        let mut write_key = Zeroizing::new([0; 32]);
        let mut read_key = Zeroizing::new([0; 32]);
        write_key.copy_from_slice(&keys[..32]);
        read_key.copy_from_slice(&keys[32..]);
        self.state = State::Transport(TransportState {
            write_key,
            read_key,
            write_counter: 0,
            read_counter: 0,
        });
        Ok(())
    }
    pub fn encode_frame(&mut self, data: &[u8]) -> Result<Vec<u8>, Error> {
        if self.is_failed() {
            return Err(Error::Failed);
        }
        let overhead = if self.is_transport() { 16 } else { 0 };
        let length = data.len().checked_add(overhead).ok_or(Error::FrameLimit)?;
        if length > self.config.max_frame_size {
            return Err(Error::FrameLimit);
        }
        let payload = if self.is_transport() {
            self.encrypt(data)?
        } else {
            data.to_vec()
        };
        let mut frame = Vec::new();
        if !self.sent_intro {
            frame.extend(&self.intro);
        }
        write_length(payload.len(), &mut frame);
        frame.extend(payload);
        self.sent_intro = true;
        Ok(frame)
    }
    /// Append a websocket fragment. Pull frames individually so callers can perform
    /// the handshake transition before decoding a coalesced first transport frame.
    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.is_failed() {
            return Err(Error::Failed);
        }
        if self.pending.len().checked_add(bytes.len()).is_none_or(|n| {
            n > self
                .config
                .max_frame_size
                .saturating_mul(2)
                .saturating_add(6)
        }) {
            self.state = State::Failed;
            return Err(Error::FrameLimit);
        }
        self.pending.extend(bytes);
        Ok(())
    }
    pub fn next_frame(&mut self) -> Result<Option<Frame>, Error> {
        if self.is_failed() {
            return Err(Error::Failed);
        }
        if self.pending.len() < 3 {
            return Ok(None);
        }
        let length = (usize::from(self.pending[0]) << 16)
            | (usize::from(self.pending[1]) << 8)
            | usize::from(self.pending[2]);
        if length > self.config.max_frame_size {
            self.state = State::Failed;
            return Err(Error::FrameLimit);
        }
        if self.pending.len() < length + 3 {
            return Ok(None);
        }
        let payload = self.pending[3..length + 3].to_vec();
        self.pending.drain(..length + 3);
        if self.is_transport() {
            let plain = self.decrypt(&payload)?;
            match binary::decode_with_limits(&plain, self.config.binary_limits) {
                Ok(node) => Ok(Some(Frame::Node(node))),
                Err(error) => {
                    self.state = State::Failed;
                    Err(error.into())
                }
            }
        } else {
            Ok(Some(Frame::Handshake(payload)))
        }
    }
}
fn write_length(length: usize, output: &mut Vec<u8>) {
    output.extend([(length >> 16) as u8, (length >> 8) as u8, length as u8]);
}
fn nonce(counter: &mut u64) -> Result<[u8; 12], Error> {
    let current = u32::try_from(*counter).map_err(|_| Error::CounterExhausted)?;
    *counter += 1;
    let mut iv = [0; 12];
    iv[8..].copy_from_slice(&current.to_be_bytes());
    Ok(iv)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counter_does_not_reuse_nonce_after_u32_exhaustion() {
        let mut counter = u32::MAX as u64;
        assert_eq!(nonce(&mut counter).unwrap()[8..], [255; 4]);
        assert!(matches!(nonce(&mut counter), Err(Error::CounterExhausted)));
    }
}
