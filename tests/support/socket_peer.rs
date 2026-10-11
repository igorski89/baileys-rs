use baileys_rs::{
    binary::{self, BinaryNode},
    crypto::{self, KeyPair},
    defaults, proto,
};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use tokio::net::TcpStream;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message as WsMessage};

pub const ROOT_PRIVATE: [u8; 32] = [20; 32];
pub struct Peer {
    pub ws: WebSocketStream<TcpStream>,
    write_key: Vec<u8>,
    read_key: Vec<u8>,
    write_counter: u32,
    read_counter: u32,
    pub payload: proto::ClientPayload,
    pub routing: Option<Vec<u8>>,
}
struct Transcript {
    hash: [u8; 32],
    salt: Vec<u8>,
    key: Vec<u8>,
    counter: u32,
}
fn nonce(counter: &mut u32) -> [u8; 12] {
    let mut nonce = [0; 12];
    nonce[8..].copy_from_slice(&counter.to_be_bytes());
    *counter += 1;
    nonce
}
impl Transcript {
    fn authenticate(&mut self, data: &[u8]) {
        self.hash = crypto::sha256(&[self.hash.as_slice(), data].concat());
    }
    fn mix(&mut self, shared: &[u8]) {
        let keys = crypto::hkdf(shared, 64, Some(&self.salt), b"").unwrap();
        self.salt = keys[..32].to_vec();
        self.key = keys[32..].to_vec();
        self.counter = 0;
    }
    fn encrypt(&mut self, data: &[u8]) -> Vec<u8> {
        let encrypted =
            crypto::aes_encrypt_gcm(data, &self.key, &nonce(&mut self.counter), &self.hash)
                .unwrap();
        self.authenticate(&encrypted);
        encrypted
    }
    fn decrypt(&mut self, data: &[u8]) -> Vec<u8> {
        let plain = crypto::aes_decrypt_gcm(data, &self.key, &nonce(&mut self.counter), &self.hash)
            .unwrap();
        self.authenticate(data);
        plain
    }
}
fn frame(data: &[u8]) -> Vec<u8> {
    let len = data.len();
    let mut out = vec![(len >> 16) as u8, (len >> 8) as u8, len as u8];
    out.extend(data);
    out
}
fn unframe(data: &[u8]) -> &[u8] {
    let len = ((data[0] as usize) << 16) | ((data[1] as usize) << 8) | data[2] as usize;
    assert_eq!(len, data.len() - 3);
    &data[3..]
}
async fn receive(ws: &mut WebSocketStream<TcpStream>) -> Vec<u8> {
    loop {
        match ws.next().await.unwrap().unwrap() {
            WsMessage::Binary(bytes) => return bytes.to_vec(),
            WsMessage::Ping(bytes) => ws.send(WsMessage::Pong(bytes)).await.unwrap(),
            other => panic!("expected binary message, got {other:?}"),
        }
    }
}
impl Peer {
    // Tungstenite requires this callback to return its unboxed HTTP error response.
    #[allow(clippy::result_large_err)]
    pub async fn accept(stream: TcpStream) -> Self {
        let mut ws = tokio_tungstenite::accept_hdr_async(
            stream,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert_eq!(request.headers()["origin"], "https://web.whatsapp.com");
                Ok(response)
            },
        )
        .await
        .unwrap();
        let bytes = receive(&mut ws).await;
        let (routing, offset) = if bytes.starts_with(b"ED\0\x01") {
            let len = ((bytes[4] as usize) << 16) | ((bytes[5] as usize) << 8) | bytes[6] as usize;
            (Some(bytes[7..7 + len].to_vec()), 7 + len)
        } else {
            (None, 0)
        };
        assert_eq!(&bytes[offset..offset + 4], defaults::NOISE_WA_HEADER);
        let hello = proto::HandshakeMessage::decode(unframe(&bytes[offset + 4..])).unwrap();
        let client = hello.client_hello.unwrap().ephemeral.unwrap();
        let ephemeral = KeyPair::from_private([12; 32]);
        let static_key = KeyPair::from_private([13; 32]);
        let root = KeyPair::from_private(ROOT_PRIVATE);
        let intermediate = KeyPair::from_private([21; 32]);
        let details = proto::cert_chain::noise_certificate::Details {
            issuer_serial: Some(defaults::WA_CERT_SERIAL),
            key: Some(intermediate.public.to_vec()),
            ..Default::default()
        }
        .encode_to_vec();
        let leaf = b"synthetic local websocket certificate".to_vec();
        let certificate = proto::CertChain {
            intermediate: Some(proto::cert_chain::NoiseCertificate {
                signature: Some(root.sign(&details).to_vec()),
                details: Some(details),
            }),
            leaf: Some(proto::cert_chain::NoiseCertificate {
                signature: Some(intermediate.sign(&leaf).to_vec()),
                details: Some(leaf),
            }),
        }
        .encode_to_vec();
        let mut transcript = Transcript {
            hash: *defaults::NOISE_MODE,
            salt: defaults::NOISE_MODE.to_vec(),
            key: defaults::NOISE_MODE.to_vec(),
            counter: 0,
        };
        transcript.authenticate(&defaults::NOISE_WA_HEADER);
        transcript.authenticate(&client);
        transcript.authenticate(&ephemeral.public);
        transcript.mix(&ephemeral.shared_key(&client).unwrap());
        let encrypted_static = transcript.encrypt(&static_key.public);
        transcript.mix(&static_key.shared_key(&client).unwrap());
        let encrypted_certificate = transcript.encrypt(&certificate);
        let hello = proto::HandshakeMessage {
            server_hello: Some(proto::handshake_message::ServerHello {
                ephemeral: Some(ephemeral.public.to_vec()),
                r#static: Some(encrypted_static),
                payload: Some(encrypted_certificate),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec();
        // Noise frames can span several independent WebSocket messages.
        let wire = frame(&hello);
        for part in wire.chunks(37) {
            ws.send(WsMessage::Binary(part.to_vec().into()))
                .await
                .unwrap();
        }
        let finish = proto::HandshakeMessage::decode(unframe(&receive(&mut ws).await))
            .unwrap()
            .client_finish
            .unwrap();
        let client_static = transcript.decrypt(&finish.r#static.unwrap());
        transcript.mix(&ephemeral.shared_key(&client_static).unwrap());
        let payload =
            proto::ClientPayload::decode(transcript.decrypt(&finish.payload.unwrap()).as_slice())
                .unwrap();
        let keys = crypto::hkdf(b"", 64, Some(&transcript.salt), b"").unwrap();
        Self {
            ws,
            write_key: keys[32..].to_vec(),
            read_key: keys[..32].to_vec(),
            write_counter: 0,
            read_counter: 0,
            payload,
            routing,
        }
    }
    pub async fn read(&mut self) -> BinaryNode {
        let wire = receive(&mut self.ws).await;
        let bytes = crypto::aes_decrypt_gcm(
            unframe(&wire),
            &self.read_key,
            &nonce(&mut self.read_counter),
            b"",
        )
        .unwrap();
        binary::decode(&bytes).unwrap()
    }
    pub async fn send(&mut self, node: BinaryNode) {
        self.send_many(&[node]).await;
    }
    pub async fn send_many(&mut self, nodes: &[BinaryNode]) {
        let mut wire = Vec::new();
        for node in nodes {
            let bytes = crypto::aes_encrypt_gcm(
                &binary::encode(node).unwrap(),
                &self.write_key,
                &nonce(&mut self.write_counter),
                b"",
            )
            .unwrap();
            wire.extend(frame(&bytes));
        }
        self.ws.send(WsMessage::Binary(wire.into())).await.unwrap();
    }
    pub async fn result(&mut self, node: &BinaryNode) {
        self.send(
            BinaryNode::new("iq")
                .attr("id", &node.attrs["id"])
                .attr("type", "result"),
        )
        .await;
    }
    pub async fn closed(&mut self) {
        while let Some(message) = self.ws.next().await {
            if matches!(message, Ok(WsMessage::Close(_))) {
                break;
            }
            message.unwrap();
        }
    }
}
