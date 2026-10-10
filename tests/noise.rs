use baileys_rs::{
    binary,
    crypto::KeyPair,
    noise::{Config, Error, Frame, NoiseHandler},
    proto::HandshakeMessage,
};
use prost::Message;
use serde_json::Value;
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/noise.json")).unwrap()
}
fn bytes(f: &Value, name: &str) -> Vec<u8> {
    hex(f[name].as_str().unwrap())
}
fn handler(f: &Value) -> NoiseHandler {
    NoiseHandler::new(
        KeyPair::from_private(bytes(f, "ephemeralPrivate").try_into().unwrap()),
        Config {
            certificate_public_key: bytes(f, "root").try_into().unwrap(),
            ..Config::default()
        },
    )
    .unwrap()
}
fn init(h: &mut NoiseHandler, f: &Value) {
    let hello = HandshakeMessage::decode(bytes(f, "hello").as_slice()).unwrap();
    let noise_key = KeyPair::from_private(bytes(f, "staticPrivate").try_into().unwrap());
    assert_eq!(
        h.process_handshake(&hello, &noise_key).unwrap(),
        bytes(f, "encryptedStatic")
    );
    assert_eq!(
        h.encrypt(&bytes(f, "clientPayload")).unwrap(),
        bytes(f, "encryptedPayload")
    );
    h.finish_init().unwrap();
}

#[test]
fn matches_upstream_handshake_and_transport_ciphertext() {
    let f = fixtures();
    let mut h = handler(&f);
    init(&mut h, &f);
    assert!(h.is_transport());
    for frame in f["clientFrames"].as_array().unwrap() {
        assert_eq!(
            h.encode_frame(&bytes(&f, "nodeBytes")).unwrap(),
            hex(frame.as_str().unwrap())
        );
    }
    let expected = binary::decode(&bytes(&f, "nodeBytes")).unwrap();
    let mut combined = Vec::new();
    for frame in f["serverFrames"].as_array().unwrap() {
        combined.extend(hex(frame.as_str().unwrap()));
    }
    h.push_bytes(&combined).unwrap();
    assert_eq!(h.next_frame().unwrap(), Some(Frame::Node(expected.clone())));
    assert_eq!(h.next_frame().unwrap(), Some(Frame::Node(expected)));
    assert_eq!(h.next_frame().unwrap(), None);
}

#[test]
fn reassembles_authenticated_frames_at_every_split() {
    let f = fixtures();
    let frame = hex(f["serverFrames"][0].as_str().unwrap());
    let expected = binary::decode(&bytes(&f, "nodeBytes")).unwrap();
    for offset in 0..frame.len() {
        let mut h = handler(&f);
        init(&mut h, &f);
        h.push_bytes(&frame[..offset]).unwrap();
        assert_eq!(h.next_frame().unwrap(), None, "offset {offset}");
        h.push_bytes(&frame[offset..]).unwrap();
        assert_eq!(h.next_frame().unwrap(), Some(Frame::Node(expected.clone())));
    }
}

#[test]
fn transitions_between_coalesced_handshake_and_transport_frames() {
    let f = fixtures();
    let hello = bytes(&f, "hello");
    let mut combined = vec![
        (hello.len() >> 16) as u8,
        (hello.len() >> 8) as u8,
        hello.len() as u8,
    ];
    combined.extend(&hello);
    combined.extend(hex(f["serverFrames"][0].as_str().unwrap()));
    let mut h = handler(&f);
    h.push_bytes(&combined).unwrap();
    assert_eq!(h.next_frame().unwrap(), Some(Frame::Handshake(hello)));
    init(&mut h, &f);
    assert_eq!(
        h.next_frame().unwrap(),
        Some(Frame::Node(
            binary::decode(&bytes(&f, "nodeBytes")).unwrap()
        ))
    );
}

#[test]
fn rejects_invalid_certificate_chain_and_untrusted_authorities() {
    let f = fixtures();
    let key = KeyPair::from_private(bytes(&f, "staticPrivate").try_into().unwrap());
    for invalid in f["invalid"].as_array().unwrap() {
        let mut h = handler(&f);
        let hello = HandshakeMessage::decode(bytes(invalid, "hello").as_slice()).unwrap();
        assert!(
            matches!(
                h.process_handshake(&hello, &key),
                Err(Error::Certificate(_))
            ),
            "{}",
            invalid["kind"]
        );
        assert!(h.is_failed());
        assert!(matches!(h.finish_init(), Err(Error::Failed)));
        assert!(matches!(
            h.process_handshake(&hello, &key),
            Err(Error::Failed)
        ));
    }
    let mut h = NoiseHandler::new(
        KeyPair::from_private(bytes(&f, "ephemeralPrivate").try_into().unwrap()),
        Config::default(),
    )
    .unwrap();
    let hello = HandshakeMessage::decode(bytes(&f, "hello").as_slice()).unwrap();
    assert!(matches!(
        h.process_handshake(&hello, &key),
        Err(Error::Certificate("intermediate signature"))
    ));
}

#[test]
fn authentication_failure_and_replay_fail_the_connection() {
    let f = fixtures();
    let frame = hex(f["serverFrames"][0].as_str().unwrap());
    for offset in 3..frame.len() {
        let mut h = handler(&f);
        init(&mut h, &f);
        let mut tampered = frame.clone();
        tampered[offset] ^= 1;
        h.push_bytes(&tampered).unwrap();
        assert!(h.next_frame().is_err(), "tampered offset {offset}");
        assert!(h.is_failed());
        assert!(matches!(h.push_bytes(&frame), Err(Error::Failed)));
    }
    let mut h = handler(&f);
    init(&mut h, &f);
    h.push_bytes(&frame).unwrap();
    h.next_frame().unwrap();
    h.push_bytes(&frame).unwrap();
    assert!(h.next_frame().is_err());
    assert!(h.is_failed());
}

#[test]
fn formats_routing_intro_and_handles_unencrypted_frames() {
    let f = fixtures();
    let mut h = NoiseHandler::new(
        KeyPair::from_private([10; 32]),
        Config {
            routing_info: Some(b"routing".to_vec()),
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(h.encode_frame(b"hello").unwrap(), bytes(&f, "routedFrame"));
    assert_eq!(h.encode_frame(b"again").unwrap(), b"\0\0\x05again");
    h.push_bytes(b"\0\0\x05first\0\0\x06second").unwrap();
    assert_eq!(
        h.next_frame().unwrap(),
        Some(Frame::Handshake(b"first".to_vec()))
    );
    assert_eq!(
        h.next_frame().unwrap(),
        Some(Frame::Handshake(b"second".to_vec()))
    );
    assert_eq!(h.next_frame().unwrap(), None);
}

#[test]
fn enforces_handshake_state_and_frame_size_limits() {
    let f = fixtures();
    let mut h = handler(&f);
    assert!(matches!(h.finish_init(), Err(Error::State)));
    let key = KeyPair::from_private([11; 32]);
    assert!(matches!(
        h.process_handshake(&HandshakeMessage::default(), &key),
        Err(Error::Handshake(_))
    ));
    assert!(h.is_failed());
    let mut h = NoiseHandler::new(
        KeyPair::from_private([10; 32]),
        Config {
            max_frame_size: 4,
            ..Config::default()
        },
    )
    .unwrap();
    assert!(matches!(h.encode_frame(b"12345"), Err(Error::FrameLimit)));
    assert_eq!(&h.encode_frame(b"1").unwrap()[..4], b"WA\x06\x03");
    h.push_bytes(&[0, 0, 5]).unwrap();
    assert!(matches!(h.next_frame(), Err(Error::FrameLimit)));
    assert!(h.is_failed());
}
