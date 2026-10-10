use baileys_rs::binary::{
    BinaryNode, DecodeLimits, Error, NodeContent, decode, decode_with_limits, encode,
};
use flate2::{Compression, write::ZlibEncoder};
use std::io::Write;

#[test]
fn matches_upstream_encoded_bytes_and_decoded_nodes() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/binary.json")).unwrap();
    for fixture in fixtures.as_array().unwrap() {
        let name = fixture["name"].as_str().unwrap();
        if let Some(repeated) = fixture.get("repeated") {
            let bytes = vec![
                repeated["byte"].as_u64().unwrap() as u8;
                repeated["length"].as_u64().unwrap() as usize
            ];
            let node = BinaryNode::new("enc").with_content(NodeContent::Bytes(bytes.clone()));
            let mut wire: Vec<u8> = serde_json::from_value(fixture["prefix"].clone()).unwrap();
            wire.extend(bytes);
            assert_eq!(encode(&node).unwrap(), wire, "{name}");
            assert_eq!(decode(&wire).unwrap(), node, "{name}");
        } else {
            let wire: Vec<u8> = serde_json::from_value(fixture["wire"].clone()).unwrap();
            if let Some(input) = fixture.get("input") {
                let input: BinaryNode = serde_json::from_value(input.clone()).unwrap();
                assert_eq!(encode(&input).unwrap(), wire, "{name}");
            }
            let expected: BinaryNode = serde_json::from_value(fixture["decoded"].clone()).unwrap();
            assert_eq!(decode(&wire).unwrap(), expected, "{name}");
        }
    }
}

#[test]
fn rejects_truncated_frames_at_every_offset() {
    let node = BinaryNode::new("message")
        .attr("to", "123:2@lid")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("enc").with_content(NodeContent::Bytes(vec![1, 2, 3, 4])),
        ]));
    let wire = encode(&node).unwrap();
    for end in 0..wire.len() {
        assert!(decode(&wire[..end]).is_err(), "offset {end}");
    }
    assert_eq!(decode(&wire).unwrap(), node);
}

#[test]
fn enforces_resource_limits_for_compressed_and_nested_frames() {
    let node = BinaryNode::new("iq").with_content(NodeContent::Bytes(vec![0; 1024]));
    let wire = encode(&node).unwrap();
    let mut compressor = ZlibEncoder::new(Vec::new(), Compression::default());
    compressor.write_all(&wire[1..]).unwrap();
    let mut compressed = vec![2];
    compressed.extend(compressor.finish().unwrap());
    let limits = DecodeLimits {
        max_bytes: 100,
        ..DecodeLimits::default()
    };
    assert!(matches!(
        decode_with_limits(&compressed, limits),
        Err(Error::Limit("inflated bytes"))
    ));
    let node =
        BinaryNode::new("iq").with_content(NodeContent::Children(vec![BinaryNode::new("item")]));
    let wire = encode(&node).unwrap();
    assert!(matches!(
        decode_with_limits(
            &wire,
            DecodeLimits {
                max_nodes: 1,
                ..DecodeLimits::default()
            }
        ),
        Err(Error::Limit("nodes"))
    ));
    assert!(matches!(
        decode_with_limits(
            &wire,
            DecodeLimits {
                max_depth: 0,
                ..DecodeLimits::default()
            }
        ),
        Err(Error::Limit("depth"))
    ));
}

#[test]
fn supports_incoming_facebook_and_interop_identifiers() {
    // Raw iq node with a to attribute encoded with the decoder-only JID tags.
    let prefix = [0, 248, 3, 25, 17];
    let mut facebook = prefix.to_vec();
    facebook.extend([246, 252, 1, b'u', 1, 0, 252, 4, b't', b'e', b's', b't']);
    let decoded = decode(&facebook).unwrap();
    assert_eq!(decoded.attrs.values().next().unwrap(), "u:256@test");
    let mut interop = prefix.to_vec();
    interop.extend([245, 252, 1, b'u', 0, 2, 0, 3]);
    let decoded = decode(&interop).unwrap();
    assert_eq!(decoded.attrs.values().next().unwrap(), "3-u:2@interop");
}

#[test]
fn accessors_handle_content_and_stanza_errors() {
    let node = BinaryNode::new("iq").with_content(NodeContent::Children(vec![
        BinaryNode::new("value").with_content(NodeContent::Bytes(vec![0, 1, 2])),
        BinaryNode::new("error")
            .attr("code", "403")
            .attr("text", "Forbidden"),
    ]));
    assert_eq!(node.children("value").count(), 1);
    assert_eq!(node.child_uint("value", 3).unwrap(), Some(258));
    assert!(node.child_uint("value", 4).is_err());
    assert!(matches!(
        node.assert_error_free(),
        Err(Error::Stanza {
            code: Some(403),
            ..
        })
    ));
}
