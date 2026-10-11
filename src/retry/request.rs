use crate::{
    auth,
    auth::Credentials,
    binary::{BinaryNode, NodeContent},
    crypto::KeyPair,
    relay,
};
fn bytes(tag: &str, bytes: Vec<u8>) -> BinaryNode {
    BinaryNode::new(tag).with_content(NodeContent::Bytes(bytes))
}
pub fn receipt(
    node: &BinaryNode,
    id: &str,
    count: u64,
    credentials: &Credentials,
    prekey: Option<(u32, &KeyPair)>,
) -> Result<BinaryNode, relay::Error> {
    let account = credentials.account.as_ref().ok_or(relay::Error::Identity)?;
    let identity = relay::encode_device_identity(account, true);
    let mut retry = BinaryNode::new("retry").attr("count", count.to_string());
    for field in ["id", "t"] {
        if let Some(value) = node.attrs.get(field) {
            retry.attrs.insert(field.into(), value.clone());
        }
    }
    retry = retry.attr("v", "1").attr("error", "0");
    let mut children = vec![
        retry,
        bytes(
            "registration",
            u32::from(credentials.registration_id)
                .to_be_bytes()
                .to_vec(),
        ),
    ];
    if let Some((id, prekey)) = prekey {
        let signed = &credentials.signed_pre_key;
        if id > 0xffffff || signed.key_id > 0xffffff {
            return Err(auth::Error::PreKeyRange.into());
        }
        children.push(
            BinaryNode::new("keys").with_content(NodeContent::Children(vec![
                bytes("type", vec![5]),
                bytes("identity", credentials.signed_identity_key.public.to_vec()),
                BinaryNode::new("key").with_content(NodeContent::Children(vec![
                    bytes("id", id.to_be_bytes()[1..].to_vec()),
                    bytes("value", prekey.public.to_vec()),
                ])),
                BinaryNode::new("skey").with_content(NodeContent::Children(vec![
                    bytes("id", signed.key_id.to_be_bytes()[1..].to_vec()),
                    bytes("value", signed.key_pair.public.to_vec()),
                    bytes("signature", signed.signature.clone()),
                ])),
                bytes("device-identity", identity),
            ])),
        );
    }
    let mut receipt = BinaryNode::new("receipt")
        .attr("id", id)
        .attr("type", "retry");
    if let Some(from) = node.attrs.get("from") {
        receipt.attrs.insert("to".into(), from.clone());
    }
    for field in ["recipient", "participant"] {
        if let Some(value) = node.attrs.get(field).filter(|s| !s.is_empty()) {
            receipt.attrs.insert(field.into(), value.clone());
        }
    }
    Ok(receipt.with_content(NodeContent::Children(children)))
}
