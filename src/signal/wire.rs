//! Encrypt IQ and retry-receipt prekey bundle helpers.
use super::{
    Error,
    direct::{Bundle, PublicPreKey, Repository},
};
use crate::{
    binary::{BinaryNode, NodeContent},
    crypto,
    jid::Jid,
};
pub struct SessionBundle {
    pub jid: String,
    pub bundle: Bundle,
}
fn bytes(node: &BinaryNode, tag: &str, length: usize) -> Result<Vec<u8>, Error> {
    let value = node
        .child_buffer(tag)
        .filter(|b| b.len() == length)
        .ok_or(Error::Invalid)?;
    Ok(value.to_vec())
}
fn uint(node: &BinaryNode, tag: &str, length: usize) -> Result<u32, Error> {
    let bytes = bytes(node, tag, length)?;
    Ok(bytes.iter().fold(0, |n, b| (n << 8) | u32::from(*b)))
}
fn pre_key(node: &BinaryNode) -> Result<PublicPreKey, Error> {
    Ok(PublicPreKey {
        id: uint(node, "id", 3)?,
        public: crypto::signal_public_key(&bytes(node, "value", 32)?)?.to_vec(),
    })
}
fn bundle(node: &BinaryNode, registration: &BinaryNode) -> Result<Bundle, Error> {
    let signed = node.child("skey").ok_or(Error::Invalid)?;
    Ok(Bundle {
        registration_id: uint(registration, "registration", 4)?,
        identity: crypto::signal_public_key(&bytes(node, "identity", 32)?)?.to_vec(),
        signed: pre_key(signed)?,
        signature: bytes(signed, "signature", 64)?,
        pre_key: node.child("key").map(pre_key).transpose()?,
    })
}
pub fn parse_session_bundles(node: &BinaryNode) -> Result<Vec<SessionBundle>, Error> {
    let users: Vec<_> = node
        .child("list")
        .map(|n| n.children("user").collect())
        .unwrap_or_default();
    for user in &users {
        user.assert_error_free()?;
    }
    users
        .into_iter()
        .map(|user| {
            let jid = user
                .attrs
                .get("jid")
                .filter(|jid| Jid::parse(jid).is_some())
                .ok_or(Error::Address)?
                .clone();
            Ok(SessionBundle {
                jid,
                bundle: bundle(user, user)?,
            })
        })
        .collect()
}
/// A malformed/missing retry bundle is absent, matching upstream's extractor.
pub fn extract_retry_bundle(receipt: &BinaryNode) -> Option<Bundle> {
    let keys = receipt.child("keys")?;
    if keys.child_buffer("type") != Some([5].as_slice()) {
        return None;
    }
    bundle(keys, receipt).ok()
}
pub fn inject_session_bundles(node: &BinaryNode, repository: &Repository) -> Result<usize, Error> {
    let bundles = parse_session_bundles(node)?;
    let count = bundles.len();
    for item in bundles {
        repository.inject_session(&item.jid, &item.bundle)?;
    }
    Ok(count)
}
pub fn session_query(jids: &[String]) -> Result<BinaryNode, Error> {
    let users = jids
        .iter()
        .map(|jid| {
            Jid::parse(jid)
                .filter(|j| !j.user.is_empty())
                .ok_or(Error::Address)?;
            Ok(BinaryNode::new("user").attr("jid", jid))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(BinaryNode::new("iq")
        .attr("xmlns", "encrypt")
        .attr("type", "get")
        .attr("to", "@s.whatsapp.net")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("key").with_content(NodeContent::Children(users)),
        ])))
}
