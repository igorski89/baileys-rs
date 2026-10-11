use baileys_rs::{
    binary::{BinaryNode, NodeContent},
    proto,
};
use serde_json::Value;
pub fn fixture() -> Value {
    serde_json::from_str(include_str!("../fixtures/receipts.json")).unwrap()
}
pub fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
pub fn node(value: &Value) -> BinaryNode {
    let mut node = BinaryNode::new(value["tag"].as_str().unwrap());
    for (name, value) in value["attrs"].as_object().unwrap() {
        node.attrs
            .insert(name.clone(), value.as_str().unwrap().into());
    }
    if let Some(children) = value["content"].as_array() {
        node.content = Some(NodeContent::Children(
            children.iter().map(self::node).collect(),
        ));
    }
    node
}
pub fn keys(value: &Value) -> Vec<proto::MessageKey> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|k| proto::MessageKey {
            remote_jid: k["remoteJid"].as_str().map(str::to_owned),
            id: k["id"].as_str().map(str::to_owned),
            participant: k["participant"].as_str().map(str::to_owned),
            from_me: k["fromMe"].as_bool(),
        })
        .collect()
}
