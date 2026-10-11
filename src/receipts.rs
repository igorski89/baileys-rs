//! Message receipt grouping and acknowledgement stanza construction.
use crate::{
    binary::{BinaryNode, NodeContent},
    proto,
};
use indexmap::IndexMap;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing ids in receipt")]
    MissingIds,
}
impl Error {
    pub fn status_code(&self) -> u16 {
        500
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Read,
    ReadSelf,
    HistorySync,
    PeerMessage,
    Sender,
    Inactive,
    Played,
}
impl Type {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::ReadSelf => "read-self",
            Self::HistorySync => "hist_sync",
            Self::PeerMessage => "peer_msg",
            Self::Sender => "sender",
            Self::Inactive => "inactive",
            Self::Played => "played",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Group {
    pub jid: Option<String>,
    pub participant: Option<String>,
    pub message_ids: Vec<Option<String>>,
}
/// Preserve first-seen order, duplicates and the upstream colon-delimited grouping key.
/// Missing fields are retained so the binary encoder can omit undefined attributes.
pub fn aggregate(keys: &[proto::MessageKey]) -> Vec<Group> {
    let mut groups = IndexMap::<String, Group>::new();
    for key in keys.iter().filter(|key| key.from_me != Some(true)) {
        let composite = format!(
            "{}:{}",
            key.remote_jid.as_deref().unwrap_or("undefined"),
            key.participant.as_deref().unwrap_or("")
        );
        let group = groups.entry(composite).or_insert_with(|| Group {
            jid: key.remote_jid.clone(),
            participant: key.participant.clone(),
            message_ids: vec![],
        });
        group.message_ids.push(key.id.clone());
    }
    groups.into_values().collect()
}
pub fn receipt(group: &Group, kind: Option<Type>, timestamp: u64) -> Result<BinaryNode, Error> {
    let first = group.message_ids.first().ok_or(Error::MissingIds)?;
    let mut node = BinaryNode::new("receipt");
    if let Some(id) = first {
        node.attrs.insert("id".into(), id.clone());
    }
    if matches!(kind, Some(Type::Read | Type::ReadSelf)) {
        node.attrs.insert("t".into(), timestamp.to_string());
    }
    if kind == Some(Type::Sender)
        && group
            .jid
            .as_deref()
            .is_some_and(|jid| jid.ends_with("@s.whatsapp.net") || jid.ends_with("@lid"))
    {
        if let Some(jid) = &group.jid {
            node.attrs.insert("recipient".into(), jid.clone());
        }
        if let Some(participant) = &group.participant {
            node.attrs.insert("to".into(), participant.clone());
        }
    } else {
        if let Some(jid) = &group.jid {
            node.attrs.insert("to".into(), jid.clone());
        }
        if let Some(participant) = group.participant.as_ref().filter(|s| !s.is_empty()) {
            node.attrs.insert("participant".into(), participant.clone());
        }
    }
    if let Some(kind) = kind {
        node.attrs.insert("type".into(), kind.as_str().into());
    }
    if group.message_ids.len() > 1 {
        node.content = Some(NodeContent::Children(vec![
            BinaryNode::new("list").with_content(NodeContent::Children(
                group.message_ids[1..]
                    .iter()
                    .map(|id| {
                        let mut item = BinaryNode::new("item");
                        if let Some(id) = id {
                            item.attrs.insert("id".into(), id.clone());
                        }
                        item
                    })
                    .collect(),
            )),
        ]));
    }
    Ok(node)
}
pub fn acknowledgement(node: &BinaryNode, error_code: Option<u32>, me: Option<&str>) -> BinaryNode {
    let mut ack = BinaryNode::new("ack");
    for (output, input) in [("id", "id"), ("to", "from")] {
        if let Some(value) = node.attrs.get(input) {
            ack.attrs.insert(output.into(), value.clone());
        }
    }
    ack.attrs.insert("class".into(), node.tag.clone());
    if let Some(code) = error_code.filter(|code| *code != 0) {
        ack.attrs.insert("error".into(), code.to_string());
    }
    for field in ["participant", "recipient", "type"] {
        if let Some(value) = node.attrs.get(field).filter(|s| !s.is_empty()) {
            ack.attrs.insert(field.into(), value.clone());
        }
    }
    if node.tag == "message"
        && let Some(me) = me.filter(|s| !s.is_empty())
    {
        ack.attrs.insert("from".into(), me.into());
    }
    ack
}
pub fn reject_call(me: &str, id: &str, from: &str) -> BinaryNode {
    BinaryNode::new("call")
        .attr("from", me)
        .attr("to", from)
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("reject")
                .attr("call-id", id)
                .attr("call-creator", from)
                .attr("count", "0"),
        ]))
}
