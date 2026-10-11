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
    #[error("missing receipt sender")]
    MissingFrom,
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

#[derive(Debug, Clone, PartialEq)]
pub struct MessageUpdate {
    pub key: proto::MessageKey,
    pub status: proto::web_message_info::Status,
    pub message_timestamp: f64,
}
#[derive(Debug, Clone, PartialEq)]
pub struct UserUpdate {
    pub key: proto::MessageKey,
    pub user_jid: String,
    pub receipt_timestamp: Option<f64>,
    pub read_timestamp: Option<f64>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct Incoming {
    pub key: proto::MessageKey,
    pub ids: Vec<Option<String>>,
    pub messages: Vec<MessageUpdate>,
    pub users: Vec<UserUpdate>,
    pub retry: bool,
}
pub fn status(kind: Option<&str>) -> Option<proto::web_message_info::Status> {
    use proto::web_message_info::Status;
    match kind {
        None => Some(Status::DeliveryAck),
        Some("sender") => Some(Status::ServerAck),
        Some("played") => Some(Status::Played),
        Some("read" | "read-self") => Some(Status::Read),
        _ => None,
    }
}
/// Incoming events preserve JavaScript number coercion, including non-finite group times.
pub fn incoming(node: &BinaryNode, me: &str, me_lid: Option<&str>) -> Result<Incoming, Error> {
    let from = node.attrs.get("from").ok_or(Error::MissingFrom)?;
    let participant = node.attrs.get("participant").cloned();
    let recipient = node.attrs.get("recipient").cloned();
    let kind = node.attrs.get("type").map(String::as_str);
    let identity = if from.contains("lid") {
        me_lid.unwrap_or("")
    } else {
        me
    };
    let mine = crate::jid::are_same_user(
        participant
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(from),
        identity,
    );
    let remote = if !mine || from.ends_with("@g.us") {
        Some(from.clone())
    } else {
        recipient.clone()
    };
    let from_me = recipient.as_ref().is_none_or(|s| s.is_empty())
        || (matches!(kind, Some("retry" | "sender")) && mine);
    let key = proto::MessageKey {
        remote_jid: remote.clone(),
        id: Some(String::new()),
        from_me: Some(from_me),
        participant,
    };
    let mut ids = vec![node.attrs.get("id").cloned()];
    if let Some(first) = node.all_children().first() {
        ids.extend(
            first
                .all_children()
                .iter()
                .filter(|n| n.tag == "item")
                .map(|n| n.attrs.get("id").cloned()),
        );
    }
    let mut result = Incoming {
        key,
        ids,
        messages: vec![],
        users: vec![],
        retry: kind == Some("retry"),
    };
    if let Some(status) = status(kind)
        && (status as i32 >= proto::web_message_info::Status::ServerAck as i32 || !mine)
    {
        let group = remote
            .as_deref()
            .is_some_and(|jid| jid.ends_with("@g.us") || jid == crate::jid::STORIES_JID);
        if group {
            if let Some(participant) = result.key.participant.as_deref().filter(|s| !s.is_empty()) {
                let user = crate::jid::Jid::parse(participant)
                    .map(|j| j.normalized())
                    .unwrap_or_default();
                let timestamp = node
                    .attrs
                    .get("t")
                    .map(|t| crate::privacy::number(t))
                    .unwrap_or(f64::NAN);
                for id in &result.ids {
                    let mut key = result.key.clone();
                    key.id = id.clone();
                    result.users.push(UserUpdate {
                        key,
                        user_jid: user.clone(),
                        receipt_timestamp: (status == proto::web_message_info::Status::DeliveryAck)
                            .then_some(timestamp),
                        read_timestamp: (status != proto::web_message_info::Status::DeliveryAck)
                            .then_some(timestamp),
                    });
                }
            }
        } else {
            let timestamp = node
                .attrs
                .get("t")
                .map(|t| crate::privacy::number(t))
                .unwrap_or(0.0);
            let timestamp = if timestamp.is_nan() || timestamp == 0.0 {
                0.0
            } else {
                timestamp
            };
            for id in &result.ids {
                let mut key = result.key.clone();
                key.id = id.clone();
                result.messages.push(MessageUpdate {
                    key,
                    status,
                    message_timestamp: timestamp,
                });
            }
        }
    }
    Ok(result)
}
