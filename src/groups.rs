//! Group metadata and management IQs translated from the upstream groups socket.
use crate::{
    binary::{BinaryNode, NodeContent},
    jid::{self, Jid},
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{text}")]
    Response {
        code: u16,
        text: String,
        node: Box<BinaryNode>,
    },
}
impl Error {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::Response { code, .. } => *code,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AddressingMode {
    Pn,
    Lid,
}
/// A present numeric attribute; `None` represents the source's NaN/non-finite value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Number(pub Option<f64>);
fn deserialize_number<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Number>, D::Error> {
    Option::<f64>::deserialize(deserializer).map(|value| Some(Number(value)))
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Participant {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phone_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    pub admin: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<String>,
    pub addressing_mode: AddressingMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_owner_pn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_owner_username: Option<String>,
    /// Missing or non-numeric wire attributes correspond to upstream NaN.
    pub subject_time: Option<f64>,
    pub size: Option<f64>,
    pub creation: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_pn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_username: Option<String>,
    #[serde(rename = "owner_country_code", skip_serializing_if = "Option::is_none")]
    pub owner_country_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc_owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc_owner_pn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc_owner_username: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_number"
    )]
    pub desc_time: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linked_parent: Option<String>,
    pub restrict: bool,
    pub announce: bool,
    pub is_community: bool,
    pub is_community_announce: bool,
    pub join_approval_mode: bool,
    pub member_add_mode: bool,
    pub participants: Vec<Participant>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_number"
    )]
    pub ephemeral_duration: Option<Number>,
}
fn attr(node: &BinaryNode, key: &str) -> Option<String> {
    node.attrs.get(key).cloned()
}
fn nonempty(node: &BinaryNode, key: &str) -> Option<String> {
    attr(node, key).filter(|s| !s.is_empty())
}
fn normalized(node: &BinaryNode, key: &str) -> Option<String> {
    nonempty(node, key).map(|s| jid::normalized_user(&s))
}
fn numeric(node: &BinaryNode, key: &str) -> Option<f64> {
    node.attrs.get(key).and_then(|s| {
        let n = crate::privacy::number(s);
        n.is_finite().then_some(n)
    })
}
pub fn extract_metadata(result: &BinaryNode) -> Result<Metadata, Error> {
    let Some(group) = result.child("group") else {
        if let Some(error) = result.child("error") {
            return Err(Error::Response {
                code: numeric(error, "code")
                    .filter(|c| *c >= 0.0 && *c <= f64::from(u16::MAX))
                    .map(|c| c as u16)
                    .unwrap_or(500),
                text: nonempty(error, "text")
                    .unwrap_or_else(|| "group metadata query failed".into()),
                node: Box::new(error.clone()),
            });
        }
        return Err(Error::Response {
            code: 500,
            text: "Invalid group metadata response: missing <group> node".into(),
            node: Box::new(result.clone()),
        });
    };
    let id = nonempty(group, "id").ok_or_else(|| Error::Response {
        code: 500,
        text: "Invalid group metadata response: missing group id".into(),
        node: Box::new(group.clone()),
    })?;
    let description = group.child("description");
    let participants = group
        .children("participant")
        .map(|node| {
            let id = attr(node, "jid");
            let phone_number = attr(node, "phone_number").filter(|pn| {
                id.as_deref()
                    .and_then(Jid::parse)
                    .is_some_and(|j| j.is_lid_user())
                    && Jid::parse(pn).is_some_and(|j| j.is_pn_user())
            });
            let lid = attr(node, "lid").filter(|lid| {
                id.as_deref()
                    .and_then(Jid::parse)
                    .is_some_and(|j| j.is_pn_user())
                    && Jid::parse(lid).is_some_and(|j| j.is_lid_user())
            });
            Participant {
                id,
                phone_number,
                lid,
                username: nonempty(node, "participant_username")
                    .or_else(|| nonempty(node, "username")),
                admin: nonempty(node, "type"),
            }
        })
        .collect::<Vec<_>>();
    Ok(Metadata {
        id: if id.contains('@') {
            id
        } else {
            format!("{id}@g.us")
        },
        notify: attr(group, "notify"),
        addressing_mode: if group
            .attrs
            .get("addressing_mode")
            .is_some_and(|s| s == "lid")
        {
            AddressingMode::Lid
        } else {
            AddressingMode::Pn
        },
        subject: attr(group, "subject"),
        subject_owner: attr(group, "s_o"),
        subject_owner_pn: attr(group, "s_o_pn"),
        subject_owner_username: attr(group, "s_o_username"),
        subject_time: numeric(group, "s_t"),
        size: if nonempty(group, "size").is_some() {
            numeric(group, "size")
        } else {
            Some(participants.len() as f64)
        },
        creation: numeric(group, "creation"),
        owner: normalized(group, "creator"),
        owner_pn: normalized(group, "creator_pn"),
        owner_username: nonempty(group, "creator_username"),
        owner_country_code: attr(group, "creator_country_code"),
        desc: description.and_then(|d| d.child_string("body")),
        desc_id: description.and_then(|d| attr(d, "id")),
        desc_owner: description.and_then(|d| normalized(d, "participant")),
        desc_owner_pn: description.and_then(|d| normalized(d, "participant_pn")),
        desc_owner_username: description.and_then(|d| nonempty(d, "participant_username")),
        desc_time: description.map(|d| Number(numeric(d, "t"))),
        linked_parent: group
            .child("linked_parent")
            .and_then(|n| nonempty(n, "jid")),
        restrict: group.child("locked").is_some(),
        announce: group.child("announcement").is_some(),
        is_community: group.child("parent").is_some(),
        is_community_announce: group.child("default_sub_group").is_some(),
        join_approval_mode: group.child("membership_approval_mode").is_some(),
        member_add_mode: group.child_string("member_add_mode").as_deref() == Some("all_member_add"),
        participants,
        ephemeral_duration: group
            .child("ephemeral")
            .filter(|n| nonempty(n, "expiration").is_some())
            .map(|n| Number(numeric(n, "expiration"))),
    })
}
pub fn extract_participating(result: &BinaryNode) -> Result<IndexMap<String, Metadata>, Error> {
    let mut groups = IndexMap::new();
    if let Some(node) = result.child("groups") {
        for group in node.children("group") {
            let metadata = extract_metadata(
                &BinaryNode::new("result").with_content(NodeContent::Children(vec![group.clone()])),
            )?;
            groups.insert(metadata.id.clone(), metadata);
        }
    }
    Ok(groups)
}
#[derive(Debug, Clone, Copy)]
pub enum ParticipantAction {
    Add,
    Remove,
    Promote,
    Demote,
    Modify,
}
impl ParticipantAction {
    pub fn tag(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Remove => "remove",
            Self::Promote => "promote",
            Self::Demote => "demote",
            Self::Modify => "modify",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum RequestAction {
    Approve,
    Reject,
}
impl RequestAction {
    pub fn tag(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Reject => "reject",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum Setting {
    Announcement,
    NotAnnouncement,
    Locked,
    Unlocked,
}
impl Setting {
    pub fn tag(self) -> &'static str {
        match self {
            Self::Announcement => "announcement",
            Self::NotAnnouncement => "not_announcement",
            Self::Locked => "locked",
            Self::Unlocked => "unlocked",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum MemberAddMode {
    AdminAdd,
    AllMemberAdd,
}
impl MemberAddMode {
    pub fn text(self) -> &'static str {
        match self {
            Self::AdminAdd => "admin_add",
            Self::AllMemberAdd => "all_member_add",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum JoinApprovalMode {
    On,
    Off,
}
impl JoinApprovalMode {
    pub fn text(self) -> &'static str {
        match self {
            Self::On => "on",
            Self::Off => "off",
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub enum QueryType {
    Get,
    Set,
}
pub fn query(jid: &str, kind: QueryType, content: Vec<BinaryNode>) -> BinaryNode {
    BinaryNode::new("iq")
        .attr(
            "type",
            match kind {
                QueryType::Get => "get",
                QueryType::Set => "set",
            },
        )
        .attr("xmlns", "w:g2")
        .attr("to", jid)
        .with_content(NodeContent::Children(content))
}
pub fn participant_nodes(participants: &[String]) -> Vec<BinaryNode> {
    participants
        .iter()
        .map(|jid| BinaryNode::new("participant").attr("jid", jid))
        .collect()
}
pub fn description_node(description: Option<&str>, previous: Option<&str>, id: &str) -> BinaryNode {
    let mut node = BinaryNode::new("description");
    if let Some(description) = description.filter(|s| !s.is_empty()) {
        node = node.attr("id", id).with_content(NodeContent::Children(vec![
            BinaryNode::new("body")
                .with_content(NodeContent::Bytes(description.as_bytes().to_vec())),
        ]));
    } else {
        node = node.attr("delete", "true");
    }
    if let Some(previous) = previous.filter(|s| !s.is_empty()) {
        node = node.attr("prev", previous);
    }
    node
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantResult {
    pub status: String,
    pub jid: Option<String>,
    pub content: Option<BinaryNode>,
}
pub fn participant_results(
    result: Option<&BinaryNode>,
    include_content: bool,
) -> Vec<ParticipantResult> {
    result
        .into_iter()
        .flat_map(|n| n.children("participant"))
        .map(|node| ParticipantResult {
            status: nonempty(node, "error").unwrap_or_else(|| "200".into()),
            jid: attr(node, "jid"),
            content: include_content.then(|| node.clone()),
        })
        .collect()
}
