//! Community metadata and sub-group listings from the upstream communities socket.
use crate::{
    binary::BinaryNode,
    groups::{self, Number},
    jid,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Participant {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub admin: Option<String>,
}
/// Community metadata as upstream's extractCommunityMetadata reports it. Numeric
/// fields are None where the source value is NaN or infinite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
    pub subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_owner: Option<String>,
    pub subject_time: Option<f64>,
    pub size: usize,
    pub creation: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linked_parent: Option<String>,
    pub restrict: bool,
    pub announce: bool,
    pub is_community: bool,
    pub is_community_announce: bool,
    pub join_approval_mode: bool,
    pub member_add_mode: bool,
    pub participants: Vec<Participant>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ephemeral_duration: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addressing_mode: Option<String>,
}
fn attr(node: &BinaryNode, key: &str) -> Option<String> {
    node.attrs.get(key).cloned()
}
fn nonempty(node: &BinaryNode, key: &str) -> Option<String> {
    attr(node, key).filter(|s| !s.is_empty())
}
// Number(value || 0), with non-finite results reported as None.
fn number_or_zero(node: &BinaryNode, key: &str) -> Option<f64> {
    let value = nonempty(node, key).map_or(0.0, |s| crate::privacy::number(&s));
    value.is_finite().then_some(value)
}
pub fn extract_metadata(result: &BinaryNode) -> Result<Metadata, groups::Error> {
    // Upstream dereferences the missing node; report it as an invalid response.
    let community = result
        .child("community")
        .ok_or_else(|| groups::Error::Response {
            code: 500,
            text: "Invalid community metadata response: missing <community> node".into(),
            node: Box::new(result.clone()),
        })?;
    let description = community.child("description");
    let participants: Vec<_> = community
        .children("participant")
        .map(|node| Participant {
            id: attr(node, "jid"),
            admin: nonempty(node, "type"),
        })
        .collect();
    let id = attr(community, "id");
    Ok(Metadata {
        id: match id {
            Some(id) if id.contains('@') => id,
            id => format!("{}@g.us", id.unwrap_or_default()),
        },
        subject: attr(community, "subject").unwrap_or_default(),
        subject_owner: attr(community, "s_o"),
        subject_time: number_or_zero(community, "s_t"),
        size: participants.len(),
        creation: number_or_zero(community, "creation"),
        owner: nonempty(community, "creator").map(|s| jid::normalized_user(&s)),
        desc: description.and_then(|d| d.child_string("body")),
        desc_id: description.and_then(|d| attr(d, "id")),
        linked_parent: community
            .child("linked_parent")
            .and_then(|n| nonempty(n, "jid")),
        restrict: community.child("locked").is_some(),
        announce: community.child("announcement").is_some(),
        is_community: community.child("parent").is_some(),
        is_community_announce: community.child("default_sub_community").is_some(),
        join_approval_mode: community.child("membership_approval_mode").is_some(),
        member_add_mode: community.child_string("member_add_mode").as_deref()
            == Some("all_member_add"),
        participants,
        ephemeral_duration: community
            .child("ephemeral")
            .and_then(|n| nonempty(n, "expiration"))
            .map(|s| {
                let value = crate::privacy::number(&s);
                Number(value.is_finite().then_some(value))
            }),
        addressing_mode: community.child_string("addressing_mode"),
    })
}
/// Every `community` entry of a participating query, keyed by its ID.
pub fn extract_participating(
    result: &BinaryNode,
) -> Result<indexmap::IndexMap<String, Metadata>, groups::Error> {
    let mut data = indexmap::IndexMap::new();
    if let Some(communities) = result.child("communities") {
        for community in communities.children("community") {
            let wrapper =
                BinaryNode::new("result").with_content(crate::binary::NodeContent::Children(vec![
                    community.clone(),
                ]));
            let metadata = extract_metadata(&wrapper)?;
            data.insert(metadata.id.clone(), metadata);
        }
    }
    Ok(data)
}
/// The groups.update projection upstream emits for fetched communities.
impl From<Metadata> for groups::Metadata {
    fn from(community: Metadata) -> Self {
        Self {
            id: community.id,
            notify: None,
            addressing_mode: if community.addressing_mode.as_deref() == Some("lid") {
                groups::AddressingMode::Lid
            } else {
                groups::AddressingMode::Pn
            },
            subject: Some(community.subject),
            subject_owner: community.subject_owner,
            subject_owner_pn: None,
            subject_owner_username: None,
            subject_time: community.subject_time,
            size: Some(community.size as f64),
            creation: community.creation,
            owner: community.owner,
            owner_pn: None,
            owner_username: None,
            owner_country_code: None,
            desc: community.desc,
            desc_id: community.desc_id,
            desc_owner: None,
            desc_owner_pn: None,
            desc_owner_username: None,
            desc_time: None,
            linked_parent: community.linked_parent,
            restrict: community.restrict,
            announce: community.announce,
            is_community: community.is_community,
            is_community_announce: community.is_community_announce,
            join_approval_mode: community.join_approval_mode,
            member_add_mode: community.member_add_mode,
            participants: community
                .participants
                .into_iter()
                .map(|p| groups::Participant {
                    id: p.id,
                    phone_number: None,
                    lid: None,
                    username: None,
                    admin: p.admin,
                })
                .collect(),
            ephemeral_duration: community.ephemeral_duration,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinkedGroup {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<Number>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkedGroups {
    pub community_jid: String,
    pub is_community: bool,
    pub linked_groups: Vec<LinkedGroup>,
}
pub fn extract_linked_groups(result: &BinaryNode) -> Vec<LinkedGroup> {
    let number = |node: &BinaryNode, key: &str| {
        nonempty(node, key).map(|s| {
            let value = crate::privacy::number(&s);
            Number(value.is_finite().then_some(value))
        })
    };
    result
        .child("sub_groups")
        .into_iter()
        .flat_map(|n| n.children("group"))
        .map(|group| LinkedGroup {
            id: nonempty(group, "id").map(|id| format!("{id}@g.us")),
            subject: attr(group, "subject").unwrap_or_default(),
            creation: number(group, "creation"),
            owner: nonempty(group, "creator").map(|s| jid::normalized_user(&s)),
            size: number(group, "size"),
        })
        .collect()
}
