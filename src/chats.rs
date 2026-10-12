//! Profile, block-list, business-profile, dirty-bit and call-link stanzas.
use crate::{
    binary::{BinaryNode, NodeContent},
    jid::{self, Jid, S_WHATSAPP_NET},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error(
        "Illegal no-jid profile update. Please specify either your ID or the ID of the chat you wish to update"
    )]
    NoJid,
    #[error("Unable to resolve PN JID for LID: {0}")]
    UnresolvedPn(String),
    #[error("Unable to resolve LID for PN JID: {0}")]
    UnresolvedLid(String),
    #[error("Invalid jid: {0}")]
    InvalidJid(String),
}
impl Error {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::NoJid => 500,
            _ => 400,
        }
    }
}
fn iq(xmlns: &str, kind: &str) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("to", S_WHATSAPP_NET)
        .attr("type", kind)
        .attr("xmlns", xmlns)
}
fn content_string(node: Option<&BinaryNode>) -> Option<String> {
    match &node?.content {
        Some(NodeContent::Text(text)) => Some(text.clone()),
        Some(NodeContent::Bytes(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}
pub fn default_disappearing_mode_query(duration: u32) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("xmlns", "disappearing_mode")
        .attr("to", S_WHATSAPP_NET)
        .attr("type", "set")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("disappearing_mode").attr("duration", duration.to_string()),
        ]))
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BotInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persona_id: Option<String>,
}
pub fn bot_list_query() -> BinaryNode {
    BinaryNode::new("iq")
        .attr("xmlns", "bot")
        .attr("to", S_WHATSAPP_NET)
        .attr("type", "get")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("bot").attr("v", "2"),
        ]))
}
/// Bots listed in every `all` section, in response order.
pub fn extract_bot_list(response: &BinaryNode) -> Vec<BotInfo> {
    response
        .child("bot")
        .into_iter()
        .flat_map(|bots| bots.children("section"))
        .filter(|section| section.attrs.get("type").is_some_and(|t| t == "all"))
        .flat_map(|section| section.children("bot"))
        .map(|bot| BotInfo {
            jid: bot.attrs.get("jid").cloned(),
            persona_id: bot.attrs.get("persona_id").cloned(),
        })
        .collect()
}
/// The `target` of a profile-picture update: absent for the account's own number.
pub fn profile_picture_target(jid: &str, me_id: &str) -> Result<Option<String>, Error> {
    if jid.is_empty() {
        return Err(Error::NoJid);
    }
    let normalized = jid::normalized_user(jid);
    Ok((normalized != jid::normalized_user(me_id) && !normalized.is_empty()).then_some(normalized))
}
/// Set (with image bytes) or remove (without) a profile picture.
pub fn profile_picture_update_query(target: Option<&str>, image: Option<Vec<u8>>) -> BinaryNode {
    let mut node = iq("w:profile:picture", "set");
    if let Some(target) = target {
        node = node.attr("target", target);
    }
    match image {
        Some(image) => node.with_content(NodeContent::Children(vec![
            BinaryNode::new("picture")
                .attr("type", "image")
                .with_content(NodeContent::Bytes(image)),
        ])),
        None => node,
    }
}
pub fn profile_status_query(status: &str) -> BinaryNode {
    iq("status", "set").with_content(NodeContent::Children(vec![
        BinaryNode::new("status").with_content(NodeContent::Bytes(status.as_bytes().to_vec())),
    ]))
}
/// Which stored mapping a block-list update needs before its stanza can be built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockLookup {
    /// A LID user; blocking needs its phone-number JID.
    PnForLid(String),
    /// A phone-number user; both actions need its LID.
    LidForPn(String),
}
pub fn block_lookup(jid: &str) -> Result<BlockLookup, Error> {
    let normalized = jid::normalized_user(jid);
    match Jid::parse(&normalized) {
        Some(parsed) if parsed.is_lid_user() || parsed.is_hosted_lid_user() => {
            Ok(BlockLookup::PnForLid(normalized))
        }
        Some(parsed) if parsed.is_pn_user() || parsed.is_hosted_pn_user() => {
            Ok(BlockLookup::LidForPn(normalized))
        }
        _ => Err(Error::InvalidJid(jid.into())),
    }
}
/// Build the block-list stanza from the mapping `block_lookup` asked for.
/// Unblocking a LID user needs no mapping.
pub fn block_status_query(
    jid: &str,
    block: bool,
    lookup: &BlockLookup,
    mapped: Option<&str>,
) -> Result<BinaryNode, Error> {
    let mapped = mapped.filter(|s| !s.is_empty());
    let (lid, pn) = match lookup {
        BlockLookup::PnForLid(lid) => {
            let pn = if block {
                Some(jid::normalized_user(
                    mapped.ok_or_else(|| Error::UnresolvedPn(jid.into()))?,
                ))
            } else {
                None
            };
            (lid.clone(), pn)
        }
        BlockLookup::LidForPn(pn) => (
            mapped
                .ok_or_else(|| Error::UnresolvedLid(jid.into()))?
                .to_owned(),
            block.then(|| jid::normalized_user(pn)),
        ),
    };
    let mut item = BinaryNode::new("item")
        .attr("action", if block { "block" } else { "unblock" })
        .attr("jid", lid);
    if let Some(pn) = pn {
        item = item.attr("pn_jid", pn);
    }
    Ok(BinaryNode::new("iq")
        .attr("xmlns", "blocklist")
        .attr("to", S_WHATSAPP_NET)
        .attr("type", "set")
        .with_content(NodeContent::Children(vec![item])))
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusinessHours {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// Raw attributes of each business_hours_config entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub business_config: Option<Vec<indexmap::IndexMap<String, String>>>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusinessProfile {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    pub description: String,
    pub website: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    pub business_hours: BusinessHours,
}
pub fn business_profile_query(jid: &str) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("to", "s.whatsapp.net")
        .attr("xmlns", "w:biz")
        .attr("type", "get")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("business_profile")
                .attr("v", "244")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("profile").attr("jid", jid),
                ])),
        ]))
}
pub fn extract_business_profile(response: &BinaryNode) -> Option<BusinessProfile> {
    let profile = response.child("business_profile")?.child("profile")?;
    let hours = profile.child("business_hours");
    Some(BusinessProfile {
        wid: profile.attrs.get("jid").cloned(),
        address: content_string(profile.child("address")),
        description: content_string(profile.child("description")).unwrap_or_default(),
        website: content_string(profile.child("website"))
            .filter(|s| !s.is_empty())
            .into_iter()
            .collect(),
        email: content_string(profile.child("email")),
        category: content_string(
            profile
                .child("categories")
                .and_then(|n| n.child("category")),
        ),
        business_hours: BusinessHours {
            timezone: hours.and_then(|n| n.attrs.get("timezone").cloned()),
            business_config: hours.map(|n| {
                n.children("business_hours_config")
                    .map(|config| config.attrs.clone())
                    .collect()
            }),
        },
    })
}
/// Acknowledge a dirty bit; a zero or empty timestamp is omitted as upstream's truthiness check does.
pub fn clean_dirty_bits_node(kind: &str, from_timestamp: Option<&str>, id: &str) -> BinaryNode {
    let mut clean = BinaryNode::new("clean").attr("type", kind);
    if let Some(timestamp) = from_timestamp.filter(|s| !s.is_empty() && *s != "0") {
        clean = clean.attr("timestamp", timestamp);
    }
    BinaryNode::new("iq")
        .attr("to", S_WHATSAPP_NET)
        .attr("type", "set")
        .attr("xmlns", "urn:xmpp:whatsapp:dirty")
        .attr("id", id)
        .with_content(NodeContent::Children(vec![clean]))
}
/// The account-sync dirty-bit decision: the previous timestamp to clean from, and the
/// new last-sync value (None where upstream stores NaN).
#[derive(Debug, Clone, PartialEq)]
pub struct AccountSyncDirty {
    pub clean_from: Option<u64>,
    pub last_account_sync_timestamp: Option<u64>,
}
pub fn account_sync_dirty(node: &BinaryNode, last: Option<u64>) -> Option<AccountSyncDirty> {
    let dirty = node.child("dirty")?;
    if dirty.attrs.get("type").is_none_or(|t| t != "account_sync") {
        return None;
    }
    let timestamp = dirty.attrs.get("timestamp").filter(|s| !s.is_empty())?;
    let value = crate::privacy::number(timestamp);
    Some(AccountSyncDirty {
        clean_from: last.filter(|t| *t != 0),
        last_account_sync_timestamp: (value.is_finite() && value >= 0.0).then_some(value as u64),
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PictureType {
    Preview,
    Image,
}
/// A profile-picture token applies to other users only, never groups or the own account.
pub fn profile_picture_uses_token(jid: &str, me_id: &str, me_lid: Option<&str>) -> bool {
    let normalized = jid::normalized_user(jid);
    let user = normalized.ends_with("@s.whatsapp.net") || normalized.ends_with("@lid");
    let own = normalized == jid::normalized_user(me_id)
        || me_lid.is_some_and(|lid| !lid.is_empty() && normalized == jid::normalized_user(lid));
    user && !own
}
pub fn profile_picture_url_query(
    jid: &str,
    kind: PictureType,
    token_content: Vec<BinaryNode>,
) -> BinaryNode {
    let mut picture = BinaryNode::new("picture")
        .attr(
            "type",
            match kind {
                PictureType::Preview => "preview",
                PictureType::Image => "image",
            },
        )
        .attr("query", "url");
    if !token_content.is_empty() {
        picture = picture.with_content(NodeContent::Children(token_content));
    }
    BinaryNode::new("iq")
        .attr("target", jid::normalized_user(jid))
        .attr("to", S_WHATSAPP_NET)
        .attr("type", "get")
        .attr("xmlns", "w:profile:picture")
        .with_content(NodeContent::Children(vec![picture]))
}
pub fn extract_profile_picture_url(response: &BinaryNode) -> Option<String> {
    response.child("picture")?.attrs.get("url").cloned()
}
pub fn call_link_query(video: bool, start_time: Option<u64>, id: &str) -> BinaryNode {
    let mut link =
        BinaryNode::new("link_create").attr("media", if video { "video" } else { "audio" });
    if let Some(start) = start_time {
        link = link.with_content(NodeContent::Children(vec![
            BinaryNode::new("event").attr("start_time", start.to_string()),
        ]));
    }
    BinaryNode::new("call")
        .attr("id", id)
        .attr("to", "@call")
        .with_content(NodeContent::Children(vec![link]))
}
pub fn extract_call_link_token(response: &BinaryNode) -> Option<String> {
    response.child("link_create")?.attrs.get("token").cloned()
}
