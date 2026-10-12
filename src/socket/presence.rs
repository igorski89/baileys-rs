//! Presence wire helpers and socket APIs.
use super::{Command, Error, Socket};
use crate::{
    auth::Contact,
    binary::{BinaryNode, NodeContent},
    jid,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, atomic::Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Presence {
    Available,
    Unavailable,
    Composing,
    Recording,
    Paused,
}
impl Presence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
            Self::Composing => "composing",
            Self::Recording => "recording",
            Self::Paused => "paused",
        }
    }
    pub fn is_global(self) -> bool {
        matches!(self, Self::Available | Self::Unavailable)
    }
}
#[derive(Clone)]
pub struct IgnoreJid(Arc<dyn Fn(&str) -> bool + Send + Sync>);
impl IgnoreJid {
    pub fn new(callback: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(callback))
    }
    pub fn test(&self, jid: &str) -> bool {
        (self.0)(jid)
    }
}
impl Default for IgnoreJid {
    fn default() -> Self {
        Self::new(|_| false)
    }
}
impl std::fmt::Debug for IgnoreJid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IgnoreJid(<callback>)")
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Data {
    pub last_known_presence: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub last_seen: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub group_online_count: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub id: Option<String>,
    pub presences: IndexMap<String, Data>,
}
pub fn update_node(
    me: &Contact,
    presence: Presence,
    to: Option<&str>,
) -> Result<Option<BinaryNode>, Error> {
    if presence.is_global() {
        return Ok(me.name.as_ref().filter(|n| !n.is_empty()).map(|name| {
            BinaryNode::new("presence")
                .attr("name", name.replace('@', ""))
                .attr("type", presence.as_str())
        }));
    }
    let to = to.ok_or(Error::Config)?;
    let destination = jid::Jid::parse(to).ok_or(Error::Config)?;
    let from = if destination.server == "lid" {
        me.lid.as_deref().ok_or(Error::Config)?
    } else {
        &me.id
    };
    let mut child = BinaryNode::new(if presence == Presence::Recording {
        "composing"
    } else {
        presence.as_str()
    });
    if presence == Presence::Recording {
        child = child.attr("media", "audio");
    }
    Ok(Some(
        BinaryNode::new("chatstate")
            .attr("from", from)
            .attr("to", to)
            .with_content(NodeContent::Children(vec![child])),
    ))
}
pub fn decode(node: &BinaryNode, ignore: &IgnoreJid) -> Result<Option<Update>, Error> {
    let id = node.attrs.get("from").cloned();
    if let Some(id) = &id
        && ignore.test(id)
        && id != jid::S_WHATSAPP_NET
    {
        return Ok(None);
    }
    let data = match node.tag.as_str() {
        "presence" => Data {
            last_known_presence: if node.attrs.get("type").is_some_and(|t| t == "unavailable") {
                "unavailable"
            } else {
                "available"
            }
            .into(),
            last_seen: node
                .attrs
                .get("last")
                .filter(|s| !s.is_empty() && s.as_str() != "deny")
                .map(|s| crate::privacy::number(s)),
            group_online_count: node
                .attrs
                .get("count")
                .filter(|s| !s.is_empty())
                .map(|s| crate::privacy::number(s)),
        },
        "chatstate" => {
            let Some(NodeContent::Children(children)) = &node.content else {
                return Ok(None);
            };
            let child = children.first().ok_or(Error::Config)?;
            let kind = if child.attrs.get("media").is_some_and(|s| s == "audio") {
                "recording"
            } else if child.tag == "paused" {
                "available"
            } else {
                &child.tag
            };
            Data {
                last_known_presence: kind.into(),
                last_seen: None,
                group_online_count: None,
            }
        }
        _ => return Ok(None),
    };
    let participant = node
        .attrs
        .get("participant")
        .filter(|s| !s.is_empty())
        .or(id.as_ref())
        .cloned()
        .unwrap_or_else(|| "undefined".into());
    Ok(Some(Update {
        id,
        presences: IndexMap::from([(participant, data)]),
    }))
}
/// Only regular PN/LID destinations use privacy tokens; retain the caller's JID on the node.
pub fn subscription_target(to: &str) -> Option<String> {
    let normalized = jid::normalized_user(to);
    jid::Jid::parse(&normalized)
        .filter(|j| j.is_pn_user() || j.is_lid_user())
        .map(|_| normalized)
}
pub fn subscription_node(to: &str, id: &str, content: Vec<BinaryNode>) -> BinaryNode {
    let mut node = BinaryNode::new("presence")
        .attr("to", to)
        .attr("id", id)
        .attr("type", "subscribe");
    if !content.is_empty() {
        node = node.with_content(NodeContent::Children(content));
    }
    node
}
impl Socket {
    pub async fn send_presence_update(
        &self,
        presence: Presence,
        to: Option<&str>,
    ) -> Result<(), Error> {
        let (reply, received) = tokio::sync::oneshot::channel();
        self.inner
            .commands
            .send(Command::Presence {
                presence,
                to: to.map(str::to_owned),
                reply,
            })
            .await
            .map_err(|_| self.closed_error())?;
        received.await.map_err(|_| self.closed_error())?
    }
    pub async fn presence_subscribe(&self, to: &str) -> Result<(), Error> {
        let content = if let Some(normalized) = subscription_target(to) {
            let store = self.inner.store.clone();
            tokio::task::spawn_blocking(move || {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|t| t.as_secs())
                    .unwrap_or(0);
                crate::privacy::Repository::new(store).build_content(&normalized, vec![], now)
            })
            .await
            .map_err(|_| Error::Task)?
        } else {
            vec![]
        };
        let sequence = self
            .inner
            .sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::TagExhausted)?;
        self.send_node(subscription_node(
            to,
            &format!("{}{sequence}", self.inner.prefix),
            content,
        ))
        .await
    }
}
