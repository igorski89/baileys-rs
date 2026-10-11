//! WhatsApp USync query construction, protocol parsing and device selection.
use crate::{
    binary::{self, BinaryNode, NodeContent},
    jid::Jid,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Binary(#[from] binary::Error),
    #[error("USync query must contain a protocol")]
    NoProtocol,
    #[error("invalid USync result: {0}")]
    Invalid(&'static str),
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Binary(binary::Error::Stanza { code, .. }) => {
                code.and_then(|n| n.try_into().ok())
            }
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct User {
    pub id: Option<String>,
    pub lid: Option<String>,
    pub phone: Option<String>,
    pub username: Option<String>,
    pub username_key: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub persona_id: Option<String>,
}
impl User {
    pub fn with_id(id: impl Into<String>) -> Self {
        Self {
            id: Some(id.into()),
            ..Default::default()
        }
    }
    pub fn with_phone(phone: impl Into<String>) -> Self {
        Self {
            phone: Some(phone.into()),
            ..Default::default()
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Devices,
    Contact,
    Status,
    DisappearingMode,
    Bot,
    Lid,
    Username,
}
impl Protocol {
    pub fn name(self) -> &'static str {
        match self {
            Self::Devices => "devices",
            Self::Contact => "contact",
            Self::Status => "status",
            Self::DisappearingMode => "disappearing_mode",
            Self::Bot => "bot",
            Self::Lid => "lid",
            Self::Username => "username",
        }
    }
    pub fn query_element(self) -> BinaryNode {
        let node = BinaryNode::new(self.name());
        match self {
            Self::Devices => node.attr("version", "2"),
            Self::Bot => children(node, vec![BinaryNode::new("profile").attr("v", "1")]),
            _ => node,
        }
    }
    pub fn user_element(self, user: &User) -> Option<BinaryNode> {
        let node = BinaryNode::new(self.name());
        match self {
            Self::Contact => {
                if let Some(phone) = nonempty(&user.phone) {
                    return Some(node.with_content(NodeContent::Text(phone.into())));
                }
                if let Some(username) = nonempty(&user.username) {
                    let mut node = node.attr("username", username);
                    if let Some(pin) = nonempty(&user.username_key) {
                        node = node.attr("pin", pin);
                    }
                    if let Some(lid) = nonempty(&user.lid) {
                        node = node.attr("lid", lid);
                    }
                    return Some(node);
                }
                Some(if let Some(kind) = nonempty(&user.kind) {
                    node.attr("type", kind)
                } else {
                    node
                })
            }
            Self::Lid => nonempty(&user.lid).map(|lid| node.attr("jid", lid)),
            Self::Bot => {
                let mut profile = BinaryNode::new("profile");
                if let Some(persona) = &user.persona_id {
                    profile = profile.attr("persona_id", persona);
                }
                Some(children(node, vec![profile]))
            }
            _ => None,
        }
    }
    pub fn parse(self, node: &BinaryNode) -> Result<Option<Value>, Error> {
        if node.tag != self.name() {
            return Ok(None);
        }
        if !matches!(self, Self::Lid | Self::Bot) {
            node.assert_error_free()?;
        }
        Ok(match self {
            Self::Contact => Some(Value::Contact(
                node.attrs.get("type").is_some_and(|s| s == "in"),
            )),
            Self::Lid => node.attrs.get("val").cloned().map(Value::Text),
            // Upstream accepts only strings here; decoded binary content is bytes.
            Self::Username => match &node.content {
                Some(NodeContent::Text(s)) => Some(Value::Text(s.clone())),
                _ => None,
            },
            Self::Status => {
                let status = match &node.content {
                    Some(NodeContent::Text(s)) => Some(s.clone()),
                    Some(NodeContent::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                    _ => None,
                };
                let status = if status.as_ref().is_none_or(String::is_empty) {
                    if node.attrs.get("code").is_some_and(|s| s == "401") {
                        Some(String::new())
                    } else {
                        None
                    }
                } else {
                    status
                };
                Some(Value::Status(Status {
                    status,
                    set_at: time_ms(node, "t")?,
                }))
            }
            Self::DisappearingMode => Some(Value::DisappearingMode(DisappearingMode {
                duration: integer(node, "duration"),
                set_at: time_ms(node, "t")?,
            })),
            Self::Devices => {
                let device_list = node
                    .child("device-list")
                    .map(|list| {
                        list.children("device")
                            .map(|node| Device {
                                id: integer(node, "id").and_then(|n| n.try_into().ok()),
                                key_index: integer(node, "key-index")
                                    .and_then(|n| n.try_into().ok()),
                                is_hosted: node.attrs.get("is_hosted").is_some_and(|s| s == "true"),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let key_index = node.child("key-index-list").map(|n| KeyIndex {
                    timestamp: integer(n, "ts"),
                    expected_timestamp: integer(n, "expected_ts"),
                    signed_key_index: match &n.content {
                        Some(NodeContent::Bytes(b)) => Some(b.clone()),
                        _ => None,
                    },
                });
                Some(Value::Devices(DeviceInfo {
                    device_list,
                    key_index,
                }))
            }
            Self::Bot => {
                // This extra nesting follows the upstream parser, even though
                // its caller dispatches by the outer bot tag.
                let profile = node
                    .child("bot")
                    .and_then(|n| n.child("profile"))
                    .ok_or(Error::Invalid("missing bot profile"))?;
                let commands = profile.child("commands");
                let prompts = profile.child("prompts");
                Some(Value::Bot(BotProfile {
                    is_default: profile.child("default").is_some(),
                    jid: node.attrs.get("jid").cloned(),
                    name: profile.child_string("name"),
                    attributes: profile.child_string("attributes"),
                    description: profile.child_string("description"),
                    category: profile.child_string("category"),
                    persona_id: profile.attrs.get("persona_id").cloned(),
                    commands_description: commands.and_then(|n| n.child_string("description")),
                    commands: commands
                        .map(|n| {
                            n.children("command")
                                .map(|n| BotCommand {
                                    name: n.child_string("name"),
                                    description: n.child_string("description"),
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                    prompts: prompts
                        .map(|n| {
                            n.children("prompt")
                                .map(|n| {
                                    format!(
                                        "{} {}",
                                        n.child_string("emoji")
                                            .unwrap_or_else(|| "undefined".into()),
                                        n.child_string("text")
                                            .unwrap_or_else(|| "undefined".into())
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                }))
            }
        })
    }
}
fn nonempty(v: &Option<String>) -> Option<&str> {
    v.as_deref().filter(|s| !s.is_empty())
}
fn children(node: BinaryNode, content: Vec<BinaryNode>) -> BinaryNode {
    node.with_content(NodeContent::Children(content))
}
fn integer(node: &BinaryNode, name: &str) -> Option<u64> {
    node.attrs.get(name)?.parse().ok()
}
fn time_ms(node: &BinaryNode, name: &str) -> Result<u64, Error> {
    integer(node, name)
        .unwrap_or(0)
        .checked_mul(1000)
        .ok_or(Error::Invalid("timestamp overflow"))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub protocols: Vec<Protocol>,
    pub users: Vec<User>,
    pub context: String,
    pub mode: String,
}
impl Default for Query {
    fn default() -> Self {
        Self {
            protocols: vec![],
            users: vec![],
            context: "interactive".into(),
            mode: "query".into(),
        }
    }
}
impl Query {
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocols.push(protocol);
        self
    }
    pub fn with_user(mut self, user: User) -> Self {
        self.users.push(user);
        self
    }
    pub fn stanza(&self, sid: &str) -> Result<BinaryNode, Error> {
        if self.protocols.is_empty() {
            return Err(Error::NoProtocol);
        }
        let users = self
            .users
            .iter()
            .map(|user| {
                let mut node = BinaryNode::new("user");
                if nonempty(&user.phone).is_none()
                    && let Some(id) = &user.id
                {
                    node = node.attr("jid", id);
                }
                children(
                    node,
                    self.protocols
                        .iter()
                        .filter_map(|p| p.user_element(user))
                        .collect(),
                )
            })
            .collect();
        let query = children(
            BinaryNode::new("query"),
            self.protocols.iter().map(|p| p.query_element()).collect(),
        );
        Ok(children(
            BinaryNode::new("iq")
                .attr("to", "@s.whatsapp.net")
                .attr("type", "get")
                .attr("xmlns", "usync"),
            vec![children(
                BinaryNode::new("usync")
                    .attr("context", &self.context)
                    .attr("mode", &self.mode)
                    .attr("sid", sid)
                    .attr("last", "true")
                    .attr("index", "0"),
                vec![query, children(BinaryNode::new("list"), users)],
            )],
        ))
    }
    pub fn parse(&self, node: &BinaryNode) -> Result<Option<ResultList>, Error> {
        if !node.attrs.get("type").is_some_and(|s| s == "result") {
            return Ok(None);
        }
        let mut list = vec![];
        if let Some(users) = node.child("usync").and_then(|n| n.child("list")) {
            for user in users.all_children() {
                let Some(id) = user.attrs.get("jid").filter(|s| !s.is_empty()) else {
                    continue;
                };
                let mut protocols = IndexMap::new();
                for content in user.all_children() {
                    if let Some(protocol) = self
                        .protocols
                        .iter()
                        .rev()
                        .find(|p| p.name() == content.tag)
                        && let Some(value) = protocol.parse(content)?
                    {
                        protocols.insert(content.tag.clone(), value);
                    }
                }
                list.push(ResultUser {
                    id: id.clone(),
                    protocols,
                });
            }
        }
        Ok(Some(ResultList {
            list,
            side_list: vec![],
        }))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultList {
    pub list: Vec<ResultUser>,
    pub side_list: Vec<ResultUser>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultUser {
    pub id: String,
    #[serde(flatten)]
    pub protocols: IndexMap<String, Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Contact(bool),
    Text(String),
    Devices(DeviceInfo),
    Status(Status),
    DisappearingMode(DisappearingMode),
    Bot(BotProfile),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    pub id: Option<u16>,
    pub key_index: Option<u32>,
    pub is_hosted: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceInfo {
    pub device_list: Vec<Device>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_index: Option<KeyIndex>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyIndex {
    pub timestamp: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "crate::serialization::optional_bytes"
    )]
    pub signed_key_index: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_timestamp: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct Status {
    pub status: Option<String>,
    pub set_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct DisappearingMode {
    pub duration: Option<u64>,
    pub set_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BotCommand {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BotProfile {
    pub is_default: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persona_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commands_description: Option<String>,
    pub commands: Vec<BotCommand>,
    pub prompts: Vec<String>,
}
/// Preserve upstream's filtering and domain propagation within each user's list.
pub fn extract_device_jids(
    results: &[ResultUser],
    my_jid: &str,
    my_lid: &str,
    exclude_zero: bool,
) -> Result<Vec<Jid>, Error> {
    let me = Jid::parse(my_jid).ok_or(Error::Invalid("own JID"))?;
    let mut extracted = vec![];
    for result in results {
        let decoded = Jid::parse(&result.id).ok_or(Error::Invalid("result JID"))?;
        let Some(Value::Devices(devices)) = result.protocols.get("devices") else {
            continue;
        };
        let mut domain = decoded.domain_type;
        for device in &devices.device_list {
            let Some(id) = device.id else { continue };
            if (exclude_zero && id == 0)
                || ((me.user == decoded.user || my_lid == decoded.user) && me.device == Some(id))
                || (id != 0 && device.key_index.is_none_or(|i| i == 0))
            {
                continue;
            }
            if device.is_hosted {
                domain = if domain == 1 { 129 } else { 128 };
            }
            let server = match domain {
                1 => "lid",
                128 => "hosted",
                129 => "hosted.lid",
                _ => &decoded.server,
            };
            extracted.push(Jid {
                user: decoded.user.clone(),
                server: server.into(),
                device: Some(id),
                domain_type: domain,
            });
        }
    }
    Ok(extracted)
}
/// Build the contact query used by onWhatsApp, excluding unsupported LID inputs.
pub fn contact_query(inputs: &[String]) -> Query {
    let mut query = Query::default().with_protocol(Protocol::Contact);
    for input in inputs {
        if Jid::parse(input).is_some_and(|j| j.is_lid_user()) {
            continue;
        }
        let phone = input.replacen('+', "", 1);
        let user = phone
            .split('@')
            .next()
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("");
        query.users.push(User::with_phone(format!("+{user}")));
    }
    query
}
