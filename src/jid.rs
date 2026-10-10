//! WhatsApp identifiers, including companion devices and LID/hosted domains.
use serde::{Deserialize, Serialize};

pub const S_WHATSAPP_NET: &str = "@s.whatsapp.net";
pub const OFFICIAL_BIZ_JID: &str = "16505361212@c.us";
pub const SERVER_JID: &str = "server@c.us";
pub const PSA_WID: &str = "0@c.us";
pub const STORIES_JID: &str = "status@broadcast";
pub const META_AI_JID: &str = "13135550002@c.us";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jid {
    pub user: String,
    pub server: String,
    pub device: Option<u16>,
    pub domain_type: u8,
}

impl Jid {
    /// Parse the upstream user[_agent][:device]@server representation.
    pub fn parse(value: &str) -> Option<Self> {
        let (combined, server) = value.split_once('@')?;
        let mut parts = combined.split(':');
        let mut user_agent = parts.next()?.split('_');
        let user = user_agent.next()?.to_owned();
        let agent = user_agent.next().filter(|s| !s.is_empty());
        let device = parts
            .next()
            .filter(|s| !s.is_empty())
            .map(str::parse)
            .transpose()
            .ok()?;
        let domain_type = match server {
            "lid" => 1,
            "hosted" => 128,
            "hosted.lid" => 129,
            _ => agent.map(str::parse).transpose().ok()?.unwrap_or(0),
        };
        Some(Self {
            user,
            server: server.to_owned(),
            device,
            domain_type,
        })
    }

    pub fn normalized(&self) -> String {
        encode(
            &self.user,
            if self.server == "c.us" {
                "s.whatsapp.net"
            } else {
                &self.server
            },
            None,
            None,
        )
    }

    pub fn is_group(&self) -> bool {
        self.server == "g.us"
    }
    pub fn is_broadcast(&self) -> bool {
        self.server == "broadcast"
    }
    pub fn is_status_broadcast(&self) -> bool {
        self.is_broadcast() && self.user == "status"
    }
    pub fn is_newsletter(&self) -> bool {
        self.server == "newsletter"
    }
    pub fn is_pn_user(&self) -> bool {
        self.server == "s.whatsapp.net"
    }
    pub fn is_lid_user(&self) -> bool {
        self.server == "lid"
    }
    pub fn is_hosted_pn_user(&self) -> bool {
        self.server == "hosted"
    }
    pub fn is_hosted_lid_user(&self) -> bool {
        self.server == "hosted.lid"
    }
    pub fn is_meta_ai(&self) -> bool {
        self.server == "bot"
    }
    pub fn is_bot(&self) -> bool {
        self.server == "c.us"
            && self.user.len() == 11
            && ((self.user.starts_with("1313555")
                && self.user[7..].bytes().all(|c| c.is_ascii_digit()))
                || (self.user.starts_with("131655500")
                    && self.user[9..].bytes().all(|c| c.is_ascii_digit())))
    }
}

pub fn encode(user: &str, server: &str, device: Option<u16>, agent: Option<u8>) -> String {
    let mut value = user.to_owned();
    if let Some(agent) = agent.filter(|a| *a != 0) {
        value.push_str(&format!("_{agent}"));
    }
    if let Some(device) = device.filter(|d| *d != 0) {
        value.push_str(&format!(":{device}"));
    }
    format!("{value}@{server}")
}

pub fn normalized_user(value: &str) -> String {
    Jid::parse(value)
        .map(|jid| jid.normalized())
        .unwrap_or_default()
}

pub fn are_same_user(first: &str, second: &str) -> bool {
    Jid::parse(first).map(|j| j.user) == Jid::parse(second).map(|j| j.user)
}

pub fn transfer_device(from: &str, to: &str) -> Option<String> {
    let target = Jid::parse(to)?;
    Some(encode(
        &target.user,
        &target.server,
        Jid::parse(from).and_then(|j| j.device),
        None,
    ))
}
