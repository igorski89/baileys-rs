//! Signal addressing and transactional WhatsApp group sender keys.
pub mod group;
pub mod mapping;
use crate::{auth, crypto, jid::Jid};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Auth(#[from] auth::Error),
    #[error(transparent)]
    Crypto(#[from] crypto::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error("invalid Signal address")]
    Address,
    #[error(transparent)]
    Shared(std::sync::Arc<Error>),
    #[error("mapping resolver aborted")]
    ResolverAborted,
    #[error("invalid sender-key message or record")]
    Invalid,
    #[error("unsupported sender-key version")]
    Version,
    #[error("no sender-key session")]
    NoSession,
    #[error("sender-key signing private key is unavailable")]
    NoSigningKey,
    #[error("invalid sender-key signature")]
    Signature,
    #[error("sender-key message was already consumed")]
    Replay,
    #[error("sender-key message exceeds the 2000-iteration skip limit")]
    Future,
    #[error("sender-key counter exhausted")]
    Counter,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
    pub user: String,
    pub device: u16,
}
impl Address {
    pub fn from_jid(jid: &str) -> Result<Self, Error> {
        let jid = Jid::parse(jid)
            .filter(|j| !j.user.is_empty())
            .ok_or(Error::Address)?;
        if jid.device == Some(99) && !matches!(jid.server.as_str(), "hosted" | "hosted.lid") {
            return Err(Error::Address);
        }
        let user = if jid.domain_type == 0 {
            jid.user
        } else {
            format!("{}_{}", jid.user, jid.domain_type)
        };
        Ok(Self {
            user,
            device: jid.device.unwrap_or(0),
        })
    }
}
impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.user, self.device)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SenderKeyName {
    pub group: String,
    pub sender: Address,
}
impl SenderKeyName {
    pub fn new(group: impl Into<String>, jid: &str) -> Result<Self, Error> {
        Ok(Self {
            group: group.into(),
            sender: Address::from_jid(jid)?,
        })
    }
    /// Matches upstream's Java-style UTF-16 hash, including signed overflow.
    pub fn hash_code(&self) -> i32 {
        fn hash(s: &str) -> i32 {
            s.encode_utf16()
                .fold(0_i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)))
        }
        hash(&self.group) ^ hash(&self.sender.to_string())
    }
}
impl std::fmt::Display for SenderKeyName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}::{}::{}",
            self.group, self.sender.user, self.sender.device
        )
    }
}
