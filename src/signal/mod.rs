//! Signal addressing and transactional WhatsApp group sender keys.
pub mod direct;
pub mod group;
pub mod mapping;
pub mod wire;
use crate::{auth, crypto, jid::Jid};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Auth(#[from] auth::Error),
    #[error(transparent)]
    Binary(#[from] crate::binary::Error),
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
    #[error("missing one-time prekey")]
    MissingPreKey,
    #[error("invalid Signal message authentication code")]
    Mac,
    #[error("invalid Signal message or record")]
    Invalid,
    #[error("unsupported Signal message version")]
    Version,
    #[error("no Signal session")]
    NoSession,
    #[error("sender-key signing private key is unavailable")]
    NoSigningKey,
    #[error("invalid Signal signature")]
    Signature,
    #[error("Signal message was already consumed")]
    Replay,
    #[error("Signal message exceeds the 2000-counter skip limit")]
    Future,
    #[error("Signal counter exhausted")]
    Counter,
}
impl Error {
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Auth(error) => error.status_code(),
            Self::Binary(crate::binary::Error::Stanza { code, .. }) => {
                code.and_then(|n| n.try_into().ok())
            }
            Self::Shared(error) => error.status_code(),
            _ => None,
        }
    }
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
