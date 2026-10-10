//! WhatsApp binary stanza codec and typed node accessors.
mod codec;
pub use codec::{DecodeLimits, decode, decode_with_limits, encode};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryNode {
    pub tag: String,
    pub attrs: IndexMap<String, String>,
    pub content: Option<NodeContent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeContent {
    Children(Vec<BinaryNode>),
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unexpected end of binary node")]
    EndOfStream,
    #[error("invalid binary node: {0}")]
    Invalid(&'static str),
    #[error("binary node exceeds {0} limit")]
    Limit(&'static str),
    #[error("invalid compressed binary node: {0}")]
    Compression(#[from] std::io::Error),
    #[error("server stanza error {code:?}: {text}")]
    Stanza { code: Option<u32>, text: String },
}

impl BinaryNode {
    pub fn new(tag: impl Into<String>) -> Self {
        Self {
            tag: tag.into(),
            attrs: IndexMap::new(),
            content: None,
        }
    }
    pub fn attr(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.attrs.insert(name.into(), value.into());
        self
    }
    pub fn with_content(mut self, content: NodeContent) -> Self {
        self.content = Some(content);
        self
    }
    pub fn all_children(&self) -> &[Self] {
        match &self.content {
            Some(NodeContent::Children(nodes)) => nodes,
            _ => &[],
        }
    }
    pub fn children<'a>(&'a self, tag: &'a str) -> impl Iterator<Item = &'a Self> {
        self.all_children()
            .iter()
            .filter(move |node| node.tag == tag)
    }
    pub fn child(&self, tag: &str) -> Option<&Self> {
        self.all_children().iter().find(|node| node.tag == tag)
    }
    pub fn child_buffer(&self, tag: &str) -> Option<&[u8]> {
        match &self.child(tag)?.content {
            Some(NodeContent::Bytes(bytes)) => Some(bytes),
            _ => None,
        }
    }
    pub fn child_string(&self, tag: &str) -> Option<String> {
        match &self.child(tag)?.content {
            Some(NodeContent::Text(text)) => Some(text.clone()),
            Some(NodeContent::Bytes(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        }
    }
    pub fn child_uint(&self, tag: &str, length: usize) -> Result<Option<u64>, Error> {
        let Some(bytes) = self.child_buffer(tag) else {
            return Ok(None);
        };
        if length > 8 {
            return Err(Error::Limit("integer width"));
        }
        let bytes = bytes.get(..length).ok_or(Error::EndOfStream)?;
        Ok(Some(bytes.iter().fold(0, |n, b| n * 256 + u64::from(*b))))
    }
    pub fn assert_error_free(&self) -> Result<(), Error> {
        if let Some(error) = self.child("error") {
            return Err(Error::Stanza {
                code: error.attrs.get("code").and_then(|s| s.parse().ok()),
                text: error
                    .attrs
                    .get("text")
                    .cloned()
                    .unwrap_or_else(|| "Unknown error".into()),
            });
        }
        Ok(())
    }
}
