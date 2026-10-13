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
    /// The `name` (or `config_code`) and value attributes of the children with a tag, as a table.
    pub fn dictionary(&self, tag: &str) -> IndexMap<String, String> {
        let mut dictionary = IndexMap::new();
        for child in self.children(tag) {
            let key = child
                .attrs
                .get("name")
                .or_else(|| child.attrs.get("config_code"));
            let value = child
                .attrs
                .get("value")
                .filter(|value| !value.is_empty())
                .or_else(|| child.attrs.get("config_value"));
            if let (Some(key), Some(value)) = (key, value) {
                dictionary.insert(key.clone(), value.clone());
            }
        }
        dictionary
    }
    /// Decode the `message` children of a stanza as stored messages.
    pub fn messages(&self) -> Result<Vec<crate::proto::WebMessageInfo>, Error> {
        use prost::Message as _;
        self.children("message")
            .map(|child| match &child.content {
                Some(NodeContent::Bytes(bytes)) => {
                    crate::proto::WebMessageInfo::decode(bytes.as_slice())
                        .map_err(|_| Error::Invalid("stored message"))
                }
                _ => Err(Error::Invalid("stored message")),
            })
            .collect()
    }
    /// Upstream's indented debug rendering of a node, with byte content as hex.
    pub fn to_tree_string(&self) -> String {
        self.tree_string(0)
    }
    fn tree_string(&self, depth: usize) -> String {
        let tabs = |count: usize| "\t".repeat(count);
        let children = match &self.content {
            None => String::new(),
            Some(NodeContent::Text(text)) if text.is_empty() => String::new(),
            Some(NodeContent::Text(text)) => tabs(depth + 1) + text,
            Some(NodeContent::Bytes(bytes)) => {
                tabs(depth + 1) + &bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            }
            Some(NodeContent::Children(children)) => children
                .iter()
                .map(|child| tabs(depth + 2) + &child.tree_string(depth + 2))
                .collect::<Vec<_>>()
                .join("\n"),
        };
        let attrs = self
            .attrs
            .iter()
            .map(|(key, value)| format!("{key}='{value}'"))
            .collect::<Vec<_>>()
            .join(" ");
        if children.is_empty() {
            format!("<{} {attrs}/>", self.tag)
        } else {
            format!(
                "<{} {attrs}>\n{children}\n{}</{}>",
                self.tag,
                tabs(depth),
                self.tag
            )
        }
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
