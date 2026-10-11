//! Privacy settings IQs and cached read-receipt policy, separate from trusted-contact tokens.
use crate::binary::{BinaryNode, NodeContent};
use indexmap::IndexMap;
pub type Settings = IndexMap<String, Option<String>>;
macro_rules! values {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name { $($variant),+ }
        impl $name { pub fn as_str(self) -> &'static str { match self { $(Self::$variant => $text),+ } } }
    }
}
values!(Value { All => "all", Contacts => "contacts", ContactBlacklist => "contact_blacklist", None => "none" });
values!(Online { All => "all", MatchLastSeen => "match_last_seen" });
values!(GroupAdd { All => "all", Contacts => "contacts", ContactBlacklist => "contact_blacklist" });
values!(ReadReceipts { All => "all", None => "none" });
values!(Call { All => "all", Known => "known" });
values!(Messages { All => "all", Contacts => "contacts" });
pub fn query() -> BinaryNode {
    BinaryNode::new("iq")
        .attr("xmlns", "privacy")
        .attr("to", "@s.whatsapp.net")
        .attr("type", "get")
        .with_content(NodeContent::Children(vec![BinaryNode::new("privacy")]))
}
pub fn update(name: &str, value: &str) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("xmlns", "privacy")
        .attr("to", "@s.whatsapp.net")
        .attr("type", "set")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("privacy").with_content(NodeContent::Children(vec![
                BinaryNode::new("category")
                    .attr("name", name)
                    .attr("value", value),
            ])),
        ]))
}
pub fn extract(response: &BinaryNode) -> Settings {
    let mut settings = Settings::new();
    if let Some(node) = response.all_children().first() {
        for category in node.all_children().iter().filter(|n| n.tag == "category") {
            let name = category
                .attrs
                .get("name")
                .or_else(|| category.attrs.get("config_code"))
                .map(String::as_str)
                .unwrap_or("undefined");
            let value = category
                .attrs
                .get("value")
                .filter(|s| !s.is_empty())
                .or_else(|| category.attrs.get("config_value"))
                .cloned();
            settings.insert(name.into(), value);
        }
    }
    settings
}
