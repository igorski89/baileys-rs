//! Connection startup queries and persisted AB-property negotiation.
use super::{Command, ConnectionState, Error, Inner, Socket};
use crate::{
    binary::{BinaryNode, NodeContent},
    jid,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use indexmap::IndexMap;
use std::sync::Arc;
pub type Properties = IndexMap<String, Option<String>>;
pub fn props_query(hash: Option<&str>) -> BinaryNode {
    let mut props = BinaryNode::new("props").attr("protocol", "1");
    if let Some(hash) = hash.filter(|hash| !hash.is_empty()) {
        props = props.attr("hash", hash);
    }
    BinaryNode::new("iq")
        .attr("to", jid::S_WHATSAPP_NET)
        .attr("xmlns", "abt")
        .attr("type", "get")
        .with_content(NodeContent::Children(vec![props]))
}
pub fn blocklist_query() -> BinaryNode {
    BinaryNode::new("iq")
        .attr("xmlns", "blocklist")
        .attr("to", jid::S_WHATSAPP_NET)
        .attr("type", "get")
}
pub fn extract_blocklist(response: &BinaryNode) -> Vec<Option<String>> {
    response
        .child("list")
        .into_iter()
        .flat_map(|n| n.children("item"))
        .map(|n| n.attrs.get("jid").cloned())
        .collect()
}
pub fn extract_props(response: &BinaryNode) -> Properties {
    let mut props = Properties::new();
    if let Some(node) = response.child("props") {
        for prop in node.children("prop") {
            let name = prop
                .attrs
                .get("name")
                .or_else(|| prop.attrs.get("config_code"))
                .map(String::as_str)
                .unwrap_or("undefined");
            let value = prop
                .attrs
                .get("value")
                .filter(|v| !v.is_empty())
                .or_else(|| prop.attrs.get("config_value"))
                .cloned();
            props.insert(name.into(), value);
        }
    }
    props
}
impl Socket {
    pub async fn fetch_blocklist(&self) -> Result<Vec<Option<String>>, Error> {
        Ok(extract_blocklist(
            &self.query(blocklist_query(), None).await?,
        ))
    }
    pub async fn fetch_props(&self) -> Result<Properties, Error> {
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        let response = self
            .query(props_query(credentials.last_prop_hash.as_deref()), None)
            .await?;
        if let Some(hash) = response
            .child("props")
            .and_then(|p| p.attrs.get("hash"))
            .filter(|h| !h.is_empty())
        {
            let (reply, received) = tokio::sync::oneshot::channel();
            self.inner
                .commands
                .send(Command::LastPropHash {
                    hash: hash.clone(),
                    reply,
                })
                .await
                .map_err(|_| Error::Closed(428))?;
            received.await.map_err(|_| Error::Closed(428))??;
        }
        let props = extract_props(&response);
        self.apply_server_props(
            &props
                .iter()
                .filter_map(|(name, value)| {
                    value.as_ref().map(|value| (name.clone(), value.clone()))
                })
                .collect(),
        )?;
        Ok(props)
    }
    /// Like Promise.all, the first failure returns while the other queries finish.
    pub async fn execute_init_queries(&self) -> Result<(), Error> {
        let mut queries = FuturesUnordered::new();
        for kind in 0..3 {
            let socket = self.clone();
            queries.push(tokio::spawn(async move {
                match kind {
                    0 => {
                        socket.fetch_props().await?;
                    }
                    1 => {
                        socket.fetch_blocklist().await?;
                    }
                    _ => {
                        socket.fetch_privacy_settings(false).await?;
                    }
                }
                Ok::<_, Error>(())
            }));
        }
        while let Some(result) = queries.next().await {
            result.map_err(|_| Error::Task)??;
        }
        Ok(())
    }
}
pub(super) fn spawn_worker(inner: &Arc<Inner>, enabled: bool) {
    if !enabled {
        return;
    }
    let weak = Arc::downgrade(inner);
    let mut state = inner.state.clone();
    tokio::spawn(async move {
        loop {
            let current = state.borrow().clone();
            match current {
                ConnectionState::Closed { .. } => break,
                ConnectionState::Open => {
                    if let Some(inner) = weak.upgrade() {
                        tokio::spawn(async move {
                            if let Err(error) = (Socket { inner }).execute_init_queries().await {
                                tracing::warn!(?error, "startup queries failed");
                            }
                        });
                    }
                    break;
                }
                ConnectionState::Connecting => (),
            }
            if state.changed().await.is_err() {
                break;
            }
        }
    });
}
