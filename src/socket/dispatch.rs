//! Admission before buffering and a shared queue for supported offline handlers.
use super::{ConnectionState, Error, Inner, Socket};
use crate::{auth::Contact, binary::BinaryNode, jid, offline};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

pub(super) struct OnlineNode {
    pub node: BinaryNode,
    pub scope: super::events::NodeScope,
}

// Receipt ownership follows jidDecode's user component even for malformed devices.
// Filtering must precede address validation and cryptographic work.
fn user(jid: Option<&str>) -> Option<&str> {
    jid.and_then(|jid| jid.split_once('@')).map(|(user, _)| {
        user.split(':')
            .next()
            .unwrap_or("")
            .split('_')
            .next()
            .unwrap_or("")
    })
}
pub fn ignore_target<'a>(node: &'a BinaryNode, me: Option<&Contact>) -> Option<&'a str> {
    let from = node.attrs.get("from").map(String::as_str);
    if node.tag == "receipt" && from.is_some_and(|s| !s.is_empty()) {
        let from = from.unwrap();
        let identity = if from.contains("lid") {
            me.and_then(|m| m.lid.as_deref())
        } else {
            me.map(|m| m.id.as_str())
        };
        let participant = node
            .attrs
            .get("participant")
            .map(String::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(from);
        if user(Some(participant)) == user(identity) && !from.ends_with("@g.us") {
            return node.attrs.get("recipient").map(String::as_str);
        }
    }
    from
}
pub fn is_ignored(
    node: &BinaryNode,
    me: Option<&Contact>,
    filter: &super::presence::IgnoreJid,
) -> bool {
    ignore_target(node, me)
        .is_some_and(|jid| !jid.is_empty() && jid != jid::S_WHATSAPP_NET && filter.test(jid))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Ignored,
    Offline,
    Online,
}
pub fn admission(
    node: &BinaryNode,
    me: Option<&Contact>,
    filter: &super::presence::IgnoreJid,
) -> Admission {
    if is_ignored(node, me, filter) {
        Admission::Ignored
    } else if node.attrs.get("offline").is_some_and(|s| !s.is_empty()) {
        Admission::Offline
    } else {
        Admission::Online
    }
}
/// Notifications handled by the ordered notification worker.
pub(super) fn worker_notification(node: &BinaryNode, config: &super::Config) -> bool {
    let kind = node.attrs.get("type").map(String::as_str);
    if kind == Some("server_sync") {
        config.process_app_state_notifications
    } else {
        config.process_notifications
            && (matches!(kind, Some("devices" | "encrypt" | "privacy_token"))
                || crate::notifications::is_supported(kind))
    }
}
pub(super) fn spawn_worker(inner: &Arc<Inner>, mut receiver: mpsc::UnboundedReceiver<BinaryNode>) {
    let mut handlers: HashMap<String, offline::Handler<Error>> = HashMap::new();
    for kind in ["message", "call", "receipt", "notification"] {
        let weak = Arc::downgrade(inner);
        handlers.insert(
            kind.into(),
            Arc::new(move |node| {
                let weak = weak.clone();
                Box::pin(async move {
                    let Some(inner) = weak.upgrade() else {
                        return Err(Error::Closed(428));
                    };
                    let socket = Socket { inner };
                    match kind {
                        "message" => socket.handle_message(&node).await,
                        "call" => socket.handle_call(&node).await,
                        "receipt" => socket.handle_receipt(&node).await,
                        _ if node.attrs.get("type").is_some_and(|t| t == "mediaretry") => {
                            match crate::media::decode_retry(&node) {
                                Ok(event) => {
                                    socket
                                        .inner
                                        .events
                                        .send(super::Event::MediaUpdated(vec![event]))?;
                                }
                                Err(error) => {
                                    tracing::debug!(?error, "media notification decode failed")
                                }
                            }
                            socket.send_message_ack(&node, None).await
                        }
                        _ => socket.handle_notification(&node).await.map(|_| ()),
                    }
                })
            }),
        );
    }
    let weak = Arc::downgrade(inner);
    let deps = offline::Dependencies::new(
        move || {
            weak.upgrade().is_some_and(|inner| {
                !matches!(*inner.state.borrow(), ConnectionState::Closed { .. })
            })
        },
        |failure, context| tracing::warn!(?failure, context, "offline node processing failed"),
    );
    let processor = offline::Processor::new(handlers, deps);
    tokio::spawn(async move {
        while let Some(node) = receiver.recv().await {
            processor.enqueue(node.tag.clone(), node);
        }
    });
}
