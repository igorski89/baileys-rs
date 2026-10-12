use super::{Error, Event, Inner, Socket};
use crate::{binary::BinaryNode, calls, messages::own};
use std::sync::Arc;
use tokio::sync::mpsc;
impl Socket {
    /// Process a call once, emitting its event before acknowledgement. Cache/decode
    /// failures are logged; acknowledgement is attempted even after those failures.
    /// Disable Config::process_calls when handling raw Node events manually.
    pub async fn handle_call(&self, node: &BinaryNode) -> Result<(), Error> {
        let result = calls::process(node, &self.inner.call_offer_cache).await;
        match result {
            Ok(call) => {
                if let Err(error) = self.inner.events.send(Event::Calls(vec![call.clone()])) {
                    tracing::warn!(?error, "call event publication failed");
                    if let Err(error) = self.send_message_ack(node, None).await {
                        tracing::warn!(?error, "call acknowledgement failed");
                    }
                    return Ok(());
                }
                // The source's async call listener starts publication separately from the ACK.
                match calls::message(&call) {
                    Ok(Some(message)) => {
                        let socket = self.clone();
                        tokio::spawn(async move {
                            if let Err(error) = socket
                                .publish_message(
                                    &message,
                                    None,
                                    if call.offline {
                                        own::UpsertKind::Append
                                    } else {
                                        own::UpsertKind::Notify
                                    },
                                )
                                .await
                            {
                                tracing::warn!(?error, "call message publication failed");
                            }
                        });
                    }
                    Ok(None) => (),
                    Err(error) => tracing::warn!(?error, "call message generation failed"),
                }
            }
            Err(error) => tracing::warn!(?error, "call processing failed"),
        }
        if let Err(error) = self.send_message_ack(node, None).await {
            tracing::warn!(?error, "call acknowledgement failed");
        }
        Ok(())
    }
}
pub(super) fn spawn_worker(
    inner: &Arc<Inner>,
    mut receiver: mpsc::UnboundedReceiver<super::dispatch::OnlineNode>,
) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        while let Some(online) = receiver.recv().await {
            let super::dispatch::OnlineNode { node, scope } = online;
            let Some(inner) = weak.upgrade() else { break };
            tokio::spawn(async move {
                let _scope = scope;
                if let Err(error) = (Socket { inner }).handle_call(&node).await {
                    tracing::warn!(?error, "call handling failed");
                }
            });
        }
    });
}
