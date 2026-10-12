use super::{Error, Inner, Socket};
use crate::{app_state, binary::BinaryNode, receipts};
use std::sync::Arc;
use tokio::sync::mpsc;

impl Socket {
    /// Dispatch a server_sync notification once. Disable automatic processing
    /// when dispatching raw Node events manually. ACK failure is warning-only,
    /// including when processing has already failed, as in upstream's finally.
    pub async fn handle_app_state_notification(&self, node: &BinaryNode) -> Result<bool, Error> {
        if node.tag != "notification" || node.attrs.get("type").is_none_or(|t| t != "server_sync") {
            return Ok(false);
        }
        let result = {
            let _serial = self.inner.notification_lock.lock().await;
            self.process_app_state_notification(node).await
        };
        self.finish_app_state_notification(node, result).await
    }
    async fn process_app_state_notification(&self, node: &BinaryNode) -> Result<(), Error> {
        if let Some(collection) = node.child("collection") {
            let name = collection
                .attrs
                .get("name")
                .ok_or(app_state::Error::Input("notification collection name"))?
                .clone();
            let options = self
                .inner
                .app_state_options
                .lock()
                .map_err(|_| Error::Config)?
                .clone();
            self.resync_app_state(&[name], false, &options).await?;
        }
        Ok(())
    }
    async fn finish_app_state_notification(
        &self,
        node: &BinaryNode,
        result: Result<(), Error>,
    ) -> Result<bool, Error> {
        if let Err(error) = self
            .send_node(receipts::acknowledgement(node, None, None))
            .await
        {
            tracing::warn!(?error, "failed to acknowledge app-state notification");
        }
        result?;
        Ok(true)
    }
}
pub(super) fn spawn_worker(inner: &Arc<Inner>, mut receiver: mpsc::UnboundedReceiver<BinaryNode>) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        while let Some(node) = receiver.recv().await {
            let Some(inner) = weak.upgrade() else { break };
            // Acquire in arrival order, but release before ACK completion so
            // queued processing can continue as in upstream's finally block.
            let guard = inner.notification_lock.clone().lock_owned().await;
            tokio::spawn(async move {
                let socket = Socket { inner };
                let result = socket.process_app_state_notification(&node).await;
                drop(guard);
                if let Err(error) = socket.finish_app_state_notification(&node, result).await {
                    tracing::warn!(?error, "app-state notification processing failed");
                }
            });
        }
    });
}
