use super::{Command, Error, RelayOptions, Socket, load_credentials};
use crate::{jid, messages, proto, relay};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;
/// Retry counters and pending placeholder resends. Built-in stores are emptied
/// when the connection ends; caller-supplied ones are left alone.
pub(super) struct RetryRequests {
    counts: crate::cache::Cache<u64>,
    placeholders: crate::cache::Cache<Option<messages::Envelope>>,
    own_counts: Option<Arc<crate::cache::MemoryStore<u64>>>,
    own_placeholders: Option<Arc<crate::cache::MemoryStore<Option<messages::Envelope>>>>,
    pub serial: tokio::sync::Mutex<()>,
}
impl RetryRequests {
    pub(super) fn new(
        counts: Option<crate::cache::Cache<u64>>,
        placeholders: Option<crate::cache::Cache<Option<messages::Envelope>>>,
    ) -> Self {
        use crate::cache::{Cache, MemoryStore};
        let ttl = Duration::from_secs(3600);
        let own_counts = counts.is_none().then(|| Arc::new(MemoryStore::new(ttl)));
        let own_placeholders = placeholders
            .is_none()
            .then(|| Arc::new(MemoryStore::new(ttl)));
        Self {
            counts: counts
                .or_else(|| own_counts.clone().map(|store| Cache::from_arc(store)))
                .expect("a retry counter store"),
            placeholders: placeholders
                .or_else(|| own_placeholders.clone().map(|store| Cache::from_arc(store)))
                .expect("a placeholder store"),
            own_counts,
            own_placeholders,
            serial: Default::default(),
        }
    }
    pub(super) fn clear(&self) {
        if let Some(store) = &self.own_counts {
            store.clear();
        }
        if let Some(store) = &self.own_placeholders {
            store.clear();
        }
    }
    pub(super) async fn count(&self, key: &str) -> u64 {
        self.counts.get(key).await.unwrap_or(0)
    }
    pub(super) async fn increment(&self, key: &str) {
        let count = self.count(key).await.saturating_add(1);
        self.counts.set(key, count).await;
    }
}
impl Socket {
    pub async fn send_peer_data_operation_message(
        &self,
        request: proto::message::PeerDataOperationRequestMessage,
    ) -> Result<String, Error> {
        let credentials = load_credentials(self.inner.store.clone()).await?;
        let me = jid::Jid::parse(credentials.me_id()?)
            .ok_or(relay::Error::Recipient)?
            .normalized();
        let message = proto::Message {
            protocol_message: Some(Box::new(proto::message::ProtocolMessage {
                peer_data_operation_request_message: Some(request),
                r#type: Some(
                    proto::message::protocol_message::Type::PeerDataOperationRequestMessage as i32,
                ),
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut options = RelayOptions::default();
        options
            .stanza
            .additional_attributes
            .insert("category".into(), "peer".into());
        options
            .stanza
            .additional_attributes
            .insert("push_priority".into(), "high_force".into());
        options
            .stanza
            .additional_nodes
            .push(crate::binary::BinaryNode::new("meta").attr("appdata", "default"));
        self.relay_message(&me, message, options).await
    }
    /// Retain original metadata while waiting for a phone's placeholder response.
    /// Ask the phone for `count` messages older than the given one. The reply arrives
    /// as an on-demand history notification; the returned ID is the request stanza's.
    pub async fn fetch_message_history(
        &self,
        count: i32,
        oldest: &proto::MessageKey,
        oldest_timestamp_ms: i64,
    ) -> Result<String, Error> {
        self.send_peer_data_operation_message(crate::history::on_demand_request(
            count,
            oldest,
            oldest_timestamp_ms,
        ))
        .await
    }
    pub async fn request_placeholder_resend(
        &self,
        key: &proto::MessageKey,
        data: Option<messages::Envelope>,
    ) -> Result<Option<String>, Error> {
        load_credentials(self.inner.store.clone()).await?.me_id()?;
        let id = key
            .id
            .as_deref()
            .filter(|id| !id.is_empty())
            .ok_or(messages::Error::Envelope("missing id"))?
            .to_owned();
        let placeholders = &self.inner.retry_requests.placeholders;
        if placeholders.get(&id).await.is_some() {
            return Ok(None);
        }
        placeholders.set(&id, data).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        if !self.has_pending_placeholder_resend(&id).await? {
            return Ok(Some("RESOLVED".into()));
        }
        let state = Arc::downgrade(&self.inner.retry_requests);
        let expiry_id = id;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(8)).await;
            if let Some(state) = state.upgrade()
                && state.placeholders.get(&expiry_id).await.is_some()
            {
                state.placeholders.del(&expiry_id).await;
            }
        });
        let request=proto::message::PeerDataOperationRequestMessage {
            peer_data_operation_request_type:Some(proto::message::PeerDataOperationRequestType::PlaceholderMessageResend as i32),
            placeholder_message_resend_request:vec![proto::message::peer_data_operation_request_message::PlaceholderMessageResendRequest {message_key:Some(key.clone())}],..Default::default()
        };
        Ok(Some(
            Box::pin(self.send_peer_data_operation_message(request)).await?,
        ))
    }
    /// Remove a pending placeholder when the original message or PDO response arrives.
    /// The optional metadata is returned for the incoming-processing layer to merge.
    pub async fn resolve_placeholder_resend(
        &self,
        id: &str,
    ) -> Result<Option<messages::Envelope>, Error> {
        let placeholders = &self.inner.retry_requests.placeholders;
        let Some(data) = placeholders.get(id).await else {
            return Ok(None);
        };
        placeholders.del(id).await;
        Ok(data)
    }
    pub async fn has_pending_placeholder_resend(&self, id: &str) -> Result<bool, Error> {
        Ok(self
            .inner
            .retry_requests
            .placeholders
            .get(id)
            .await
            .is_some())
    }
    /// Send a retry receipt, with source limits, session recreation and phone scheduling.
    /// Count/prekey state is retained after an ambiguous transport failure.
    pub async fn send_retry_request(
        &self,
        node: &crate::binary::BinaryNode,
        mut force_include_keys: bool,
    ) -> Result<(), Error> {
        let _serial = self.inner.retry_requests.serial.lock().await;
        let credentials = load_credentials(self.inner.store.clone()).await?;
        let envelope = messages::decode_node(
            node,
            credentials.me_id()?,
            credentials
                .me
                .as_ref()
                .and_then(|m| m.lid.as_deref())
                .unwrap_or(""),
        )?;
        let key = envelope
            .message
            .key
            .ok_or(messages::Error::Envelope("missing key"))?;
        let id = key
            .id
            .as_deref()
            .ok_or(messages::Error::Envelope("missing id"))?;
        let composite = format!(
            "{}:{}",
            id,
            key.participant.as_deref().unwrap_or("undefined")
        );
        let count = if let Some(manager) = &self.inner.retry_manager {
            if manager.has_exceeded_max_retries(id)? {
                manager.mark_retry_failed(id)?;
                return Ok(());
            }
            manager.increment_retry_count(id)?
        } else {
            let counts = &self.inner.retry_requests.counts;
            let count = counts.get(&composite).await.unwrap_or(0);
            if count >= self.inner.max_msg_retry_count {
                counts.del(&composite).await;
                return Ok(());
            }
            count.saturating_add(1)
        };
        self.inner
            .retry_requests
            .counts
            .set(&composite, count)
            .await;
        if self.inner.enable_auto_session_recreation
            && count > 1
            && let Some(manager) = &self.inner.retry_manager
        {
            let repository = self.inner.decryptor.direct.clone();
            let from = node
                .attrs
                .get("from")
                .cloned()
                .ok_or(messages::Error::Envelope("missing from"))?;
            let manager = manager.clone();
            match tokio::task::spawn_blocking(move || {
                let has_session = repository.validate_session(&from)?;
                let recreate = manager.should_recreate_session(&from, has_session, None)?;
                if recreate.recreate {
                    // Source deletes the literal PN address, without alias resolution.
                    repository.delete_sessions(&[from])?;
                }
                Ok::<_, Error>(recreate.recreate)
            })
            .await
            .map_err(|_| Error::Task)?
            {
                Ok(recreate) => force_include_keys |= recreate,
                Err(error) => tracing::debug!(?error, "session recreation check failed"),
            }
        }
        if count <= 2 {
            if let Some(manager) = &self.inner.retry_manager {
                let inner = Arc::downgrade(&self.inner);
                let key = key.clone();
                manager.schedule_phone_request(
                    id,
                    move || {
                        if let Some(inner) = inner.upgrade() {
                            tokio::spawn(async move {
                                if let Err(error) = (Socket { inner })
                                    .request_placeholder_resend(&key, None)
                                    .await
                                {
                                    tracing::debug!(?error, "scheduled phone resend failed");
                                }
                            });
                        }
                    },
                    None,
                )?;
            } else {
                Box::pin(self.request_placeholder_resend(&key, None)).await?;
            }
        }
        let (reply, receiver) = oneshot::channel();
        self.inner
            .commands
            .send(Command::RetryReceipt {
                node: node.clone(),
                count,
                include_keys: count > 1 || force_include_keys,
                reply,
            })
            .await
            .map_err(|_| self.closed_error())?;
        receiver.await.map_err(|_| self.closed_error())??;
        tracing::info!(
            id,
            from = ?node.attrs.get("from"),
            participant = ?node.attrs.get("participant"),
            retry_count = count,
            "sent retry receipt"
        );
        Ok(())
    }
}
