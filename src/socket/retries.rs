use super::{Command, Error, RelayOptions, Socket, load_credentials};
use crate::{jid, messages, proto, relay};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
#[derive(Clone)]
struct Placeholder {
    data: Option<messages::Envelope>,
    expires: Instant,
}
#[derive(Default)]
struct Cache {
    counts: HashMap<String, (u64, Instant)>,
    placeholders: HashMap<String, Placeholder>,
}
#[derive(Default)]
pub(super) struct RetryRequests {
    cache: Mutex<Cache>,
    pub serial: tokio::sync::Mutex<()>,
}
impl RetryRequests {
    pub(super) fn clear(&self) {
        if let Ok(mut c) = self.cache.lock() {
            c.counts.clear();
            c.placeholders.clear();
        }
    }
    pub(super) fn count(&self, key: &str) -> Result<u64, Error> {
        let mut cache = self.cache.lock().map_err(|_| Error::Task)?;
        cache
            .counts
            .retain(|_, (_, expires)| *expires > Instant::now());
        Ok(cache.counts.get(key).map(|(c, _)| *c).unwrap_or(0))
    }
    pub(super) fn increment(&self, key: &str) -> Result<(), Error> {
        let mut cache = self.cache.lock().map_err(|_| Error::Task)?;
        cache
            .counts
            .retain(|_, (_, expires)| *expires > Instant::now());
        let count = cache
            .counts
            .get(key)
            .map(|(c, _)| *c)
            .unwrap_or(0)
            .saturating_add(1);
        cache.counts.insert(
            key.into(),
            (count, Instant::now() + Duration::from_secs(3600)),
        );
        Ok(())
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
        {
            let mut cache = self
                .inner
                .retry_requests
                .cache
                .lock()
                .map_err(|_| Error::Task)?;
            cache.placeholders.retain(|_, v| v.expires > Instant::now());
            if cache.placeholders.contains_key(&id) {
                return Ok(None);
            }
            cache.placeholders.insert(
                id.clone(),
                Placeholder {
                    data,
                    expires: Instant::now() + Duration::from_secs(3600),
                },
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        if !self.has_pending_placeholder_resend(&id)? {
            return Ok(Some("RESOLVED".into()));
        }
        let state = Arc::downgrade(&self.inner.retry_requests);
        let expiry_id = id;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(8)).await;
            if let Some(state) = state.upgrade()
                && let Ok(mut cache) = state.cache.lock()
            {
                cache.placeholders.remove(&expiry_id);
            }
        });
        let request=proto::message::PeerDataOperationRequestMessage {
            peer_data_operation_request_type:Some(proto::message::PeerDataOperationRequestType::PlaceholderMessageResend as i32),
            placeholder_message_resend_request:vec![proto::message::peer_data_operation_request_message::PlaceholderMessageResendRequest {message_key:Some(key.clone())}],..Default::default()
        };
        Ok(Some(self.send_peer_data_operation_message(request).await?))
    }
    /// Remove a pending placeholder when the original message or PDO response arrives.
    /// The optional metadata is returned for the incoming-processing layer to merge.
    pub fn resolve_placeholder_resend(
        &self,
        id: &str,
    ) -> Result<Option<messages::Envelope>, Error> {
        let mut cache = self
            .inner
            .retry_requests
            .cache
            .lock()
            .map_err(|_| Error::Task)?;
        cache.placeholders.retain(|_, p| p.expires > Instant::now());
        Ok(cache.placeholders.remove(id).and_then(|p| p.data))
    }
    pub fn has_pending_placeholder_resend(&self, id: &str) -> Result<bool, Error> {
        let mut cache = self
            .inner
            .retry_requests
            .cache
            .lock()
            .map_err(|_| Error::Task)?;
        cache.placeholders.retain(|_, p| p.expires > Instant::now());
        Ok(cache.placeholders.contains_key(id))
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
            let mut cache = self
                .inner
                .retry_requests
                .cache
                .lock()
                .map_err(|_| Error::Task)?;
            cache
                .counts
                .retain(|_, (_, expires)| *expires > Instant::now());
            let count = cache.counts.get(&composite).map(|(c, _)| *c).unwrap_or(0);
            if count >= self.inner.max_msg_retry_count {
                cache.counts.remove(&composite);
                return Ok(());
            }
            count.saturating_add(1)
        };
        self.inner
            .retry_requests
            .cache
            .lock()
            .map_err(|_| Error::Task)?
            .counts
            .insert(
                composite,
                (count, Instant::now() + Duration::from_secs(3600)),
            );
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
                self.request_placeholder_resend(&key, None).await?;
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
        receiver.await.map_err(|_| self.closed_error())?
    }
}
