use super::{Error, Event, Inner, Socket, load_credentials};
use crate::{
    binary::BinaryNode,
    jid,
    messages::{
        self,
        incoming::{self, Plan},
        own,
    },
};
use std::{
    sync::{Arc, atomic::Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
impl Socket {
    /// Update delivery receipt activity (matching upstream's connection isOnline state).
    pub fn set_active_receipts(&self, active: bool) {
        self.inner.active_receipts.store(active, Ordering::Relaxed);
    }
    /// Decrypt and process a stanza once. Disable Config::process_messages when
    /// dispatching raw Node events manually; explicit decryption consumes counters.
    pub async fn handle_message(&self, node: &BinaryNode) -> Result<(), Error> {
        let _serial = self.inner.message_lock.lock().await;
        let mut acked = false;
        let result = self.incoming_message_work(node, &mut acked).await;
        if result.is_err()
            && !acked
            && let Err(error) = self.send_message_ack(node, Some(500)).await
        {
            tracing::debug!(?error, "incoming error acknowledgement failed");
        }
        result
    }
    async fn incoming_message_work(
        &self,
        node: &BinaryNode,
        acked: &mut bool,
    ) -> Result<(), Error> {
        if node
            .child("enc")
            .is_some_and(|n| n.attrs.get("type").is_some_and(|t| t == "msmsg"))
        {
            *acked = true;
            return self.send_message_ack(node, Some(495)).await;
        }
        let credentials = load_credentials(self.inner.store.clone()).await?;
        let decoder = self.inner.decryptor.clone();
        let message_node = node.clone();
        let auth = credentials.clone();
        let mut decrypted = tokio::task::spawn_blocking(move || {
            let me = auth.me_id()?;
            let lid = auth
                .me
                .as_ref()
                .and_then(|m| m.lid.as_deref())
                .unwrap_or("");
            let envelope = messages::decode_node(&message_node, me, lid)?;
            // Receive-level mappings apply to plaintext as well as encrypted content.
            let alt = envelope
                .key_metadata
                .participant_alt
                .as_deref()
                .filter(|s| !s.is_empty())
                .or(envelope
                    .key_metadata
                    .remote_jid_alt
                    .as_deref()
                    .filter(|s| !s.is_empty()));
            if let Some(alt) = alt {
                let key = envelope
                    .message
                    .key
                    .as_ref()
                    .ok_or(messages::Error::Envelope("missing received key"))?;
                let primary = key
                    .participant
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .or(key.remote_jid.as_deref())
                    .ok_or(messages::Error::Envelope("missing mapping address"))?;
                let mapping = decoder.direct.lid_mapping();
                if jid::Jid::parse(alt).is_some_and(|j| j.is_lid_user()) {
                    if mapping.get_pn_for_lid(alt)?.is_none() {
                        mapping.store_mappings(&[crate::signal::mapping::Mapping {
                            lid: alt.into(),
                            pn: primary.into(),
                        }])?;
                        decoder.direct.migrate_session(primary, alt)?;
                    }
                } else {
                    mapping.store_mappings(&[crate::signal::mapping::Mapping {
                        lid: primary.into(),
                        pn: alt.into(),
                    }])?;
                    decoder.direct.migrate_session(alt, primary)?;
                }
            }
            Ok::<_, Error>(messages::decrypt_node(
                &message_node,
                me,
                lid,
                decoder.as_ref(),
            )?)
        })
        .await
        .map_err(|_| Error::Task)??;
        for error in decrypted
            .mapping_errors
            .iter()
            .chain(&decrypted.distribution_errors)
        {
            tracing::debug!(?error, "received message mapping/distribution warning");
        }
        let envelope = &mut decrypted.envelope;
        let key = envelope
            .message
            .key
            .as_ref()
            .ok_or(messages::Error::Envelope("missing received key"))?
            .clone();
        let remote = key
            .remote_jid
            .as_deref()
            .ok_or(messages::Error::Envelope("missing received chat"))?;
        let id = key
            .id
            .as_deref()
            .ok_or(messages::Error::Envelope("missing received id"))?;
        if let Some(manager) = &self.inner.retry_manager
            && let Some(content) = &envelope.message.message
        {
            manager.add_recent_message(remote, id, content.clone())?;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Config)?
            .as_secs();
        let plan = incoming::plan(
            envelope,
            node,
            self.inner.active_receipts.load(Ordering::Relaxed),
            now,
        );
        match plan {
            Plan::Drop { nack } => {
                *acked = true;
                return self.send_message_ack(node, nack).await;
            }
            Plan::Retry => {
                if !matches!(
                    *self.inner.state.borrow(),
                    super::ConnectionState::Closed { .. }
                ) && let Err(error) = self
                    .send_retry_request(node, node.child("enc").is_none())
                    .await
                {
                    tracing::debug!(?error, "incoming decrypt retry failed");
                }
                if !self.inner.retry_request_delay.is_zero() {
                    tokio::time::sleep(self.inner.retry_request_delay).await;
                }
                *acked = true;
                self.send_message_ack(node, Some(500)).await?;
            }
            Plan::Placeholder => {
                let socket = self.clone();
                let key = key.clone();
                let data = envelope.clone();
                tokio::spawn(async move {
                    match socket.request_placeholder_resend(&key, Some(data)).await {
                        Ok(Some(request_id)) if request_id != "RESOLVED" => {
                            let _ = socket.inner.events.send(Event::MessageStubsUpdated(vec![
                                incoming::StubUpdate {
                                    key,
                                    parameters: vec![incoming::NO_MESSAGE.into(), request_id],
                                },
                            ]));
                        }
                        Err(error) => tracing::debug!(?error, "placeholder resend failed"),
                        _ => {}
                    }
                });
                *acked = true;
                self.send_message_ack(node, None).await?;
            }
            Plan::Newsletter => {
                if let Some(manager) = &self.inner.retry_manager {
                    manager.cancel_pending_phone_request(id)?;
                }
                *acked = true;
                self.send_message_ack(node, None).await?;
            }
            Plan::Delivery {
                kind,
                participant,
                history,
            } => {
                if let Some(manager) = &self.inner.retry_manager {
                    manager.cancel_pending_phone_request(id)?;
                }
                *acked = true;
                self.send_receipt(remote, participant.as_deref(), &[id.into()], kind)
                    .await?;
                if history {
                    self.send_receipt(
                        &jid::normalized_user(remote),
                        None,
                        &[id.into()],
                        Some(crate::receipts::Type::HistorySync),
                    )
                    .await?;
                }
            }
        }
        let cleaned = incoming::clean(
            &mut envelope.message,
            credentials.me_id()?,
            credentials
                .me
                .as_ref()
                .and_then(|m| m.lid.as_deref())
                .unwrap_or(""),
        );
        if let Some(manager) = &self.inner.retry_manager
            && let Some(content) = &envelope.message.message
        {
            manager.update_recent_message(remote, id, content.clone())?;
        }
        cleaned?;
        self.publish_message(
            &envelope.message,
            Some(messages::EnvelopeMetadata::from(&*envelope)),
            if node.attrs.get("offline").is_some_and(|s| !s.is_empty()) {
                own::UpsertKind::Append
            } else {
                own::UpsertKind::Notify
            },
        )
        .await
    }
}
pub(super) fn spawn_worker(inner: &Arc<Inner>, mut receiver: mpsc::UnboundedReceiver<BinaryNode>) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        while let Some(node) = receiver.recv().await {
            let Some(inner) = weak.upgrade() else { break };
            if let Err(error) = (Socket { inner }).handle_message(&node).await {
                tracing::debug!(?error, "incoming message handling failed");
            }
        }
    });
}
