use super::{Error, Event, RelayOptions, Socket, load_credentials};
use crate::{
    auth::{KeyCategory, KeyMutation, KeyStore},
    binary::BinaryNode,
    proto, receipts, relay,
    signal::{self, wire},
};
impl Socket {
    /// Handle status/user updates and retry resends, then acknowledge even on resend errors.
    pub async fn handle_receipt(&self, node: &BinaryNode) -> Result<(), Error> {
        let _serial = self.inner.receipt_lock.lock().await;
        let result = self.receipt_work(node).await;
        if let Err(error) = self.send_message_ack(node, None).await {
            tracing::debug!(?error, "receipt acknowledgement failed");
        }
        result
    }
    async fn receipt_work(&self, node: &BinaryNode) -> Result<(), Error> {
        let c = load_credentials(self.inner.store.clone()).await?;
        let mut parsed = receipts::incoming(
            node,
            c.me_id()?,
            c.me.as_ref().and_then(|c| c.lid.as_deref()),
        )?;
        if !parsed.messages.is_empty() {
            let _ = self
                .inner
                .events
                .send(Event::MessagesUpdated(parsed.messages));
        }
        if !parsed.users.is_empty() {
            let _ = self
                .inner
                .events
                .send(Event::MessageReceiptsUpdated(parsed.users));
        }
        if parsed.retry {
            parsed.key.participant = parsed
                .key
                .participant
                .filter(|s| !s.is_empty())
                .or_else(|| node.attrs.get("from").cloned());
            if let (Some(id), Some(participant)) = (
                parsed
                    .ids
                    .first()
                    .and_then(Option::as_deref)
                    .filter(|s| !s.is_empty()),
                parsed.key.participant.as_deref(),
            ) {
                let counter = format!("{id}:{participant}");
                if parsed.key.from_me == Some(true)
                    && self.inner.retry_requests.count(&counter)? < self.inner.max_msg_retry_count
                {
                    self.inner.retry_requests.increment(&counter)?;
                    if let Err(error) = self
                        .send_messages_again(&parsed.key, &parsed.ids, node)
                        .await
                    {
                        tracing::debug!(?error, "retry resend failed");
                    }
                }
            }
        }
        Ok(())
    }
    async fn send_messages_again(
        &self,
        key: &proto::MessageKey,
        ids: &[Option<String>],
        node: &BinaryNode,
    ) -> Result<(), Error> {
        let remote = key.remote_jid.as_deref().ok_or(relay::Error::Recipient)?;
        let participant = key
            .participant
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(remote);
        let retry = node.child("retry").ok_or(Error::Config)?;
        let raw_count = retry
            .attrs
            .get("count")
            .map(|s| crate::privacy::number(s))
            .unwrap_or(f64::NAN);
        let count = if raw_count == 0.0 || raw_count.is_nan() {
            1.0
        } else {
            raw_count
        };
        let mut messages = vec![];
        for id in ids {
            let mut message = if let (Some(manager), Some(id)) = (&self.inner.retry_manager, id) {
                manager.get_recent_message(remote, id)?.map(|c| c.message)
            } else {
                None
            };
            if message.is_none() {
                let mut lookup = key.clone();
                lookup.id = id.clone();
                message = self.inner.get_message.get(lookup).await?;
            }
            if message.is_some()
                && let (Some(manager), Some(id)) = (&self.inner.retry_manager, id)
            {
                manager.mark_retry_success(id)?;
            }
            messages.push(message);
        }
        let address = signal::Address::from_jid(participant)?.to_string();
        let repository = self.inner.decryptor.direct.clone();
        let target = participant.to_owned();
        let manager = self.inner.retry_manager.clone();
        let node = node.clone();
        let msg_id = ids.first().cloned().flatten();
        let auto = self.inner.enable_auto_session_recreation;
        let injected = tokio::task::spawn_blocking(move || {
            let injected = if let Some(bundle) = wire::extract_retry_bundle(&node) {
                match repository.inject_session(&target, &bundle) {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::debug!(?error, "retry bundle injection failed");
                        false
                    }
                }
            } else {
                false
            };
            if !injected
                && let Some(bytes) = node.child_buffer("registration").filter(|b| b.len() >= 4)
            {
                let reg = u32::from_be_bytes(bytes[..4].try_into().unwrap());
                if let Some((_, stored)) = repository.session_info(&target)?
                    && stored != 0
                    && stored != reg
                {
                    repository.delete_sessions(std::slice::from_ref(&target))?;
                }
            }
            if let (Some(manager), Some(id)) = (&manager, &msg_id)
                && let Some((base, _)) = repository.session_info(&target)?
            {
                if count == 2.0 {
                    manager.save_base_key(&address, id, &base)?;
                } else if count > 2.0 {
                    if manager.has_same_base_key(&address, id, &base)? {
                        repository.delete_sessions(std::slice::from_ref(&target))?;
                    }
                    manager.delete_base_key(&address, id)?;
                }
            }
            if auto
                && count > 1.0
                && !injected
                && let Some(manager) = manager
            {
                let check = (|| {
                    let exists = repository.validate_session(&target)?;
                    let decision = manager.should_recreate_session(&target, exists, None)?;
                    if decision.recreate {
                        repository.delete_sessions(std::slice::from_ref(&target))?;
                    }
                    Ok::<_, Error>(())
                })();
                if let Err(error) = check {
                    tracing::debug!(?error, "outgoing recreation check failed");
                }
            }
            Ok::<_, Error>(injected)
        })
        .await
        .map_err(|_| Error::Task)??;
        if !injected {
            self.assert_sessions(&[participant.into()], true).await?;
        }
        if remote.ends_with("@g.us") {
            let store = self.inner.store.clone();
            let remote = remote.to_owned();
            tokio::task::spawn_blocking(move || {
                store.set(&[KeyMutation::delete(KeyCategory::SenderKeyMemory, remote)])
            })
            .await
            .map_err(|_| Error::Task)??;
        }
        let send_all =
            crate::jid::Jid::parse(participant).is_none_or(|j| j.device.unwrap_or(0) == 0);
        for (id, message) in ids.iter().zip(messages) {
            if let (Some(id), Some(message)) = (id.as_deref().filter(|s| !s.is_empty()), message) {
                let counter = format!("{id}:{participant}");
                if self.inner.retry_requests.count(&counter)? < self.inner.max_msg_retry_count {
                    self.inner.retry_requests.increment(&counter)?;
                    let mut options = RelayOptions {
                        message_id: Some(id.into()),
                        ..Default::default()
                    };
                    if send_all {
                        options.use_user_devices_cache = false;
                    } else {
                        if !raw_count.is_finite()
                            || raw_count < 0.0
                            || raw_count.fract() != 0.0
                            || raw_count > u32::MAX as f64
                        {
                            return Err(Error::Config);
                        }
                        options.stanza.retry = Some(relay::Retry {
                            jid: participant.into(),
                            count: raw_count as u32,
                        });
                    }
                    self.relay_message(remote, message, options).await?;
                }
            }
        }
        Ok(())
    }
}
