use super::{Error, Event, Socket};
use crate::{binary::BinaryNode, messages::own, proto};

/// Server NACK for a 1:1 message missing a privacy token, also the reach-out timelock reason.
pub const MESSAGE_ACCOUNT_RESTRICTION: &str = "463";
pub const SMAX_INVALID: &str = "479";
pub const ACCOUNT_RESTRICTED_TEXT: &str = "Your account has been restricted";

/// The messages.update entry upstream emits for an errored message acknowledgement.
pub fn bad_ack_update(node: &BinaryNode) -> Option<own::Update> {
    let error = node.attrs.get("error").filter(|s| !s.is_empty())?;
    let mut parameters = vec![error.clone()];
    if error == MESSAGE_ACCOUNT_RESTRICTION {
        parameters.push(ACCOUNT_RESTRICTED_TEXT.into());
    }
    Some(own::Update {
        key: proto::MessageKey {
            remote_jid: node.attrs.get("from").cloned(),
            from_me: Some(true),
            id: node.attrs.get("id").cloned(),
            participant: None,
        },
        info: proto::WebMessageInfo {
            status: Some(proto::web_message_info::Status::Error as i32),
            message_stub_parameters: parameters,
            ..Default::default()
        },
        runtime_fields: Default::default(),
    })
}
struct Recovering {
    socket: Socket,
    jid: String,
}
impl Drop for Recovering {
    fn drop(&mut self) {
        if let Ok(mut set) = self.socket.inner.restricted_recoveries.lock() {
            set.remove(&self.jid);
        }
    }
}
impl Socket {
    /// Process an `ack` of class `message`: an error marks the sent message as failed.
    /// A missing-token rejection also starts one background token issuance per chat;
    /// the message itself is never retried.
    pub async fn handle_bad_ack(&self, node: &BinaryNode) -> Result<bool, Error> {
        if node.tag != "ack" || node.attrs.get("class").is_none_or(|c| c != "message") {
            return Ok(false);
        }
        let Some(update) = bad_ack_update(node) else {
            return Ok(true);
        };
        let error = node.attrs.get("error").map(String::as_str);
        if error == Some(MESSAGE_ACCOUNT_RESTRICTION) {
            tracing::warn!(
                id = ?node.attrs.get("id"),
                from = ?node.attrs.get("from"),
                "error 463: account restricted or missing tctoken for contact"
            );
            if let Some(from) = node.attrs.get("from").filter(|s| !s.is_empty())
                && self
                    .inner
                    .restricted_recoveries
                    .lock()
                    .map_err(|_| Error::Task)?
                    .insert(from.clone())
            {
                let guard = Recovering {
                    socket: self.clone(),
                    jid: from.clone(),
                };
                tokio::spawn(async move {
                    let socket = guard.socket.clone();
                    if let Err(error) = socket.recover_privacy_token(&guard.jid).await {
                        tracing::debug!(?error, "failed 463 token recovery issuance");
                    }
                });
            }
        } else if error == Some(SMAX_INVALID) {
            tracing::warn!(
                id = ?node.attrs.get("id"),
                from = ?node.attrs.get("from"),
                "smax-invalid (479): stanza rejected by server"
            );
        } else {
            tracing::warn!(attrs = ?node.attrs, "received error in ack");
        }
        self.inner
            .events
            .send(Event::MessageUpdates(vec![update]))?;
        Ok(true)
    }
    async fn recover_privacy_token(&self, from: &str) -> Result<(), Error> {
        let repo = crate::privacy::Repository::with_mappings(
            self.inner.store.clone(),
            self.inner.decryptor.direct.lid_mapping().clone(),
        );
        let to_lid = self.server_props()?.lid_trusted_token_issue_to_lid;
        let lookup = repo.clone();
        let jid = from.to_owned();
        let (storage, issuance) = tokio::task::spawn_blocking(move || {
            Ok::<_, Error>((
                lookup.storage_jid(&jid)?,
                lookup.issuance_jid(&jid, to_lid)?,
            ))
        })
        .await
        .map_err(|_| Error::Task)??;
        let result = self.issue_privacy_tokens(&[issuance], None).await?;
        tokio::task::spawn_blocking(move || repo.store_from_iq(&result, &storage))
            .await
            .map_err(|_| Error::Task)??;
        Ok(())
    }
}
