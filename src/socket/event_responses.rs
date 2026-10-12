use super::{Error, Event, Socket};
use crate::{
    auth::Credentials,
    jid,
    messages::{self, KeyMetadata},
    proto, relay,
};
impl Socket {
    pub(super) async fn process_event_response(
        &self,
        message: &proto::WebMessageInfo,
        metadata: Option<&KeyMetadata>,
        credentials: &Credentials,
    ) -> Result<(), Error> {
        let Some(content) = relay::normalize(message.message.as_ref()) else {
            return Ok(());
        };
        // Match processMessage's protocol/reaction branch precedence.
        if content.protocol_message.is_some() || content.reaction_message.is_some() {
            return Ok(());
        }
        let Some(encrypted) = content.enc_event_response_message.as_ref() else {
            return Ok(());
        };
        let Some(creation_key) = encrypted.event_creation_message_key.as_ref() else {
            // A typed lookup cannot accept undefined; retain the source default
            // lookup's no-response behavior and continue ordinary chat effects.
            tracing::warn!("event response missing creation key");
            return Ok(());
        };
        // Upstream uses the configured lookup, independent of the retry cache.
        let Some(original) = self.inner.get_message.get(creation_key.clone()).await? else {
            tracing::warn!(?creation_key, "event creation message not found");
            return Ok(());
        };
        let result = async {
            let me = jid::normalized_user(credentials.me_id()?);
            let creator = creation_key
                .participant
                .as_deref()
                .filter(|s| !s.is_empty())
                .or(creation_key.remote_jid.as_deref())
                .unwrap_or("");
            let pn = if jid::Jid::parse(creator).is_some_and(|j| j.is_lid_user()) {
                let repository = self.inner.decryptor.direct.clone();
                let creator = creator.to_owned();
                tokio::task::spawn_blocking(move || {
                    repository.lid_mapping().get_pn_for_lid(&creator)
                })
                .await
                .map_err(|_| Error::Task)??
                .unwrap_or_default()
            } else {
                creator.into()
            };
            let creator_jid = jid::normalized_user(&pn);
            let responder = messages::key_author(message.key.as_ref(), metadata, &me);
            let Some(secret) = original
                .message_context_info
                .as_ref()
                .and_then(|c| c.message_secret.as_deref())
            else {
                tracing::warn!(?creation_key, "event response missing messageSecret");
                return Ok(());
            };
            let key = message
                .key
                .as_ref()
                .ok_or(messages::Error::Envelope("missing event response key"))?;
            let ctx = messages::EventContext {
                event_creator_jid: &creator_jid,
                event_msg_id: creation_key
                    .id
                    .as_deref()
                    .ok_or(messages::Error::Envelope("missing event creation id"))?,
                event_enc_key: secret,
                responder_jid: &responder,
            };
            let value = proto::message::PollEncValue {
                enc_payload: encrypted.enc_payload.clone(),
                enc_iv: encrypted.enc_iv.clone(),
            };
            let response = messages::decrypt_event_response(&value, &ctx)?;
            let update = messages::EventResponseUpdate {
                key: creation_key.clone(),
                event_response_message_key: key.clone(),
                sender_timestamp_ms: response.timestamp_ms,
                response,
            };
            let _ = self
                .inner
                .events
                .send(Event::EventResponsesUpdated(vec![update]));
            Ok::<_, Error>(())
        }
        .await;
        if let Err(error) = result {
            tracing::warn!(?error, ?creation_key, "failed to decrypt event response");
        }
        Ok(())
    }
}
