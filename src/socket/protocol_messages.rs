use super::{Command, Error, Event, Socket};
use crate::{
    messages::{self, own},
    proto, relay,
};
use prost::Message as _;
use tokio::sync::oneshot;
impl Socket {
    pub(super) async fn process_phone_protocol(
        &self,
        message: &proto::WebMessageInfo,
    ) -> Result<(), Error> {
        let Some(protocol) =
            relay::normalize(message.message.as_ref()).and_then(|m| m.protocol_message.as_deref())
        else {
            return Ok(());
        };
        use proto::message::protocol_message::Type;
        let kind = protocol.r#type.and_then(|t| Type::try_from(t).ok());
        // These operations are only accepted from another device on our account.
        if message.key.as_ref().and_then(|k| k.from_me) != Some(true) {
            return Ok(());
        }
        match kind {
            Some(Type::AppStateSyncKeyShare) => {
                let keys = protocol
                    .app_state_sync_key_share
                    .as_ref()
                    .ok_or(messages::Error::Envelope("missing app-state key share"))?
                    .keys
                    .clone();
                if keys.is_empty() {
                    return Ok(());
                }
                let (reply, response) = oneshot::channel();
                self.inner
                    .commands
                    .send(Command::AppStateKeys { keys, reply })
                    .await
                    .map_err(|_| Error::Closed(428))?;
                response.await.map_err(|_| Error::Closed(428))??;
                self.schedule_app_state_retry().await;
            }
            Some(Type::PeerDataOperationRequestResponseMessage) => {
                let Some(response) = protocol
                    .peer_data_operation_request_response_message
                    .as_ref()
                else {
                    return Ok(());
                };
                for result in &response.peer_data_operation_result {
                    let Some(bytes) = result
                        .placeholder_message_resend_response
                        .as_ref()
                        .and_then(|r| r.web_message_info_bytes.as_deref())
                    else {
                        continue;
                    };
                    let decoded = match proto::WebMessageInfo::decode(bytes) {
                        Ok(message) => message,
                        Err(error) => {
                            tracing::warn!(?error,request_id=?response.stanza_id,"failed to decode placeholder response");
                            continue;
                        }
                    };
                    let cached = if let Some(id) = decoded
                        .key
                        .as_ref()
                        .and_then(|k| k.id.as_deref())
                        .filter(|s| !s.is_empty())
                    {
                        self.resolve_placeholder_resend(id)?
                    } else {
                        None
                    };
                    let (message, metadata) =
                        messages::protocol::restore_placeholder(decoded, cached);
                    let _ = self.inner.events.send(Event::MessagesUpserted(own::Upsert {
                        messages: vec![message],
                        kind: own::UpsertKind::Notify,
                        request_id: response.stanza_id.clone(),
                        metadata: metadata.into_iter().collect(),
                    }));
                }
            }
            Some(Type::LidMigrationMappingSync) => {
                let bytes = protocol
                    .lid_migration_mapping_sync_message
                    .as_ref()
                    .and_then(|p| p.encoded_mapping_payload.as_deref())
                    .ok_or(messages::Error::Envelope("missing LID sync payload"))?;
                let payload = proto::LidMigrationMappingSyncPayload::decode(bytes)?;
                let pairs = messages::protocol::lid_sync_mappings(&payload);
                let repository = self.inner.decryptor.direct.clone();
                tokio::task::spawn_blocking(move || {
                    repository.lid_mapping().store_mappings(&pairs)?;
                    for pair in pairs {
                        repository.migrate_session(&pair.pn, &pair.lid)?;
                    }
                    Ok::<_, Error>(())
                })
                .await
                .map_err(|_| Error::Task)??;
            }
            _ => {}
        }
        Ok(())
    }
}
