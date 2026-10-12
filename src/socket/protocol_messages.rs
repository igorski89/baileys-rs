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
            Some(Type::HistorySyncNotification) => {
                let Some(notification) = protocol.history_sync_notification.clone() else {
                    return Ok(());
                };
                if !self.inner.history_policy.should_process(&notification) {
                    return Ok(());
                }
                let demand = notification.sync_type == Some(6);
                let latest = if demand {
                    None
                } else {
                    let (reply, response) = oneshot::channel();
                    self.inner
                        .commands
                        .send(Command::RecordHistory {
                            key: message.key.clone(),
                            timestamp: message.message_timestamp,
                            reply,
                        })
                        .await
                        .map_err(|_| Error::Closed(428))?;
                    Some(response.await.map_err(|_| Error::Closed(428))??)
                };
                let downloader = self
                    .inner
                    .history_downloader
                    .get_or_try_init(|| async {
                        let client = self
                            .inner
                            .media_http
                            .get_or_try_init(|| async { crate::media::HttpClient::new() })
                            .await?
                            .clone();
                        Ok::<_, Error>(crate::history::Downloader::from_http(
                            client,
                            Default::default(),
                        ))
                    })
                    .await?;
                let chunk_order = notification.chunk_order;
                let peer_data_request_session_id =
                    notification.peer_data_request_session_id.clone();
                let data = downloader.download_and_process(notification).await?;
                let repository = self.inner.decryptor.direct.clone();
                let mappings = data.lid_pn_mappings.clone();
                let chats = data.chats.clone();
                let store = self.inner.store.clone();
                tokio::task::spawn_blocking(move || {
                    if !mappings.is_empty()
                        && let Err(error) = repository.lid_mapping().store_mappings(&mappings)
                    {
                        tracing::warn!(?error, "failed to store history PN-LID mappings");
                    }
                    let privacy = crate::privacy::Repository::with_mappings(
                        store,
                        repository.lid_mapping().clone(),
                    );
                    privacy.store_from_history(&chats)?;
                    Ok::<_, crate::signal::Error>(())
                })
                .await
                .map_err(|_| Error::Task)??;
                let _ = self
                    .inner
                    .events
                    .send(Event::MessagingHistorySet(crate::history::Set {
                        data,
                        is_latest: latest,
                        chunk_order,
                        peer_data_request_session_id,
                    }));
            }
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
                        metadata_indices: vec![],
                        runtime_fields: vec![],
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
