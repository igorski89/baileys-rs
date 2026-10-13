use super::{Error, Event, Socket};
use crate::{
    binary::BinaryNode,
    messages,
    newsletters::{self, Effect, Key, Metadata, ReachoutTimelock, Update, path, query_id},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

impl Socket {
    /// Run a persisted MEX query and return the value under `data_path`.
    pub async fn execute_mex_query(
        &self,
        variables: Value,
        query_id: &str,
        data_path: &str,
    ) -> Result<Value, Error> {
        let result = self
            .query(newsletters::query(&variables, query_id), None)
            .await?;
        Ok(newsletters::response(&result, data_path)?)
    }
    pub async fn newsletter_create(
        &self,
        name: &str,
        description: Option<&str>,
    ) -> Result<Metadata, Error> {
        let response = self
            .execute_mex_query(
                newsletters::create_variables(name, description),
                query_id::CREATE,
                path::CREATE,
            )
            .await?;
        Ok(newsletters::parse_create_response(&response)?)
    }
    pub async fn newsletter_update(&self, jid: &str, updates: &Update) -> Result<Value, Error> {
        self.execute_mex_query(
            updates.variables(jid),
            query_id::UPDATE_METADATA,
            path::UPDATE,
        )
        .await
    }
    pub async fn newsletter_subscribers(&self, jid: &str) -> Result<Value, Error> {
        self.newsletter_query(jid, query_id::SUBSCRIBERS, path::SUBSCRIBERS)
            .await
    }
    /// Metadata by invite code or JID; `None` when the server returns no newsletter.
    pub async fn newsletter_metadata(&self, kind: Key, key: &str) -> Result<Option<Value>, Error> {
        let result = self
            .execute_mex_query(kind.variables(key), query_id::METADATA, path::METADATA)
            .await?;
        Ok(newsletters::parse_metadata(&result))
    }
    async fn newsletter_query(
        &self,
        jid: &str,
        query_id: &str,
        data_path: &str,
    ) -> Result<Value, Error> {
        self.execute_mex_query(json!({"newsletter_id": jid}), query_id, data_path)
            .await
    }
    pub async fn newsletter_follow(&self, jid: &str) -> Result<Value, Error> {
        self.newsletter_query(jid, query_id::FOLLOW, path::JOIN)
            .await
    }
    pub async fn newsletter_unfollow(&self, jid: &str) -> Result<Value, Error> {
        self.newsletter_query(jid, query_id::UNFOLLOW, path::LEAVE)
            .await
    }
    pub async fn newsletter_mute(&self, jid: &str) -> Result<Value, Error> {
        self.newsletter_query(jid, query_id::MUTE, path::MUTE).await
    }
    pub async fn newsletter_unmute(&self, jid: &str) -> Result<Value, Error> {
        self.newsletter_query(jid, query_id::UNMUTE, path::UNMUTE)
            .await
    }
    pub async fn newsletter_update_name(&self, jid: &str, name: &str) -> Result<Value, Error> {
        let updates = Update {
            name: Some(name.into()),
            ..Default::default()
        };
        self.newsletter_update(jid, &updates).await
    }
    pub async fn newsletter_update_description(
        &self,
        jid: &str,
        description: &str,
    ) -> Result<Value, Error> {
        let updates = Update {
            description: Some(description.into()),
            ..Default::default()
        };
        self.newsletter_update(jid, &updates).await
    }
    pub async fn newsletter_update_picture(
        &self,
        jid: &str,
        content: &crate::media::Input,
    ) -> Result<Value, Error> {
        let http = self
            .inner
            .media_http
            .get_or_try_init(|| async { crate::media::HttpClient::new() })
            .await?;
        let image =
            crate::media::generate_profile_picture(content, None, http, &Default::default())
                .await?;
        let updates = Update {
            picture: Some(STANDARD.encode(image)),
            ..Default::default()
        };
        self.newsletter_update(jid, &updates).await
    }
    pub async fn newsletter_remove_picture(&self, jid: &str) -> Result<Value, Error> {
        let updates = Update {
            picture: Some(String::new()),
            ..Default::default()
        };
        self.newsletter_update(jid, &updates).await
    }
    /// React to a newsletter message; no reaction removes the existing one.
    pub async fn newsletter_react_message(
        &self,
        jid: &str,
        server_id: &str,
        reaction: Option<&str>,
    ) -> Result<(), Error> {
        self.query(newsletters::react_message(jid, server_id, reaction), None)
            .await?;
        Ok(())
    }
    pub async fn newsletter_fetch_messages(
        &self,
        jid: &str,
        count: u64,
        since: Option<u64>,
        after: Option<u64>,
    ) -> Result<BinaryNode, Error> {
        self.query(newsletters::fetch_messages(jid, count, since, after), None)
            .await
    }
    /// Subscribe to live updates; returns the subscription duration when granted.
    pub async fn subscribe_newsletter_updates(&self, jid: &str) -> Result<Option<String>, Error> {
        let result = self
            .query(newsletters::subscribe_updates(jid), None)
            .await?;
        Ok(newsletters::live_updates_duration(&result))
    }
    /// The `admin_count` of the server response, absent when it has none.
    pub async fn newsletter_admin_count(&self, jid: &str) -> Result<Option<Value>, Error> {
        let response = self
            .newsletter_query(jid, query_id::ADMIN_COUNT, path::ADMIN_COUNT)
            .await?;
        if response.is_null() {
            return Err(newsletters::Error::Malformed.into());
        }
        Ok(response
            .as_object()
            .and_then(|response| response.get("admin_count"))
            .cloned())
    }
    pub async fn newsletter_change_owner(
        &self,
        jid: &str,
        new_owner_jid: &str,
    ) -> Result<(), Error> {
        self.execute_mex_query(
            json!({"newsletter_id": jid, "user_id": new_owner_jid}),
            query_id::CHANGE_OWNER,
            path::CHANGE_OWNER,
        )
        .await?;
        Ok(())
    }
    pub async fn newsletter_demote(&self, jid: &str, user_jid: &str) -> Result<(), Error> {
        self.execute_mex_query(
            json!({"newsletter_id": jid, "user_id": user_jid}),
            query_id::DEMOTE,
            path::DEMOTE,
        )
        .await?;
        Ok(())
    }
    pub async fn newsletter_delete(&self, jid: &str) -> Result<(), Error> {
        self.newsletter_query(jid, query_id::DELETE, path::DELETE)
            .await?;
        Ok(())
    }
    /// Fetch and publish the account's outgoing-message restriction state.
    pub async fn fetch_account_reachout_timelock(&self) -> Result<ReachoutTimelock, Error> {
        let result = self
            .execute_mex_query(
                json!({}),
                query_id::REACHOUT_TIMELOCK,
                path::REACHOUT_TIMELOCK,
            )
            .await?;
        let state = newsletters::parse_reachout_timelock(&result);
        let _ = self
            .inner
            .events
            .send(Event::ReachoutTimelockUpdated(state.clone()));
        Ok(state)
    }
    /// Fetch the account's new-chat message quota and usage.
    pub async fn fetch_new_chat_message_cap(&self) -> Result<Value, Error> {
        self.execute_mex_query(
            json!({"input": {"type": "INDIVIDUAL_NEW_CHAT_MSG"}}),
            query_id::MESSAGE_CAPPING_INFO,
            path::MESSAGE_CAPPING_INFO,
        )
        .await
    }
    pub(super) async fn apply_newsletter_effects(&self, effects: Vec<Effect>) -> Result<(), Error> {
        for effect in effects {
            let event = match effect {
                Effect::Reaction {
                    id,
                    server_id,
                    code,
                } => Event::NewsletterReaction {
                    id,
                    server_id,
                    code,
                },
                Effect::View {
                    id,
                    server_id,
                    count,
                } => Event::NewsletterView {
                    id,
                    server_id,
                    count,
                },
                Effect::Participant {
                    id,
                    author,
                    user,
                    action,
                    new_role,
                } => Event::NewsletterParticipantsUpdated {
                    id,
                    author,
                    user,
                    new_role,
                    action,
                },
                Effect::Settings { id, update } => Event::NewsletterSettingsUpdated { id, update },
                Effect::Timelock(state) => Event::ReachoutTimelockUpdated(state),
                Effect::MessageCapping(payload) => Event::MessageCappingUpdated(payload),
                Effect::LidMapping { lid, pn } => Event::LidMappingUpdated { lid: Some(lid), pn },
                Effect::Message(message) => {
                    let author = message
                        .key
                        .as_ref()
                        .and_then(|key| key.remote_jid.clone())
                        .unwrap_or_default();
                    let envelope = messages::Envelope {
                        message: *message,
                        key_metadata: Default::default(),
                        category: None,
                        retry_count: None,
                        sender: author.clone(),
                        author,
                    };
                    // Upstream logs a failed plaintext message without failing the notification.
                    if let Err(error) = self
                        .upsert_message(&envelope, messages::own::UpsertKind::Append)
                        .await
                    {
                        tracing::error!(?error, "failed to process plaintext newsletter message");
                    }
                    continue;
                }
                Effect::StoreMappings(pairs) => {
                    let mapping = self.inner.decryptor.direct.lid_mapping().clone();
                    tokio::task::spawn_blocking(move || {
                        let pairs: Vec<_> = pairs
                            .into_iter()
                            .map(|(lid, pn)| crate::signal::mapping::Mapping { lid, pn })
                            .collect();
                        mapping.store_mappings(&pairs)
                    })
                    .await
                    .map_err(|_| Error::Task)??;
                    continue;
                }
            };
            self.inner.events.send(event)?;
        }
        Ok(())
    }
}
