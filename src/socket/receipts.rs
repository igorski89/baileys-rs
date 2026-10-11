use super::{Error, Socket, load_credentials};
use crate::{
    binary::BinaryNode,
    privacy_settings::{self, Settings},
    proto,
    receipts::{self, Group, Type},
};
use std::time::{SystemTime, UNIX_EPOCH};
impl Socket {
    pub async fn send_receipt(
        &self,
        jid: &str,
        participant: Option<&str>,
        ids: &[String],
        kind: Option<Type>,
    ) -> Result<(), Error> {
        let group = Group {
            jid: Some(jid.into()),
            participant: participant.map(str::to_owned),
            message_ids: ids.iter().cloned().map(Some).collect(),
        };
        self.send_receipt_group(&group, kind).await
    }
    async fn send_receipt_group(&self, group: &Group, kind: Option<Type>) -> Result<(), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Config)?
            .as_secs();
        self.send_node(receipts::receipt(group, kind, now)?).await
    }
    pub async fn send_receipts(
        &self,
        keys: &[proto::MessageKey],
        kind: Option<Type>,
    ) -> Result<(), Error> {
        for group in receipts::aggregate(keys) {
            self.send_receipt_group(&group, kind).await?;
        }
        Ok(())
    }
    pub async fn read_messages(&self, keys: &[proto::MessageKey]) -> Result<(), Error> {
        let settings = self.fetch_privacy_settings(false).await?;
        let kind = if settings.get("readreceipts").and_then(Option::as_deref) == Some("all") {
            Type::Read
        } else {
            Type::ReadSelf
        };
        self.send_receipts(keys, Some(kind)).await
    }
    pub async fn send_message_ack(
        &self,
        node: &BinaryNode,
        error: Option<u32>,
    ) -> Result<(), Error> {
        let credentials = load_credentials(self.inner.store.clone()).await?;
        self.send_node(receipts::acknowledgement(
            node,
            error,
            Some(credentials.me_id()?),
        ))
        .await
    }
    pub async fn reject_call(&self, id: &str, from: &str) -> Result<(), Error> {
        let credentials = load_credentials(self.inner.store.clone()).await?;
        self.query(receipts::reject_call(credentials.me_id()?, id, from), None)
            .await?;
        Ok(())
    }
    /// Updates preserve the cached snapshot, as upstream does. Force a fetch to refresh it.
    pub async fn fetch_privacy_settings(&self, force: bool) -> Result<Settings, Error> {
        if !force
            && let Some(settings) = self
                .inner
                .privacy_settings
                .lock()
                .map_err(|_| Error::Task)?
                .clone()
        {
            return Ok(settings);
        }
        let response = self.query(privacy_settings::query(), None).await?;
        let settings = privacy_settings::extract(&response);
        *self
            .inner
            .privacy_settings
            .lock()
            .map_err(|_| Error::Task)? = Some(settings.clone());
        Ok(settings)
    }
    pub async fn update_privacy(&self, name: &str, value: &str) -> Result<(), Error> {
        self.query(privacy_settings::update(name, value), None)
            .await?;
        Ok(())
    }
    pub async fn update_last_seen_privacy(
        &self,
        value: privacy_settings::Value,
    ) -> Result<(), Error> {
        self.update_privacy("last", value.as_str()).await
    }
    pub async fn update_profile_picture_privacy(
        &self,
        value: privacy_settings::Value,
    ) -> Result<(), Error> {
        self.update_privacy("profile", value.as_str()).await
    }
    pub async fn update_status_privacy(&self, value: privacy_settings::Value) -> Result<(), Error> {
        self.update_privacy("status", value.as_str()).await
    }
    pub async fn update_online_privacy(
        &self,
        value: privacy_settings::Online,
    ) -> Result<(), Error> {
        self.update_privacy("online", value.as_str()).await
    }
    pub async fn update_read_receipts_privacy(
        &self,
        value: privacy_settings::ReadReceipts,
    ) -> Result<(), Error> {
        self.update_privacy("readreceipts", value.as_str()).await
    }
    pub async fn update_groups_add_privacy(
        &self,
        value: privacy_settings::GroupAdd,
    ) -> Result<(), Error> {
        self.update_privacy("groupadd", value.as_str()).await
    }
    pub async fn update_call_privacy(&self, value: privacy_settings::Call) -> Result<(), Error> {
        self.update_privacy("calladd", value.as_str()).await
    }
    pub async fn update_messages_privacy(
        &self,
        value: privacy_settings::Messages,
    ) -> Result<(), Error> {
        self.update_privacy("messages", value.as_str()).await
    }
}
