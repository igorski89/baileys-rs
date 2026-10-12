use super::{Error, Socket};
use crate::{chats, usync};
use std::time::Duration;

impl Socket {
    fn next_tag(&self) -> Result<String, Error> {
        let sequence = self
            .inner
            .sequence
            .try_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |n| n.checked_add(1),
            )
            .map_err(|_| Error::TagExhausted)?;
        Ok(format!("{}{sequence}", self.inner.prefix))
    }
    /// Set the account's default disappearing-message duration in seconds; zero disables it.
    pub async fn update_default_disappearing_mode(&self, duration: u32) -> Result<(), Error> {
        self.query(chats::default_disappearing_mode_query(duration), None)
            .await?;
        Ok(())
    }
    pub async fn get_bot_list_v2(&self) -> Result<Vec<chats::BotInfo>, Error> {
        Ok(chats::extract_bot_list(
            &self.query(chats::bot_list_query(), None).await?,
        ))
    }
    async fn usync_lookup(
        &self,
        protocol: usync::Protocol,
        jids: &[String],
    ) -> Result<Option<Vec<usync::ResultUser>>, Error> {
        let mut query = usync::Query::default().with_protocol(protocol);
        for jid in jids {
            query = query.with_user(usync::User::with_id(jid));
        }
        Ok(self.execute_usync(&query).await?.map(|result| result.list))
    }
    /// Fetch the "about" status of the given users.
    pub async fn fetch_status(
        &self,
        jids: &[String],
    ) -> Result<Option<Vec<usync::ResultUser>>, Error> {
        self.usync_lookup(usync::Protocol::Status, jids).await
    }
    pub async fn fetch_disappearing_duration(
        &self,
        jids: &[String],
    ) -> Result<Option<Vec<usync::ResultUser>>, Error> {
        self.usync_lookup(usync::Protocol::DisappearingMode, jids)
            .await
    }
    async fn own_id(&self) -> Result<String, Error> {
        Ok(super::load_credentials(self.inner.store.clone())
            .await?
            .me_id()?
            .to_owned())
    }
    /// Update the profile picture of the account or a group.
    pub async fn update_profile_picture(
        &self,
        jid: &str,
        content: &crate::media::Input,
        dimensions: Option<(u32, u32)>,
    ) -> Result<(), Error> {
        let target = chats::profile_picture_target(jid, &self.own_id().await?)?;
        let http = self
            .inner
            .media_http
            .get_or_try_init(|| async { crate::media::HttpClient::new() })
            .await?;
        let image =
            crate::media::generate_profile_picture(content, dimensions, http, &Default::default())
                .await?;
        self.query(
            chats::profile_picture_update_query(target.as_deref(), Some(image)),
            None,
        )
        .await?;
        Ok(())
    }
    /// Remove the profile picture of the account or a group.
    pub async fn remove_profile_picture(&self, jid: &str) -> Result<(), Error> {
        let target = chats::profile_picture_target(jid, &self.own_id().await?)?;
        self.query(
            chats::profile_picture_update_query(target.as_deref(), None),
            None,
        )
        .await?;
        Ok(())
    }
    /// Update the account's "about" status.
    pub async fn update_profile_status(&self, status: &str) -> Result<(), Error> {
        self.query(chats::profile_status_query(status), None)
            .await?;
        Ok(())
    }
    pub async fn update_profile_name(&self, name: &str) -> Result<(), Error> {
        self.chat_modify(
            crate::app_state::ChatModification::PushNameSetting(name.into()),
            "",
        )
        .await
    }
    /// Block or unblock a user. The stored PN/LID mapping supplies the other identity.
    pub async fn update_block_status(&self, jid: &str, block: bool) -> Result<(), Error> {
        let lookup = chats::block_lookup(jid)?;
        let mapping = self.inner.decryptor.direct.lid_mapping().clone();
        let wanted = lookup.clone();
        let mapped = tokio::task::spawn_blocking(move || match wanted {
            // The phone number is only looked up when blocking.
            chats::BlockLookup::PnForLid(lid) if block => mapping.get_pn_for_lid(&lid),
            chats::BlockLookup::PnForLid(_) => Ok(None),
            chats::BlockLookup::LidForPn(pn) => mapping.get_lid_for_pn(&pn),
        })
        .await
        .map_err(|_| Error::Task)??;
        self.query(
            chats::block_status_query(jid, block, &lookup, mapped.as_deref())?,
            None,
        )
        .await?;
        Ok(())
    }
    pub async fn get_business_profile(
        &self,
        jid: &str,
    ) -> Result<Option<chats::BusinessProfile>, Error> {
        Ok(chats::extract_business_profile(
            &self.query(chats::business_profile_query(jid), None).await?,
        ))
    }
    /// Acknowledge an `account_sync` or `groups` dirty bit, optionally from a timestamp.
    pub async fn clean_dirty_bits(
        &self,
        kind: &str,
        from_timestamp: Option<u64>,
    ) -> Result<(), Error> {
        let timestamp = from_timestamp.map(|t| t.to_string());
        self.send_node(chats::clean_dirty_bits_node(
            kind,
            timestamp.as_deref(),
            &self.next_tag()?,
        ))
        .await
    }
    /// Fetch a profile picture URL. A stored privacy token is attached for other users
    /// when the server property enables it.
    pub async fn profile_picture_url(
        &self,
        jid: &str,
        kind: chats::PictureType,
        timeout: Option<Duration>,
    ) -> Result<Option<String>, Error> {
        let credentials = super::load_credentials(self.inner.store.clone()).await?;
        let own = credentials.me.as_ref();
        let uses_token = own
            .is_none_or(|me| chats::profile_picture_uses_token(jid, &me.id, me.lid.as_deref()))
            && {
                let normalized = crate::jid::normalized_user(jid);
                normalized.ends_with("@s.whatsapp.net") || normalized.ends_with("@lid")
            };
        let token = if self.server_props()?.profile_pic_privacy_token && uses_token {
            let repo = crate::privacy::Repository::with_mappings(
                self.inner.store.clone(),
                self.inner.decryptor.direct.lid_mapping().clone(),
            );
            let normalized = crate::jid::normalized_user(jid);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Error::Config)?
                .as_secs();
            tokio::task::spawn_blocking(move || repo.build_content(&normalized, vec![], now))
                .await
                .map_err(|_| Error::Task)?
        } else {
            vec![]
        };
        Ok(chats::extract_profile_picture_url(
            &self
                .query(chats::profile_picture_url_query(jid, kind, token), timeout)
                .await?,
        ))
    }
    /// Create an audio or video call link, optionally scheduled, returning its token.
    pub async fn create_call_link(
        &self,
        video: bool,
        start_time: Option<u64>,
        timeout: Option<Duration>,
    ) -> Result<Option<String>, Error> {
        let node = chats::call_link_query(video, start_time, &self.next_tag()?);
        Ok(chats::extract_call_link_token(
            &self.query(node, timeout).await?,
        ))
    }
    /// Refresh participating groups and acknowledge the `groups` dirty bit.
    pub(super) async fn handle_groups_dirty(&self) -> Result<(), Error> {
        self.group_fetch_all_participating().await?;
        self.clean_dirty_bits("groups", None).await
    }
}
