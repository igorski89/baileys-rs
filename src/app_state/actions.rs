use super::{ChatMutation, Error};
use crate::{
    auth::{AccountSettings, Contact},
    messages::own::ChatUpdate,
    proto,
};
use proto::sync_action_value as action;

#[derive(Debug, Clone, PartialEq)]
pub struct ChatCondition {
    pub id: String,
    pub message_range: Option<action::SyncActionMessageRange>,
}
impl ChatCondition {
    /// None is the source's undefined result: the chat has not arrived yet.
    /// History chats take precedence over chat upserts, including absent timestamps.
    pub fn evaluate(
        &self,
        history_chat: Option<Option<i64>>,
        chat_upsert: Option<Option<i64>>,
    ) -> Option<bool> {
        let timestamp = history_chat.or(chat_upsert)?;
        Some(self.message_range.as_ref().is_none_or(|range| {
            // An explicitly encoded zero is a truthy protobuf Long in JS.
            let last = range
                .last_message_timestamp
                .or(range.last_system_message_timestamp)
                .unwrap_or(0) as f64;
            last >= timestamp.unwrap_or(0) as f64
        }))
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct StarUpdate {
    pub key: proto::MessageKey,
    pub starred: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub struct LabelEdit {
    pub id: Option<String>,
    pub name: Option<String>,
    pub color: Option<i32>,
    pub deleted: Option<bool>,
    pub predefined_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct LabelAssociation {
    pub add: bool,
    pub is_chat: bool,
    pub chat_id: Option<String>,
    pub label_id: Option<String>,
    pub message_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ChatLock {
    pub id: Option<String>,
    pub locked: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub enum SettingUpdate {
    Locale(String),
    TimeFormat(action::TimeFormatAction),
    RelayAllCalls(action::PrivacySettingRelayAllCalls),
    StatusPrivacy(action::StatusPrivacyAction),
    DisableLinkPreviews(action::PrivacySettingDisableLinkPreviewsAction),
    NotificationActivity(i32),
    ChannelsPersonalisedRecommendation(
        action::PrivacySettingChannelsPersonalisedRecommendationAction,
    ),
}
#[derive(Debug, Clone, PartialEq)]
pub enum ActionEvent {
    Chat(ChatUpdate),
    DeleteMessages(Vec<proto::MessageKey>),
    Contacts(Vec<Contact>),
    PushName(String),
    UnarchiveChats(bool),
    Stars(Vec<StarUpdate>),
    DeleteChats(Vec<Option<String>>),
    LabelEdit(LabelEdit),
    LabelAssociation(LabelAssociation),
    Setting(SettingUpdate),
    LidMapping { lid: Option<String>, pn: String },
    Lock(ChatLock),
}
pub struct ActionContext<'a> {
    pub me: Option<&'a Contact>,
    pub initial_sync: bool,
    pub account_settings: Option<&'a mut AccountSettings>,
}
fn nonempty(s: &Option<String>) -> Option<String> {
    s.as_ref().filter(|s| !s.is_empty()).cloned()
}
/// Apply branch precedence from processSyncAction. Native indices must be arrays
/// of strings/null; chat/contact operations require a chat ID.
pub fn process_action(
    mutation: &ChatMutation,
    context: &mut ActionContext<'_>,
) -> Result<Vec<ActionEvent>, Error> {
    let index = mutation
        .index
        .as_array()
        .ok_or(Error::Input("sync action index must be an array"))?;
    let values: Vec<Option<String>> = index
        .iter()
        .map(|v| match v {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(s) => Ok(Some(s.clone())),
            _ => Err(Error::Input("sync action index must contain strings")),
        })
        .collect::<Result<_, _>>()?;
    let at = |i: usize| values.get(i).cloned().flatten();
    let kind = at(0);
    let id = at(1);
    let msg_id = at(2);
    let from_me = at(3).as_deref() == Some("1");
    let required_id = || id.clone().ok_or(Error::Input("sync action chat id"));
    let key = || proto::MessageKey {
        remote_jid: id.clone(),
        id: msg_id.clone(),
        from_me: Some(from_me),
        ..Default::default()
    };
    let a = mutation.sync_action.value.as_ref();
    let condition = |range: Option<action::SyncActionMessageRange>| {
        context.initial_sync.then(|| ChatCondition {
            id: id.clone().unwrap_or_default(),
            message_range: range,
        })
    };
    if let Some(mute) = a.and_then(|a| a.mute_action) {
        return Ok(vec![ActionEvent::Chat(ChatUpdate {
            id: required_id()?,
            mute_end_time: Some(
                mute.muted
                    .unwrap_or(false)
                    .then_some(mute.mute_end_timestamp.unwrap_or(0) as f64),
            ),
            conditional: condition(None),
            ..Default::default()
        })]);
    }
    if a.is_some_and(|a| a.archive_chat_action.is_some())
        || matches!(kind.as_deref(), Some("archive" | "unarchive"))
    {
        let archive = a.and_then(|a| a.archive_chat_action.as_ref());
        let archived = archive.and_then(|a| a.archived).or_else(|| {
            archive
                .is_none()
                .then_some(kind.as_deref() == Some("archive"))
        });
        let range = context
            .account_settings
            .as_ref()
            .filter(|s| s.unarchive_chats)
            .and_then(|_| archive.and_then(|a| a.message_range.clone()));
        return Ok(vec![ActionEvent::Chat(ChatUpdate {
            id: required_id()?,
            archived,
            clear_archived: archive.is_some_and(|a| a.archived.is_none()),
            conditional: condition(range),
            ..Default::default()
        })]);
    }
    if let Some(read) = a.and_then(|a| a.mark_chat_as_read_action.as_ref()) {
        let clear = context.initial_sync && read.read.unwrap_or(false);
        return Ok(vec![ActionEvent::Chat(ChatUpdate {
            id: required_id()?,
            unread_count: (!clear).then_some(if read.read.unwrap_or(false) { 0 } else { -1 }),
            clear_unread_count: clear,
            conditional: condition(read.message_range.clone()),
            ..Default::default()
        })]);
    }
    if a.is_some_and(|a| a.delete_message_for_me_action.is_some())
        || kind.as_deref() == Some("deleteMessageForMe")
    {
        return Ok(vec![ActionEvent::DeleteMessages(vec![key()])]);
    }
    if let Some(contact) = a.and_then(|a| a.contact_action.as_ref()) {
        let Some(id) = id.filter(|s| !s.is_empty()) else {
            return Ok(vec![]);
        };
        // Source classification uses suffix checks rather than strict JID parsing.
        let pn = id.ends_with("@s.whatsapp.net");
        let lid = nonempty(&contact.lid_jid);
        let mut events = vec![ActionEvent::Contacts(vec![Contact {
            id: id.clone(),
            name: nonempty(&contact.full_name)
                .or_else(|| nonempty(&contact.first_name))
                .or_else(|| nonempty(&contact.username)),
            username: nonempty(&contact.username),
            lid: lid.clone(),
            phone_number: if pn {
                Some(id.clone())
            } else {
                nonempty(&contact.pn_jid)
            },
            ..Default::default()
        }])];
        if let Some(lid) = lid.filter(|lid| lid.ends_with("@lid") && pn) {
            events.push(ActionEvent::LidMapping {
                lid: Some(lid),
                pn: id,
            });
        }
        return Ok(events);
    }
    if let Some(name) = a
        .and_then(|a| a.push_name_setting.as_ref())
        .and_then(|s| nonempty(&s.name))
    {
        return Ok(
            if context.me.and_then(|me| me.name.as_deref()) != Some(&name) {
                vec![ActionEvent::PushName(name)]
            } else {
                vec![]
            },
        );
    } else if a.is_some_and(|a| a.push_name_setting.is_some()) {
        return Ok(vec![]);
    }
    if let Some(pin) = a.and_then(|a| a.pin_action) {
        return Ok(vec![ActionEvent::Chat(ChatUpdate {
            id: required_id()?,
            pinned: Some(
                pin.pinned
                    .unwrap_or(false)
                    .then_some(a.and_then(|a| a.timestamp).unwrap_or(0) as f64),
            ),
            conditional: condition(None),
            ..Default::default()
        })]);
    }
    if let Some(setting) = a.and_then(|a| a.unarchive_chats_setting) {
        let value = setting.unarchive_chats.unwrap_or(false);
        tracing::info!(
            "archive setting updated => '{}'",
            setting
                .unarchive_chats
                .map_or_else(|| "undefined".to_owned(), |v| v.to_string())
        );
        if let Some(settings) = context.account_settings.as_mut() {
            settings.unarchive_chats = value;
        }
        return Ok(vec![ActionEvent::UnarchiveChats(value)]);
    }
    if a.is_some_and(|a| a.star_action.is_some()) || kind.as_deref() == Some("star") {
        let starred = a
            .and_then(|a| a.star_action)
            .and_then(|s| s.starred)
            .unwrap_or_else(|| values.last().and_then(|s| s.as_deref()) == Some("1"));
        return Ok(vec![ActionEvent::Stars(vec![StarUpdate {
            key: key(),
            starred,
        }])]);
    }
    if a.is_some_and(|a| a.delete_chat_action.is_some()) || kind.as_deref() == Some("deleteChat") {
        return Ok(if context.initial_sync {
            vec![]
        } else {
            vec![ActionEvent::DeleteChats(vec![id])]
        });
    }
    if let Some(label) = a.and_then(|a| a.label_edit_action.as_ref()) {
        return Ok(vec![ActionEvent::LabelEdit(LabelEdit {
            id,
            name: label.name.clone(),
            color: label.color,
            deleted: label.deleted,
            predefined_id: label
                .predefined_id
                .filter(|v| *v != 0)
                .map(|v| v.to_string()),
        })]);
    }
    if let Some(label) = a.and_then(|a| a.label_association_action) {
        return Ok(vec![ActionEvent::LabelAssociation(LabelAssociation {
            add: label.labeled.unwrap_or(false),
            is_chat: kind.as_deref() == Some("label_jid"),
            label_id: id,
            chat_id: at(2),
            message_id: if kind.as_deref() == Some("label_jid") {
                None
            } else {
                at(3)
            },
        })]);
    }
    if let Some(locale) = a
        .and_then(|a| a.locale_setting.as_ref())
        .and_then(|s| nonempty(&s.locale))
    {
        return Ok(vec![ActionEvent::Setting(SettingUpdate::Locale(locale))]);
    }
    if let Some(setting) = a.and_then(|a| a.time_format_action) {
        return Ok(vec![ActionEvent::Setting(SettingUpdate::TimeFormat(
            setting,
        ))]);
    }
    if let Some(mapping) = a.and_then(|a| a.pn_for_lid_chat_action.as_ref()) {
        return Ok(nonempty(&mapping.pn_jid)
            .map(|pn| ActionEvent::LidMapping { lid: id, pn })
            .into_iter()
            .collect());
    }
    if let Some(setting) = a.and_then(|a| a.privacy_setting_relay_all_calls) {
        return Ok(vec![ActionEvent::Setting(SettingUpdate::RelayAllCalls(
            setting,
        ))]);
    }
    if let Some(setting) = a.and_then(|a| a.status_privacy.as_ref()) {
        return Ok(vec![ActionEvent::Setting(SettingUpdate::StatusPrivacy(
            setting.clone(),
        ))]);
    }
    if let Some(lock) = a.and_then(|a| a.lock_chat_action) {
        return Ok(vec![ActionEvent::Lock(ChatLock {
            id,
            locked: lock.locked.unwrap_or(false),
        })]);
    }
    if let Some(setting) = a.and_then(|a| a.privacy_setting_disable_link_previews_action) {
        return Ok(vec![ActionEvent::Setting(
            SettingUpdate::DisableLinkPreviews(setting),
        )]);
    }
    if let Some(setting) = a
        .and_then(|a| a.notification_activity_setting_action)
        .and_then(|a| a.notification_activity_setting)
        .filter(|v| *v != 0)
    {
        return Ok(vec![ActionEvent::Setting(
            SettingUpdate::NotificationActivity(setting),
        )]);
    }
    if let Some(contact) = a.and_then(|a| a.lid_contact_action.as_ref()) {
        let id = required_id()?;
        return Ok(vec![ActionEvent::Contacts(vec![Contact {
            id: id.clone(),
            lid: Some(id),
            name: nonempty(&contact.full_name)
                .or_else(|| nonempty(&contact.first_name))
                .or_else(|| nonempty(&contact.username)),
            username: nonempty(&contact.username),
            ..Default::default()
        }])]);
    }
    if let Some(setting) =
        a.and_then(|a| a.privacy_setting_channels_personalised_recommendation_action)
    {
        return Ok(vec![ActionEvent::Setting(
            SettingUpdate::ChannelsPersonalisedRecommendation(setting),
        )]);
    }
    Ok(vec![])
}
