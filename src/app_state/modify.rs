//! Chat modifications expressed as outgoing app-state patches.
use super::{Error, PatchCreate};
use crate::{auth::MinimalMessage, jid, proto};
use proto::{
    SyncActionValue,
    sync_action_value::{self as action, SyncActionMessageRange},
    syncd_mutation::SyncdOperation,
};

/// The last messages of a chat, as a list to convert or a prepared range.
#[derive(Debug, Clone, PartialEq)]
pub enum LastMessages {
    List(Vec<MinimalMessage>),
    Range(SyncActionMessageRange),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarredMessage {
    pub id: String,
    pub from_me: Option<bool>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuickReply {
    pub shortcut: Option<String>,
    pub message: Option<String>,
    /// Identifies the reply; defaults to the current time in seconds.
    pub timestamp: Option<String>,
    pub deleted: Option<bool>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelBody {
    pub id: String,
    pub name: Option<String>,
    pub color: Option<i32>,
    pub predefined_id: Option<i32>,
    pub deleted: Option<bool>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ChatModification {
    Archive {
        archive: bool,
        last_messages: Option<LastMessages>,
    },
    PushNameSetting(String),
    Pin(bool),
    /// Mute until the given millisecond timestamp; None or zero removes the mute.
    Mute(Option<i64>),
    Clear {
        last_messages: Option<LastMessages>,
    },
    DeleteForMe {
        delete_media: bool,
        key: proto::MessageKey,
        timestamp: i64,
    },
    /// Only the first message is used, as upstream does.
    Star {
        messages: Vec<StarredMessage>,
        star: bool,
    },
    MarkRead {
        read: bool,
        last_messages: Option<LastMessages>,
    },
    Delete {
        last_messages: Option<LastMessages>,
    },
    /// None removes the contact.
    Contact(Option<action::ContactAction>),
    DisableLinkPreviews(action::PrivacySettingDisableLinkPreviewsAction),
    AddLabel(LabelBody),
    AddChatLabel {
        label_id: String,
    },
    RemoveChatLabel {
        label_id: String,
    },
    AddMessageLabel {
        message_id: String,
        label_id: String,
    },
    RemoveMessageLabel {
        message_id: String,
        label_id: String,
    },
    QuickReply(QuickReply),
}
fn range(last: Option<LastMessages>) -> Result<Option<SyncActionMessageRange>, Error> {
    let list = match last {
        None => return Ok(None),
        Some(LastMessages::Range(range)) => return Ok(Some(range)),
        Some(LastMessages::List(list)) => list,
    };
    let last_message_timestamp = list
        .last()
        .and_then(|m| m.message_timestamp)
        .map(|t| t as i64);
    let mut messages = vec![];
    for message in list {
        let mut key = message.key.unwrap_or_default();
        let remote = key.remote_jid.as_deref().filter(|s| !s.is_empty());
        if key.id.as_deref().is_none_or(str::is_empty) || remote.is_none() {
            return Err(Error::Modification("Incomplete key"));
        }
        let participant = key.participant.as_deref().filter(|s| !s.is_empty());
        if remote.is_some_and(|s| s.ends_with("@g.us"))
            && key.from_me != Some(true)
            && participant.is_none()
        {
            return Err(Error::Modification(
                "Expected not from me message to have participant",
            ));
        }
        if message.message_timestamp.is_none_or(|t| t == 0) {
            return Err(Error::Modification(
                "Missing timestamp in last message list",
            ));
        }
        if let Some(participant) = participant {
            key.participant = Some(jid::normalized_user(participant));
        }
        // The source passes the minimal message through; only its key is a range field.
        messages.push(action::SyncActionMessage {
            key: Some(key),
            timestamp: None,
        });
    }
    Ok(Some(SyncActionMessageRange {
        last_message_timestamp,
        last_system_message_timestamp: None,
        messages,
    }))
}
fn flag(value: Option<bool>) -> String {
    if value == Some(true) { "1" } else { "0" }.into()
}
/// Convert a modification to the patch upstream's chatModificationToAppPatch creates.
pub fn chat_modification_patch(
    modification: ChatModification,
    jid: &str,
    now_ms: i64,
) -> Result<PatchCreate, Error> {
    use ChatModification as M;
    let jid = jid.to_owned();
    let mut value = SyncActionValue::default();
    let mut operation = SyncdOperation::Set;
    let (index, name, api_version) = match modification {
        M::Mute(mute) => {
            let mute = mute.filter(|t| *t != 0);
            value.mute_action = Some(action::MuteAction {
                muted: Some(mute.is_some()),
                mute_end_timestamp: mute,
                ..Default::default()
            });
            (vec!["mute".into(), jid], "regular_high", 2)
        }
        M::Archive {
            archive,
            last_messages,
        } => {
            value.archive_chat_action = Some(action::ArchiveChatAction {
                archived: Some(archive),
                message_range: range(last_messages)?,
            });
            (vec!["archive".into(), jid], "regular_low", 3)
        }
        M::MarkRead {
            read,
            last_messages,
        } => {
            value.mark_chat_as_read_action = Some(action::MarkChatAsReadAction {
                read: Some(read),
                message_range: range(last_messages)?,
            });
            (vec!["markChatAsRead".into(), jid], "regular_low", 3)
        }
        M::DeleteForMe {
            delete_media,
            key,
            timestamp,
        } => {
            value.delete_message_for_me_action = Some(action::DeleteMessageForMeAction {
                delete_media: Some(delete_media),
                message_timestamp: Some(timestamp),
            });
            (
                vec![
                    "deleteMessageForMe".into(),
                    jid,
                    key.id.ok_or(Error::Modification("Incomplete key"))?,
                    flag(key.from_me),
                    "0".into(),
                ],
                "regular_high",
                3,
            )
        }
        M::Clear { last_messages } => {
            value.clear_chat_action = Some(action::ClearChatAction {
                message_range: range(last_messages)?,
            });
            // The option is 0 when starred messages are kept.
            (
                vec!["clearChat".into(), jid, "1".into(), "0".into()],
                "regular_high",
                6,
            )
        }
        M::Pin(pin) => {
            value.pin_action = Some(action::PinAction { pinned: Some(pin) });
            (vec!["pin_v1".into(), jid], "regular_low", 5)
        }
        M::Contact(contact) => {
            if contact.is_none() {
                operation = SyncdOperation::Remove;
            }
            value.contact_action = Some(contact.unwrap_or_default());
            (vec!["contact".into(), jid], "critical_unblock_low", 2)
        }
        M::DisableLinkPreviews(setting) => {
            value.privacy_setting_disable_link_previews_action = Some(setting);
            (vec!["setting_disableLinkPreviews".into()], "regular", 8)
        }
        M::Star { messages, star } => {
            let key = messages
                .into_iter()
                .next()
                .ok_or(Error::Modification("Incomplete key"))?;
            value.star_action = Some(action::StarAction {
                starred: Some(star),
            });
            (
                vec!["star".into(), jid, key.id, flag(key.from_me), "0".into()],
                "regular_low",
                2,
            )
        }
        M::Delete { last_messages } => {
            value.delete_chat_action = Some(action::DeleteChatAction {
                message_range: range(last_messages)?,
            });
            (
                vec!["deleteChat".into(), jid, "1".into()],
                "regular_high",
                6,
            )
        }
        M::PushNameSetting(name) => {
            value.push_name_setting = Some(action::PushNameSetting { name: Some(name) });
            (vec!["setting_pushName".into()], "critical_block", 1)
        }
        M::QuickReply(reply) => {
            value.quick_reply_action = Some(action::QuickReplyAction {
                count: Some(0),
                deleted: Some(reply.deleted.unwrap_or(false)),
                keywords: vec![],
                message: Some(reply.message.unwrap_or_default()),
                shortcut: Some(reply.shortcut.unwrap_or_default()),
            });
            let timestamp = reply
                .timestamp
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| now_ms.div_euclid(1000).to_string());
            (vec!["quick_reply".into(), timestamp], "regular", 2)
        }
        M::AddLabel(label) => {
            value.label_edit_action = Some(action::LabelEditAction {
                name: label.name,
                color: label.color,
                predefined_id: label.predefined_id,
                deleted: label.deleted,
                ..Default::default()
            });
            (vec!["label_edit".into(), label.id], "regular", 3)
        }
        modification @ (M::AddChatLabel { .. } | M::RemoveChatLabel { .. }) => {
            let labeled = matches!(modification, M::AddChatLabel { .. });
            let (M::AddChatLabel { label_id } | M::RemoveChatLabel { label_id }) = modification
            else {
                unreachable!("matched a chat label modification")
            };
            value.label_association_action = Some(action::LabelAssociationAction {
                labeled: Some(labeled),
            });
            (vec!["label_jid".into(), label_id, jid], "regular", 3)
        }
        modification @ (M::AddMessageLabel { .. } | M::RemoveMessageLabel { .. }) => {
            let labeled = matches!(modification, M::AddMessageLabel { .. });
            let (M::AddMessageLabel {
                message_id,
                label_id,
            }
            | M::RemoveMessageLabel {
                message_id,
                label_id,
            }) = modification
            else {
                unreachable!("matched a message label modification")
            };
            value.label_association_action = Some(action::LabelAssociationAction {
                labeled: Some(labeled),
            });
            (
                vec![
                    "label_message".into(),
                    label_id,
                    jid,
                    message_id,
                    "0".into(),
                    "0".into(),
                ],
                "regular",
                3,
            )
        }
    };
    value.timestamp = Some(now_ms);
    Ok(PatchCreate {
        name: name.into(),
        index,
        sync_action: value,
        api_version,
        operation: operation as i32,
    })
}
