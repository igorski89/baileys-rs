//! Source group events derived from message stubs and member labels.
use super::{Error, KeyMetadata, own::ChatUpdate};
use crate::{jid, proto, relay};
use serde_json::Value;
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ParticipantAction {
    Modify,
    Remove,
    Add,
    Demote,
    Promote,
}
impl ParticipantAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Modify => "modify",
            Self::Remove => "remove",
            Self::Add => "add",
            Self::Demote => "demote",
            Self::Promote => "promote",
        }
    }
}
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Change {
    Announce(bool),
    Restrict(bool),
    Subject(Option<String>),
    Description(Option<String>),
    InviteCode(Option<String>),
    MemberAddMode(bool),
    JoinApprovalMode(bool),
}
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Author {
    pub jid: Option<String>,
    pub pn: Option<String>,
    pub username: Option<String>,
}
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum GroupEvent {
    Participants {
        id: String,
        author: Author,
        action: ParticipantAction,
        /// Raw JSON preserves source participant fields and malformed primitive entries.
        participants: Vec<Value>,
    },
    Changed {
        id: String,
        author: Author,
        change: Change,
    },
    JoinRequest {
        id: String,
        author: Author,
        participant: Option<Value>,
        participant_pn: Option<Value>,
        action: Option<String>,
        method: Option<String>,
    },
    MemberTag {
        group_id: String,
        label: String,
        participant: Option<String>,
        participant_alt: Option<String>,
        message_timestamp: f64,
    },
}
fn source_json(value: Value) -> Value {
    match value {
        Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            if f.fract() == 0.0 && f >= i64::MIN as f64 && f < i64::MAX as f64 {
                Value::from(f as i64)
            } else if f.fract() == 0.0 && f >= 0.0 && f < u64::MAX as f64 {
                Value::from(f as u64)
            } else {
                Value::from(f)
            }
        }
        Value::Array(values) => Value::Array(values.into_iter().map(source_json).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(k, v)| (k, source_json(v)))
                .collect(),
        ),
        other => other,
    }
}
fn parse_participants(values: &[String]) -> Result<Vec<Value>, Error> {
    values
        .iter()
        .map(|v| {
            serde_json::from_str(v)
                .map(source_json)
                .map_err(Error::from)
        })
        .collect()
}
fn includes_me(values: &[Value], me: &str) -> Result<bool, Error> {
    for v in values {
        if v.is_null() {
            return Err(Error::Envelope("null group participant"));
        }
        // jidDecode ignores non-string values; only a null participant itself
        // throws when the source accesses its phoneNumber property.
        let text = v.get("phoneNumber").and_then(Value::as_str).unwrap_or("");
        if jid::are_same_user(me, text) {
            return Ok(true);
        }
    }
    Ok(false)
}
/// Emit in source order. Removal can publish its participant event before a
/// malformed self-membership test fails; callers keep already emitted updates.
pub fn process(
    message: &proto::WebMessageInfo,
    metadata: Option<&KeyMetadata>,
    me: &str,
    chat: &mut ChatUpdate,
    mut emit: impl FnMut(GroupEvent),
) -> Result<(), Error> {
    let key = message
        .key
        .as_ref()
        .ok_or(Error::Envelope("missing group message key"))?;
    let content = relay::normalize(message.message.as_ref());
    if let Some(protocol) = content.and_then(|m| m.protocol_message.as_deref()) {
        if protocol.r#type
            == Some(proto::message::protocol_message::Type::GroupMemberLabelChange as i32)
            && let Some(label) = protocol
                .member_label
                .as_ref()
                .and_then(|m| m.label.as_ref())
                .filter(|s| !s.is_empty())
        {
            emit(GroupEvent::MemberTag {
                group_id: chat.id.clone(),
                label: label.clone(),
                participant: key.participant.clone(),
                participant_alt: metadata.and_then(|m| m.participant_alt.clone()),
                message_timestamp: message.message_timestamp.unwrap_or(0) as f64,
            });
        }
        return Ok(());
    }
    if content
        .is_some_and(|m| m.reaction_message.is_some() || m.enc_event_response_message.is_some())
    {
        return Ok(());
    }
    use proto::web_message_info::StubType;
    let Some(kind) = message
        .message_stub_type
        .and_then(|t| StubType::try_from(t).ok())
    else {
        return Ok(());
    };
    let id = key
        .remote_jid
        .clone()
        .ok_or(Error::Envelope("missing group JID"))?;
    let author = Author {
        jid: key.participant.clone(),
        pn: metadata.and_then(|m| m.participant_alt.clone()),
        username: metadata.and_then(|m| m.participant_username.clone()),
    };
    let fields = &message.message_stub_parameters;
    let first = fields.first().cloned();
    let action = match kind {
        StubType::GroupParticipantChangeNumber => Some(ParticipantAction::Modify),
        StubType::GroupParticipantLeave | StubType::GroupParticipantRemove => {
            Some(ParticipantAction::Remove)
        }
        StubType::GroupParticipantAdd
        | StubType::GroupParticipantInvite
        | StubType::GroupParticipantAddRequestJoin => Some(ParticipantAction::Add),
        StubType::GroupParticipantDemote => Some(ParticipantAction::Demote),
        StubType::GroupParticipantPromote => Some(ParticipantAction::Promote),
        _ => None,
    };
    if let Some(action) = action {
        let participants = parse_participants(fields)?;
        if action == ParticipantAction::Add && includes_me(&participants, me)? {
            chat.read_only = Some(false);
        }
        emit(GroupEvent::Participants {
            id,
            author,
            action,
            participants: participants.clone(),
        });
        if action == ParticipantAction::Remove && includes_me(&participants, me)? {
            chat.read_only = Some(true);
        }
        return Ok(());
    }
    let change = match kind {
        StubType::GroupChangeAnnounce => Some(Change::Announce(
            first.as_deref().is_some_and(|s| s == "true" || s == "on"),
        )),
        StubType::GroupChangeRestrict => Some(Change::Restrict(
            first.as_deref().is_some_and(|s| s == "true" || s == "on"),
        )),
        StubType::GroupChangeSubject => {
            chat.name = Some(first.clone());
            Some(Change::Subject(first))
        }
        StubType::GroupChangeDescription => {
            chat.description = Some(first.clone());
            Some(Change::Description(first))
        }
        StubType::GroupChangeInviteLink => Some(Change::InviteCode(first)),
        StubType::GroupMemberAddMode => Some(Change::MemberAddMode(
            first.as_deref() == Some("all_member_add"),
        )),
        StubType::GroupMembershipJoinApprovalMode => {
            Some(Change::JoinApprovalMode(first.as_deref() == Some("on")))
        }
        StubType::GroupMembershipJoinApprovalRequestNonAdminAdd => {
            let p: Value = source_json(serde_json::from_str(
                fields
                    .first()
                    .ok_or(Error::Envelope("missing join participant"))?,
            )?);
            if p.is_null() {
                return Err(Error::Envelope("null join participant"));
            }
            emit(GroupEvent::JoinRequest {
                id,
                author,
                participant: p.get("lid").cloned(),
                participant_pn: p.get("pn").cloned(),
                action: fields.get(1).cloned(),
                method: fields.get(2).cloned(),
            });
            return Ok(());
        }
        _ => None,
    };
    if let Some(change) = change {
        emit(GroupEvent::Changed { id, author, change });
    }
    Ok(())
}
