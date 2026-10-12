//! Group, picture and account-sync notification parsing with generated stub messages.
use crate::{
    binary::{BinaryNode, NodeContent},
    groups, jid, proto,
};
use proto::web_message_info::StubType;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Upstream dereferences the first child of these notifications unconditionally.
    #[error("notification has no child node")]
    MissingChild,
    #[error(transparent)]
    Group(#[from] groups::Error),
}
/// A created group with the acting participant, published as groups.upsert.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupUpsert {
    pub metadata: groups::Metadata,
    pub author: Option<String>,
    pub author_pn: Option<String>,
    pub author_username: Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// chats.upsert followed by groups.upsert for a `create` group notification.
    GroupCreated(Box<GroupUpsert>),
    /// contacts.update with imgUrl `changed` or `removed`.
    Picture { id: String, removed: bool },
    /// creds.update replacing the default disappearing mode. Non-numeric source
    /// attributes are NaN, as upstream's unary plus produces.
    DisappearingMode { duration: f64, timestamp: f64 },
    /// blocklist.update for one account_sync item.
    Blocklist { jid: Option<String>, add: bool },
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Notification {
    pub effects: Vec<Effect>,
    /// Stub message for upsertMessage(.., 'append'), with its final key and timestamp.
    pub message: Option<proto::WebMessageInfo>,
}
/// Notification types parsed by [`process`].
pub fn is_supported(kind: Option<&str>) -> bool {
    matches!(kind, Some("w:gp2" | "picture" | "account_sync"))
}

// Fields assigned by processNotification before handleNotification completes the key.
#[derive(Default)]
struct Partial {
    assigned: bool,
    key_participant: Option<Option<String>>,
    participant: Option<String>,
    stub_type: Option<StubType>,
    stub_parameters: Vec<String>,
    message: Option<proto::Message>,
}
impl Partial {
    fn stub(&mut self, kind: StubType, parameters: Vec<String>) {
        self.assigned = true;
        self.stub_type = Some(kind);
        self.stub_parameters = parameters;
    }
}
fn attr(node: &BinaryNode, name: &str) -> Option<String> {
    node.attrs.get(name).cloned()
}
fn truthy(node: &BinaryNode, name: &str) -> Option<String> {
    attr(node, name).filter(|s| !s.is_empty())
}
// protobufjs fromObject applies String() to repeated string elements.
fn string(value: Option<String>) -> String {
    value.unwrap_or_else(|| "undefined".into())
}
fn same_user(first: Option<&str>, second: Option<&str>) -> bool {
    let user = |value: Option<&str>| value.and_then(jid::Jid::parse).map(|jid| jid.user);
    user(first) == user(second)
}
// Buffer/string content toString(); other content is not a scalar.
fn content_string(node: &BinaryNode) -> Option<String> {
    match &node.content {
        Some(NodeContent::Text(text)) => Some(text.clone()),
        Some(NodeContent::Bytes(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    }
}
// Long.fromValue(number) reinterpreted as unsigned, as fromObject does for uint64.
fn long(value: f64) -> u64 {
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    if value.is_nan() {
        0
    } else if value <= -TWO_63 {
        i64::MIN as u64
    } else if value + 1.0 >= TWO_63 {
        i64::MAX as u64
    } else {
        value.trunc() as i64 as u64
    }
}
// JavaScript ToUint32, used by fromObject's `>>> 0`.
fn uint32(value: f64) -> u32 {
    if !value.is_finite() {
        return 0;
    }
    value.trunc().rem_euclid(4_294_967_296.0) as u32
}
fn group(
    full: &BinaryNode,
    child: Option<&BinaryNode>,
    msg: &mut Partial,
    effects: &mut Vec<Effect>,
) -> Result<(), Error> {
    let Some(child) = child else { return Ok(()) };
    let acting_lid = attr(full, "participant");
    let acting_pn = attr(full, "participant_pn");
    let affected = child.child("participant");
    let affected_lid = affected
        .and_then(|p| truthy(p, "jid"))
        .or_else(|| acting_lid.clone());
    let affected_pn = affected
        .and_then(|p| truthy(p, "phone_number"))
        .or_else(|| acting_pn.clone());
    let request = || {
        let mut value = serde_json::Map::new();
        if let Some(lid) = &affected_lid {
            value.insert("lid".into(), lid.clone().into());
        }
        if let Some(pn) = &affected_pn {
            value.insert("pn".into(), pn.clone().into());
        }
        serde_json::Value::Object(value).to_string()
    };
    match child.tag.as_str() {
        "create" => {
            let metadata = groups::extract_metadata(child)?;
            msg.stub(
                StubType::GroupCreate,
                vec![string(metadata.subject.clone())],
            );
            msg.key_participant = Some(metadata.owner.clone());
            effects.push(Effect::GroupCreated(Box::new(GroupUpsert {
                metadata,
                author: acting_lid,
                author_pn: acting_pn,
                author_username: attr(full, "participant_username"),
            })));
        }
        "ephemeral" | "not_ephemeral" => {
            msg.assigned = true;
            msg.message = Some(proto::Message {
                protocol_message: Some(Box::new(proto::message::ProtocolMessage {
                    r#type: Some(proto::message::protocol_message::Type::EphemeralSetting as i32),
                    ephemeral_expiration: Some(uint32(
                        truthy(child, "expiration").map_or(0.0, |s| crate::privacy::number(&s)),
                    )),
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        "modify" => msg.stub(
            StubType::GroupParticipantChangeNumber,
            child
                .children("participant")
                .map(|p| string(attr(p, "jid")))
                .collect(),
        ),
        "promote" | "demote" | "remove" | "add" | "leave" => {
            let mut kind = match child.tag.as_str() {
                "promote" => StubType::GroupParticipantPromote,
                "demote" => StubType::GroupParticipantDemote,
                "remove" => StubType::GroupParticipantRemove,
                "add" => StubType::GroupParticipantAdd,
                _ => StubType::GroupParticipantLeave,
            };
            let participants: Vec<_> = child.children("participant").collect();
            // A sender removing themselves has left the group.
            if child.tag == "remove"
                && let [only] = participants.as_slice()
            {
                let id = attr(only, "jid");
                if same_user(id.as_deref(), acting_lid.as_deref())
                    || same_user(id.as_deref(), acting_pn.as_deref())
                {
                    kind = StubType::GroupParticipantLeave;
                }
            }
            let parameters = participants
                .into_iter()
                .map(|p| {
                    let id = attr(p, "jid");
                    let lid_user =
                        |v: &Option<String>| v.as_ref().is_some_and(|s| s.ends_with("@lid"));
                    let pn_user = |v: &Option<String>| {
                        v.as_ref().is_some_and(|s| s.ends_with("@s.whatsapp.net"))
                    };
                    let phone = attr(p, "phone_number");
                    let lid = attr(p, "lid");
                    let mut value = serde_json::Map::new();
                    if let Some(id) = &id {
                        value.insert("id".into(), id.clone().into());
                    }
                    if lid_user(&id) && pn_user(&phone) {
                        value.insert("phoneNumber".into(), phone.into());
                    }
                    if pn_user(&id) && lid_user(&lid) {
                        value.insert("lid".into(), lid.into());
                    }
                    if let Some(username) =
                        truthy(p, "participant_username").or_else(|| truthy(p, "username"))
                    {
                        value.insert("username".into(), username.into());
                    }
                    value.insert("admin".into(), truthy(p, "type").into());
                    serde_json::Value::Object(value).to_string()
                })
                .collect();
            msg.stub(kind, parameters);
        }
        "subject" => msg.stub(
            StubType::GroupChangeSubject,
            vec![string(attr(child, "subject"))],
        ),
        "description" => msg.stub(
            StubType::GroupChangeDescription,
            child
                .child("body")
                .and_then(content_string)
                .filter(|s| !s.is_empty())
                .into_iter()
                .collect(),
        ),
        "announcement" | "not_announcement" => msg.stub(
            StubType::GroupChangeAnnounce,
            vec![
                if child.tag == "announcement" {
                    "on"
                } else {
                    "off"
                }
                .into(),
            ],
        ),
        "locked" | "unlocked" => msg.stub(
            StubType::GroupChangeRestrict,
            vec![if child.tag == "locked" { "on" } else { "off" }.into()],
        ),
        "invite" => msg.stub(
            StubType::GroupChangeInviteLink,
            vec![string(attr(child, "code"))],
        ),
        "member_add_mode" => {
            // An empty Buffer is truthy in the source; an empty string is not.
            let mode = match &child.content {
                Some(NodeContent::Text(text)) if text.is_empty() => None,
                Some(NodeContent::Children(children)) => {
                    Some(vec!["[object Object]"; children.len()].join(","))
                }
                _ => content_string(child),
            };
            if let Some(mode) = mode {
                msg.stub(StubType::GroupMemberAddMode, vec![mode]);
            }
        }
        "membership_approval_mode" => {
            if let Some(mode) = child.child("group_join") {
                msg.stub(
                    StubType::GroupMembershipJoinApprovalMode,
                    vec![string(attr(mode, "state"))],
                );
            }
        }
        "created_membership_requests" => msg.stub(
            StubType::GroupMembershipJoinApprovalRequestNonAdminAdd,
            vec![
                request(),
                "created".into(),
                string(attr(child, "request_method")),
            ],
        ),
        "revoked_membership_requests" => {
            let denied = same_user(affected_lid.as_deref(), acting_lid.as_deref());
            msg.stub(
                StubType::GroupMembershipJoinApprovalRequestNonAdminAdd,
                vec![
                    request(),
                    if denied { "revoked" } else { "rejected" }.into(),
                ],
            );
        }
        _ => (),
    }
    Ok(())
}
/// Parse one notification as upstream's processNotification and handleNotification
/// do for these types. Other notification types yield an empty result.
pub fn process(node: &BinaryNode, me_id: &str) -> Result<Notification, Error> {
    let mut effects = vec![];
    let mut msg = Partial::default();
    let child = node.all_children().first();
    let from = node
        .attrs
        .get("from")
        .map(|s| jid::normalized_user(s))
        .unwrap_or_default();
    match node.attrs.get("type").map(String::as_str) {
        Some("w:gp2") => group(node, child, &mut msg, &mut effects)?,
        Some("picture") => {
            let set = node.child("set");
            let delete = node.child("delete");
            let changed = set.or(delete);
            effects.push(Effect::Picture {
                id: Some(from.clone())
                    .filter(|s| !s.is_empty())
                    .or_else(|| changed.and_then(|n| truthy(n, "hash")))
                    .unwrap_or_default(),
                removed: set.is_none(),
            });
            if from.ends_with("@g.us") {
                msg.stub(
                    StubType::GroupChangeIcon,
                    set.map(|n| string(attr(n, "id"))).into_iter().collect(),
                );
                msg.participant = changed.and_then(|n| attr(n, "author"));
                msg.key_participant = Some(set.and_then(|n| attr(n, "author")));
            }
        }
        Some("account_sync") => {
            let child = child.ok_or(Error::MissingChild)?;
            let number = |name: &str| {
                child
                    .attrs
                    .get(name)
                    .map_or(f64::NAN, |s| crate::privacy::number(s))
            };
            match child.tag.as_str() {
                "disappearing_mode" => effects.push(Effect::DisappearingMode {
                    duration: number("duration"),
                    timestamp: number("t"),
                }),
                "blocklist" => {
                    effects.extend(child.children("item").map(|item| Effect::Blocklist {
                        jid: attr(item, "jid"),
                        add: item.attrs.get("action").is_some_and(|s| s == "block"),
                    }))
                }
                _ => (),
            }
        }
        _ => (),
    }
    let message = msg.assigned.then(|| {
        let remote = attr(node, "from");
        let participant = attr(node, "participant");
        let sender = participant
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| remote.clone());
        proto::WebMessageInfo {
            key: Some(proto::MessageKey {
                from_me: Some(same_user(sender.as_deref(), Some(me_id))),
                id: attr(node, "id"),
                participant: match msg.key_participant {
                    Some(participant) => participant,
                    None => participant.clone(),
                },
                remote_jid: remote,
            }),
            participant: msg.participant.or(participant),
            message_timestamp: Some(long(
                node.attrs
                    .get("t")
                    .map_or(f64::NAN, |s| crate::privacy::number(s)),
            )),
            message_stub_type: msg.stub_type.map(|kind| kind as i32),
            message_stub_parameters: msg.stub_parameters,
            message: msg.message,
            ..Default::default()
        }
    });
    Ok(Notification { effects, message })
}
