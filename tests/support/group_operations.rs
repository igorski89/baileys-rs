use baileys_rs::{
    binary::{self, BinaryNode, NodeContent},
    groups::*,
    socket::{Error, Socket},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
pub fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
        .collect()
}
fn source_node(node: &BinaryNode) -> Value {
    let mut result = json!({"tag":node.tag,"attrs":node.attrs});
    if let Some(content) = &node.content {
        result["content"] = match content {
            NodeContent::Bytes(bytes) => json!({"type":"Buffer","data":STANDARD.encode(bytes)}),
            NodeContent::Text(text) => json!(text),
            NodeContent::Children(nodes) => {
                json!(nodes.iter().map(source_node).collect::<Vec<_>>())
            }
        };
    }
    result
}
fn participants(result: Vec<ParticipantResult>) -> Value {
    Value::Array(
        result
            .into_iter()
            .map(|p| {
                let mut value = json!({"status":p.status});
                if let Some(jid) = p.jid {
                    value["jid"] = json!(jid);
                }
                if let Some(content) = p.content {
                    value["content"] = source_node(&content);
                }
                value
            })
            .collect(),
    )
}
pub fn assert_request(mut request: BinaryNode, wire: &str) {
    assert!(!request.attrs.shift_remove("id").unwrap().is_empty());
    let mut scrub = |tag: &str, key: &str| {
        if let Some(NodeContent::Children(nodes)) = &mut request.content
            && let Some(node) = nodes.iter_mut().find(|n| n.tag == tag)
            && let Some(value) = node.attrs.get_mut(key)
        {
            assert!(value.starts_with("3EB0"));
            assert!(value.bytes().all(|b| b.is_ascii_hexdigit()));
            *value = "FIXED-ID".into();
        }
    };
    scrub("create", "key");
    scrub("description", "id");
    assert_eq!(
        binary::encode(&request).unwrap(),
        hex(wire),
        "{}",
        request.tag
    );
}
pub async fn execute(socket: &Socket, case: &Value) -> Result<Value, Error> {
    let args = case["args"].as_array().unwrap();
    let s = |i: usize| args[i].as_str().unwrap();
    let list = |i: usize| {
        args[i]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    let result = match case["method"].as_str().unwrap() {
        "groupMetadata" => json!(socket.group_metadata(s(0)).await?),
        "groupFetchAllParticipating" => json!(socket.group_fetch_all_participating().await?),
        "groupCreate" => json!(socket.group_create(s(0), &list(1)).await?),
        "groupLeave" => {
            socket.group_leave(s(0)).await?;
            Value::Null
        }
        "groupUpdateSubject" => {
            socket.group_update_subject(s(0), s(1)).await?;
            Value::Null
        }
        "groupRequestParticipantsList" => {
            json!(socket.group_request_participants_list(s(0)).await?)
        }
        "groupRequestParticipantsUpdate" => participants(
            socket
                .group_request_participants_update(
                    s(0),
                    &list(1),
                    if s(2) == "approve" {
                        RequestAction::Approve
                    } else {
                        RequestAction::Reject
                    },
                )
                .await?,
        ),
        "groupParticipantsUpdate" => participants(
            socket
                .group_participants_update(
                    s(0),
                    &list(1),
                    match s(2) {
                        "add" => ParticipantAction::Add,
                        "remove" => ParticipantAction::Remove,
                        "promote" => ParticipantAction::Promote,
                        "demote" => ParticipantAction::Demote,
                        "modify" => ParticipantAction::Modify,
                        _ => panic!("unknown action"),
                    },
                )
                .await?,
        ),
        "groupUpdateDescription" => {
            socket
                .group_update_description(s(0), args[1].as_str())
                .await?;
            Value::Null
        }
        "groupInviteCode" => json!(socket.group_invite_code(s(0)).await?),
        "groupRevokeInvite" => json!(socket.group_revoke_invite(s(0)).await?),
        "groupAcceptInvite" => json!(socket.group_accept_invite(s(0)).await?),
        "groupRevokeInviteV4" => json!(socket.group_revoke_invite_v4(s(0), s(1)).await?),
        "groupGetInviteInfo" => json!(socket.group_get_invite_info(s(0)).await?),
        "groupToggleEphemeral" => {
            socket
                .group_toggle_ephemeral(s(0), args[1].as_u64().unwrap())
                .await?;
            Value::Null
        }
        "groupSettingUpdate" => {
            socket
                .group_setting_update(
                    s(0),
                    match s(1) {
                        "announcement" => Setting::Announcement,
                        "not_announcement" => Setting::NotAnnouncement,
                        "locked" => Setting::Locked,
                        "unlocked" => Setting::Unlocked,
                        _ => panic!("unknown setting"),
                    },
                )
                .await?;
            Value::Null
        }
        "groupMemberAddMode" => {
            socket
                .group_member_add_mode(
                    s(0),
                    if s(1) == "admin_add" {
                        MemberAddMode::AdminAdd
                    } else {
                        MemberAddMode::AllMemberAdd
                    },
                )
                .await?;
            Value::Null
        }
        "groupJoinApprovalMode" => {
            socket
                .group_join_approval_mode(
                    s(0),
                    if s(1) == "on" {
                        JoinApprovalMode::On
                    } else {
                        JoinApprovalMode::Off
                    },
                )
                .await?;
            Value::Null
        }
        method => panic!("unknown method {method}"),
    };
    Ok(result)
}
