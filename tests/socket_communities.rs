#[path = "support/json_numbers.rs"]
mod json_numbers;
#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{self, BinaryNode, NodeContent},
    communities,
    crypto::KeyPair,
    groups::*,
    socket::{Config, Error, Event, Socket},
};
use serde_json::{Value, json};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/communities.json")).unwrap()
}
#[test]
fn community_metadata_matches_original_parser() {
    for case in fixture()["metadata"].as_array().unwrap() {
        let node = binary::decode(&hex(case["wire"].as_str().unwrap())).unwrap();
        match communities::extract_metadata(&node) {
            Ok(metadata) => {
                assert_eq!(case["error"], false);
                assert_eq!(
                    json_numbers::canonical(&json!(metadata)),
                    json_numbers::canonical(&case["expected"])
                );
            }
            Err(error) => {
                assert_eq!(case["error"], true);
                assert_eq!(error.status_code(), 500);
            }
        }
    }
}
// Generated IDs are random; check their form, then adopt the fixture's fixed value.
fn adopt_ids(actual: &mut BinaryNode, expected: &BinaryNode) {
    for key in ["key", "id"] {
        if matches!(actual.tag.as_str(), "create" | "description")
            && let (Some(mine), Some(theirs)) = (actual.attrs.get_mut(key), expected.attrs.get(key))
            && mine != theirs
        {
            assert!(mine.starts_with("3EB0"), "{mine}");
            assert!(mine.bytes().all(|b| b.is_ascii_hexdigit()), "{mine}");
            if actual.tag == "description" && theirs.len() == 12 {
                assert_eq!(mine.len(), 12);
            }
            *mine = theirs.clone();
        }
    }
    if let (Some(NodeContent::Children(mine)), Some(NodeContent::Children(theirs))) =
        (&mut actual.content, &expected.content)
    {
        for (mine, theirs) in mine.iter_mut().zip(theirs) {
            adopt_ids(mine, theirs);
        }
    }
}
fn participants(result: Vec<ParticipantResult>) -> Value {
    json!(
        result
            .into_iter()
            .map(|p| {
                let mut value = json!({"status": p.status});
                if let Some(jid) = p.jid {
                    value["jid"] = json!(jid);
                }
                if let Some(content) = p.content {
                    value["content"] = json!({"tag": content.tag, "attrs": content.attrs});
                }
                value
            })
            .collect::<Vec<_>>()
    )
}
async fn execute(socket: &Socket, case: &Value) -> Result<Value, Error> {
    let args = case["args"].as_array().unwrap();
    let s = |i: usize| args[i].as_str().unwrap();
    let list = |i: usize| -> Vec<String> {
        args[i]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect()
    };
    // Created groups are reported through group metadata, which has its own fixtures.
    let created = |group: Option<Metadata>| group.map_or(Value::Null, |g| json!({"id": g.id}));
    Ok(match case["method"].as_str().unwrap() {
        "communityMetadata" => json!(socket.community_metadata(s(0)).await?),
        "communityFetchAllParticipating" => {
            json!(socket.community_fetch_all_participating().await?)
        }
        "communityCreate" => created(socket.community_create(s(0), s(1)).await?),
        "communityCreateGroup" => {
            created(socket.community_create_group(s(0), &list(1), s(2)).await?)
        }
        "communityLeave" => json!(socket.community_leave(s(0)).await?),
        "communityUpdateSubject" => json!(socket.community_update_subject(s(0), s(1)).await?),
        "communityLinkGroup" => json!(socket.community_link_group(s(0), s(1)).await?),
        "communityUnlinkGroup" => json!(socket.community_unlink_group(s(0), s(1)).await?),
        "communityFetchLinkedGroups" => json!(socket.community_fetch_linked_groups(s(0)).await?),
        "communityRequestParticipantsList" => {
            json!(socket.community_request_participants_list(s(0)).await?)
        }
        "communityRequestParticipantsUpdate" => participants(
            socket
                .community_request_participants_update(
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
        "communityParticipantsUpdate" => participants(
            socket
                .community_participants_update(
                    s(0),
                    &list(1),
                    match s(2) {
                        "add" => ParticipantAction::Add,
                        "remove" => ParticipantAction::Remove,
                        "promote" => ParticipantAction::Promote,
                        "demote" => ParticipantAction::Demote,
                        _ => ParticipantAction::Modify,
                    },
                )
                .await?,
        ),
        "communityUpdateDescription" => {
            json!(
                socket
                    .community_update_description(s(0), args[1].as_str())
                    .await?
            )
        }
        "communityInviteCode" => json!(socket.community_invite_code(s(0)).await?),
        "communityRevokeInvite" => json!(socket.community_revoke_invite(s(0)).await?),
        "communityAcceptInvite" => json!(socket.community_accept_invite(s(0)).await?),
        "communityRevokeInviteV4" => json!(socket.community_revoke_invite_v4(s(0), s(1)).await?),
        "communityGetInviteInfo" => json!(socket.community_get_invite_info(s(0)).await?),
        "communityToggleEphemeral" => json!(
            socket
                .community_toggle_ephemeral(s(0), args[1].as_u64().unwrap())
                .await?
        ),
        "communitySettingUpdate" => json!(
            socket
                .community_setting_update(
                    s(0),
                    match s(1) {
                        "announcement" => Setting::Announcement,
                        "not_announcement" => Setting::NotAnnouncement,
                        "locked" => Setting::Locked,
                        _ => Setting::Unlocked,
                    },
                )
                .await?
        ),
        "communityMemberAddMode" => json!(
            socket
                .community_member_add_mode(
                    s(0),
                    if s(1) == "admin_add" {
                        MemberAddMode::AdminAdd
                    } else {
                        MemberAddMode::AllMemberAdd
                    },
                )
                .await?
        ),
        "communityJoinApprovalMode" => json!(
            socket
                .community_join_approval_mode(
                    s(0),
                    if s(1) == "on" {
                        JoinApprovalMode::On
                    } else {
                        JoinApprovalMode::Off
                    },
                )
                .await?
        ),
        method => panic!("unknown method {method}"),
    })
}
#[tokio::test]
async fn community_operations_match_original_requests_over_encrypted_transport() {
    timeout(Duration::from_secs(12), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            url: format!("ws://{}", listener.local_addr().unwrap()),
            initialize_session: false,
            fire_init_queries: false,
            announce_presence_on_connect: false,
            query_timeout: Duration::from_secs(3),
            noise: baileys_rs::noise::Config {
                certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = SqliteAuthStore::in_memory().unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(Contact {
            id: "100:7@s.whatsapp.net".into(),
            ..Default::default()
        });
        store.save_credentials(&creds).unwrap();
        let operations = fixture()["operations"].as_array().unwrap().clone();
        assert_eq!(operations.len(), 42);
        let served = operations.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            for case in served {
                for (wire, response) in case["requests"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .zip(case["responses"].as_array().unwrap())
                {
                    let mut request = peer.read().await;
                    let id = request.attrs.shift_remove("id").unwrap();
                    let expected = binary::decode(&hex(wire.as_str().unwrap())).unwrap();
                    adopt_ids(&mut request, &expected);
                    assert_eq!(request, expected, "{}", case["method"]);
                    let mut result = binary::decode(&hex(response.as_str().unwrap())).unwrap();
                    result.attrs.insert("id".into(), id);
                    peer.send(result).await;
                }
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        for case in operations {
            while events.try_recv().is_ok() {}
            let result = execute(&socket, &case).await;
            assert_eq!(result.is_err(), case["error"].as_bool().unwrap(), "{case}");
            let Ok(result) = result else { continue };
            let expected = match case["method"].as_str().unwrap() {
                "communityCreate" | "communityCreateGroup" if !case["expected"].is_null() => {
                    json!({"id": case["expected"]["id"]})
                }
                _ => case["expected"].clone(),
            };
            assert_eq!(
                json_numbers::canonical(&result),
                json_numbers::canonical(&expected),
                "{}",
                case["method"]
            );
            // Fetching participating communities publishes them as group updates.
            if case["method"] == "communityFetchAllParticipating" {
                let updated = loop {
                    if let Event::GroupsUpdated(groups) = events.recv().await.unwrap() {
                        break groups;
                    }
                };
                let ids: Vec<_> = updated.iter().map(|g| json!(g.id)).collect();
                let wanted: Vec<_> = case["events"][0]["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|g| g["id"].clone())
                    .collect();
                assert_eq!(ids, wanted);
            }
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
