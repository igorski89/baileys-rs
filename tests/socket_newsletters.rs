#[path = "support/json_numbers.rs"]
mod json_numbers;
#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, KeyCategory, KeyStore, KeyValue, SqliteAuthStore},
    binary::{self, BinaryNode, NodeContent},
    crypto::KeyPair,
    newsletters::{self, Effect, Key, ReachoutTimelock, Update},
    proto,
    socket::{Config, Error, Event, Socket},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
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
    serde_json::from_str(include_str!("fixtures/newsletters.json")).unwrap()
}
fn node(v: &Value) -> BinaryNode {
    let mut n = BinaryNode::new(v["tag"].as_str().unwrap());
    n.attrs = serde_json::from_value(v["attrs"].clone()).unwrap();
    n.content = match &v["content"] {
        Value::Array(children) => Some(NodeContent::Children(children.iter().map(node).collect())),
        Value::String(text) => Some(NodeContent::Text(text.clone())),
        Value::Object(buffer) => Some(NodeContent::Bytes(
            STANDARD.decode(buffer["data"].as_str().unwrap()).unwrap(),
        )),
        _ => None,
    };
    n
}
// Absent fields are omitted, as the fixture drops undefined values.
fn object(fields: &[(&str, Option<Value>)]) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|(name, value)| Some(((*name).to_owned(), value.clone()?)))
            .collect(),
    )
}
fn text(value: &Option<String>) -> Option<Value> {
    value.as_ref().map(|value| json!(value))
}
// The fixture records dates as epoch milliseconds, null when invalid.
fn timelock(state: &ReachoutTimelock) -> Value {
    object(&[
        ("isActive", Some(json!(state.is_active))),
        (
            "timeEnforcementEnds",
            state.time_enforcement_ends.map(|ms| json!({"$date": ms})),
        ),
        ("enforcementType", Some(json!(state.enforcement_type))),
    ])
}
#[test]
fn newsletter_and_mex_notifications_match_original_handlers() {
    let fixture = fixture();
    let now = fixture["now"].as_f64().unwrap();
    let cases = fixture["notifications"].as_array().unwrap();
    assert_eq!(cases.len(), 178);
    for case in cases {
        let input = node(&case["node"]);
        let result = if case["node"]["attrs"]["type"] == "mex" {
            newsletters::mex_notification(&input, now)
        } else {
            Ok(newsletters::notification(&input))
        };
        assert_eq!(result.is_err(), case["error"].as_bool().unwrap(), "{case}");
        let Ok(effects) = result else { continue };
        let (mut events, mut upserts, mut stores) = (vec![], vec![], vec![]);
        for effect in effects {
            let (name, data) = match effect {
                Effect::Reaction {
                    id,
                    server_id,
                    code,
                } => (
                    "newsletter.reaction",
                    object(&[
                        ("id", text(&id)),
                        ("server_id", text(&server_id)),
                        (
                            "reaction",
                            Some(object(&[("code", text(&code)), ("count", Some(json!(1)))])),
                        ),
                    ]),
                ),
                Effect::View {
                    id,
                    server_id,
                    count,
                } => (
                    "newsletter.view",
                    object(&[
                        ("id", text(&id)),
                        ("server_id", text(&server_id)),
                        ("count", Some(json!(count))),
                    ]),
                ),
                Effect::Participant {
                    id,
                    author,
                    user,
                    action,
                    new_role,
                } => (
                    "newsletter-participants.update",
                    object(&[
                        ("id", text(&id)),
                        ("author", text(&author)),
                        ("user", text(&user)),
                        ("action", text(&action)),
                        ("new_role", text(&new_role)),
                    ]),
                ),
                Effect::Settings { id, update } => (
                    "newsletter-settings.update",
                    object(&[("id", text(&id)), ("update", Some(update))]),
                ),
                Effect::Timelock(state) => (
                    "connection.update",
                    json!({"reachoutTimeLock": timelock(&state)}),
                ),
                Effect::MessageCapping(payload) => ("message-capping.update", payload),
                Effect::LidMapping { lid, pn } => {
                    ("lid-mapping.update", json!({"lid": lid, "pn": pn}))
                }
                Effect::Message(message) => {
                    upserts.push(*message);
                    continue;
                }
                Effect::StoreMappings(pairs) => {
                    stores.push(json!(
                        pairs
                            .iter()
                            .map(|(lid, pn)| json!({"lid": lid, "pn": pn}))
                            .collect::<Vec<_>>()
                    ));
                    continue;
                }
            };
            events.push(json!({"name": name, "data": data}));
        }
        assert_eq!(
            json_numbers::canonical(&json!(events)),
            json_numbers::canonical(&case["events"]),
            "{case}"
        );
        assert_eq!(json!(stores), case["stores"], "{case}");
        let expected: Vec<_> = case["upserts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|upsert| {
                assert_eq!(upsert["kind"], "append");
                proto::WebMessageInfo::decode(hex(upsert["wire"].as_str().unwrap()).as_slice())
                    .unwrap()
            })
            .collect();
        assert_eq!(upserts, expected, "{case}");
    }
}
async fn execute(socket: &Socket, case: &Value) -> Result<Value, Error> {
    let args = case["args"].as_array().unwrap();
    let s = |i: usize| args[i].as_str().unwrap();
    let optional = |i: usize| args.get(i).and_then(Value::as_str);
    let number = |i: usize| args.get(i).and_then(Value::as_u64);
    let done = |()| Value::Null;
    Ok(match case["method"].as_str().unwrap() {
        "newsletterCreate" => json!(socket.newsletter_create(s(0), optional(1)).await?),
        "newsletterUpdate" => {
            let field = |name: &str| args[1][name].as_str().map(str::to_owned);
            let updates = Update {
                name: field("name"),
                description: field("description"),
                picture: field("picture"),
            };
            socket.newsletter_update(s(0), &updates).await?
        }
        "newsletterSubscribers" => socket.newsletter_subscribers(s(0)).await?,
        "newsletterMetadata" => {
            let kind = if s(0) == "invite" {
                Key::Invite
            } else {
                Key::Jid
            };
            json!(socket.newsletter_metadata(kind, s(1)).await?)
        }
        "newsletterFollow" => socket.newsletter_follow(s(0)).await?,
        "newsletterUnfollow" => socket.newsletter_unfollow(s(0)).await?,
        "newsletterMute" => socket.newsletter_mute(s(0)).await?,
        "newsletterUnmute" => socket.newsletter_unmute(s(0)).await?,
        "newsletterUpdateName" => socket.newsletter_update_name(s(0), s(1)).await?,
        "newsletterUpdateDescription" => socket.newsletter_update_description(s(0), s(1)).await?,
        "newsletterRemovePicture" => socket.newsletter_remove_picture(s(0)).await?,
        "newsletterReactMessage" => done(
            socket
                .newsletter_react_message(s(0), s(1), optional(2))
                .await?,
        ),
        "newsletterFetchMessages" => {
            let result = socket
                .newsletter_fetch_messages(s(0), number(1).unwrap(), number(2), number(3))
                .await?;
            json!(result.all_children().len())
        }
        "subscribeNewsletterUpdates" => socket
            .subscribe_newsletter_updates(s(0))
            .await?
            .map_or(Value::Null, |duration| json!({"duration": duration})),
        "newsletterAdminCount" => json!(socket.newsletter_admin_count(s(0)).await?),
        "newsletterChangeOwner" => done(socket.newsletter_change_owner(s(0), s(1)).await?),
        "newsletterDemote" => done(socket.newsletter_demote(s(0), s(1)).await?),
        "newsletterDelete" => done(socket.newsletter_delete(s(0)).await?),
        "fetchAccountReachoutTimelock" => {
            timelock(&socket.fetch_account_reachout_timelock().await?)
        }
        "fetchNewChatMessageCap" => socket.fetch_new_chat_message_cap().await?,
        method => panic!("unknown method {method}"),
    })
}
fn config(listener: &TcpListener) -> Config {
    Config {
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
    }
}
fn store() -> SqliteAuthStore {
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100:7@s.whatsapp.net".into(),
        ..Default::default()
    });
    store.save_credentials(&creds).unwrap();
    store
}
#[tokio::test]
async fn newsletter_operations_match_original_requests_over_encrypted_transport() {
    timeout(Duration::from_secs(20), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let operations = fixture()["operations"].as_array().unwrap().clone();
        assert_eq!(operations.len(), 92);
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
                    let mut expected = binary::decode(&hex(wire.as_str().unwrap())).unwrap();
                    assert!(expected.attrs.shift_remove("id").is_some());
                    assert_eq!(request, expected, "{}", case["method"]);
                    let mut result = binary::decode(&hex(response.as_str().unwrap())).unwrap();
                    result.attrs.insert("id".into(), id);
                    peer.send(result).await;
                }
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        for case in operations {
            while events.try_recv().is_ok() {}
            let result = execute(&socket, &case).await;
            assert_eq!(result.is_err(), case["error"].as_bool().unwrap(), "{case}");
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    // Upstream's plain JS errors carry no status; Boom errors do.
                    let status = case["statusCode"].as_u64().unwrap_or(500);
                    assert_eq!(error.status_code(), Some(status as u16), "{case}");
                    if let Some(message) = case["message"].as_str() {
                        assert_eq!(error.to_string(), message, "{case}");
                    }
                    if let Error::Mex(newsletters::Error::Server { data, .. }) = &error
                        && case.get("statusCode").is_some()
                    {
                        assert_eq!(**data, case["data"], "{case}");
                    }
                    continue;
                }
            };
            let expected = match case["method"].as_str().unwrap() {
                // The raw result stanza is returned; its child count identifies it.
                "newsletterFetchMessages" => json!(
                    binary::decode(&hex(case["responses"][0].as_str().unwrap()))
                        .unwrap()
                        .all_children()
                        .len()
                ),
                _ => case["expected"].clone(),
            };
            assert_eq!(
                json_numbers::canonical(&result),
                json_numbers::canonical(&expected),
                "{case}"
            );
            if case["method"] == "fetchAccountReachoutTimelock" {
                let state = loop {
                    if let Event::ReachoutTimelockUpdated(state) = events.recv().await.unwrap() {
                        break state;
                    }
                };
                assert_eq!(
                    json_numbers::canonical(&timelock(&state)),
                    json_numbers::canonical(&case["events"][0]["data"]["reachoutTimeLock"]),
                    "{case}"
                );
            } else {
                assert_eq!(case["events"], json!([]));
            }
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn newsletter_and_mex_notifications_publish_events_store_mappings_and_ack() {
    timeout(Duration::from_secs(12), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let jid = "120363000000000000@newsletter";
        let message = proto::Message {
            conversation: Some("Plain newsletter text".into()),
            ..Default::default()
        };
        let plaintext = message.encode_to_vec();
        let mex = |body: Value, operation: &str| {
            BinaryNode::new("notification")
                .attr("from", "s.whatsapp.net")
                .attr("type", "mex")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("update")
                        .attr("op_name", operation)
                        .with_content(NodeContent::Bytes(body.to_string().into_bytes())),
                ]))
        };
        let notifications = vec![
            BinaryNode::new("notification")
                .attr("id", "N1")
                .attr("from", jid)
                .attr("type", "newsletter")
                .attr("participant", "300@lid")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("reaction")
                        .attr("message_id", "175")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("reaction")
                                .with_content(NodeContent::Bytes("👍".as_bytes().to_vec())),
                        ])),
                    BinaryNode::new("view")
                        .attr("message_id", "175")
                        .with_content(NodeContent::Bytes(b"42".to_vec())),
                    BinaryNode::new("participant")
                        .attr("jid", "400@lid")
                        .attr("action", "promote")
                        .attr("role", "ADMIN"),
                    BinaryNode::new("update").with_content(NodeContent::Children(vec![
                        BinaryNode::new("settings").with_content(NodeContent::Children(vec![
                            BinaryNode::new("name")
                                .with_content(NodeContent::Bytes(b"Renamed".to_vec())),
                        ])),
                    ])),
                    BinaryNode::new("message")
                        .attr("message_id", "M1")
                        .attr("t", "1700000000")
                        .with_content(NodeContent::Children(vec![
                            BinaryNode::new("plaintext").with_content(NodeContent::Bytes(plaintext)),
                        ])),
                ])),
            mex(
                json!({"data": {"xwa2_notify_account_reachout_timelock": {
                    "is_active": true, "time_enforcement_ends": "1700000500", "enforcement_type": "BIZ_QUALITY"}}}),
                "NotificationUserReachoutTimelockUpdate",
            )
            .attr("id", "N2"),
            mex(
                json!({"data": {"xwa2_notify_new_chat_messages_capping_info_update": {"total_quota": 100, "used_quota": 99}}}),
                "MessageCappingInfoNotification",
            )
            .attr("id", "N3"),
            mex(
                json!({"data": {"xwa2_notify_linked_profiles": {"jid": "900@lid", "added_profiles": ["200@s.whatsapp.net"]}}}),
                "NotificationLinkedProfilesUpdates",
            )
            .attr("id", "N4"),
            // A malformed payload fails processing but is still acknowledged.
            mex(Value::Null, "NotificationUserReachoutTimelockUpdate").attr("id", "N5"),
        ];
        let sent = notifications.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            for notification in sent {
                let id = notification.attrs["id"].clone();
                peer.send(notification).await;
                let ack = peer.read().await;
                assert_eq!(ack.tag, "ack");
                assert_eq!(ack.attrs["id"], id);
                assert_eq!(ack.attrs["class"], "notification");
            }
            let _ = done_tx.send(());
            peer.closed().await;
        });
        let store = store();
        let (socket, mut events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut seen = vec![];
        while seen.len() < 8 {
            match events.recv().await.unwrap() {
                Event::NewsletterReaction {
                    id,
                    server_id,
                    code,
                } => {
                    assert_eq!(id.as_deref(), Some(jid));
                    assert_eq!(server_id.as_deref(), Some("175"));
                    assert_eq!(code.as_deref(), Some("👍"));
                    seen.push("reaction");
                }
                Event::NewsletterView { count, .. } => {
                    assert_eq!(count, 42.0);
                    seen.push("view");
                }
                Event::NewsletterParticipantsUpdated {
                    author,
                    user,
                    new_role,
                    action,
                    ..
                } => {
                    assert_eq!(author.as_deref(), Some("300@lid"));
                    assert_eq!(user.as_deref(), Some("400@lid"));
                    assert_eq!(new_role.as_deref(), Some("ADMIN"));
                    assert_eq!(action.as_deref(), Some("promote"));
                    seen.push("participant");
                }
                Event::NewsletterSettingsUpdated { id, update } => {
                    assert_eq!(id.as_deref(), Some(jid));
                    assert_eq!(update, json!({"name": "Renamed"}));
                    seen.push("settings");
                }
                Event::MessagesUpserted(upsert) => {
                    let key = upsert.messages[0].key.clone().unwrap();
                    assert_eq!(key.remote_jid.as_deref(), Some(jid));
                    assert_eq!(key.id.as_deref(), Some("M1"));
                    assert_eq!(key.from_me, Some(false));
                    assert_eq!(upsert.messages[0].message, Some(message.clone()));
                    assert_eq!(upsert.messages[0].message_timestamp, Some(1_700_000_000));
                    seen.push("message");
                }
                Event::ReachoutTimelockUpdated(state) => {
                    assert_eq!(
                        state,
                        ReachoutTimelock {
                            is_active: true,
                            time_enforcement_ends: Some(1_700_000_500_000.0),
                            enforcement_type: "BIZ_QUALITY".into(),
                        }
                    );
                    seen.push("timelock");
                }
                Event::MessageCappingUpdated(payload) => {
                    assert_eq!(payload, json!({"total_quota": 100, "used_quota": 99}));
                    seen.push("capping");
                }
                Event::LidMappingUpdated { lid, pn } => {
                    assert_eq!(lid.as_deref(), Some("900@lid"));
                    assert_eq!(pn, "200@s.whatsapp.net");
                    seen.push("mapping");
                }
                _ => {}
            }
        }
        assert_eq!(
            seen,
            [
                "reaction",
                "view",
                "participant",
                "settings",
                "message",
                "timelock",
                "capping",
                "mapping"
            ]
        );
        done_rx.await.unwrap();
        let stored = store
            .get(KeyCategory::LidMapping, &["200".to_owned()])
            .unwrap();
        assert!(
            matches!(stored.get("200"), Some(KeyValue::LidMapping(lid)) if lid == "900"),
            "{stored:?}"
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn newsletter_picture_update_sends_a_square_jpeg_in_the_update_request() {
    timeout(Duration::from_secs(20), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let (checked_tx, checked_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let mut request = peer.read().await;
            request.attrs.shift_remove("id");
            let body: Value =
                serde_json::from_slice(request.child_buffer("query").unwrap()).unwrap();
            let picture = body["variables"]["updates"]["picture"].as_str().unwrap();
            // Apart from the picture itself the request is an ordinary metadata update.
            let update = Update {
                picture: Some(picture.into()),
                ..Default::default()
            };
            let mut expected = newsletters::query(
                &update.variables("123@newsletter"),
                newsletters::query_id::UPDATE_METADATA,
            );
            expected.attrs.shift_remove("id");
            assert_eq!(request, expected);
            let jpeg = STANDARD.decode(picture).unwrap();
            let decoded =
                image::load_from_memory_with_format(&jpeg, image::ImageFormat::Jpeg).unwrap();
            assert_eq!((decoded.width(), decoded.height()), (640, 640));
            // The wide red source is cropped to the centre and scaled up.
            let pixel = decoded.to_rgb8().get_pixel(320, 320).0;
            assert!(
                pixel[0] > 200 && pixel[1] < 60 && pixel[2] < 60,
                "{pixel:?}"
            );
            checked_tx.send(()).unwrap();
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut png = std::io::Cursor::new(Vec::new());
        image::RgbImage::from_pixel(64, 16, image::Rgb([230, 20, 20]))
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        let input = baileys_rs::media::Input::Bytes(png.into_inner().into());
        // The server never answers; the request it received is what is under test.
        let pending = tokio::spawn({
            let socket = socket.clone();
            async move {
                socket
                    .newsletter_update_picture("123@newsletter", &input)
                    .await
            }
        });
        checked_rx.await.unwrap();
        pending.abort();
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
