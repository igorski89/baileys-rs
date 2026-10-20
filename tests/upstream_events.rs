use baileys_rs::{
    app_state::{ChatLock, LabelAssociation, LabelEdit, SettingUpdate},
    calls, history,
    messages::{self, own},
    newsletters::ReachoutTimelock,
    proto, receipts,
    socket::{ConnectionState, Event, presence},
};
use serde_json::{Value, json};

/// Every variant is named here, so a new one has to choose its upstream event.
fn expected(event: &Event) -> &'static [&'static str] {
    match event {
        Event::Connection(_)
        | Event::Online(_)
        | Event::Qr(_)
        | Event::PairingComplete
        | Event::PendingNotifications
        | Event::ReachoutTimelockUpdated(_) => &["connection.update"],
        Event::CredentialsUpdated => &["creds.update"],
        Event::Node(_) => &[],
        Event::PresenceUpdated(_) => &["presence.update"],
        Event::Calls(_) => &["call"],
        Event::MediaUpdated(_) => &["messages.media-update"],
        Event::ContactsUpdated(_) => &["contacts.update"],
        Event::ContactsUpserted(_) => &["contacts.upsert"],
        Event::MessagesDeleted(_) => &["messages.delete"],
        Event::MessagesStarred(_)
        | Event::MessageStubsUpdated(_)
        | Event::MessageContentsUpdated(_)
        | Event::MessagesUpdated(_)
        | Event::MessageUpdates(_)
        | Event::MessageMutations(_)
        | Event::EventResponsesUpdated(_) => &["messages.update"],
        Event::ChatsDeleted(_) => &["chats.delete"],
        Event::LabelEdited(_) => &["labels.edit"],
        Event::LabelAssociationUpdated(_) => &["labels.association"],
        Event::SettingUpdated(_) => &["settings.update"],
        Event::LidMappingUpdated { .. } => &["lid-mapping.update"],
        Event::ChatLocked(_) => &["chats.lock"],
        Event::GroupsUpdated(_) => &["groups.update"],
        Event::GroupsUpserted(_) => &["groups.upsert"],
        Event::BlocklistUpdated { .. } => &["blocklist.update"],
        Event::NewsletterReaction { .. } => &["newsletter.reaction"],
        Event::NewsletterView { .. } => &["newsletter.view"],
        Event::NewsletterParticipantsUpdated { .. } => &["newsletter-participants.update"],
        Event::NewsletterSettingsUpdated { .. } => &["newsletter-settings.update"],
        Event::MessageCappingUpdated(_) => &["message-capping.update"],
        Event::GroupEffects(_) => &["groups.update"],
        Event::MessageReceiptsUpdated(_) => &["message-receipt.update"],
        Event::MessagesUpserted(_) => &["messages.upsert"],
        Event::MessageReactions(_) => &["messages.reaction"],
        Event::ChatsUpdated(_) => &["chats.update"],
        Event::ChatsUpserted(_) => &["chats.upsert"],
        Event::MessagingHistorySet(_) => &["messaging-history.set"],
        Event::MessagingHistoryStatus(_) => &["messaging-history.status"],
    }
}

fn key(id: &str) -> proto::MessageKey {
    proto::MessageKey {
        remote_jid: Some("200@s.whatsapp.net".into()),
        from_me: Some(false),
        id: Some(id.into()),
        participant: None,
    }
}

fn internal_markers(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields
            .iter()
            .any(|(name, v)| name.starts_with("__baileys") || internal_markers(v)),
        Value::Array(values) => values.iter().any(internal_markers),
        _ => false,
    }
}

fn events() -> Vec<Event> {
    let metadata = messages::EnvelopeMetadata {
        key_metadata: messages::KeyMetadata {
            remote_jid_alt: Some("201@lid".into()),
            addressing_mode: "pn".into(),
            ..Default::default()
        },
        category: Some("peer".into()),
        retry_count: Some(2),
        author: "200@s.whatsapp.net".into(),
        sender: "200@s.whatsapp.net".into(),
        protobuf: false,
    };
    let message = proto::WebMessageInfo {
        key: Some(key("M")),
        message_timestamp: Some(10),
        message: Some(proto::Message {
            conversation: Some("hi".into()),
            message_context_info: Some(proto::MessageContextInfo {
                message_secret: Some(vec![1, 2, 3]),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut sync = proto::HistorySync {
        sync_type: 0,
        conversations: vec![proto::Conversation {
            id: "200@s.whatsapp.net".into(),
            messages: vec![proto::HistorySyncMsg {
                message: Some(message.clone()),
                msg_order_id: None,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    vec![
        Event::Connection(ConnectionState::Connecting),
        Event::Connection(ConnectionState::Open),
        Event::Connection(ConnectionState::Closed { code: 401 }),
        Event::Online(true),
        Event::Qr("ref,key".into()),
        Event::PairingComplete,
        Event::PendingNotifications,
        Event::ReachoutTimelockUpdated(ReachoutTimelock {
            is_active: true,
            time_enforcement_ends: Some(1_800_000_000_000.0),
            enforcement_type: "DEFAULT".into(),
        }),
        Event::CredentialsUpdated,
        Event::Node(baileys_rs::binary::BinaryNode::new("iq")),
        Event::PresenceUpdated(presence::Update {
            id: Some("200@s.whatsapp.net".into()),
            presences: [(
                "200@s.whatsapp.net".to_string(),
                presence::Data {
                    last_known_presence: "composing".into(),
                    last_seen: None,
                    group_online_count: None,
                },
            )]
            .into_iter()
            .collect(),
        }),
        Event::Calls(vec![calls::Event {
            chat_id: Some("200@s.whatsapp.net".into()),
            from: Some("200@s.whatsapp.net".into()),
            caller_pn: None,
            is_group: Some(false),
            group_jid: None,
            id: "CALL".into(),
            date_ms: Some(1_800_000_000_123),
            is_video: Some(false),
            status: calls::Status::Offer,
            offline: false,
            latency_ms: None,
        }]),
        Event::MediaUpdated(vec![baileys_rs::media::RetryEvent {
            key: key("M"),
            media: Some(baileys_rs::media::RetryCiphertext {
                ciphertext: vec![1, 2],
                iv: vec![3],
            }),
            error: None,
        }]),
        Event::ContactsUpdated(vec![baileys_rs::auth::Contact {
            id: "200@s.whatsapp.net".into(),
            notify: Some("Alice".into()),
            ..Default::default()
        }]),
        Event::MessagesDeleted(vec![key("M")]),
        Event::ChatsDeleted(vec![Some("200@s.whatsapp.net".into())]),
        Event::LabelEdited(LabelEdit {
            id: Some("1".into()),
            name: Some("Work".into()),
            color: Some(2),
            deleted: Some(false),
            predefined_id: None,
        }),
        Event::LabelAssociationUpdated(LabelAssociation {
            add: true,
            is_chat: true,
            chat_id: Some("200@s.whatsapp.net".into()),
            label_id: Some("1".into()),
            message_id: None,
        }),
        Event::SettingUpdated(SettingUpdate::Locale("en".into())),
        Event::LidMappingUpdated {
            lid: Some("201@lid".into()),
            pn: "200@s.whatsapp.net".into(),
        },
        Event::ChatLocked(ChatLock {
            id: Some("200@s.whatsapp.net".into()),
            locked: true,
        }),
        Event::BlocklistUpdated {
            blocklist: vec![Some("200@s.whatsapp.net".into())],
            add: false,
        },
        Event::NewsletterReaction {
            id: Some("1@newsletter".into()),
            server_id: Some("7".into()),
            code: Some("👍".into()),
        },
        Event::NewsletterView {
            id: Some("1@newsletter".into()),
            server_id: Some("7".into()),
            count: f64::NAN,
        },
        Event::MessageCappingUpdated(json!({"total_quota":1})),
        Event::MessagesUpdated(vec![receipts::MessageUpdate {
            key: key("M"),
            status: proto::web_message_info::Status::Read,
            message_timestamp: 25.0,
        }]),
        Event::MessageReceiptsUpdated(vec![receipts::UserUpdate {
            key: key("M"),
            user_jid: "300@s.whatsapp.net".into(),
            receipt_timestamp: Some(20.0),
            read_timestamp: None,
        }]),
        Event::MessagesUpserted(own::Upsert {
            messages: vec![message],
            kind: own::UpsertKind::Notify,
            request_id: None,
            metadata: vec![metadata],
            metadata_indices: vec![],
            runtime_fields: vec![],
        }),
        Event::ChatsUpdated(vec![own::ChatUpdate {
            id: "200@s.whatsapp.net".into(),
            name: Some(None),
            ephemeral_expiration: Some(Some(86400)),
            ..Default::default()
        }]),
        Event::MessagingHistorySet(history::Set {
            data: history::process(&mut sync).unwrap(),
            is_latest: Some(true),
            chunk_order: None,
            peer_data_request_session_id: None,
            buffered: true,
        }),
        Event::MessagingHistoryStatus(history::StatusUpdate {
            sync_type: 3,
            status: history::ProgressState::Complete,
            explicit: true,
        }),
    ]
}

#[test]
fn every_event_maps_to_its_upstream_name_without_internal_markers() {
    for event in events() {
        let entries = event.upstream().unwrap();
        let names: Vec<_> = entries.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, expected(&event), "{event:?}");
        for (name, value) in &entries {
            assert!(!internal_markers(value), "{name}: {value}");
        }
    }
}

fn single(event: Event) -> Value {
    let mut entries = event.upstream().unwrap();
    assert_eq!(entries.len(), 1);
    entries.remove(0).1
}

#[test]
fn payloads_follow_json_stringify_of_the_upstream_values() {
    let all = events();
    let find = |name: &str| {
        all.iter()
            .find_map(|e| {
                let mut entries = e.upstream().unwrap();
                (entries.first().map(|(n, _)| *n) == Some(name)).then(|| entries.remove(0).1)
            })
            .unwrap()
    };
    let upsert = find("messages.upsert");
    let message = &upsert["messages"][0];
    assert_eq!(upsert["type"], "notify");
    assert_eq!(message["category"], "peer");
    assert_eq!(message["retryCount"], 2);
    assert_eq!(message["key"]["remoteJidAlt"], "201@lid");
    assert_eq!(message["key"]["addressingMode"], "pn");
    assert!(message["key"].get("isViewOnce").is_none());
    assert_eq!(message["message"]["conversation"], "hi");
    // Bytes in protobuf messages are base64, as protobufjs' toJSON writes them.
    assert_eq!(
        message["message"]["messageContextInfo"]["messageSecret"],
        "AQID"
    );
    assert_eq!(
        baileys_rs::proto_json::bytes_to_value(&[1]),
        json!({"type":"Buffer","data":[1]})
    );

    let call = find("call");
    assert_eq!(call[0]["date"], "2027-01-15T08:00:00.123Z");
    assert_eq!(call[0]["status"], "offer");
    assert!(call[0].get("dateMs").is_none());

    assert_eq!(
        single(Event::Connection(ConnectionState::Closed { code: 401 }))["lastDisconnect"]["error"]
            ["output"]["statusCode"],
        401
    );
    assert_eq!(single(Event::Qr("ref".into())), json!({"qr":"ref"}));
    assert_eq!(
        find("presence.update"),
        json!({"id":"200@s.whatsapp.net","presences":{"200@s.whatsapp.net":{"lastKnownPresence":"composing"}}})
    );
    assert_eq!(
        find("messages.media-update")[0]["media"]["ciphertext"],
        json!({"type":"Buffer","data":[1,2]})
    );
    assert_eq!(
        find("blocklist.update"),
        json!({"blocklist":["200@s.whatsapp.net"],"type":"remove"})
    );
    assert_eq!(
        find("newsletter.reaction"),
        json!({"id":"1@newsletter","server_id":"7","reaction":{"code":"👍","count":1}})
    );
    assert_eq!(find("newsletter.view")["count"], Value::Null);
    assert_eq!(
        find("connection.update"),
        json!({"connection":"connecting","receivedPendingNotifications":false})
    );
    let lock = all
        .iter()
        .find(|e| matches!(e, Event::ReachoutTimelockUpdated(_)))
        .unwrap()
        .clone();
    assert_eq!(
        single(lock),
        json!({"reachoutTimeLock":{"isActive":true,"timeEnforcementEnds":"2027-01-15T08:00:00.000Z","enforcementType":"DEFAULT"}})
    );
    // An undefined name is dropped; a disabled expiration would be null.
    assert_eq!(
        find("chats.update"),
        json!([{"id":"200@s.whatsapp.net","ephemeralExpiration":86400}])
    );
    assert_eq!(
        find("messaging-history.status"),
        json!({"syncType":3,"status":"complete","explicit":true})
    );
}

/// Keys of received messages carry upstream's runtime properties wherever
/// upstream spreads `message.key` (captured from a live session).
#[test]
fn received_keys_keep_their_runtime_properties() {
    let metadata = messages::KeyMetadata {
        remote_jid_alt: Some("41767906626@s.whatsapp.net".into()),
        addressing_mode: "lid".into(),
        ..Default::default()
    };
    let key = |id: &str| proto::MessageKey {
        remote_jid: Some("238092300603504@lid".into()),
        from_me: Some(false),
        id: Some(id.into()),
        participant: None,
    };
    let lid_key = |id: &str| {
        json!({"remoteJid":"238092300603504@lid","fromMe":false,"id":id,
            "remoteJidAlt":"41767906626@s.whatsapp.net","addressingMode":"lid"})
    };
    let chat = single(Event::ChatsUpdated(vec![own::ChatUpdate {
        id: "238092300603504@lid".into(),
        messages: Some(vec![proto::WebMessageInfo {
            key: Some(key("M")),
            ..Default::default()
        }]),
        message_key_metadata: Some(Box::new(metadata.clone())),
        ..Default::default()
    }]));
    assert_eq!(chat[0]["messages"][0]["message"]["key"], lid_key("M"));
    let mutations = single(Event::MessageMutations(vec![
        own::Mutation::Revoke {
            key: key("TARGET"),
            original_key: key("REVOKE"),
            key_metadata: Some(metadata.clone()),
        },
        own::Mutation::Edit {
            key: key("EDITED"),
            message: Box::default(),
            timestamp: 1.0,
            key_metadata: Some(metadata.clone()),
        },
    ]));
    assert_eq!(mutations[0]["key"], lid_key("TARGET"));
    assert_eq!(mutations[0]["update"]["key"], lid_key("REVOKE"));
    assert_eq!(mutations[1]["key"], lid_key("EDITED"));
    let reaction = single(Event::MessageReactions(vec![own::ReactionUpdate {
        key: Some(key("T")),
        reaction: proto::Reaction {
            key: Some(key("R")),
            text: Some("👍".into()),
            sender_timestamp_ms: Some(1792506612077),
            ..Default::default()
        },
        key_metadata: Some(metadata),
    }]));
    assert_eq!(
        reaction[0]["reaction"]["senderTimestampMs"],
        json!({"low":1505249645,"high":417,"unsigned":false})
    );
    // isViewOnce appears only when true.
    assert_eq!(reaction[0]["reaction"]["key"], lid_key("R"));
}

/// A set processMessage emits outside a buffer spreads processHistoryMessage's
/// result, as a live session showed it.
#[test]
fn unbuffered_history_sets_have_the_direct_shape() {
    let Event::MessagingHistorySet(mut set) = events()
        .into_iter()
        .find(|e| matches!(e, Event::MessagingHistorySet(_)))
        .unwrap()
    else {
        unreachable!()
    };
    set.buffered = false;
    let value = single(Event::MessagingHistorySet(set));
    let keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
    assert_eq!(
        keys,
        [
            "chats",
            "contacts",
            "messages",
            "lidPnMappings",
            "pastParticipants",
            "syncType",
            "progress",
            "isLatest",
            "chunkOrder",
            "peerDataRequestSessionId"
        ]
    );
}

/// The buffered history set, as a live session showed it: unset
/// notification fields are null, empty past participants are absent and
/// chats keep only Conversation fields.
#[test]
fn history_sets_have_the_buffered_shape() {
    let set = events()
        .into_iter()
        .find_map(|e| match e {
            Event::MessagingHistorySet(_) => Some(single(e)),
            _ => None,
        })
        .unwrap();
    let fields = set.as_object().unwrap();
    assert_eq!(fields["chunkOrder"], Value::Null);
    assert_eq!(fields["peerDataRequestSessionId"], Value::Null);
    assert!(fields.contains_key("progress"));
    assert!(!fields.contains_key("pastParticipants"));
    assert!(!fields["chats"].as_array().unwrap().is_empty());
    for chat in fields["chats"].as_array().unwrap() {
        assert!(chat.get("lastMessageRecvTimestamp").is_none(), "{chat}");
    }
}
