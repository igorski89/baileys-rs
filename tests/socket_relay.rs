#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::*,
    binary::{BinaryNode, NodeContent},
    crypto::{self, KeyPair},
    jid, messages, proto, relay,
    signal::{self, direct, group},
    socket::{Config, RelayOptions, ServerProps, Socket},
};
use prost::Message as _;
use socket_peer::{Peer, ROOT_PRIVATE};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn store(temp: &tempfile::TempDir) -> SqliteAuthStore {
    let store = SqliteAuthStore::open(temp.path()).unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    let mut creds: Credentials = serde_json::from_value(fixture["empty"].clone()).unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/connection.json")).unwrap();
    let node =
        baileys_rs::binary::decode(&hex(fixture["pair"][0]["stanza"].as_str().unwrap())).unwrap();
    baileys_rs::connection::configure_successful_pairing(&node, &creds)
        .unwrap()
        .apply(&mut creds);
    store.save_credentials(&creds).unwrap();
    store
}
fn receiver() -> (SqliteAuthStore, direct::Repository, direct::Bundle) {
    let store = SqliteAuthStore::in_memory().unwrap();
    let c = store.load_credentials().unwrap();
    let bundle = direct::Bundle {
        registration_id: c.registration_id.into(),
        identity: crypto::signal_public_key(&c.signed_identity_key.public)
            .unwrap()
            .to_vec(),
        signed: direct::PublicPreKey {
            id: c.signed_pre_key.key_id,
            public: crypto::signal_public_key(&c.signed_pre_key.key_pair.public)
                .unwrap()
                .to_vec(),
        },
        signature: c.signed_pre_key.signature.clone(),
        pre_key: None,
    };
    (store.clone(), direct::Repository::new(store), bundle)
}
async fn listener() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        connect_timeout: Duration::from_secs(2),
        query_timeout: Duration::from_secs(2),
        initialize_session: false,
        ..Default::default()
    };
    (listener, config)
}
fn options(id: &str) -> RelayOptions {
    RelayOptions {
        message_id: Some(id.into()),
        ..Default::default()
    }
}
fn plaintext(repo: &direct::Repository, me: &str, enc: &BinaryNode) -> proto::Message {
    let kind = if enc.attrs["type"] == "pkmsg" {
        direct::MessageType::PreKey
    } else {
        direct::MessageType::Signal
    };
    let Some(NodeContent::Bytes(bytes)) = &enc.content else {
        panic!("missing bytes")
    };
    let bytes = repo.decrypt(me, kind, bytes).unwrap();
    proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap()
}
fn group_query_result(request: &BinaryNode) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("id", &request.attrs["id"])
        .attr("type", "result")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("group")
                .attr("id", "group")
                .attr("subject", "Synthetic")
                .attr("addressing_mode", "lid")
                .attr("creation", "0")
                .attr("s_t", "0")
                .with_content(NodeContent::Children(vec![
                    BinaryNode::new("participant").attr("jid", "800@lid"),
                    BinaryNode::new("ephemeral").attr("expiration", "60"),
                ])),
        ]))
}

#[tokio::test]
async fn relay_discovers_devices_encrypts_direct_group_status_retry_and_peer_and_issues_tokens() {
    timeout(Duration::from_secs(12), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let me = store
            .load_credentials()
            .unwrap()
            .me_id()
            .unwrap()
            .to_owned();
        let me_lid = "987654:7@lid";
        let (bob_store, bob, bob_bundle) = receiver();
        let (_, own, own_bundle) = receiver();
        let (_, phone, phone_bundle) = receiver();
        let sending = direct::Repository::new(store.clone());
        sending
            .lid_mapping()
            .store_mappings(&[signal::mapping::Mapping {
                pn: "200@s.whatsapp.net".into(),
                lid: "800@lid".into(),
            }])
            .unwrap();
        sending.inject_session("800:1@lid", &bob_bundle).unwrap();
        sending.inject_session("987654:2@lid", &own_bundle).unwrap();
        sending.inject_session("987654@lid", &phone_bundle).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        store
            .set(&[KeyMutation::put(
                "800@lid",
                KeyValue::Tctoken(TcToken {
                    token: vec![5, 6],
                    timestamp: Some(now.into()),
                    ..Default::default()
                }),
            )])
            .unwrap();
        let (listener, mut config) = listener().await;
        let patch_calls = std::sync::Arc::new(std::sync::Mutex::new(vec![]));
        let observed = patch_calls.clone();
        config.patch_message_before_sending =
            relay::MessagePatch::new(move |message, recipients| {
                observed
                    .lock()
                    .unwrap()
                    .push(recipients.map(<[String]>::to_vec));
                if recipients == Some(["800@lid".to_owned()].as_slice()) {
                    let mut reporting = message.clone();
                    reporting
                        .message_context_info
                        .get_or_insert_with(Default::default)
                        .message_secret = Some(vec![3; 32]);
                    return Ok(relay::Patch::PerRecipient(vec![relay::PatchedMessage {
                        recipient_jid: Some("800@lid".into()),
                        message: reporting,
                    }]));
                }
                Ok(relay::Patch::Shared(Box::new(message.clone())))
            });
        let content = proto::Message {
            conversation: Some("Network relay".into()),
            ..Default::default()
        };
        let expected = content.clone();
        let routing = store.load_credentials().unwrap().routing_info.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(
                peer.payload.username,
                Some(jid::Jid::parse(&me).unwrap().user.parse::<u64>().unwrap())
            );
            assert_eq!(peer.payload.device, Some(7));
            assert_eq!(peer.routing, routing);
            peer.send(BinaryNode::new("success").attr("lid", me_lid))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let discovery = peer.read().await;
            let usync = discovery.child("usync").unwrap();
            assert_eq!(usync.attrs["context"], "message");
            let ids = usync
                .child("list")
                .unwrap()
                .all_children()
                .iter()
                .map(|u| u.attrs["jid"].as_str())
                .collect::<Vec<_>>();
            assert_eq!(ids, ["987654@lid", "800@lid"]);
            let devices = |jid: &str, ids: &[u16]| {
                BinaryNode::new("user")
                    .attr("jid", jid)
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("devices").with_content(NodeContent::Children(vec![
                            BinaryNode::new("device-list").with_content(NodeContent::Children(
                                ids.iter()
                                    .map(|id| {
                                        BinaryNode::new("device")
                                            .attr("id", id.to_string())
                                            .attr("key-index", "1")
                                    })
                                    .collect(),
                            )),
                        ])),
                    ]))
            };
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &discovery.attrs["id"])
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("usync").with_content(NodeContent::Children(vec![
                            BinaryNode::new("list").with_content(NodeContent::Children(vec![
                                devices("987654@lid", &[7, 2]),
                                devices("800@lid", &[1]),
                            ])),
                        ])),
                    ])),
            )
            .await;
            let direct = peer.read().await;
            assert_eq!(direct.attrs["id"], "DIRECT");
            assert_eq!(direct.attrs["to"], "800@lid");
            assert!(direct.child("reporting").is_none());
            assert_eq!(direct.child_buffer("tctoken"), Some([5, 6].as_slice()));
            let participants = direct.child("participants").unwrap().all_children();
            assert_eq!(participants.len(), 2);
            assert_eq!(participants[0].attrs["jid"], "987654:2@lid");
            assert_eq!(
                plaintext(&own, &me, participants[0].child("enc").unwrap()),
                relay::device_sent_message("800@lid", &expected)
            );
            assert_eq!(
                plaintext(&bob, &me, participants[1].child("enc").unwrap()),
                expected
            );
            let issuance = peer.read().await;
            assert_eq!(issuance.attrs["xmlns"], "privacy");
            let token = issuance.child("tokens").unwrap().child("token").unwrap();
            assert_eq!(token.attrs["jid"], "200@s.whatsapp.net");
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &issuance.attrs["id"])
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("tokens").with_content(NodeContent::Children(vec![
                            BinaryNode::new("token")
                                .attr("jid", "200@s.whatsapp.net")
                                .attr("t", &token.attrs["t"])
                                .attr("type", "trusted_contact")
                                .with_content(NodeContent::Bytes(vec![7, 8])),
                        ])),
                    ])),
            )
            .await;
            let repeat = peer.read().await;
            assert_eq!(repeat.attrs["id"], "DIRECT2");
            assert_eq!(repeat.child_buffer("tctoken"), Some([7, 8].as_slice()));
            let participants = repeat.child("participants").unwrap().all_children();
            assert_eq!(
                plaintext(&own, &me, participants[0].child("enc").unwrap()),
                relay::device_sent_message("800@lid", &expected)
            );
            assert_eq!(
                plaintext(&bob, &me, participants[1].child("enc").unwrap()),
                expected
            );
            let groups = group::Repository::new(bob_store);
            let group_sender = signal::SenderKeyName::new("group@g.us", me_lid).unwrap();
            for id in ["GROUP1", "GROUP2"] {
                let query = peer.read().await;
                assert_eq!(query.attrs["xmlns"], "w:g2");
                peer.send(group_query_result(&query)).await;
                let stanza = peer.read().await;
                assert_eq!(stanza.attrs["id"], id);
                assert_eq!(stanza.attrs["addressing_mode"], "lid");
                assert_eq!(stanza.attrs["expiration"], "60");
                assert!(stanza.child("tctoken").is_none());
                if id == "GROUP1" {
                    let distribution = plaintext(
                        &bob,
                        &me,
                        stanza.child("participants").unwrap().all_children()[0]
                            .child("enc")
                            .unwrap(),
                    )
                    .sender_key_distribution_message
                    .unwrap();
                    groups
                        .process_distribution(
                            &group_sender,
                            distribution
                                .axolotl_sender_key_distribution_message
                                .as_deref()
                                .unwrap(),
                        )
                        .unwrap();
                } else {
                    assert!(stanza.child("participants").is_none());
                }
                let bytes = groups
                    .decrypt(&group_sender, stanza.child_buffer("enc").unwrap())
                    .unwrap();
                assert_eq!(
                    proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap(),
                    expected
                );
            }
            let status = peer.read().await;
            assert_eq!(status.attrs["id"], "STATUS");
            let name = signal::SenderKeyName::new(jid::STORIES_JID, me_lid).unwrap();
            let distribution = plaintext(
                &bob,
                &me,
                status.child("participants").unwrap().all_children()[0]
                    .child("enc")
                    .unwrap(),
            )
            .sender_key_distribution_message
            .unwrap();
            groups
                .process_distribution(
                    &name,
                    distribution
                        .axolotl_sender_key_distribution_message
                        .as_deref()
                        .unwrap(),
                )
                .unwrap();
            let bytes = groups
                .decrypt(&name, status.child_buffer("enc").unwrap())
                .unwrap();
            assert_eq!(
                proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap(),
                expected
            );
            let retry = peer.read().await;
            assert_eq!(retry.attrs["id"], "RETRY");
            assert_eq!(retry.attrs["to"], "group@g.us");
            assert_eq!(retry.attrs["participant"], "800:1@lid");
            assert_eq!(retry.child("enc").unwrap().attrs["count"], "2");
            let payload = plaintext(&bob, &me, retry.child("enc").unwrap());
            assert!(payload.sender_key_distribution_message.is_some());
            assert_eq!(payload.conversation, expected.conversation);
            assert_eq!(
                plaintext(
                    &bob,
                    &me,
                    retry.child("participants").unwrap().all_children()[0]
                        .child("enc")
                        .unwrap()
                ),
                expected
            );
            assert!(retry.child("device-identity").is_some());
            let peer_message = peer.read().await;
            assert_eq!(peer_message.attrs["category"], "peer");
            assert!(peer_message.child("participants").is_none());
            assert!(peer_message.child("tctoken").is_none());
            let payload = plaintext(&phone, &me, peer_message.child("enc").unwrap());
            assert_eq!(
                payload
                    .device_sent_message
                    .unwrap()
                    .destination_jid
                    .as_deref(),
                Some("987654@lid")
            );
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        assert_eq!(
            socket
                .relay_message("800@lid", content.clone(), options("DIRECT"))
                .await
                .unwrap(),
            "DIRECT"
        );
        let privacy = baileys_rs::privacy::Repository::new(store.clone());
        timeout(Duration::from_secs(2), async {
            loop {
                if privacy.read_index().unwrap() == ["800@lid"] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            privacy.relay_token("800@lid", now).unwrap(),
            Some(vec![7, 8])
        );
        socket
            .relay_message("800@lid", content.clone(), options("DIRECT2"))
            .await
            .unwrap();
        for id in ["GROUP1", "GROUP2"] {
            socket
                .relay_message("group@g.us", content.clone(), options(id))
                .await
                .unwrap();
        }
        let mut status = options("STATUS");
        status.status_jid_list = vec!["800@lid".into()];
        socket
            .relay_message(jid::STORIES_JID, content.clone(), status)
            .await
            .unwrap();
        let mut retry = options("RETRY");
        retry.stanza.retry = Some(relay::Retry {
            jid: "800:1@lid".into(),
            count: 2,
        });
        socket
            .relay_message("group@g.us", content, retry)
            .await
            .unwrap();
        let mut peer = options("PEER");
        peer.stanza
            .additional_attributes
            .insert("category".into(), "peer".into());
        socket
            .relay_message(
                "987654@lid",
                proto::Message {
                    protocol_message: Some(Default::default()),
                    ..Default::default()
                },
                peer,
            )
            .await
            .unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
        let calls = patch_calls.lock().unwrap();
        assert_eq!(calls[0], Some(vec!["800@lid".into()]));
        assert_eq!(calls[1], Some(vec!["987654:2@lid".into()]));
        assert_eq!(calls[2], Some(vec!["800:1@lid".into()]));
    })
    .await
    .unwrap();
}

#[test]
fn server_props_keep_defaults_and_prefer_numeric_keys() {
    let mut props = ServerProps::default();
    assert!(props.privacy_token_on_1_to_1);
    assert!(props.profile_pic_privacy_token);
    assert!(!props.lid_trusted_token_issue_to_lid);
    props.apply(&indexmap::IndexMap::from([
        ("10518".into(), "".into()),
        (
            "privacy_token_sending_on_all_1_on_1_messages".into(),
            "true".into(),
        ),
        ("lid_trusted_token_issue_to_lid".into(), "1".into()),
    ]));
    assert!(!props.privacy_token_on_1_to_1);
    assert!(props.profile_pic_privacy_token);
    assert!(props.lid_trusted_token_issue_to_lid);
    props.apply(&indexmap::IndexMap::from([("9666".into(), "false".into())]));
    assert!(!props.profile_pic_privacy_token);
}

#[tokio::test]
async fn relay_rolls_back_preparation_but_retains_counters_after_transport_failure() {
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let (_, _, bundle) = receiver();
        direct::Repository::new(store.clone())
            .inject_session("800:1@lid", &bundle)
            .unwrap();
        let snapshot = || {
            serde_json::to_value(
                store
                    .get(KeyCategory::Session, &["800_1.1".into()])
                    .unwrap(),
            )
            .unwrap()
        };
        let before = snapshot();
        assert_ne!(before, serde_json::json!({}));
        let (listener, config) = listener().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let probe = peer.read().await;
            assert_eq!(probe.child("probe").unwrap().tag, "probe");
            peer.result(&probe).await;
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut creds = store.load_credentials().unwrap();
        let account = creds.account.take();
        store.save_credentials(&creds).unwrap();
        let mut retry = options("FAILED");
        retry.stanza.retry = Some(relay::Retry {
            jid: "800:1@lid".into(),
            count: 2,
        });
        let message = proto::Message {
            conversation: Some("Failure boundaries".into()),
            ..Default::default()
        };
        let error = socket
            .relay_message("group@g.us", message.clone(), retry.clone())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            baileys_rs::socket::Error::Relay(relay::Error::Identity)
        ));
        assert_eq!(snapshot(), before);
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("type", "get")
                    .with_content(NodeContent::Children(vec![BinaryNode::new("probe")])),
                None,
            )
            .await
            .unwrap();
        creds.account = account;
        store.save_credentials(&creds).unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
        let error = socket
            .relay_message("group@g.us", message, retry)
            .await
            .unwrap_err();
        assert_eq!(error.status_code(), Some(428));
        assert_ne!(snapshot(), before);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn relay_patch_hooks_cover_group_distribution_retry_newsletter_and_rollback() {
    use std::sync::{Arc, Mutex};
    timeout(Duration::from_secs(8), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let me = store
            .load_credentials()
            .unwrap()
            .me_id()
            .unwrap()
            .to_owned();
        let (bob_store, bob, bundle) = receiver();
        let (_, carol, carol_bundle) = receiver();
        let sending = direct::Repository::new(store.clone());
        sending.inject_session("800:1@lid", &bundle).unwrap();
        sending
            .lid_mapping()
            .store_mappings(&[signal::mapping::Mapping {
                pn: "500@s.whatsapp.net".into(),
                lid: "900@lid".into(),
            }])
            .unwrap();
        sending.inject_session("900:1@lid", &carol_bundle).unwrap();
        let calls = Arc::new(Mutex::new(vec![]));
        let observed = calls.clone();
        let reject_distribution = std::sync::atomic::AtomicBool::new(false);
        let (listener, mut config) = listener().await;
        config.patch_message_before_sending =
            relay::MessagePatch::new(move |message, recipients| {
                observed.lock().unwrap().push((
                    message.conversation.clone(),
                    recipients.map(<[String]>::to_vec),
                    message.sender_key_distribution_message.is_some(),
                ));
                if recipients.is_none() {
                    reject_distribution.store(
                        message.conversation.as_deref() == Some("Reject distribution"),
                        std::sync::atomic::Ordering::SeqCst,
                    );
                    if message.conversation.as_deref() == Some("Reject group array") {
                        return Ok(relay::Patch::PerRecipient(vec![]));
                    }
                    let mut patched = message.clone();
                    patched.conversation = Some("Patched group".into());
                    return Ok(relay::Patch::Shared(Box::new(patched)));
                }
                if message.sender_key_distribution_message.is_some() {
                    if reject_distribution.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        return Err(relay::Error::Patch(
                            "synthetic distribution rejection".into(),
                        ));
                    }
                    let mut patched = message.clone();
                    patched.conversation = Some("Patched distribution".into());
                    return Ok(relay::Patch::Shared(Box::new(patched)));
                }
                if recipients == Some(["800:1@lid".to_owned()].as_slice()) {
                    if message.conversation.as_deref() == Some("Reject fanout") {
                        return Err(relay::Error::Patch("synthetic rejection".into()));
                    }
                    return Ok(relay::Patch::PerRecipient(vec![relay::PatchedMessage {
                        recipient_jid: Some("500:1@s.whatsapp.net".into()),
                        message: proto::Message {
                            conversation: Some("Patched extra recipient".into()),
                            ..Default::default()
                        },
                    }]));
                }
                if message.conversation.as_deref() == Some("Array newsletter") {
                    return Ok(relay::Patch::PerRecipient(vec![]));
                }
                let mut patched = message.clone();
                patched.conversation = Some("Patched newsletter".into());
                Ok(relay::Patch::Shared(Box::new(patched)))
            });
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let stanza = peer.read().await;
            assert_eq!(stanza.attrs["id"], "PATCHGROUP");
            let distribution = plaintext(
                &bob,
                &me,
                stanza.child("participants").unwrap().all_children()[0]
                    .child("enc")
                    .unwrap(),
            );
            assert_eq!(
                distribution.conversation.as_deref(),
                Some("Patched distribution")
            );
            let distribution = distribution.sender_key_distribution_message.unwrap();
            let name = signal::SenderKeyName::new("group@g.us", "987654:7@lid").unwrap();
            let groups = group::Repository::new(bob_store);
            groups
                .process_distribution(
                    &name,
                    distribution
                        .axolotl_sender_key_distribution_message
                        .as_deref()
                        .unwrap(),
                )
                .unwrap();
            let bytes = groups
                .decrypt(&name, stanza.child_buffer("enc").unwrap())
                .unwrap();
            let payload = proto::Message::decode(messages::unpad(&bytes).unwrap()).unwrap();
            assert_eq!(payload.conversation.as_deref(), Some("Patched group"));
            let retry = peer.read().await;
            assert_eq!(retry.attrs["id"], "PATCHRETRY");
            assert_eq!(
                plaintext(&bob, &me, retry.child("enc").unwrap())
                    .conversation
                    .as_deref(),
                Some("Original")
            );
            let participant = &retry.child("participants").unwrap().all_children()[0];
            assert_eq!(participant.attrs["jid"], "500:1@s.whatsapp.net");
            assert_eq!(
                plaintext(&carol, &me, participant.child("enc").unwrap())
                    .conversation
                    .as_deref(),
                Some("Patched extra recipient")
            );
            let newsletter = peer.read().await;
            assert_eq!(newsletter.attrs["id"], "PATCHNEWS");
            assert_eq!(
                proto::Message::decode(newsletter.child_buffer("plaintext").unwrap())
                    .unwrap()
                    .conversation
                    .as_deref(),
                Some("Patched newsletter")
            );
            let array = peer.read().await;
            assert_eq!(array.attrs["id"], "ARRAYNEWS");
            assert!(array.child_buffer("plaintext").unwrap().is_empty());
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let mut metadata = baileys_rs::groups::extract_metadata(&group_query_result(
            &BinaryNode::new("iq").attr("id", "cached"),
        ))
        .unwrap();
        metadata.participants[0].id = Some("800:1@lid".into());
        let mut group = options("PATCHGROUP");
        group.cached_group_metadata = Some(metadata);
        let original = proto::Message {
            conversation: Some("Original".into()),
            ..Default::default()
        };
        socket
            .relay_message("group@g.us", original.clone(), group.clone())
            .await
            .unwrap();
        let session_before = serde_json::to_value(
            store
                .get(KeyCategory::Session, &["800_1.1".into(), "900_1.1".into()])
                .unwrap(),
        )
        .unwrap();
        let group_before = serde_json::to_value(
            store
                .get(
                    KeyCategory::SenderKey,
                    &[signal::SenderKeyName::new("group@g.us", "987654:7@lid")
                        .unwrap()
                        .to_string()],
                )
                .unwrap(),
        )
        .unwrap();
        let rejected = proto::Message {
            conversation: Some("Reject group array".into()),
            ..Default::default()
        };
        assert!(matches!(
            socket
                .relay_message("group@g.us", rejected, group)
                .await
                .unwrap_err(),
            baileys_rs::socket::Error::Relay(relay::Error::Patch(_))
        ));
        let mut status = options("REJECTEDSTATUS");
        status.status_jid_list = vec!["800:1@lid".into()];
        let rejected = proto::Message {
            conversation: Some("Reject distribution".into()),
            ..Default::default()
        };
        let status_name = signal::SenderKeyName::new(jid::STORIES_JID, "987654:7@lid")
            .unwrap()
            .to_string();
        assert!(
            store
                .get(KeyCategory::SenderKey, std::slice::from_ref(&status_name))
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            socket
                .relay_message(jid::STORIES_JID, rejected, status)
                .await
                .unwrap_err(),
            baileys_rs::socket::Error::Relay(relay::Error::Patch(_))
        ));
        assert!(
            store
                .get(KeyCategory::SenderKey, &[status_name])
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .get(KeyCategory::SenderKeyMemory, &[jid::STORIES_JID.into()])
                .unwrap()
                .is_empty()
        );
        let mut retry = options("PATCHRETRY");
        retry.stanza.retry = Some(relay::Retry {
            jid: "800:1@lid".into(),
            count: 1,
        });
        let rejected = proto::Message {
            conversation: Some("Reject fanout".into()),
            ..Default::default()
        };
        assert!(matches!(
            socket
                .relay_message("group@g.us", rejected, retry.clone())
                .await
                .unwrap_err(),
            baileys_rs::socket::Error::Relay(relay::Error::Patch(_))
        ));
        assert_eq!(
            serde_json::to_value(
                store
                    .get(KeyCategory::Session, &["800_1.1".into(), "900_1.1".into()])
                    .unwrap()
            )
            .unwrap(),
            session_before
        );
        assert_eq!(
            serde_json::to_value(
                store
                    .get(
                        KeyCategory::SenderKey,
                        &[signal::SenderKeyName::new("group@g.us", "987654:7@lid")
                            .unwrap()
                            .to_string()]
                    )
                    .unwrap()
            )
            .unwrap(),
            group_before
        );
        socket
            .relay_message("group@g.us", original.clone(), retry)
            .await
            .unwrap();
        socket
            .relay_message("42@newsletter", original, options("PATCHNEWS"))
            .await
            .unwrap();
        socket
            .relay_message(
                "42@newsletter",
                proto::Message {
                    conversation: Some("Array newsletter".into()),
                    ..Default::default()
                },
                options("ARRAYNEWS"),
            )
            .await
            .unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 9);
        assert_eq!(calls[0].1, None);
        assert_eq!(calls[1].1, Some(vec!["800:1@lid".into()]));
        assert!(calls[1].2);
        assert_eq!(calls[2].1, None);
        assert_eq!(calls[3].1, None);
        assert!(calls[4].2);
        assert_eq!(calls[7].1, Some(vec![]));
        assert_eq!(calls[8].1, Some(vec![]));
    })
    .await
    .unwrap();
}
