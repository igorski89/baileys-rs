#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{self, BinaryNode},
    crypto::KeyPair,
    messages::own::UpsertKind,
    proto,
    socket::{Config, Event, Socket},
};
use prost::Message;
use serde_json::Value;
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
// A bare JID stands for a key holding only the sender of the invite.
fn key(value: &Value) -> proto::MessageKey {
    let text = |name: &str| value[name].as_str().map(str::to_owned);
    match value.as_str() {
        Some(jid) => proto::MessageKey {
            remote_jid: Some(jid.into()),
            ..Default::default()
        },
        None => proto::MessageKey {
            remote_jid: text("remoteJid"),
            from_me: value["fromMe"].as_bool(),
            id: text("id"),
            participant: text("participant"),
        },
    }
}
#[tokio::test]
async fn v4_invite_acceptance_matches_original_requests_updates_and_stub_messages() {
    timeout(Duration::from_secs(20), async {
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
        let fixture: Value = serde_json::from_str(include_str!("fixtures/invite-v4.json")).unwrap();
        let me: Contact = serde_json::from_value(fixture["me"].clone()).unwrap();
        let store = SqliteAuthStore::in_memory().unwrap();
        let mut creds = store.load_credentials().unwrap();
        creds.me = Some(me.clone());
        store.save_credentials(&creds).unwrap();
        let cases = fixture["cases"].as_array().unwrap().clone();
        assert_eq!(cases.len(), 40);
        let served = cases.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            // Keep the account's LID, which a bare success would reset.
            peer.send(BinaryNode::new("success").attr("lid", "900:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            for case in served {
                for wire in case["requests"].as_array().unwrap() {
                    let mut request = peer.read().await;
                    let id = request.attrs.shift_remove("id").unwrap();
                    let expected = binary::decode(&hex(wire.as_str().unwrap())).unwrap();
                    assert_eq!(request, expected, "{case}");
                    let mut result =
                        binary::decode(&hex(case["response"].as_str().unwrap())).unwrap();
                    result.attrs.insert("id".into(), id);
                    peer.send(result).await;
                }
            }
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        for case in cases {
            while events.try_recv().is_ok() {}
            let key = key(&case["key"]);
            let invite = proto::message::GroupInviteMessage::decode(
                hex(case["invite"].as_str().unwrap()).as_slice(),
            )
            .unwrap();
            let result = if case["kind"] == "group" {
                socket.group_accept_invite_v4(&key, &invite).await
            } else {
                socket.community_accept_invite_v4(&key, &invite).await
            };
            // The fixture stubs upsertMessage. Upstream's own getChatId rejects a stub
            // without a chat after the request, the update and the upsert have gone out,
            // and the buffered function still flushes; the same events are expected here.
            if invite.group_jid.is_none() && !case["error"].as_bool().unwrap() {
                assert!(
                    matches!(&result, Err(e) if e.to_string().contains("missing chat JID")),
                    "{result:?} {case}"
                );
            } else {
                assert_eq!(
                    result.is_err(),
                    case["error"].as_bool().unwrap(),
                    "{result:?} {case}"
                );
                let Ok(joined) = &result else { continue };
                assert_eq!(joined.as_deref(), case["expected"].as_str(), "{case}");
            }
            let (mut updates, mut upserts) = (vec![], vec![]);
            // Buffered events are released shortly after the call returns.
            let expected_updates: usize = case["updates"]
                .as_array()
                .unwrap()
                .iter()
                .map(|batch| batch.as_array().unwrap().len())
                .sum();
            let expected_upserts = case["upserts"].as_array().unwrap().len();
            while updates.len() < expected_updates || upserts.len() < expected_upserts {
                let event = timeout(Duration::from_secs(2), events.recv())
                    .await
                    .unwrap_or_else(|_| panic!("missing events for {case}"))
                    .unwrap();
                match event {
                    Event::MessageUpdates(list) => updates.extend(
                        list.into_iter()
                            .map(|update| (update.key, update.info.message.unwrap())),
                    ),
                    Event::MessageContentsUpdated(list) => updates.extend(
                        list.into_iter()
                            .map(|update| (update.key, *update.message.unwrap())),
                    ),
                    Event::MessagesUpserted(upsert) => {
                        assert_eq!(upsert.kind, UpsertKind::Notify);
                        upserts.extend(upsert.messages);
                    }
                    _ => {}
                }
            }
            let wanted: Vec<_> = case["updates"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|batch| batch.as_array().unwrap())
                .map(|update| {
                    let message =
                        proto::Message::decode(hex(update["message"].as_str().unwrap()).as_slice());
                    (self::key(&update["key"]), message.unwrap())
                })
                .collect();
            assert_eq!(updates, wanted, "{case}");
            let wanted: Vec<_> = case["upserts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|upsert| {
                    assert_eq!(upsert["type"], "notify");
                    proto::WebMessageInfo::decode(hex(upsert["wire"].as_str().unwrap()).as_slice())
                        .unwrap()
                })
                .collect();
            assert_eq!(upserts.len(), wanted.len(), "{case}");
            for (mut mine, mut theirs) in upserts.into_iter().zip(wanted) {
                // The message ID and time are generated; check their form, then align them.
                let id = mine.key.as_mut().unwrap().id.take().unwrap();
                assert!(id.starts_with("3EB0") && id.len() == 22, "{id}");
                assert_eq!(theirs.key.as_mut().unwrap().id.take().unwrap(), "FIXED-ID");
                assert!(mine.message_timestamp.take().unwrap() >= 1_700_000_000);
                assert_eq!(theirs.message_timestamp.take(), Some(1_700_000_000));
                // The account is embedded as JSON; field order is not significant.
                let account = |message: &mut proto::WebMessageInfo| -> Contact {
                    let parameters = std::mem::take(&mut message.message_stub_parameters);
                    assert_eq!(parameters.len(), 1);
                    serde_json::from_str(&parameters[0]).unwrap()
                };
                assert_eq!(account(&mut mine), account(&mut theirs), "{case}");
                assert_eq!(mine, theirs, "{case}");
            }
        }
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
