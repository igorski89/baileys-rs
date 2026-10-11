#[path = "support/socket_peer.rs"]
mod socket_peer;
#[path = "support/receipt_helpers.rs"]
mod support;
use baileys_rs::{
    auth::*,
    binary::{self, BinaryNode, NodeContent},
    crypto::KeyPair,
    privacy_settings as settings,
    receipts::{self, Type},
    socket::{Config, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use support::{fixture, hex, keys, node};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
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
fn compare_read(mut received: BinaryNode, wire: &str, kind: &str) {
    let t = received.attrs["t"].parse::<u64>().unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(t.abs_diff(now) < 3);
    received.attrs.insert("t".into(), "1700000000".into());
    let mut expected = binary::decode(&hex(wire)).unwrap();
    expected.attrs.insert("type".into(), kind.into());
    assert_eq!(
        binary::encode(&received).unwrap(),
        binary::encode(&expected).unwrap()
    );
}
#[tokio::test]
async fn socket_receipts_acks_call_rejection_and_cached_privacy_policy() {
    timeout(Duration::from_secs(12), async {
        let temp = tempfile::tempdir().unwrap();
        let store = store(&temp);
        let me = store
            .load_credentials()
            .unwrap()
            .me_id()
            .unwrap()
            .to_owned();
        let routing = store.load_credentials().unwrap().routing_info.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Config {
            url: format!("ws://{}", listener.local_addr().unwrap()),
            noise: baileys_rs::noise::Config {
                certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
                ..Default::default()
            },
            initialize_session: false,
            query_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        let fixture = fixture();
        let server_fixture = fixture.clone();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert_eq!(peer.payload.device, Some(7));
            assert_eq!(peer.routing, routing);
            peer.send(BinaryNode::new("success").attr("lid", "987654:7@lid"))
                .await;
            assert!(peer.read().await.child("unified_session").is_some());
            let sender = server_fixture["receiptCases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| {
                    c["config"]["type"] == "sender" && c["config"]["jid"] == "200@s.whatsapp.net"
                })
                .unwrap();
            assert_eq!(
                binary::encode(&peer.read().await).unwrap(),
                hex(sender["wire"].as_str().unwrap())
            );
            let wires = server_fixture["readCases"][0]["wires"].as_array().unwrap();
            for wire in wires {
                let mut expected = binary::decode(&hex(wire.as_str().unwrap())).unwrap();
                expected.attrs.shift_remove("t");
                expected.attrs.insert("type".into(), "played".into());
                assert_eq!(
                    binary::encode(&peer.read().await).unwrap(),
                    binary::encode(&expected).unwrap()
                );
            }
            let request = peer.read().await;
            let mut normalized = request.clone();
            normalized.attrs.shift_remove("id");
            assert_eq!(
                binary::encode(&normalized).unwrap(),
                hex(server_fixture["privacyQueries"][0].as_str().unwrap())
            );
            let mut response = node(&server_fixture["privacyResponse"]);
            response
                .attrs
                .insert("id".into(), request.attrs["id"].clone());
            response.attrs.insert("type".into(), "result".into());
            peer.send(response).await;
            for wire in wires {
                compare_read(peer.read().await, wire.as_str().unwrap(), "read");
            }
            for update in server_fixture["updates"].as_array().unwrap() {
                let request = peer.read().await;
                let mut normalized = request.clone();
                normalized.attrs.shift_remove("id");
                assert_eq!(
                    binary::encode(&normalized).unwrap(),
                    hex(update["wire"].as_str().unwrap())
                );
                peer.result(&request).await;
            }
            // Updates leave the cached snapshot intact; no privacy query before these receipts.
            for wire in wires {
                compare_read(peer.read().await, wire.as_str().unwrap(), "read");
            }
            let force = peer.read().await;
            assert_eq!(force.attrs["xmlns"], "privacy");
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &force.attrs["id"])
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("privacy").with_content(NodeContent::Children(vec![
                            BinaryNode::new("category")
                                .attr("name", "readreceipts")
                                .attr("value", "none"),
                        ])),
                    ])),
            )
            .await;
            for wire in wires {
                compare_read(peer.read().await, wire.as_str().unwrap(), "read-self");
            }
            for (tag, code) in [("message", None), ("receipt", Some(500))] {
                let incoming = BinaryNode::new(tag)
                    .attr("id", "INCOMING")
                    .attr("from", "group@g.us")
                    .attr("participant", "800@lid")
                    .attr("recipient", "200@lid")
                    .attr("type", "unknown");
                assert_eq!(
                    peer.read().await,
                    receipts::acknowledgement(&incoming, code, Some(&me))
                );
            }
            let reject = peer.read().await;
            let mut normalized = reject.clone();
            normalized.attrs.shift_remove("id");
            let mut expected =
                binary::decode(&hex(server_fixture["rejectCall"].as_str().unwrap())).unwrap();
            expected.attrs.insert("from".into(), me);
            assert_eq!(
                binary::encode(&normalized).unwrap(),
                binary::encode(&expected).unwrap()
            );
            peer.result(&reject).await;
            let failed = peer.read().await;
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &failed.attrs["id"])
                    .attr("type", "error")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error").attr("code", "403"),
                    ])),
            )
            .await;
            for wire in wires {
                compare_read(peer.read().await, wire.as_str().unwrap(), "read-self");
            }
            peer.closed().await;
        });
        let (socket, _) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        assert_eq!(
            socket
                .send_receipt("200@lid", None, &[], None)
                .await
                .unwrap_err()
                .status_code(),
            Some(500)
        );
        socket
            .send_receipt(
                "200@s.whatsapp.net",
                Some("800:1@lid"),
                &["ONE".into(), "TWO".into(), "TWO".into()],
                Some(Type::Sender),
            )
            .await
            .unwrap();
        let keys = keys(&fixture["keys"])[..5].to_vec();
        socket
            .send_receipts(&keys, Some(Type::Played))
            .await
            .unwrap();
        socket.read_messages(&keys).await.unwrap();
        assert_eq!(
            socket
                .fetch_privacy_settings(false)
                .await
                .unwrap()
                .get("readreceipts")
                .unwrap()
                .as_deref(),
            Some("all")
        );
        for update in fixture["updates"].as_array().unwrap() {
            let value = update["value"].as_str().unwrap();
            match update["method"].as_str().unwrap() {
                "updateMessagesPrivacy" => socket
                    .update_messages_privacy(if value == "all" {
                        settings::Messages::All
                    } else {
                        settings::Messages::Contacts
                    })
                    .await
                    .unwrap(),
                "updateCallPrivacy" => socket
                    .update_call_privacy(if value == "all" {
                        settings::Call::All
                    } else {
                        settings::Call::Known
                    })
                    .await
                    .unwrap(),
                "updateOnlinePrivacy" => socket
                    .update_online_privacy(if value == "all" {
                        settings::Online::All
                    } else {
                        settings::Online::MatchLastSeen
                    })
                    .await
                    .unwrap(),
                "updateReadReceiptsPrivacy" => socket
                    .update_read_receipts_privacy(if value == "all" {
                        settings::ReadReceipts::All
                    } else {
                        settings::ReadReceipts::None
                    })
                    .await
                    .unwrap(),
                "updateGroupsAddPrivacy" => socket
                    .update_groups_add_privacy(match value {
                        "all" => settings::GroupAdd::All,
                        "contacts" => settings::GroupAdd::Contacts,
                        _ => settings::GroupAdd::ContactBlacklist,
                    })
                    .await
                    .unwrap(),
                method => {
                    let value = match value {
                        "all" => settings::Value::All,
                        "contacts" => settings::Value::Contacts,
                        "contact_blacklist" => settings::Value::ContactBlacklist,
                        _ => settings::Value::None,
                    };
                    match method {
                        "updateLastSeenPrivacy" => {
                            socket.update_last_seen_privacy(value).await.unwrap()
                        }
                        "updateProfilePicturePrivacy" => {
                            socket.update_profile_picture_privacy(value).await.unwrap()
                        }
                        "updateStatusPrivacy" => socket.update_status_privacy(value).await.unwrap(),
                        _ => unreachable!(),
                    }
                }
            }
        }
        socket.read_messages(&keys).await.unwrap();
        socket.fetch_privacy_settings(true).await.unwrap();
        socket.read_messages(&keys).await.unwrap();
        for (tag, code) in [("message", None), ("receipt", Some(500))] {
            socket
                .send_message_ack(
                    &BinaryNode::new(tag)
                        .attr("id", "INCOMING")
                        .attr("from", "group@g.us")
                        .attr("participant", "800@lid")
                        .attr("recipient", "200@lid")
                        .attr("type", "unknown"),
                    code,
                )
                .await
                .unwrap();
        }
        socket.reject_call("CALL", "800@lid").await.unwrap();
        assert_eq!(
            socket
                .fetch_privacy_settings(true)
                .await
                .unwrap_err()
                .status_code(),
            Some(403)
        );
        socket.read_messages(&keys).await.unwrap();
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
