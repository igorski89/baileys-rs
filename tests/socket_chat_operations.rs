#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::{BinaryNode, NodeContent},
    chats::{self, PictureType},
    crypto::KeyPair,
    signal::mapping::{Mapping, Store},
    socket::{Config, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{net::TcpListener, time::timeout};
async fn setup() -> (TcpListener, Config, SqliteAuthStore) {
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
    creds.last_account_sync_timestamp = Some(1699999999);
    store.save_credentials(&creds).unwrap();
    Store::new(store.clone())
        .store_mappings(&[Mapping {
            lid: "2000@lid".into(),
            pn: "200@s.whatsapp.net".into(),
        }])
        .unwrap();
    (listener, config, store)
}
fn reply(request: &BinaryNode, children: Vec<BinaryNode>) -> BinaryNode {
    BinaryNode::new(request.tag.clone())
        .attr("id", &request.attrs["id"])
        .attr("type", "result")
        .with_content(NodeContent::Children(children))
}
#[tokio::test]
async fn dirty_bits_and_profile_block_and_lookup_operations_use_original_stanzas() {
    timeout(Duration::from_secs(10), async {
        let (listener, config, store) = setup().await;
        let (dirty_tx, dirty_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let dirty = |kind: &str| {
                BinaryNode::new("ib")
                    .attr("from", "@s.whatsapp.net")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("dirty")
                            .attr("type", kind)
                            .attr("timestamp", "1700000002"),
                    ]))
            };
            // The account bit is cleaned from the previously stored timestamp.
            peer.send(dirty("account_sync")).await;
            let clean = peer.read().await;
            assert_eq!(
                clean,
                chats::clean_dirty_bits_node(
                    "account_sync",
                    Some("1699999999"),
                    &clean.attrs["id"]
                )
            );
            // The groups bit refreshes participating groups before it is cleaned.
            peer.send(dirty("groups")).await;
            let groups = peer.read().await;
            assert_eq!(groups.attrs["xmlns"], "w:g2");
            assert!(groups.child("participating").is_some());
            peer.send(reply(&groups, vec![BinaryNode::new("groups")]))
                .await;
            let clean = peer.read().await;
            assert_eq!(
                clean,
                chats::clean_dirty_bits_node("groups", None, &clean.attrs["id"])
            );
            dirty_tx.send(()).unwrap();
            let with_id = |expected: BinaryNode, actual: &BinaryNode| {
                assert_eq!(&expected.attr("id", &actual.attrs["id"]), actual);
            };
            let request = peer.read().await;
            with_id(chats::profile_status_query("Busy"), &request);
            peer.result(&request).await;
            let request = peer.read().await;
            with_id(
                chats::profile_picture_update_query(Some("group@g.us"), None),
                &request,
            );
            peer.result(&request).await;
            let request = peer.read().await;
            with_id(chats::profile_picture_update_query(None, None), &request);
            peer.result(&request).await;
            let request = peer.read().await;
            assert_eq!(request.attrs["xmlns"], "blocklist");
            let item = request.child("item").unwrap();
            assert_eq!(item.attrs["action"], "block");
            assert_eq!(item.attrs["jid"], "2000@lid");
            assert_eq!(item.attrs["pn_jid"], "200@s.whatsapp.net");
            peer.result(&request).await;
            let request = peer.read().await;
            let item = request.child("item").unwrap();
            assert_eq!(item.attrs["action"], "unblock");
            assert_eq!(item.attrs["jid"], "2000@lid");
            assert!(!item.attrs.contains_key("pn_jid"));
            peer.result(&request).await;
            let request = peer.read().await;
            with_id(
                chats::profile_picture_url_query(
                    "200:3@s.whatsapp.net",
                    PictureType::Image,
                    vec![],
                ),
                &request,
            );
            peer.send(reply(
                &request,
                vec![BinaryNode::new("picture").attr("url", "https://pps.example/p.jpg")],
            ))
            .await;
            let request = peer.read().await;
            with_id(
                chats::business_profile_query("500@s.whatsapp.net"),
                &request,
            );
            peer.send(reply(
                &request,
                vec![
                    BinaryNode::new("business_profile").with_content(NodeContent::Children(vec![
                        BinaryNode::new("profile")
                            .attr("jid", "500@s.whatsapp.net")
                            .with_content(NodeContent::Children(vec![
                                BinaryNode::new("description")
                                    .with_content(NodeContent::Bytes(b"Shop".to_vec())),
                            ])),
                    ])),
                ],
            ))
            .await;
            let request = peer.read().await;
            assert_eq!(request.tag, "call");
            assert_eq!(request.attrs["to"], "@call");
            let link = request.child("link_create").unwrap();
            assert_eq!(link.attrs["media"], "video");
            assert_eq!(
                link.child("event").unwrap().attrs["start_time"],
                "1700000000"
            );
            peer.send(reply(
                &request,
                vec![BinaryNode::new("link_create").attr("token", "TOKEN")],
            ))
            .await;
            let request = peer.read().await;
            with_id(chats::default_disappearing_mode_query(86400), &request);
            peer.result(&request).await;
            let request = peer.read().await;
            with_id(chats::bot_list_query(), &request);
            peer.send(reply(
                &request,
                vec![
                    BinaryNode::new("bot").with_content(NodeContent::Children(vec![
                        BinaryNode::new("section").attr("type", "all").with_content(
                            NodeContent::Children(vec![
                                BinaryNode::new("bot")
                                    .attr("jid", "1@bot")
                                    .attr("persona_id", "P1"),
                            ]),
                        ),
                    ])),
                ],
            ))
            .await;
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store.clone()).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        dirty_rx.await.unwrap();
        assert_eq!(
            store
                .load_credentials()
                .unwrap()
                .last_account_sync_timestamp,
            Some(1700000002)
        );
        socket.update_profile_status("Busy").await.unwrap();
        socket.remove_profile_picture("group@g.us").await.unwrap();
        socket
            .remove_profile_picture("100@s.whatsapp.net")
            .await
            .unwrap();
        assert_eq!(
            socket
                .remove_profile_picture("")
                .await
                .unwrap_err()
                .status_code(),
            Some(500)
        );
        socket
            .update_block_status("200:4@s.whatsapp.net", true)
            .await
            .unwrap();
        socket.update_block_status("2000@lid", false).await.unwrap();
        // Failures are decided locally, before any stanza is sent.
        for (jid, message) in [
            (
                "300@s.whatsapp.net",
                "Unable to resolve LID for PN JID: 300@s.whatsapp.net",
            ),
            ("301@lid", "Unable to resolve PN JID for LID: 301@lid"),
            ("group@g.us", "Invalid jid: group@g.us"),
        ] {
            let error = socket.update_block_status(jid, true).await.unwrap_err();
            assert_eq!(error.status_code(), Some(400));
            assert_eq!(error.to_string(), message);
        }
        assert_eq!(
            socket
                .profile_picture_url("200:3@s.whatsapp.net", PictureType::Image, None)
                .await
                .unwrap()
                .as_deref(),
            Some("https://pps.example/p.jpg")
        );
        let profile = socket
            .get_business_profile("500@s.whatsapp.net")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(profile.wid.as_deref(), Some("500@s.whatsapp.net"));
        assert_eq!(profile.description, "Shop");
        assert_eq!(
            socket
                .create_call_link(true, Some(1700000000), None)
                .await
                .unwrap()
                .as_deref(),
            Some("TOKEN")
        );
        socket
            .update_default_disappearing_mode(86400)
            .await
            .unwrap();
        let bots = socket.get_bot_list_v2().await.unwrap();
        assert_eq!(bots.len(), 1);
        assert_eq!(bots[0].persona_id.as_deref(), Some("P1"));
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
