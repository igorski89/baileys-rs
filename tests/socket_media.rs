#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::SqliteAuthStore,
    binary::{self, BinaryNode, NodeContent},
    crypto::KeyPair,
    media::MediaConnection,
    socket::{Config, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};

async fn setup() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Config {
        fire_init_queries: false,
        announce_presence_on_connect: false,
        url: format!("ws://{}", listener.local_addr().unwrap()),
        initialize_session: false,
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        query_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    (listener, config)
}
#[tokio::test]
async fn app_state_collection_iq_runs_through_encrypted_socket() {
    timeout(Duration::from_secs(8), async {
        use baileys_rs::{
            app_state::{CollectionRequest, collection_query},
            proto,
        };
        use prost::Message as _;
        let collections = vec![
            CollectionRequest {
                name: "regular".into(),
                version: 12,
                force_snapshot: false,
            },
            CollectionRequest {
                name: "critical_block".into(),
                version: 0,
                force_snapshot: false,
            },
        ];
        let expected = collection_query(&collections);
        let (listener, config) = setup().await;
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            let mut request = peer.read().await;
            let id = request.attrs.shift_remove("id").unwrap();
            assert_eq!(request, expected);
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", id)
                    .attr("type", "result")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("sync").with_content(NodeContent::Children(vec![
                            BinaryNode::new("collection")
                                .attr("name", "regular")
                                .attr("version", "12")
                                .attr("has_more_patches", "true")
                                .with_content(NodeContent::Children(vec![
                                    BinaryNode::new("patch").with_content(NodeContent::Bytes(
                                        proto::SyncdPatch::default().encode_to_vec(),
                                    )),
                                ])),
                        ])),
                    ])),
            )
            .await;
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, SqliteAuthStore::in_memory().unwrap())
            .await
            .unwrap();
        let response = socket
            .query_app_state_collections(&collections, Default::default())
            .await
            .unwrap();
        assert_eq!(response.len(), 1);
        assert!(response["regular"].has_more_patches);
        assert_eq!(
            response["regular"].patches[0]
                .version
                .as_ref()
                .unwrap()
                .version,
            Some(13)
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
fn reply(request: &BinaryNode, ttl: &str, auth: &str, hosts: &[&str]) -> BinaryNode {
    BinaryNode::new("iq")
        .attr("id", &request.attrs["id"])
        .attr("type", "result")
        .with_content(NodeContent::Children(vec![
            BinaryNode::new("media_conn")
                .attr("ttl", ttl)
                .attr("auth", auth)
                .with_content(NodeContent::Children(
                    hosts
                        .iter()
                        .map(|host| {
                            BinaryNode::new("host")
                                .attr("hostname", *host)
                                .attr("maxContentLengthBytes", "1048576")
                        })
                        .collect(),
                )),
        ]))
}
fn check_query(mut node: BinaryNode) {
    assert!(node.attrs.shift_remove("id").is_some());
    assert_eq!(
        binary::encode(&node).unwrap(),
        binary::encode(&MediaConnection::query()).unwrap()
    );
}
#[tokio::test]
async fn media_connection_queries_coalesce_refresh_expire_and_update_host() {
    timeout(Duration::from_secs(8), async {
        let (listener, config) = setup().await;
        let (done, finish) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            assert!(peer.payload.device_pairing_data.is_some());
            assert!(peer.routing.is_none());
            let request = peer.read().await;
            check_query(request.clone());
            tokio::time::sleep(Duration::from_millis(30)).await;
            peer.send(reply(
                &request,
                "60",
                "initial",
                &["first.example", "second.example"],
            ))
            .await;
            let request = peer.read().await;
            check_query(request.clone());
            peer.send(reply(&request, "0", "forced", &["changed.example"]))
                .await;
            let request = peer.read().await;
            check_query(request.clone());
            peer.send(reply(&request, "60", "renewed", &[])).await;
            let probe = peer.read().await;
            assert_eq!(probe.attrs["xmlns"], "probe");
            peer.result(&probe).await;
            peer.closed().await;
            finish.await.unwrap();
        });
        let (socket, _events) = Socket::connect(config, SqliteAuthStore::in_memory().unwrap())
            .await
            .unwrap();
        assert_eq!(socket.media_host().await, "mmg.whatsapp.net");
        let (a, b) = tokio::join!(
            socket.refresh_media_connection(false),
            socket.refresh_media_connection(false)
        );
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(
            socket.refresh_media_connection(false).await.unwrap().auth,
            "initial"
        );
        assert_eq!(socket.media_host().await, "first.example");
        assert_eq!(
            socket.refresh_media_connection(true).await.unwrap().auth,
            "forced"
        );
        assert_eq!(socket.media_host().await, "changed.example");
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(
            socket.refresh_media_connection(false).await.unwrap().auth,
            "renewed"
        );
        assert_eq!(socket.media_host().await, "changed.example");
        socket
            .query(
                BinaryNode::new("iq")
                    .attr("xmlns", "probe")
                    .attr("type", "get"),
                None,
            )
            .await
            .unwrap();
        socket.close().await.unwrap();
        assert!(socket.refresh_media_connection(false).await.is_err());
        done.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn media_connection_rejected_query_is_cached_until_connection_ends() {
    timeout(Duration::from_secs(5), async {
        let (listener, config) = setup().await;
        let (done, finish) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            let request = peer.read().await;
            check_query(request.clone());
            peer.send(
                BinaryNode::new("iq")
                    .attr("id", &request.attrs["id"])
                    .attr("type", "error")
                    .with_content(NodeContent::Children(vec![
                        BinaryNode::new("error")
                            .attr("code", "503")
                            .attr("text", "unavailable"),
                    ])),
            )
            .await;
            finish.await.unwrap();
        });
        let (socket, _events) = Socket::connect(config, SqliteAuthStore::in_memory().unwrap())
            .await
            .unwrap();
        assert_eq!(
            socket
                .refresh_media_connection(false)
                .await
                .unwrap_err()
                .status_code(),
            Some(503)
        );
        assert_eq!(
            socket
                .refresh_media_connection(true)
                .await
                .unwrap_err()
                .status_code(),
            Some(503)
        );
        assert_eq!(socket.media_host().await, "mmg.whatsapp.net");
        assert_eq!(
            socket
                .upload_media(
                    std::path::Path::new("/synthetic/missing"),
                    baileys_rs::media::MediaType::Image,
                    "hash",
                    &Default::default()
                )
                .await
                .unwrap_err()
                .status_code(),
            Some(503)
        );
        socket.close().await.unwrap();
        done.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
fn bytes(v: &serde_json::Value) -> Vec<u8> {
    v.as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|s| u8::from_str_radix(std::str::from_utf8(s).unwrap(), 16).unwrap())
        .collect()
}
fn paired_store() -> SqliteAuthStore {
    let store = SqliteAuthStore::in_memory().unwrap();
    let f: serde_json::Value = serde_json::from_str(include_str!("fixtures/auth.json")).unwrap();
    let mut creds: baileys_rs::auth::Credentials =
        serde_json::from_value(f["empty"].clone()).unwrap();
    let f: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/connection.json")).unwrap();
    baileys_rs::connection::configure_successful_pairing(
        &binary::decode(&bytes(&f["pair"][0]["stanza"])).unwrap(),
        &creds,
    )
    .unwrap()
    .apply(&mut creds);
    store.save_credentials(&creds).unwrap();
    store
}
fn message_fixture() -> baileys_rs::proto::WebMessageInfo {
    use prost::Message as _;
    let f: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_message_retry.json")).unwrap();
    baileys_rs::proto::WebMessageInfo::decode(bytes(&f[0]["input"]).as_slice()).unwrap()
}
fn notification(id: &str, tampered: bool, error: bool) -> BinaryNode {
    let mut n = BinaryNode::new("notification")
        .attr("id", id)
        .attr("from", "server@lid")
        .attr("type", "mediaretry");
    let rmr = BinaryNode::new("rmr")
        .attr("jid", "different@lid")
        .attr("from_me", "true");
    let content = if error {
        BinaryNode::new("error").attr(
            "code",
            (baileys_rs::proto::media_retry_notification::ResultType::NotFound as i32).to_string(),
        )
    } else {
        let f: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/media_message_retry.json")).unwrap();
        let data = &f[0]["event"]["media"];
        let mut cipher = bytes(&data["ciphertext"]["hex"]);
        if tampered {
            cipher[0] ^= 1;
        }
        BinaryNode::new("encrypt").with_content(NodeContent::Children(vec![
            BinaryNode::new("enc_p").with_content(NodeContent::Bytes(cipher)),
            BinaryNode::new("enc_iv").with_content(NodeContent::Bytes(bytes(&data["iv"]["hex"]))),
        ]))
    };
    n = n.with_content(NodeContent::Children(vec![rmr, content]));
    n
}
fn check_request(n: &BinaryNode, me: &str) {
    use prost::Message as _;
    assert_eq!(n.tag, "receipt");
    assert_eq!(n.attrs["type"], "server-error");
    assert_eq!(n.attrs["to"], baileys_rs::jid::normalized_user(me));
    let enc = n.child("encrypt").unwrap();
    let derived = baileys_rs::crypto::hkdf(
        &std::array::from_fn::<_, 32, _>(|i| i as u8),
        32,
        None,
        b"WhatsApp Media Retry Notification",
    )
    .unwrap();
    let plain = baileys_rs::crypto::aes_decrypt_gcm(
        enc.child_buffer("enc_p").unwrap(),
        &derived,
        enc.child_buffer("enc_iv").unwrap(),
        b"ID",
    )
    .unwrap();
    assert_eq!(
        baileys_rs::proto::ServerErrorReceipt::decode(plain.as_slice())
            .unwrap()
            .stanza_id
            .as_deref(),
        Some("ID")
    );
}
#[tokio::test]
async fn reupload_notifications_ack_matching_mutations_errors_timeout_and_close() {
    timeout(Duration::from_secs(8), async {
        let (listener, config) = setup().await;
        let store = paired_store();
        let me = store
            .load_credentials()
            .unwrap()
            .me_id()
            .unwrap()
            .to_owned();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let query = peer.read().await;
            check_query(query.clone());
            peer.send(reply(&query, "60", "AUTH", &["custom.example:8443"]))
                .await;
            check_request(&peer.read().await, &me);
            peer.send_many(&[
                notification("OTHER", false, false),
                notification("ID", false, false),
            ])
            .await;
            for id in ["OTHER", "ID"] {
                let ack = peer.read().await;
                assert_eq!(ack.tag, "ack");
                assert_eq!(ack.attrs["id"], id);
                assert_eq!(ack.attrs["class"], "notification");
                assert_eq!(ack.attrs["type"], "mediaretry");
            }
            for (tampered, error) in [(false, true), (true, false)] {
                check_request(&peer.read().await, &me);
                peer.send(notification("ID", tampered, error)).await;
                assert_eq!(peer.read().await.tag, "ack");
            }
            check_request(&peer.read().await, &me); // timeout, intentionally no response
            check_request(&peer.read().await, &me); // connection-close wait
            peer.ws.close(None).await.unwrap();
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket.refresh_media_connection(false).await.unwrap();
        let original = message_fixture();
        let mut message = original.clone();
        socket
            .update_media_message(&mut message, Some(Duration::from_secs(2)))
            .await
            .unwrap();
        let updated = message
            .message
            .as_ref()
            .unwrap()
            .document_message
            .as_ref()
            .unwrap();
        assert_eq!(updated.direct_path.as_deref(), Some("/new"));
        assert_eq!(
            updated.url.as_deref(),
            Some("https://custom.example:8443/new")
        );
        let mut emitted = false;
        while let Ok(event) = events.try_recv() {
            if let baileys_rs::socket::Event::MessageContentsUpdated(updates) = event {
                assert_eq!(updates[0].key, original.key.clone().unwrap());
                assert_eq!(updates[0].message.as_deref(), message.message.as_ref());
                emitted = true;
            }
        }
        assert!(emitted);
        for expected in [Some(404), None] {
            let mut message = original.clone();
            let error = socket
                .update_media_message(&mut message, Some(Duration::from_secs(2)))
                .await
                .unwrap_err();
            assert_eq!(error.status_code(), expected);
            assert_eq!(message, original);
        }
        let mut message = original.clone();
        assert_eq!(
            socket
                .update_media_message(&mut message, Some(Duration::from_millis(20)))
                .await
                .unwrap_err()
                .status_code(),
            Some(408)
        );
        assert_eq!(message, original);
        assert!(matches!(
            socket.update_media_message(&mut message, None).await,
            Err(baileys_rs::socket::Error::Closed(_))
        ));
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn disabled_automatic_notifications_support_manual_processing_and_updates() {
    timeout(Duration::from_secs(6), async {
        let (listener, mut config) = setup().await;
        config.process_media_notifications = false;
        let store = paired_store();
        let me = store
            .load_credentials()
            .unwrap()
            .me_id()
            .unwrap()
            .to_owned();
        let server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            check_request(&peer.read().await, &me);
            peer.send(notification("ID", false, false)).await;
            assert_eq!(peer.read().await.tag, "ack");
            peer.closed().await;
        });
        let (socket, mut events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        let update_socket = socket.clone();
        let update = tokio::spawn(async move {
            let mut message = message_fixture();
            update_socket
                .update_media_message(&mut message, None)
                .await
                .unwrap();
            message
        });
        loop {
            if let baileys_rs::socket::Event::Node(node) = events.recv().await.unwrap()
                && node.tag == "notification"
            {
                assert!(socket.handle_media_notification(&node).await.unwrap());
                break;
            }
        }
        let message = update.await.unwrap();
        assert_eq!(
            message
                .message
                .unwrap()
                .document_message
                .unwrap()
                .url
                .as_deref(),
            Some("https://mmg.whatsapp.net/new")
        );
        assert!(
            !socket
                .handle_media_notification(&BinaryNode::new("message"))
                .await
                .unwrap()
        );
        socket.close().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn expired_download_requests_phone_reupload_then_streams_refreshed_https_media() {
    timeout(Duration::from_secs(8), async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_rustls::{
            TlsAcceptor,
            rustls::{
                self,
                pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
            },
        };
        let cert = include_bytes!("fixtures/media-test-tls-cert.der");
        let key = include_bytes!("fixtures/media-test-tls-key.der");
        let tls = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(cert.to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.to_vec())),
        )
        .unwrap();
        let refreshed = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let media_host = format!("localhost:{}", refreshed.local_addr().unwrap().port());
        let old = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_url = format!("http://{}/old", old.local_addr().unwrap());
        let f: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/media.json")).unwrap();
        let sample = f["encrypted"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["type"] == "document" && c["length"] == 257)
            .unwrap();
        let encrypted = bytes(&sample["encrypted"]);
        let expected = bytes(&sample["plain"]);
        let old_http = tokio::spawn(async move {
            let (mut socket, _) = old.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
            }
            assert!(String::from_utf8(header).unwrap().starts_with("GET /old "));
            socket
                .write_all(
                    b"HTTP/1.1 404 Missing\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let new_https = tokio::spawn(async move {
            let (socket, _) = refreshed.accept().await.unwrap();
            let mut socket = TlsAcceptor::from(std::sync::Arc::new(tls))
                .accept(socket)
                .await
                .unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
            }
            let header = String::from_utf8(header).unwrap();
            assert!(header.starts_with("GET /new "));
            assert!(
                header
                    .to_ascii_lowercase()
                    .contains("origin: https://web.whatsapp.com\r\n")
            );
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        encrypted.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(&encrypted).await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let (listener, mut config) = setup().await;
        let download = reqwest::Client::builder()
            .no_proxy()
            .add_root_certificate(reqwest::Certificate::from_der(cert).unwrap())
            .build()
            .unwrap();
        config.media_http = Some(baileys_rs::media::HttpClient::with_clients(
            download,
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        ));
        let store = paired_store();
        let me = store
            .load_credentials()
            .unwrap()
            .me_id()
            .unwrap()
            .to_owned();
        let peer_server = tokio::spawn(async move {
            let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
            peer.send(BinaryNode::new("success")).await;
            assert!(peer.read().await.child("unified_session").is_some());
            let query = peer.read().await;
            check_query(query.clone());
            peer.send(reply(&query, "60", "AUTH", &[&media_host])).await;
            check_request(&peer.read().await, &me);
            peer.send(notification("ID", false, false)).await;
            assert_eq!(peer.read().await.tag, "ack");
            peer.closed().await;
        });
        let (socket, _events) = Socket::connect(config, store).await.unwrap();
        socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
        socket.refresh_media_connection(false).await.unwrap();
        let mut message = message_fixture();
        let media = message
            .message
            .as_mut()
            .unwrap()
            .document_message
            .as_mut()
            .unwrap();
        media.direct_path = None;
        media.url = Some(old_url);
        let (mut reader, writer) = tokio::io::duplex(31);
        let read = tokio::spawn(async move {
            let mut plain = Vec::new();
            reader.read_to_end(&mut plain).await.unwrap();
            plain
        });
        assert_eq!(
            socket
                .download_media_message_to(
                    &mut message,
                    &Default::default(),
                    writer,
                    true,
                    Some(Duration::from_secs(2))
                )
                .await
                .unwrap(),
            257
        );
        assert_eq!(read.await.unwrap(), expected);
        assert_eq!(
            message
                .message
                .unwrap()
                .document_message
                .unwrap()
                .direct_path
                .as_deref(),
            Some("/new")
        );
        old_http.await.unwrap();
        new_https.await.unwrap();
        socket.close().await.unwrap();
        peer_server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn published_media_update_is_retained_when_connection_closes_before_waiter_resumes() {
    timeout(Duration::from_secs(6),async {
        let (listener,config)=setup().await;let store=paired_store();let me=store.load_credentials().unwrap().me_id().unwrap().to_owned();
        let (receipt_tx,receipt_rx)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move{let mut peer=Peer::accept(listener.accept().await.unwrap().0).await;peer.send(BinaryNode::new("success")).await;assert!(peer.read().await.child("unified_session").is_some());check_request(&peer.read().await,&me);receipt_tx.send(()).unwrap();assert_eq!(peer.read().await.tag,"ack");peer.closed().await;});
        let (socket,_events)=Socket::connect(config,store).await.unwrap();socket.wait_for_open(Duration::from_secs(2)).await.unwrap();let mut message=message_fixture();
        let mut update=Box::pin(socket.update_media_message(&mut message,None));
        tokio::select!{result=&mut update=>panic!("update completed before receipt: {result:?}"),result=receipt_rx=>result.unwrap()}
        // Keep the update future suspended while the event is published and the socket ends.
        socket.handle_media_notification(&notification("ID",false,false)).await.unwrap();socket.close().await.unwrap();update.await.unwrap();
        assert_eq!(message.message.unwrap().document_message.unwrap().direct_path.as_deref(),Some("/new"));server.await.unwrap();
    }).await.unwrap();
}
