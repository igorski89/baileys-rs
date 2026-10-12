use baileys_rs::{auth::Contact, history::*, media, proto};
use prost::Message as _;
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn fixtures() -> Value {
    serde_json::from_str(include_str!("fixtures/history.json")).unwrap()
}
#[test]
fn history_privacy_tokens_match_original_snapshot_comparisons_and_index_order() {
    use baileys_rs::{
        auth::{KeyCategory, KeyMutation, KeyStore, KeyValue, SqliteAuthStore, TcToken},
        privacy,
        signal::mapping::{Mapping, Store},
    };
    for case in fixtures()["tokenCases"].as_array().unwrap() {
        let store = SqliteAuthStore::in_memory().unwrap();
        Store::new(store.clone())
            .store_mappings(&[Mapping {
                pn: "200@s.whatsapp.net".into(),
                lid: "201@lid".into(),
            }])
            .unwrap();
        for (id, v) in case["initial"].as_object().unwrap() {
            let mut v = v.clone();
            v["token"] = json!(unhex(v["token"].as_str().unwrap()));
            let entry: TcToken = serde_json::from_value(v).unwrap();
            store
                .set(&[KeyMutation::put(id, KeyValue::Tctoken(entry))])
                .unwrap();
        }
        let chats = case["chats"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| Chat {
                conversation: proto::Conversation::decode(unhex(c.as_str().unwrap()).as_slice())
                    .unwrap(),
                last_message_recv_timestamp: None,
                runtime_fields: Default::default(),
            })
            .collect::<Vec<_>>();
        privacy::Repository::new(store.clone())
            .store_from_history(&chats)
            .unwrap();
        let ids = case["expected"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let values = store.get(KeyCategory::Tctoken, &ids).unwrap();
        let mut output = json!({});
        for (id, value) in values {
            let KeyValue::Tctoken(entry) = value else {
                panic!()
            };
            let mut value = serde_json::to_value(&entry).unwrap();
            value["token"] = json!(
                entry
                    .token
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            );
            output[id] = value;
        }
        assert_eq!(
            output, case["expected"],
            "{} old={}",
            case["variant"], case["timestamp"]
        );
    }
}
#[test]
fn history_privacy_write_failure_is_suppressed_but_read_failure_propagates() {
    use baileys_rs::{
        auth::{KeyCategory, KeyStore, SqliteAuthStore},
        privacy,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteAuthStore::open(dir.path()).unwrap();
    let db = rusqlite::Connection::open(store.path().unwrap()).unwrap();
    db.execute_batch("CREATE TRIGGER reject_history_tokens BEFORE INSERT ON signal_keys WHEN NEW.category='tctoken' BEGIN SELECT RAISE(ABORT,'synthetic history token failure'); END;").unwrap();
    let chats = [Chat {
        conversation: proto::Conversation {
            id: "200@s.whatsapp.net".into(),
            tc_token: Some(vec![1]),
            tc_token_timestamp: Some(100),
            ..Default::default()
        },
        last_message_recv_timestamp: None,
        runtime_fields: Default::default(),
    }];
    let privacy = privacy::Repository::new(store.clone());
    assert!(privacy.store_from_history(&chats).unwrap().is_empty());
    assert!(
        store
            .get(
                KeyCategory::Tctoken,
                &["200@s.whatsapp.net".into(), "__index".into()]
            )
            .unwrap()
            .is_empty()
    );
    db.execute_batch("DROP TRIGGER reject_history_tokens;")
        .unwrap();
    db.execute(
        "INSERT INTO signal_keys(category,id,data) VALUES ('tctoken','200@s.whatsapp.net',?1)",
        [b"invalid-json".as_slice()],
    )
    .unwrap();
    assert!(privacy.store_from_history(&chats).is_err());
}
#[test]
fn extraction_message_previews_receipts_contacts_and_mutations_match_upstream() {
    for case in fixtures()["cases"].as_array().unwrap() {
        let mut input =
            proto::HistorySync::decode(unhex(case["input"].as_str().unwrap()).as_slice()).unwrap();
        let result = process(&mut input);
        assert_eq!(
            result.is_err(),
            case["error"].is_string(),
            "{}:{}",
            case["syncType"],
            case["variant"]
        );
        assert_eq!(
            input,
            proto::HistorySync::decode(unhex(case["mutated"].as_str().unwrap()).as_slice())
                .unwrap()
        );
        if let Ok(result) = result {
            let e = &case["expected"];
            let contacts: Vec<Contact> = serde_json::from_value(e["contacts"].clone()).unwrap();
            assert_eq!(result.contacts, contacts);
            assert_eq!(result.sync_type, e["syncType"].as_i64().unwrap() as i32);
            assert_eq!(result.progress, e["progress"].as_u64().map(|v| v as u32));
            assert_eq!(
                result.messages,
                e["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(
                        |v| proto::WebMessageInfo::decode(unhex(v.as_str().unwrap()).as_slice())
                            .unwrap()
                    )
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                serde_json::to_value(&result.lid_pn_mappings).unwrap(),
                e["mappings"]
            );
            assert_eq!(
                result.past_participants,
                e["past"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(
                        |v| proto::PastParticipants::decode(unhex(v.as_str().unwrap()).as_slice())
                            .unwrap()
                    )
                    .collect::<Vec<_>>()
            );
            assert_eq!(result.chats.len(), e["chats"].as_array().unwrap().len());
            for (chat, e) in result.chats.iter().zip(e["chats"].as_array().unwrap()) {
                assert_eq!(
                    chat.conversation,
                    proto::Conversation::decode(unhex(e["wire"].as_str().unwrap()).as_slice())
                        .unwrap()
                );
                assert_eq!(chat.last_message_recv_timestamp, e["received"].as_f64());
            }
        }
    }
}
struct Split<'a> {
    bytes: &'a [u8],
    size: usize,
}
impl Read for Split<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let count = out.len().min(self.bytes.len()).min(self.size);
        out[..count].copy_from_slice(&self.bytes[..count]);
        self.bytes = &self.bytes[count..];
        Ok(count)
    }
}
#[test]
fn strict_zlib_inflation_matches_node_at_stream_boundaries() {
    for case in fixtures()["inflation"].as_array().unwrap() {
        let bytes = unhex(case["input"].as_str().unwrap());
        for size in [1, 7, 65536] {
            let result = inflate(Split {
                bytes: &bytes,
                size,
            });
            assert_eq!(
                result.is_err(),
                case["error"].is_string(),
                "{}:{} split{size}",
                case["size"],
                case["variant"]
            );
            if let Ok(output) = result {
                assert_eq!(output, unhex(case["output"].as_str().unwrap()));
            }
        }
    }
}
#[tokio::test]
async fn inline_history_precedes_transport_and_policy_preserves_source_defaults() {
    let input = unhex(fixtures()["cases"][0]["input"].as_str().unwrap());
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&input).unwrap();
    let compressed = encoder.finish().unwrap();
    let downloader = Downloader::new(|_| async { panic!("inline must bypass the transport") });
    let data = downloader
        .download_and_process(proto::message::HistorySyncNotification {
            initial_hist_bootstrap_inline_payload: Some(compressed),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(data.chats.len(), 1);
    assert_eq!(data.messages.len(), 3);
    assert!(matches!(
        downloader
            .download_and_process(proto::message::HistorySyncNotification {
                initial_hist_bootstrap_inline_payload: Some(vec![]),
                ..Default::default()
            })
            .await,
        Err(Error::Truncated)
    ));
    for sync_type in [None, Some(0), Some(2), Some(6), Some(99)] {
        let n = proto::message::HistorySyncNotification {
            sync_type,
            ..Default::default()
        };
        assert_eq!(
            Policy::default().should_process(&n),
            matches!(sync_type, Some(0 | 6))
        );
        assert_eq!(
            Policy::new(|_| true).should_process(&n),
            matches!(sync_type, Some(0 | 2 | 6))
        );
    }
    let n = proto::message::HistorySyncNotification {
        chunk_order: Some(0),
        ..Default::default()
    };
    let message = proto::Message {
        protocol_message: Some(Box::new(proto::message::ProtocolMessage {
            history_sync_notification: Some(n.clone()),
            ..Default::default()
        })),
        ..Default::default()
    };
    let wrapped = proto::Message {
        ephemeral_message: Some(Box::new(proto::message::FutureProofMessage {
            message: Some(Box::new(message)),
        })),
        ..Default::default()
    };
    assert_eq!(notification(Some(&wrapped)), Some(n));
}
#[tokio::test]
async fn encrypted_https_history_inflates_without_buffering_compressed_download() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        use tokio_rustls::{
            TlsAcceptor,
            rustls::{
                self,
                pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
            },
        };
        let cert = include_bytes!("fixtures/media-test-tls-cert.der");
        let key = include_bytes!("fixtures/media-test-tls-key.der");
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("localhost:{}", listener.local_addr().unwrap().port());
        let mut history = proto::HistorySync::decode(
            unhex(fixtures()["cases"][0]["input"].as_str().unwrap()).as_slice(),
        )
        .unwrap();
        history.conversations[0].messages[0]
            .message
            .as_mut()
            .unwrap()
            .message = Some(proto::Message {
            conversation: Some("history-body".repeat(20000)),
            ..Default::default()
        });
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(&history.encode_to_vec()).unwrap();
        let compressed = zlib.finish().unwrap();
        let keys = media::Keys::derive(&[9; 32], media::MediaType::History).unwrap();
        let mut encrypted = vec![];
        media::encrypt_stream(compressed.as_slice(), &mut encrypted, &keys, None).unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = TlsAcceptor::from(Arc::new(tls))
                .accept(socket)
                .await
                .unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
            }
            let header = String::from_utf8(header).unwrap();
            assert!(header.starts_with("GET /history "));
            assert!(
                header
                    .to_lowercase()
                    .contains("origin: https://web.whatsapp.com")
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
            for chunk in encrypted.chunks(13) {
                socket.write_all(chunk).await.unwrap();
            }
            socket.shutdown().await.unwrap();
        });
        let client = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_der(cert).unwrap())
            .build()
            .unwrap();
        let downloader = Downloader::from_http(
            media::HttpClient::with_clients(client.clone(), client),
            media::DownloadOptions {
                host: Some(host),
                ..Default::default()
            },
        );
        let downloaded = downloader
            .download(proto::message::HistorySyncNotification {
                media_key: Some(vec![9; 32]),
                direct_path: Some("/history".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(downloaded, history);
        server.await.unwrap();
    })
    .await
    .unwrap();
}
