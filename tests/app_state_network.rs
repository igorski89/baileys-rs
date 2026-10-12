use baileys_rs::{
    app_state::*,
    binary::{BinaryNode, NodeContent},
    media, proto,
};
use prost::Message as _;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn unhex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn snapshot() -> proto::SyncdSnapshot {
    proto::SyncdSnapshot {
        version: Some(proto::SyncdVersion { version: Some(9) }),
        records: vec![],
        mac: Some(vec![1]),
        key_id: Some(proto::KeyId { id: Some(vec![2]) }),
    }
}
#[tokio::test]
async fn collection_extraction_matches_original_source() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/app_state_network.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let logged = calls.clone();
        let downloader = BlobDownloader::new(move |reference| {
            logged.lock().unwrap().push(reference.direct_path.unwrap());
            async { Ok(snapshot().encode_to_vec()) }
        });
        let input = serde_json::from_value(case["input"].clone()).unwrap();
        let collections = extract_collections(&input, &downloader).await.unwrap();
        assert_eq!(
            collections.len(),
            case["expected"].as_object().unwrap().len()
        );
        for (name, c) in collections {
            let expected = &case["expected"][name];
            assert_eq!(
                c.has_more_patches,
                expected["hasMorePatches"].as_bool().unwrap()
            );
            assert_eq!(
                c.snapshot.map(|v| v.encode_to_vec()),
                expected["snapshot"].as_str().map(unhex)
            );
            assert_eq!(
                c.patches
                    .iter()
                    .map(|p| p.encode_to_vec())
                    .collect::<Vec<_>>(),
                expected["patches"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|p| unhex(p.as_str().unwrap()))
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(
            serde_json::to_value(&*calls.lock().unwrap()).unwrap(),
            case["downloads"]
        );
    }
    let downloader = BlobDownloader::new(|_| async { panic!("no downloads expected") });
    assert!(
        extract_collections(&BinaryNode::new("iq"), &downloader)
            .await
            .unwrap()
            .is_empty()
    );
    let query = collection_query(&[
        CollectionRequest {
            name: "regular".into(),
            version: 0,
            force_snapshot: false,
        },
        CollectionRequest {
            name: "critical_block".into(),
            version: u64::MAX,
            force_snapshot: true,
        },
        CollectionRequest {
            name: "regular_low".into(),
            version: 7,
            force_snapshot: false,
        },
    ]);
    assert_eq!(query.attrs["xmlns"], "w:sync:app:state");
    let nodes = query
        .child("sync")
        .unwrap()
        .children("collection")
        .collect::<Vec<_>>();
    assert_eq!(
        nodes
            .iter()
            .map(|n| n.attrs["return_snapshot"].as_str())
            .collect::<Vec<_>>(),
        ["true", "true", "false"]
    );
    assert_eq!(nodes[1].attrs["version"], u64::MAX.to_string());
}

#[tokio::test]
async fn external_patch_order_errors_and_hash_break_preserve_source_behavior() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/app_state_codec.json")).unwrap();
    let provider = KeyProvider::new(|_| async {
        Ok(Some(proto::message::AppStateSyncKeyData {
            key_data: Some(vec![7; 32]),
            ..Default::default()
        }))
    });
    for variant in ["good", "firstAggregate", "firstPatchMac"] {
        let case = fixtures["batches"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["variant"] == variant && c["validate"] == true && c["minimum"].is_null())
            .unwrap();
        let mut patches: Vec<_> = case["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| proto::SyncdPatch::decode(unhex(p.as_str().unwrap()).as_slice()).unwrap())
            .collect();
        let blobs: Vec<_> = patches
            .iter_mut()
            .enumerate()
            .map(|(i, p)| {
                p.external_mutations = Some(proto::ExternalBlobReference {
                    direct_path: Some(i.to_string()),
                    ..Default::default()
                });
                proto::SyncdMutations {
                    mutations: std::mem::take(&mut p.mutations),
                }
                .encode_to_vec()
            })
            .collect();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let logged = calls.clone();
        let downloader = BlobDownloader::new(move |reference| {
            let i: usize = reference.direct_path.unwrap().parse().unwrap();
            logged.lock().unwrap().push(i);
            let data = blobs[i].clone();
            async move { Ok(data) }
        });
        let decoded = decode_patches_with_downloads(
            &mut patches,
            "regular",
            &new_lthash_state(),
            &provider,
            None,
            true,
            &downloader,
        )
        .await
        .unwrap();
        assert_eq!(
            decoded.state.version,
            case["state"]["version"].as_u64().unwrap()
        );
        assert_eq!(
            decoded.state.hash,
            unhex(case["state"]["hash"].as_str().unwrap())
        );
        assert_eq!(
            decoded.mutations.len(),
            case["mutations"].as_object().unwrap().len()
        );
        let reached = decoded.state.version as usize;
        assert_eq!(*calls.lock().unwrap(), (0..reached).collect::<Vec<_>>());
        assert!(patches.iter().all(|p| p.external_mutations.is_some()));
        assert_eq!(
            patches
                .iter()
                .map(|p| p.mutations.len())
                .collect::<Vec<_>>(),
            case["remaining"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .map(|(i, n)| if i < reached {
                    n.as_u64().unwrap() as usize
                } else {
                    0
                })
                .collect::<Vec<_>>()
        );
    }
    let bad = BlobDownloader::new(|_| async { Ok(vec![255]) });
    assert!(matches!(
        bad.download_patch(Default::default()).await,
        Err(Error::Protobuf(_))
    ));
    let mut patches = [proto::SyncdPatch {
        external_mutations: Some(Default::default()),
        ..Default::default()
    }];
    let failed =
        BlobDownloader::new(|_| async { Err(Error::Lookup("network unavailable".into())) });
    assert!(matches!(
        decode_patches_with_downloads(
            &mut patches,
            "regular",
            &new_lthash_state(),
            &provider,
            None,
            true,
            &failed
        )
        .await,
        Err(Error::Lookup(_))
    ));
}

#[tokio::test]
async fn encrypted_external_blob_streams_over_https() {
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
        let plaintext: Vec<_> = (0..200_003).map(|i| (i % 251) as u8).collect();
        let keys = media::Keys::derive(&[8; 32], media::MediaType::AppState).unwrap();
        let mut encrypted = Vec::new();
        media::encrypt_stream(plaintext.as_slice(), &mut encrypted, &keys, None).unwrap();
        let task = tokio::spawn(async move {
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
            assert!(header.starts_with("GET /app-state "));
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
            for chunk in encrypted.chunks(701) {
                socket.write_all(chunk).await.unwrap();
            }
            socket.shutdown().await.unwrap();
        });
        let client = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_der(cert).unwrap())
            .build()
            .unwrap();
        let downloader = BlobDownloader::from_http(
            media::HttpClient::with_clients(client.clone(), client),
            media::DownloadOptions {
                host: Some(host),
                ..Default::default()
            },
        );
        assert_eq!(
            downloader
                .download(proto::ExternalBlobReference {
                    media_key: Some(vec![8; 32]),
                    direct_path: Some("/app-state".into()),
                    ..Default::default()
                })
                .await
                .unwrap(),
            plaintext
        );
        task.await.unwrap();
        assert!(matches!(
            downloader.download(Default::default()).await,
            Err(Error::Media(_))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_collections_replace_in_download_completion_order() {
    let downloader = BlobDownloader::new(|reference| async move {
        if reference.direct_path.as_deref() == Some("slow") {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        Ok(snapshot().encode_to_vec())
    });
    let collection = |path: &str, more: &str| {
        BinaryNode::new("collection")
            .attr("name", "regular")
            .attr("has_more_patches", more)
            .with_content(NodeContent::Children(vec![
                BinaryNode::new("snapshot").with_content(NodeContent::Bytes(
                    proto::ExternalBlobReference {
                        direct_path: Some(path.into()),
                        ..Default::default()
                    }
                    .encode_to_vec(),
                )),
            ]))
    };
    let input = BinaryNode::new("iq").with_content(NodeContent::Children(vec![
        BinaryNode::new("sync").with_content(NodeContent::Children(vec![
            collection("slow", "true"),
            collection("fast", "false"),
        ])),
    ]));
    assert!(extract_collections(&input, &downloader).await.unwrap()["regular"].has_more_patches);
}
