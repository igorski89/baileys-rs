use baileys_rs::{defaults, media::*};
use futures_util::future::BoxFuture;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn info(v: &Value) -> MediaConnection {
    MediaConnection {
        hosts: v["hosts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| UploadHost {
                hostname: h["hostname"].as_str().unwrap().into(),
                max_content_length_bytes: h["maxContentLengthBytes"].as_f64().unwrap(),
            })
            .collect(),
        auth: v["auth"].as_str().unwrap().into(),
        ttl_seconds: 60.0,
        fetched_at: SystemTime::UNIX_EPOCH,
    }
}
struct Scripted {
    actions: Mutex<Vec<Value>>,
    requests: Mutex<Vec<UploadRequest>>,
}
impl UploadTransport for Scripted {
    fn upload<'a>(
        &'a self,
        request: UploadRequest,
    ) -> BoxFuture<'a, Result<Option<UploadResponse>, Error>> {
        self.requests.lock().unwrap().push(request);
        let action = self.actions.lock().unwrap().remove(0);
        Box::pin(async move {
            if action.get("error").is_some() {
                Err(Error::HttpStatus(503))
            } else {
                Ok(action
                    .get("result")
                    .map(|v| serde_json::from_value(v.clone()).unwrap()))
            }
        })
    }
}
#[tokio::test]
async fn upload_host_retry_and_auth_refresh_match_original_upstream() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/media_upload.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let transport = Scripted {
            actions: Mutex::new(case["actions"].as_array().unwrap().clone()),
            requests: Mutex::new(Vec::new()),
        };
        let options = UploadOptions {
            custom_hosts: case
                .get("custom")
                .map(|v| info(&serde_json::json!({"hosts":v,"auth":""})).hosts)
                .unwrap_or_default(),
            headers: [
                ("Origin", "override"),
                ("Content-Type", "wrong"),
                ("Custom", "value"),
            ]
            .into_iter()
            .map(|(k, v)| (k.parse().unwrap(), v.parse().unwrap()))
            .collect(),
            timeout: Some(Duration::from_millis(500)),
        };
        let refreshes = Arc::new(Mutex::new(Vec::new()));
        let initial = info(&case["initial"]);
        let updated = info(&case["updated"]);
        let refresh_error = case["refreshError"].as_bool() == Some(true);
        let result = upload_with(
            &transport,
            Path::new("/synthetic/file"),
            MediaType::Image,
            "+/==",
            &options,
            |force| {
                refreshes.lock().unwrap().push(force);
                let conn = if force {
                    updated.clone()
                } else {
                    initial.clone()
                };
                async move {
                    if force && refresh_error {
                        Err(Error::Node("refresh failure"))
                    } else {
                        Ok(conn)
                    }
                }
            },
        )
        .await;
        assert_eq!(
            *refreshes.lock().unwrap(),
            serde_json::from_value::<Vec<bool>>(case["refreshes"].clone()).unwrap(),
            "{case}"
        );
        if case.get("error").is_some() {
            assert_eq!(result.unwrap_err().status_code(), Some(500));
        } else {
            assert_eq!(
                result.unwrap(),
                serde_json::from_value::<UploadResponse>(serde_json::json!({
                    "url":case["result"]["mediaUrl"], "direct_path":case["result"]["directPath"],
                    "meta_hmac":case["result"]["meta_hmac"], "ts":case["result"]["ts"], "fbid":case["result"]["fbid"]
                })).unwrap()
            );
        }
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), case["requests"].as_array().unwrap().len());
        for (actual, expected) in requests.iter().zip(case["requests"].as_array().unwrap()) {
            assert_eq!(actual.url, expected["url"].as_str().unwrap());
            assert_eq!(
                actual.path,
                Path::new(expected["filePath"].as_str().unwrap())
            );
            for (name, value) in expected["headers"].as_object().unwrap() {
                assert_eq!(actual.headers.get(name).unwrap(), value.as_str().unwrap());
            }
            assert_eq!(
                actual.timeout.unwrap().as_millis(),
                expected["timeoutMs"].as_u64().unwrap() as u128
            );
        }
    }
}
#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}
struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    delay: Duration,
}
fn reply(status: u16, body: Vec<u8>) -> Reply {
    Reply {
        status,
        headers: vec![],
        body,
        delay: Duration::ZERO,
    }
}
async fn server(replies: Vec<Reply>) -> (String, tokio::task::JoinHandle<Vec<Request>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            while !bytes.ends_with(b"\r\n\r\n") {
                bytes.push(socket.read_u8().await.unwrap());
                assert!(bytes.len() < 16384);
            }
            let head = String::from_utf8(bytes).unwrap();
            let mut lines = head.split("\r\n");
            let mut first = lines.next().unwrap().split(' ');
            let method = first.next().unwrap().to_owned();
            let path = first.next().unwrap().to_owned();
            let headers: BTreeMap<_, _> = lines
                .filter_map(|s| s.split_once(':'))
                .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
                .collect();
            let mut body = vec![
                0;
                headers
                    .get("content-length")
                    .map(|s| s.parse().unwrap())
                    .unwrap_or(0)
            ];
            socket.read_exact(&mut body).await.unwrap();
            requests.push(Request {
                method,
                path,
                headers,
                body,
            });
            tokio::time::sleep(reply.delay).await;
            let mut head = format!(
                "HTTP/1.1 {} Test\r\nContent-Length: {}\r\nConnection: close\r\n",
                reply.status,
                reply.body.len()
            );
            for (key, value) in reply.headers {
                head += &format!("{key}: {value}\r\n");
            }
            head += "\r\n";
            if socket.write_all(head.as_bytes()).await.is_ok() {
                let _ = socket.write_all(&reply.body).await;
            }
        }
        requests
    });
    (url, task)
}
fn upload_request(url: String, path: &Path) -> UploadRequest {
    UploadRequest {
        url,
        path: path.to_owned(),
        headers: HeaderMap::new(),
        timeout: Some(Duration::from_secs(3)),
    }
}
#[tokio::test]
async fn http_upload_streams_and_replays_post_on_all_redirect_types() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let plain: Vec<_> = (0..1_048_577).map(|i| (i % 251) as u8).collect();
        let file = EncryptedFile::encrypt(&plain[..], MediaType::Document, false).unwrap();
        let encrypted = std::fs::read(file.encrypted.path()).unwrap();
        let mut first = reply(301, Vec::new());
        first.headers.push(("Location", "/second".into()));
        let mut second = reply(303, Vec::new());
        second.headers.push(("Location", "/third".into()));
        let (url, server) = server(vec![
            first,
            second,
            reply(500, br#"{"direct_path":"/accepted"}"#.to_vec()),
        ])
        .await;
        let mut request = upload_request(format!("{url}/first"), file.encrypted.path());
        request
            .headers
            .insert("Content-Length", "1".parse().unwrap());
        request.timeout = Some(Duration::ZERO);
        let response = HttpClient::new()
            .unwrap()
            .post_file(request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.direct_path.as_deref(), Some("/accepted"));
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        for (request, path) in requests.iter().zip(["/first", "/second", "/third"]) {
            assert_eq!(request.method, "POST");
            assert_eq!(request.path, path);
            assert_eq!(request.body, encrypted);
            assert_eq!(
                request.headers["content-length"],
                encrypted.len().to_string()
            );
        }
    })
    .await
    .unwrap();
}
fn hex(v: &Value) -> Vec<u8> {
    v.as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|s| u8::from_str_radix(std::str::from_utf8(s).unwrap(), 16).unwrap())
        .collect()
}
#[tokio::test]
async fn http_download_ranges_and_remote_encryption_use_real_streams() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let f: Value = serde_json::from_str(include_str!("fixtures/media.json")).unwrap();
        let sample = f["encrypted"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["type"] == "image" && e["length"] == 257)
            .unwrap();
        let encrypted = hex(&sample["encrypted"]);
        let keys = Keys::derive(
            &std::array::from_fn::<_, 32, _>(|i| i as u8),
            MediaType::Image,
        )
        .unwrap();
        let client = HttpClient::new().unwrap();
        for case in f["ranges"].as_array().unwrap() {
            let range = Range {
                start_byte: case["range"]["startByte"].as_u64(),
                end_byte: case["range"]["endByte"].as_u64(),
            };
            let plan = DownloadPlan::new(range).unwrap();
            let body = if plan.range_header().is_some() {
                let start = (plan.fetch_start as usize).min(encrypted.len());
                let end = plan
                    .fetch_end
                    .map(|e| (e as usize + 1).min(encrypted.len()))
                    .unwrap_or(encrypted.len());
                encrypted[start.min(end)..end].to_vec()
            } else {
                encrypted.clone()
            };
            let (url, server) = server(vec![reply(206, body)]).await;
            let (mut reader, writer) = tokio::io::duplex(31);
            let read = tokio::spawn(async move {
                let mut out = Vec::new();
                reader.read_to_end(&mut out).await.unwrap();
                out
            });
            let headers = [("Origin".parse().unwrap(), "override".parse().unwrap())]
                .into_iter()
                .collect();
            let result = client
                .download_to(&url, keys.clone(), range, headers, writer)
                .await;
            let out = read.await.unwrap();
            if case.get("error").is_some() {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), out.len() as u64);
                assert_eq!(out, hex(&case["output"]));
            }
            let requests = server.await.unwrap();
            assert_eq!(requests[0].headers["origin"], defaults::DEFAULT_ORIGIN);
            assert_eq!(
                requests[0].headers.get("range"),
                plan.range_header().as_ref()
            );
        }
        let plain: Vec<_> = (0..131_079).map(|i| (i % 251) as u8).collect();
        let (url, server) = server(vec![reply(200, plain.clone())]).await;
        let encrypted = client
            .encrypt_remote(&url, HeaderMap::new(), MediaType::Video, true)
            .await
            .unwrap();
        let keys = Keys::derive(encrypted.media_key.as_ref(), MediaType::Video).unwrap();
        assert_eq!(
            decrypt_verified(&std::fs::read(encrypted.encrypted.path()).unwrap(), &keys).unwrap(),
            plain
        );
        assert_eq!(
            std::fs::read(encrypted.original.as_ref().unwrap().path()).unwrap(),
            plain
        );
        assert_eq!(server.await.unwrap()[0].method, "GET");
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn http_errors_invalid_json_timeout_and_redirect_limit_are_reported() {
    let client = HttpClient::new().unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let (url, task) = server(vec![reply(404, Vec::new())]).await;
    assert_eq!(
        client
            .get(&url, HeaderMap::new())
            .await
            .unwrap_err()
            .status_code(),
        Some(404)
    );
    task.await.unwrap();
    let (url, task) = server(vec![reply(200, b"bad JSON".to_vec())]).await;
    assert!(
        client
            .post_file(upload_request(url, file.path()))
            .await
            .unwrap()
            .is_none()
    );
    task.await.unwrap();
    let mut slow = reply(200, Vec::new());
    slow.delay = Duration::from_millis(200);
    let (url, task) = server(vec![slow]).await;
    let mut request = upload_request(url, file.path());
    request.timeout = Some(Duration::from_millis(20));
    assert!(matches!(client.post_file(request).await,Err(Error::Http(e)) if e.is_timeout()));
    task.abort();
    let replies = (0..6)
        .map(|_| {
            let mut r = reply(302, Vec::new());
            r.headers.push(("Location", "/loop".into()));
            r
        })
        .collect();
    let (url, task) = server(replies).await;
    assert!(matches!(
        client.post_file(upload_request(url, file.path())).await,
        Err(Error::Redirects)
    ));
    assert_eq!(task.await.unwrap().len(), 6);
}
#[test]
fn media_connection_ttl_number_parsing_and_redacted_debug() {
    use baileys_rs::binary::{BinaryNode, NodeContent};
    let query = MediaConnection::query();
    assert_eq!(query.attrs["xmlns"], "w:m");
    let source: Value =
        serde_json::from_str(include_str!("fixtures/media_connection.json")).unwrap();
    assert_eq!(
        query.attrs,
        serde_json::from_value::<indexmap::IndexMap<String, String>>(
            source["queries"][0]["attrs"].clone()
        )
        .unwrap()
    );
    let response = BinaryNode::new("iq").with_content(NodeContent::Children(vec![
        BinaryNode::new("media_conn")
            .attr("ttl", "0.5")
            .attr("auth", "secret-auth")
            .with_content(NodeContent::Children(vec![
                BinaryNode::new("host")
                    .attr("hostname", "host")
                    .attr("maxContentLengthBytes", "bad"),
            ])),
    ]));
    let connection = MediaConnection::parse(&response, SystemTime::UNIX_EPOCH).unwrap();
    assert!(connection.hosts[0].max_content_length_bytes.is_nan());
    assert!(!connection.expired_at(SystemTime::UNIX_EPOCH + Duration::from_millis(500)));
    assert!(connection.expired_at(SystemTime::UNIX_EPOCH + Duration::from_millis(501)));
    assert!(!format!("{connection:?}").contains("secret-auth"));
    assert!(MediaConnection::parse(&BinaryNode::new("iq"), SystemTime::now()).is_err());
}
