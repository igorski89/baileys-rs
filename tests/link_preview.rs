use baileys_rs::{
    link_preview::{self, Error, Page, Source},
    proto,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::sync::Mutex;

// Serves the fixture's pages and mirrors its image stubs.
struct Pages<'a> {
    pages: &'a Value,
    fetched: Mutex<Vec<String>>,
}
impl Source for Pages<'_> {
    fn page<'a>(&'a self, url: &'a str) -> BoxFuture<'a, Result<Page, Error>> {
        Box::pin(async move {
            self.fetched.lock().unwrap().push(url.to_owned());
            let page = self
                .pages
                .get(url)
                .ok_or_else(|| Error::Fetch("fetch failed".into()))?;
            Ok(Page {
                url: page["url"].as_str().unwrap_or(url).to_owned(),
                content_type: page["type"].as_str().map(str::to_owned),
                body: page["body"].as_str().unwrap().to_owned(),
            })
        })
    }
    fn thumbnail<'a>(&'a self, url: &'a str, width: u32) -> BoxFuture<'a, Result<Vec<u8>, Error>> {
        Box::pin(async move {
            if url.contains("unreachable") {
                return Err(Error::Image("image fetch failed".into()));
            }
            Ok(format!("thumbnail:{url}:{width}").into_bytes())
        })
    }
    fn upload<'a>(
        &'a self,
        url: &'a str,
    ) -> BoxFuture<'a, Result<Option<proto::message::ImageMessage>, Error>> {
        Box::pin(async move {
            if url.contains("plain") {
                return Ok(None);
            }
            Ok(Some(proto::message::ImageMessage {
                url: Some("https://mmg.whatsapp.net/hq".into()),
                direct_path: Some("/hq".into()),
                jpeg_thumbnail: (!url.contains("nothumb"))
                    .then(|| format!("hq:{url}").into_bytes()),
                width: Some(640),
                height: Some(480),
                ..Default::default()
            }))
        })
    }
}
fn buffer(bytes: &[u8]) -> Value {
    json!({"type": "Buffer", "data": STANDARD.encode(bytes)})
}
#[tokio::test]
async fn url_info_matches_original_detection_parsing_and_thumbnails() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/link-preview.json")).unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 126);
    for case in cases {
        let source = Pages {
            pages: &fixture["pages"],
            fetched: Mutex::new(vec![]),
        };
        let result = link_preview::url_info(
            case["text"].as_str().unwrap(),
            link_preview::THUMBNAIL_WIDTH_PX,
            case["upload"].as_bool().unwrap(),
            &source,
        )
        .await;
        let fetched: Vec<_> = case["fetched"]
            .as_array()
            .unwrap()
            .iter()
            .map(|call| {
                assert_eq!(call["redirect"], "follow");
                call["url"].as_str().unwrap().to_owned()
            })
            .collect();
        assert_eq!(*source.fetched.lock().unwrap(), fetched, "{case}");
        assert_eq!(result.is_err(), !case["error"].is_null(), "{case}");
        let Ok(result) = result else { continue };
        let actual = result.map_or(Value::Null, |info| {
            let mut value = json!({
                "canonical-url": info.canonical_url,
                "matched-text": info.matched_text,
                "title": info.title,
            });
            if let Some(description) = info.description {
                value["description"] = json!(description);
            }
            if let Some(url) = info.original_thumbnail_url {
                value["originalThumbnailUrl"] = json!(url);
            }
            if let Some(thumbnail) = info.jpeg_thumbnail {
                value["jpegThumbnail"] = buffer(&thumbnail);
            }
            if let Some(image) = info.high_quality_thumbnail {
                let mut uploaded = json!({
                    "url": image.url, "directPath": image.direct_path,
                    "width": image.width, "height": image.height,
                });
                if let Some(thumbnail) = image.jpeg_thumbnail {
                    uploaded["jpegThumbnail"] = buffer(&thumbnail);
                }
                value["highQualityThumbnail"] = uploaded;
            }
            value
        });
        assert_eq!(actual, case["expected"], "{case}");
    }
}
#[tokio::test]
async fn page_fetch_follows_redirects_and_reports_final_url_type_and_lossy_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 2048];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]).into_owned();
            let response: Vec<u8> = if request.starts_with("GET /start ") {
                b"HTTP/1.1 302 Found\r\nLocation: /final?x=1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
            } else {
                assert!(request.starts_with("GET /final?x=1 "), "{request}");
                let body = b"<title>Caf\xe9 \xf0\x9f\x92\x9a</title>";
                let mut response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                response.extend_from_slice(body);
                response
            };
            stream.write_all(&response).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    let http = baileys_rs::media::HttpClient::new().unwrap();
    let (url, content_type, body) = http
        .get_page(
            &format!("http://{address}/start"),
            std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(url, format!("http://{address}/final?x=1"));
    assert_eq!(content_type.as_deref(), Some("text/html; charset=utf-8"));
    // Invalid UTF-8 is replaced, as fetch's text() does.
    assert_eq!(body, "<title>Caf\u{fffd} 💚</title>");
    let info = link_preview::page_info(&Page {
        url,
        content_type,
        body,
    })
    .unwrap();
    assert_eq!(info.title, "Caf\u{fffd} 💚");
    server.await.unwrap();
    // A server that never answers runs into the request timeout.
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = silent.local_addr().unwrap();
    let hold = tokio::spawn(async move {
        let _stream = silent.accept().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    });
    let result = http
        .get_page(
            &format!("http://{address}/"),
            std::time::Duration::from_millis(100),
        )
        .await;
    assert!(result.is_err());
    hold.abort();
}
