//! `WHATSAPP_VERSION`: the built-in version unless it pins one or asks for `latest`.
use baileys_rs::defaults::WA_VERSION;
use baileys_sqs::app::{VersionChoice, latest_version, version_choice};
use std::time::{Duration, Instant};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[test]
fn the_setting_picks_built_in_pinned_or_latest() {
    assert_eq!(version_choice(None), VersionChoice::BuiltIn);
    assert_eq!(version_choice(Some(" ")), VersionChoice::BuiltIn);
    assert_eq!(version_choice(Some("[2,3000")), VersionChoice::BuiltIn);
    assert_eq!(version_choice(Some("latest")), VersionChoice::Latest);
    assert_eq!(version_choice(Some("LATEST")), VersionChoice::Latest);
    assert_eq!(
        version_choice(Some("[2, 3000, 1036200034]")),
        VersionChoice::Pinned([2, 3000, 1036200034])
    );
}

#[tokio::test]
async fn latest_reads_the_defaults_file() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/index.ts", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let body = "1\n2\n3\n4\n5\n6\nconst version = [2, 3000, 7]\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    let version = latest_version(&reqwest::Client::new(), &url, Duration::from_secs(5)).await;
    assert_eq!(version, [2, 3000, 7]);
}

/// A server that accepts the connection and never answers must not hold up the start.
#[tokio::test]
async fn latest_falls_back_to_the_built_in_version_when_the_fetch_hangs() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/index.ts", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(stream);
    });
    let started = Instant::now();
    let version = latest_version(&reqwest::Client::new(), &url, Duration::from_millis(200)).await;
    assert_eq!(version, WA_VERSION);
    assert!(started.elapsed() < Duration::from_secs(2));
    server.abort();
}

#[tokio::test]
async fn latest_falls_back_to_the_built_in_version_when_the_fetch_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/index.ts", listener.local_addr().unwrap());
    drop(listener);
    let version = latest_version(&reqwest::Client::new(), &url, Duration::from_secs(5)).await;
    assert_eq!(version, WA_VERSION);
}
