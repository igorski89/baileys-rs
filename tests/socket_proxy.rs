#[allow(dead_code)]
#[path = "support/socket_peer.rs"]
mod socket_peer;
use baileys_rs::{
    auth::{Contact, SqliteAuthStore},
    binary::BinaryNode,
    crypto::KeyPair,
    socket::{Config, Error, Socket},
};
use socket_peer::{Peer, ROOT_PRIVATE};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
fn config(proxy: String, port: u16) -> Config {
    Config {
        // The name only exists for the proxy, which resolves it.
        url: format!("ws://wa.test:{port}"),
        proxy: Some(proxy),
        initialize_session: false,
        fire_init_queries: false,
        announce_presence_on_connect: false,
        noise: baileys_rs::noise::Config {
            certificate_public_key: KeyPair::from_private(ROOT_PRIVATE).public,
            ..Default::default()
        },
        ..Default::default()
    }
}
async fn serve(listener: TcpListener) {
    let mut peer = Peer::accept(listener.accept().await.unwrap().0).await;
    peer.send(BinaryNode::new("success")).await;
    assert!(peer.read().await.child("unified_session").is_some());
    let probe = peer.read().await;
    peer.result(&probe).await;
    peer.closed().await;
}
async fn exchange(config: Config) {
    let store = SqliteAuthStore::in_memory().unwrap();
    let mut creds = store.load_credentials().unwrap();
    creds.me = Some(Contact {
        id: "100:7@s.whatsapp.net".into(),
        ..Default::default()
    });
    store.save_credentials(&creds).unwrap();
    let (socket, _events) = Socket::connect(config, store).await.unwrap();
    socket.wait_for_open(Duration::from_secs(2)).await.unwrap();
    socket
        .query(
            BinaryNode::new("iq")
                .attr("to", "s.whatsapp.net")
                .attr("type", "get")
                .attr("xmlns", "probe"),
            None,
        )
        .await
        .unwrap();
    socket.close().await.unwrap();
}
async fn head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await.unwrap());
    }
    String::from_utf8(head).unwrap()
}
#[tokio::test]
async fn websocket_connects_through_an_http_connect_proxy_with_credentials() {
    timeout(Duration::from_secs(10), async {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = target.local_addr().unwrap().port();
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = proxy.local_addr().unwrap();
        let server = tokio::spawn(serve(target));
        let tunnel = tokio::spawn(async move {
            let mut client = proxy.accept().await.unwrap().0;
            let request = head(&mut client).await;
            assert!(request.starts_with(&format!("CONNECT wa.test:{port} HTTP/1.1\r\n")));
            assert!(request.contains(&format!("\r\nHost: wa.test:{port}\r\n")));
            // user "me", password "p@ss word"
            assert!(request.contains("\r\nProxy-Authorization: Basic bWU6cEBzcyB3b3Jk\r\n"));
            let mut upstream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            client
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            tokio::io::copy_bidirectional(&mut client, &mut upstream)
                .await
                .ok();
        });
        exchange(config(format!("http://me:p%40ss%20word@{address}"), port)).await;
        server.await.unwrap();
        tunnel.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn websocket_connects_through_a_socks5_proxy_with_and_without_credentials() {
    timeout(Duration::from_secs(10), async {
        for credentials in [false, true] {
            let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = target.local_addr().unwrap().port();
            let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = proxy.local_addr().unwrap();
            let server = tokio::spawn(serve(target));
            let tunnel = tokio::spawn(async move {
                let mut client = proxy.accept().await.unwrap().0;
                let mut greeting = [0u8; 2];
                client.read_exact(&mut greeting).await.unwrap();
                let mut methods = vec![0u8; greeting[1].into()];
                client.read_exact(&mut methods).await.unwrap();
                if credentials {
                    assert_eq!((greeting[0], methods.as_slice()), (5, [0u8, 2].as_slice()));
                    client.write_all(&[5, 2]).await.unwrap();
                    let mut auth = [0u8; 9];
                    client.read_exact(&mut auth).await.unwrap();
                    assert_eq!(&auth, b"\x01\x02me\x04pass");
                    client.write_all(&[1, 0]).await.unwrap();
                } else {
                    assert_eq!((greeting[0], methods.as_slice()), (5, [0u8].as_slice()));
                    client.write_all(&[5, 0]).await.unwrap();
                }
                let mut request = [0u8; 14];
                client.read_exact(&mut request).await.unwrap();
                assert_eq!(&request[..12], b"\x05\x01\x00\x03\x07wa.test");
                assert_eq!(u16::from_be_bytes([request[12], request[13]]), port);
                let mut upstream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                // Bound to a domain name, to exercise the variable-length reply.
                client
                    .write_all(b"\x05\x00\x00\x03\x05proxy\x00\x50")
                    .await
                    .unwrap();
                tokio::io::copy_bidirectional(&mut client, &mut upstream)
                    .await
                    .ok();
            });
            let user = if credentials { "me:pass@" } else { "" };
            exchange(config(format!("socks5://{user}{address}"), port)).await;
            server.await.unwrap();
            tunnel.await.unwrap();
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn proxy_refusals_and_unknown_schemes_fail_the_connection() {
    timeout(Duration::from_secs(10), async {
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = proxy.local_addr().unwrap();
        let refusing = tokio::spawn(async move {
            let mut client = proxy.accept().await.unwrap().0;
            head(&mut client).await;
            client
                .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                .await
                .unwrap();
            let mut client = proxy.accept().await.unwrap().0;
            let mut greeting = [0u8; 3];
            client.read_exact(&mut greeting).await.unwrap();
            client.write_all(&[5, 0]).await.unwrap();
            let mut request = [0u8; 14];
            client.read_exact(&mut request).await.unwrap();
            // Connection refused by the destination.
            client
                .write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });
        let connect = |proxy: String| async move {
            Socket::connect(config(proxy, 443), SqliteAuthStore::in_memory().unwrap())
                .await
                .map(|_| ())
                .unwrap_err()
        };
        let refused = connect(format!("http://{address}")).await;
        assert!(
            matches!(&refused, Error::Proxy(reason) if reason.contains("407")),
            "{refused:?}"
        );
        let refused = connect(format!("socks5://{address}")).await;
        assert!(
            matches!(&refused, Error::Proxy(reason) if reason.contains("code 5")),
            "{refused:?}"
        );
        let unknown = connect(format!("ftp://{address}")).await;
        assert!(matches!(unknown, Error::Config), "{unknown:?}");
        refusing.await.unwrap();
    })
    .await
    .unwrap();
}
