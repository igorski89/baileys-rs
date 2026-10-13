//! Tunnels for the WebSocket connection: HTTP CONNECT and SOCKS5.
use super::Error;
use base64::{Engine, engine::general_purpose::STANDARD};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

fn failed(reason: impl Into<String>) -> Error {
    Error::Proxy(reason.into())
}
/// Open a TCP stream to `host:port` through the proxy at `proxy`
/// (`http://`, `socks5://` or `socks5h://`, optionally with credentials).
pub(super) async fn connect(proxy: &str, host: &str, port: u16) -> Result<TcpStream, Error> {
    let proxy = url::Url::parse(proxy).map_err(|_| Error::Config)?;
    if !matches!(proxy.scheme(), "http" | "socks5" | "socks5h") {
        return Err(Error::Config);
    }
    let address = (
        proxy.host_str().ok_or(Error::Config)?,
        proxy.port_or_known_default().unwrap_or(1080),
    );
    let user = percent(proxy.username());
    let password = percent(proxy.password().unwrap_or_default());
    let credentials = (!user.is_empty() || !password.is_empty()).then_some((user, password));
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|error| failed(error.to_string()))?;
    match proxy.scheme() {
        "http" => http(&mut stream, host, port, credentials).await?,
        _ => socks5(&mut stream, host, port, credentials).await?,
    }
    Ok(stream)
}
// Credentials are percent-encoded in a URL.
fn percent(text: &str) -> String {
    let bytes = text.as_bytes();
    let (mut decoded, mut i) = (Vec::with_capacity(bytes.len()), 0);
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .filter(|_| bytes[i] == b'%')
            .and_then(|pair| std::str::from_utf8(pair).ok())
            .and_then(|pair| u8::from_str_radix(pair, 16).ok());
        match hex {
            Some(byte) => {
                decoded.push(byte);
                i += 3;
            }
            None => {
                decoded.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}
async fn http(
    stream: &mut TcpStream,
    host: &str,
    port: u16,
    credentials: Option<(String, String)>,
) -> Result<(), Error> {
    let mut request = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n");
    if let Some((user, password)) = credentials {
        request.push_str(&format!(
            "Proxy-Authorization: Basic {}\r\n",
            STANDARD.encode(format!("{user}:{password}"))
        ));
    }
    request.push_str("\r\n");
    let io = |error: std::io::Error| failed(error.to_string());
    stream.write_all(request.as_bytes()).await.map_err(io)?;
    // Read one byte at a time: what follows the header belongs to the tunnel.
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() > 16384 {
            return Err(failed("oversized CONNECT response"));
        }
        head.push(stream.read_u8().await.map_err(io)?);
    }
    let status = String::from_utf8_lossy(&head)
        .split_whitespace()
        .nth(1)
        .map(str::to_owned)
        .unwrap_or_default();
    if status != "200" {
        return Err(failed(format!("CONNECT refused with status {status}")));
    }
    Ok(())
}
async fn socks5(
    stream: &mut TcpStream,
    host: &str,
    port: u16,
    credentials: Option<(String, String)>,
) -> Result<(), Error> {
    let io = |error: std::io::Error| failed(error.to_string());
    let greeting: &[u8] = if credentials.is_some() {
        &[5, 2, 0, 2]
    } else {
        &[5, 1, 0]
    };
    stream.write_all(greeting).await.map_err(io)?;
    let mut choice = [0u8; 2];
    stream.read_exact(&mut choice).await.map_err(io)?;
    match (choice, credentials) {
        ([5, 0], _) => (),
        ([5, 2], Some((user, password))) => {
            if user.len() > 255 || password.len() > 255 {
                return Err(Error::Config);
            }
            let mut auth = vec![1, user.len() as u8];
            auth.extend_from_slice(user.as_bytes());
            auth.push(password.len() as u8);
            auth.extend_from_slice(password.as_bytes());
            stream.write_all(&auth).await.map_err(io)?;
            let mut result = [0u8; 2];
            stream.read_exact(&mut result).await.map_err(io)?;
            if result[1] != 0 {
                return Err(failed("SOCKS5 authentication rejected"));
            }
        }
        _ => return Err(failed("no acceptable SOCKS5 authentication method")),
    }
    // The name is resolved by the proxy.
    if host.len() > 255 {
        return Err(Error::Config);
    }
    let mut request = vec![5, 1, 0, 3, host.len() as u8];
    request.extend_from_slice(host.as_bytes());
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await.map_err(io)?;
    let mut reply = [0u8; 4];
    stream.read_exact(&mut reply).await.map_err(io)?;
    if reply[1] != 0 {
        return Err(failed(format!(
            "SOCKS5 connect failed with code {}",
            reply[1]
        )));
    }
    let bound = match reply[3] {
        1 => 4,
        4 => 16,
        3 => usize::from(stream.read_u8().await.map_err(io)?),
        _ => return Err(failed("malformed SOCKS5 reply")),
    };
    let mut rest = vec![0u8; bound + 2];
    stream.read_exact(&mut rest).await.map_err(io)?;
    Ok(())
}
