//! Optional HTTP input endpoint, additive to the SQS input queue.
use crate::commands::Commands;
use axum::{
    Router,
    body::Bytes,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct Endpoint {
    pub commands: Commands,
    pub auth_token: Option<String>,
}

fn reply(status: StatusCode, body: Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// Constant-time comparison, so the token cannot be inferred by timing.
fn authorized(token: &Option<String>, headers: &HeaderMap) -> bool {
    let Some(token) = token else {
        return true;
    };
    let Some(header) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let expected = format!("Bearer {token}");
    header.as_bytes().len() == expected.len()
        && bool::from(header.as_bytes().ct_eq(expected.as_bytes()))
}

async fn handle(State(endpoint): State<Endpoint>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts
        .uri
        .path_and_query()
        .map_or("/", |p| p.as_str())
        .to_owned();
    if parts.method == Method::GET && path == "/health" {
        let connected = match endpoint.commands.socket.read().await.as_ref() {
            Some(socket) => socket.user().is_some(),
            None => false,
        };
        return reply(StatusCode::OK, json!({"ok":true,"connected":connected}));
    }
    if parts.method != Method::POST || path != "/commands" {
        return reply(
            StatusCode::NOT_FOUND,
            json!({"ok":false,"error":"Not found"}),
        );
    }
    if !authorized(&endpoint.auth_token, &parts.headers) {
        return reply(
            StatusCode::UNAUTHORIZED,
            json!({"ok":false,"error":"Unauthorized"}),
        );
    }
    if endpoint.commands.socket.read().await.is_none() {
        return reply(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"ok":false,"error":"WhatsApp socket not ready"}),
        );
    }
    let error = |message: String| {
        tracing::error!(err = %message, "HTTP command error");
        let message = if message.is_empty() {
            "Internal error".into()
        } else {
            message
        };
        reply(StatusCode::BAD_REQUEST, json!({"ok":false,"error":message}))
    };
    let bytes: Bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => return error(err.to_string()),
    };
    let cmd: Value = if bytes.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&bytes) {
            Ok(cmd) => cmd,
            Err(_) => return error("Invalid JSON body".into()),
        }
    };
    if !crate::js::truthy(cmd.get("type")) || !crate::js::truthy(cmd.get("to")) {
        return reply(
            StatusCode::BAD_REQUEST,
            json!({"ok":false,"error":"Command must include \"type\" and \"to\""}),
        );
    }
    match endpoint.commands.dispatch(&cmd).await {
        Ok(()) => reply(StatusCode::OK, json!({"ok":true})),
        Err(err) => error(err.0),
    }
}

pub fn router(endpoint: Endpoint) -> Router {
    Router::new().fallback(handle).with_state(endpoint)
}

/// Serve the endpoint on `host:port` in the background.
pub async fn start(endpoint: Endpoint, host: &str, port: u16) -> std::io::Result<()> {
    if endpoint.auth_token.is_none() {
        tracing::warn!(
            "AUTH_TOKEN not set - HTTP input endpoint is unauthenticated, do not expose it publicly like this"
        );
    }
    let listener = tokio::net::TcpListener::bind((host, port)).await?;
    tracing::info!(port, host, "HTTP input endpoint listening");
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, router(endpoint)).await {
            tracing::error!(%err, "HTTP input endpoint stopped");
        }
    });
    Ok(())
}
