//! Prometheus metrics on `METRICS_PORT`. Rust-only: the TypeScript connector
//! exports none.
//!
//! The `metrics` macros used across the connector are no-ops until
//! [`install`] sets the recorder, so without `METRICS_PORT` nothing is kept.
use crate::config::SessionDb;
use axum::{Router, http::header, response::IntoResponse, routing::get};
use baileys_rs::proto;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::{sync::OnceLock, time::Duration};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Seconds: the set mimi's components share, with a longer tail for media.
const DURATION_BUCKETS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];
/// Bytes, from 1 KiB to 1 GiB.
const BYTE_BUCKETS: &[f64] = &[
    1024.0,
    16384.0,
    131072.0,
    1048576.0,
    4194304.0,
    16777216.0,
    67108864.0,
    268435456.0,
    1073741824.0,
];

/// The commands a label may name; anything else counts as `unknown`, so
/// callers cannot create unbounded series.
const COMMANDS: &[&str] = &[
    "send_text",
    "send_media",
    "send_presence",
    "send_reaction",
    "send_read_receipt",
    "send_edit",
    "send_delete",
    "send_location",
    "send_contact",
    "send_poll",
    "vote_poll",
    "request_resend",
    "raw",
];

const CONNECTION_STATES: [&str; 3] = ["connecting", "open", "closed"];

/// Install the recorder (once per process) and return its handle.
pub fn install() -> PrometheusHandle {
    HANDLE
        .get_or_init(|| {
            let handle = PrometheusBuilder::new()
                .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), DURATION_BUCKETS)
                .and_then(|b| {
                    b.set_buckets_for_metric(Matcher::Suffix("_bytes".into()), BYTE_BUCKETS)
                })
                .expect("metric buckets")
                .install_recorder()
                .expect("the metrics recorder is installed once");
            describe();
            let upkeep = handle.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    upkeep.run_upkeep();
                }
            });
            handle
        })
        .clone()
}

fn describe() {
    use metrics::{describe_counter, describe_gauge, describe_histogram};
    describe_gauge!(
        "baileys_connection_state",
        "1 for the WhatsApp connection's current state (connecting, open, closed), 0 for the others"
    );
    describe_counter!(
        "baileys_reconnects_total",
        "Connections closed and retried, by close code (none: ended without one)"
    );
    describe_counter!(
        "baileys_logged_out_total",
        "Connections closed with 401: the device was unlinked"
    );
    describe_counter!(
        "baileys_pairing_code_requests_total",
        "Pairing codes requested; above 0 on a paired device means the session was lost"
    );
    describe_counter!(
        "baileys_qr_codes_total",
        "QR codes received; above 0 on a paired device means the session was lost"
    );
    describe_counter!(
        "baileys_session_import_total",
        "Imports of a TypeScript multi-file session, by result"
    );
    describe_gauge!(
        "baileys_last_message_received_timestamp_seconds",
        "Unix time of the last new incoming message"
    );
    describe_counter!(
        "baileys_messages_total",
        "Messages in messages.upsert, by direction and content type"
    );
    describe_counter!(
        "baileys_messages_without_content_total",
        "Incoming CIPHERTEXT stubs, by reason"
    );
    describe_counter!(
        "baileys_missing_content_unrecovered_total",
        "Stubs absent from node whose content never arrived from the phone"
    );
    describe_counter!(
        "baileys_placeholder_resends_total",
        "Placeholder resends asked of the phone (requested) and answers received (answered)"
    );
    describe_counter!(
        "baileys_events_published_total",
        "Messages published to OUTPUT_QUEUE, by event or type"
    );
    describe_counter!(
        "baileys_publish_errors_total",
        "Failed publishes to OUTPUT_QUEUE, by event or type"
    );
    describe_histogram!(
        "baileys_publish_duration_seconds",
        "Time to publish to OUTPUT_QUEUE"
    );
    describe_counter!(
        "baileys_commands_total",
        "Commands handled, by type and result"
    );
    describe_histogram!(
        "baileys_command_duration_seconds",
        "Time to handle a command, by type"
    );
    describe_counter!(
        "baileys_input_poll_errors_total",
        "INPUT_QUEUE errors, by stage (receive, delete)"
    );
    describe_counter!("baileys_media_total", "Incoming media, by type and result");
    describe_histogram!(
        "baileys_media_duration_seconds",
        "Time to download or upload incoming media, by type and stage"
    );
    describe_histogram!(
        "baileys_media_bytes",
        "Size of downloaded incoming media, by type"
    );
    describe_gauge!(
        "baileys_event_queue_depth",
        "WhatsApp events waiting to be handled"
    );
    describe_counter!(
        "baileys_events_dropped_total",
        "WhatsApp events dropped because handling lagged"
    );
    describe_gauge!(
        "baileys_prekeys",
        "One-time pre-keys in the session database"
    );
    describe_gauge!(
        "baileys_signal_sessions",
        "Signal sessions in the session database"
    );
    // Exported from the start, so an alert sees 0 rather than no series.
    for name in [
        "baileys_logged_out_total",
        "baileys_pairing_code_requests_total",
        "baileys_qr_codes_total",
        "baileys_missing_content_unrecovered_total",
        "baileys_events_dropped_total",
    ] {
        metrics::counter!(name).absolute(0);
    }
}

/// Serve `GET /metrics` on `host:port` in the background.
pub async fn serve(handle: PrometheusHandle, host: &str, port: u16) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind((host, port)).await?;
    tracing::info!(port, host, "Metrics endpoint listening");
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, router(handle)).await {
            tracing::error!(%err, "Metrics endpoint stopped");
        }
    });
    Ok(())
}

pub fn router(handle: PrometheusHandle) -> Router {
    Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move {
                (
                    [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                    handle.render(),
                )
                    .into_response()
            }
        }),
    )
}

pub fn connection_state(state: &str) {
    for s in CONNECTION_STATES {
        metrics::gauge!("baileys_connection_state", "state" => s).set(if s == state {
            1.0
        } else {
            0.0
        });
    }
}

pub fn command_label(kind: Option<&str>) -> &'static str {
    kind.and_then(|k| COMMANDS.iter().find(|c| **c == k))
        .copied()
        .unwrap_or("unknown")
}

/// The message's content field (`conversation`, `imageMessage`, …) inside
/// any wrapper, `stub` for a stub, `none` for an empty message.
pub fn message_type(message: &proto::WebMessageInfo) -> &'static str {
    let content = baileys_rs::relay::normalize(message.message.as_ref());
    match baileys_rs::messages::generation::content_type(content) {
        Some(kind) => kind,
        None if message.message_stub_type.is_some() => "stub",
        None => "none",
    }
}

/// Count pre-keys and sessions in the session database every minute.
pub fn watch_store(db: &SessionDb) {
    let SessionDb::File(path) = db else {
        return;
    };
    let path = path.clone();
    tokio::spawn(async move {
        loop {
            let counts = tokio::task::spawn_blocking({
                let path = path.clone();
                move || store_counts(&path)
            })
            .await;
            match counts {
                Ok(Ok((prekeys, sessions))) => {
                    metrics::gauge!("baileys_prekeys").set(prekeys as f64);
                    metrics::gauge!("baileys_signal_sessions").set(sessions as f64);
                }
                Ok(Err(err)) => tracing::warn!(%err, "failed to count keys for metrics"),
                Err(err) => tracing::warn!(%err, "failed to count keys for metrics"),
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}

pub fn store_counts(path: &std::path::Path) -> rusqlite::Result<(i64, i64)> {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let count = |category: &str| {
        connection.query_row(
            "SELECT COUNT(*) FROM signal_keys WHERE category=?1",
            [category],
            |r| r.get(0),
        )
    };
    Ok((count("pre-key")?, count("session")?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_bounded() {
        assert_eq!(command_label(Some("send_text")), "send_text");
        assert_eq!(command_label(Some("drop table")), "unknown");
        assert_eq!(command_label(None), "unknown");
        let stub = proto::WebMessageInfo {
            message_stub_type: Some(2),
            ..Default::default()
        };
        assert_eq!(message_type(&stub), "stub");
        assert_eq!(message_type(&proto::WebMessageInfo::default()), "none");
        let image = proto::WebMessageInfo {
            message: Some(proto::Message {
                ephemeral_message: Some(Box::new(proto::message::FutureProofMessage {
                    message: Some(Box::new(proto::Message {
                        image_message: Some(Default::default()),
                        ..Default::default()
                    })),
                })),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(message_type(&image), "imageMessage");
    }
}
