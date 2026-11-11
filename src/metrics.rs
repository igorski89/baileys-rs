//! Library metrics, behind the `metrics` feature. Rust-only: upstream has
//! none.
//!
//! With the feature, counters go to whatever recorder the application
//! installs with the `metrics` crate (none: nothing is kept). Without it,
//! every function here is empty. Labels are bounded: no JIDs or message ids.

/// The counters the library exports, with their help text.
pub const COUNTERS: &[(&str, &str)] = &[
    (
        "baileys_decrypt_failures_total",
        "Encrypted nodes of incoming messages that failed to decrypt, by type (pkmsg, msg, skmsg, other)",
    ),
    (
        "baileys_retry_receipts_total",
        "Retry receipts, by direction (sent: we asked for a resend; received: a peer asked us)",
    ),
    (
        "baileys_prekey_uploads_total",
        "Pre-key uploads, by result (ok; failed: gave up after the retries)",
    ),
    (
        "baileys_app_state_syncs_total",
        "App-state collection syncs, by collection and result (ok, retry, blocked, failed)",
    ),
    (
        "baileys_history_notifications_total",
        "History sync notifications, by sync type and whether they were processed",
    ),
    (
        "baileys_lid_lookups_total",
        "JIDs looked up in the PN-LID mapping, by direction (pn_to_lid, lid_to_pn) and result (hit, miss)",
    ),
    (
        "baileys_keepalive_timeouts_total",
        "Connections closed because the server stopped answering keep-alives (408)",
    ),
    (
        "baileys_query_timeouts_total",
        "Queries to the server that got no answer in time",
    ),
];

/// Describe the library's counters to the installed recorder, and export
/// the label-less ones at 0 so an alert sees a series from the start.
#[cfg(feature = "metrics")]
pub fn describe() {
    for (name, help) in COUNTERS {
        ::metrics::describe_counter!(*name, *help);
    }
    for name in [
        "baileys_keepalive_timeouts_total",
        "baileys_query_timeouts_total",
    ] {
        ::metrics::counter!(name).absolute(0);
    }
}

#[cfg(not(feature = "metrics"))]
pub fn describe() {}

#[cfg(feature = "metrics")]
pub(crate) fn count(name: &'static str, labels: &[(&'static str, &'static str)]) {
    add(name, labels, 1);
}

#[cfg(not(feature = "metrics"))]
#[inline(always)]
pub(crate) fn count(_name: &'static str, _labels: &[(&'static str, &'static str)]) {}

#[cfg(feature = "metrics")]
pub(crate) fn add(name: &'static str, labels: &[(&'static str, &'static str)], value: u64) {
    if value > 0 {
        ::metrics::counter!(name, labels).increment(value);
    }
}

#[cfg(not(feature = "metrics"))]
#[inline(always)]
pub(crate) fn add(_name: &'static str, _labels: &[(&'static str, &'static str)], _value: u64) {}

/// An app-state collection's name as a label: one of the five known, else `other`.
pub(crate) fn collection(name: &str) -> &'static str {
    [
        "critical_block",
        "critical_unblock_low",
        "regular_high",
        "regular_low",
        "regular",
    ]
    .into_iter()
    .find(|known| *known == name)
    .unwrap_or("other")
}

/// A history notification's sync type as a label: its proto name, or `other`.
pub(crate) fn history_sync_type(value: Option<i32>) -> &'static str {
    value
        .and_then(|v| crate::proto::message::HistorySyncType::try_from(v).ok())
        .map_or("other", |t| t.as_str_name())
}
