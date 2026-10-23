//! Warnings for incoming messages whose content never arrives.
//!
//! A message that WhatsApp delivered without a copy for this device, or that
//! failed to decrypt, is published as a `CIPHERTEXT` stub with an empty
//! `message`. For "Message absent from node" the library asks the primary
//! phone once to resend it and gives up after ten seconds; the content then
//! never arrives. The TypeScript connector logs the same lines (`13949c8`).
use baileys_rs::{messages::incoming::NO_MESSAGE, proto};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

/// How long after the stub the content may still come from the phone: the
/// library requests it after 2 s and waits 8 s for the answer.
const WAIT: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub struct MissingContent {
    pending: Arc<Mutex<HashSet<String>>>,
    wait: Duration,
}

impl Default for MissingContent {
    fn default() -> Self {
        Self::with_wait(WAIT)
    }
}

impl MissingContent {
    pub fn with_wait(wait: Duration) -> Self {
        Self {
            pending: Arc::default(),
            wait,
        }
    }

    /// Look at an upserted message: warn about a stub, and note when content
    /// for a pending stub arrives.
    pub fn observe(&self, message: &proto::WebMessageInfo) {
        let key = message.key.as_ref();
        let Some(id) = key.and_then(|k| k.id.clone()).filter(|id| !id.is_empty()) else {
            return;
        };
        let chat = key
            .and_then(|k| k.remote_jid.as_deref())
            .unwrap_or_default();
        let stub =
            message.message_stub_type == Some(proto::web_message_info::StubType::Ciphertext as i32);
        if !stub {
            if message.message.is_some()
                && self.pending.lock().expect("missing content").remove(&id)
            {
                tracing::info!(
                    id,
                    chat,
                    "content of a message without content arrived from the phone"
                );
            }
            return;
        }
        let reason = message
            .message_stub_parameters
            .first()
            .map(String::as_str)
            .unwrap_or_default();
        metrics::counter!("baileys_messages_without_content_total", "reason" => reason.to_owned())
            .increment(1);
        tracing::warn!(
            id,
            chat,
            participant = key.and_then(|k| k.participant.as_deref()),
            reason,
            "message arrived without content"
        );
        if reason != NO_MESSAGE {
            return;
        }
        self.pending
            .lock()
            .expect("missing content")
            .insert(id.clone());
        let pending = self.pending.clone();
        let wait = self.wait;
        let chat = chat.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            if pending.lock().expect("missing content").remove(&id) {
                metrics::counter!("baileys_missing_content_unrecovered_total").increment(1);
                tracing::warn!(
                    id,
                    chat,
                    "message content never arrived: the primary phone did not answer the resend request; is it online?"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stub(id: &str, reason: &str) -> proto::WebMessageInfo {
        proto::WebMessageInfo {
            key: Some(proto::MessageKey {
                remote_jid: Some("1@lid".into()),
                id: Some(id.into()),
                ..Default::default()
            }),
            message_stub_type: Some(proto::web_message_info::StubType::Ciphertext as i32),
            message_stub_parameters: vec![reason.into()],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn only_absent_content_waits_for_the_phone() {
        let missing = MissingContent::with_wait(Duration::from_millis(20));
        missing.observe(&stub("A", NO_MESSAGE));
        missing.observe(&stub("B", "Bad MAC"));
        missing.observe(&stub("C", NO_MESSAGE));
        assert_eq!(missing.pending.lock().unwrap().len(), 2);
        // The phone's answer for A arrives with the same key.
        let mut answer = stub("A", "");
        answer.message_stub_type = None;
        answer.message_stub_parameters.clear();
        answer.message = Some(proto::Message {
            conversation: Some("hi".into()),
            ..Default::default()
        });
        missing.observe(&answer);
        assert!(!missing.pending.lock().unwrap().contains("A"));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(missing.pending.lock().unwrap().is_empty());
    }
}
