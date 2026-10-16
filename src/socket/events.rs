//! Typed socket delivery backed by the source event-consolidation algorithm.
mod projection;
mod upstream;
use super::{Error, Event};
use crate::event_buffer::{Batch, Buffer};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{Notify, broadcast},
    time::Instant,
};

struct Shared {
    sender: broadcast::Sender<Event>,
    buffer: Mutex<Buffer>,
    history: Mutex<crate::history::StatusTracker>,
    initial: Mutex<super::initial_sync::Coordinator>,
    counter_command: Mutex<Option<tokio::sync::mpsc::WeakSender<super::Command>>>,
    origin: Instant,
    changed: Arc<Notify>,
}
impl Drop for Shared {
    fn drop(&mut self) {
        self.changed.notify_one();
    }
}
#[derive(Clone)]
pub(super) struct Emitter(Arc<Shared>);
impl Shared {
    fn increment_counter(&self) {
        let command = self
            .counter_command
            .lock()
            .expect("counter command mutex")
            .as_ref()
            .and_then(tokio::sync::mpsc::WeakSender::upgrade);
        if let Some(command) = command {
            tokio::spawn(async move {
                let (reply, response) = tokio::sync::oneshot::channel();
                let result = async {
                    command
                        .send(super::Command::IncrementAccountSync { reply })
                        .await
                        .map_err(|_| Error::Closed(428))?;
                    response.await.map_err(|_| Error::Closed(428))?
                }
                .await;
                if let Err(error) = result {
                    tracing::warn!(?error, "initial sync counter update failed");
                }
            });
        }
    }
    fn status(&self, updates: Vec<crate::history::StatusUpdate>) {
        for update in updates {
            let _ = self.sender.send(Event::MessagingHistoryStatus(update));
        }
    }
    fn tick(&self) -> Option<std::time::Duration> {
        let mut buffer = self.buffer.lock().expect("event buffer mutex");
        let mut history = self.history.lock().expect("history progress mutex");
        let mut initial = self.initial.lock().expect("initial sync mutex");
        let now = self.origin.elapsed();
        loop {
            let mut timers = vec![];
            if let Some(at) = buffer.next_deadline() {
                timers.push((at, buffer.next_timer_registration().unwrap(), 0));
            }
            if let Some(at) = history.next_deadline() {
                timers.push((
                    at,
                    at.saturating_sub(std::time::Duration::from_secs(120)),
                    1,
                ));
            }
            if let Some(at) = initial.next_deadline() {
                timers.push((at, initial.registration_time().unwrap(), 2));
            }
            let (deadline, _, kind) = timers.into_iter().min()?;
            if deadline > now {
                return Some(deadline);
            }
            // A 120s pause timer was registered before any shorter buffer timer
            // sharing its deadline, so progress wins ties in the source clock.
            if kind == 1 {
                self.status(history.tick(deadline));
            } else if kind == 2 {
                let effects = initial.tick(deadline);
                if effects.flush
                    && let Err(error) = self.deliver(buffer.flush().1)
                {
                    tracing::error!(?error, "initial sync flush failed");
                }
                if effects.increment_counter {
                    self.increment_counter();
                }
            } else if let Err(error) = self.deliver(buffer.tick(deadline)) {
                tracing::error!(?error, "buffered event delivery failed");
            }
        }
    }
    fn deliver(&self, batches: Vec<Batch>) -> Result<usize, Error> {
        let mut receivers = 0;
        for batch in batches {
            for event in projection::decode(batch)? {
                receivers = self.sender.send(event).unwrap_or(0);
            }
        }
        Ok(receivers)
    }
}
impl Emitter {
    pub fn new(capacity: usize) -> (Self, broadcast::Receiver<Event>) {
        let (sender, receiver) = broadcast::channel(capacity);
        let shared = Arc::new(Shared {
            sender,
            buffer: Mutex::new(Buffer::new()),
            history: Mutex::new(crate::history::StatusTracker::default()),
            initial: Mutex::new(super::initial_sync::Coordinator::default()),
            counter_command: Mutex::new(None),
            origin: Instant::now(),
            changed: Arc::new(Notify::new()),
        });
        let weak = Arc::downgrade(&shared);
        let changed = shared.changed.clone();
        tokio::spawn(async move {
            loop {
                let notified = changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let deadline = {
                    let Some(shared) = weak.upgrade() else {
                        break;
                    };
                    shared.tick().map(|d| shared.origin + d)
                };
                if let Some(deadline) = deadline {
                    tokio::select! {_ = tokio::time::sleep_until(deadline)=>(),_ = &mut notified=>()}
                } else {
                    notified.await;
                }
            }
        });
        (Self(shared), receiver)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.0.sender.subscribe()
    }
    pub fn send(&self, event: Event) -> Result<usize, Error> {
        let mut buffer = self.0.buffer.lock().expect("event buffer mutex");
        match &event {
            Event::PendingNotifications => self
                .0
                .history
                .lock()
                .expect("history progress mutex")
                .pending_notifications(),
            Event::Connection(super::ConnectionState::Closed { .. }) => self
                .0
                .history
                .lock()
                .expect("history progress mutex")
                .close(),
            _ => (),
        }
        if matches!(
            event,
            Event::Connection(super::ConnectionState::Closed { .. })
        ) {
            self.0.initial.lock().expect("initial sync mutex").close();
        }
        self.0.changed.notify_one();
        if !buffer.is_buffering() {
            return Ok(self.0.sender.send(event).unwrap_or(0));
        }
        let mut receivers = 0;
        for action in projection::project(event)? {
            let result = match action {
                projection::Action::Native(event) => {
                    receivers = self.0.sender.send(event).unwrap_or(0);
                    continue;
                }
                projection::Action::Payload(name, value) => buffer.emit(name, value),
                projection::Action::Chats(updates) => buffer.emit_native_chat_updates(updates),
            };
            match result {
                Ok(batches) => receivers = self.0.deliver(batches)?,
                Err(error) => {
                    self.0.deliver(buffer.take_failed_emissions())?;
                    return Err(error.into());
                }
            }
        }
        self.0.changed.notify_one();
        Ok(receivers)
    }
    pub fn buffer(&self) {
        self.0
            .buffer
            .lock()
            .expect("event buffer mutex")
            .buffer(self.0.origin.elapsed());
        self.0.changed.notify_one();
    }
    /// Online stanza admission releases the shared buffer immediately when its handler ends.
    pub fn node_scope(&self) -> NodeScope {
        self.buffer();
        NodeScope(Some(self.clone()))
    }
    pub fn set_counter_command(&self, command: tokio::sync::mpsc::WeakSender<super::Command>) {
        *self
            .0
            .counter_command
            .lock()
            .expect("counter command mutex") = Some(command);
    }
    pub fn initial_sync_phase(&self) -> super::initial_sync::Phase {
        self.0.initial.lock().expect("initial sync mutex").phase()
    }
    pub fn start_initial_sync(&self) -> bool {
        let mut buffer = self.0.buffer.lock().expect("event buffer mutex");
        let mut history = self.0.history.lock().expect("history progress mutex");
        let mut initial = self.0.initial.lock().expect("initial sync mutex");
        let effects = initial.begin_pending_notifications();
        if effects.buffer {
            history.pending_notifications();
            buffer.buffer(self.0.origin.elapsed());
        }
        self.0.changed.notify_one();
        effects.buffer
    }
    pub fn select_initial_history(&self, will_sync: bool, counter: u32) {
        self.0
            .initial
            .lock()
            .expect("initial sync mutex")
            .select_history(will_sync, counter, self.0.origin.elapsed());
        self.0.changed.notify_one();
    }
    pub fn initial_history(&self, eligible: bool) -> Result<(), Error> {
        let mut buffer = self.0.buffer.lock().expect("event buffer mutex");
        let effects = self
            .0
            .initial
            .lock()
            .expect("initial sync mutex")
            .history_notification(eligible);
        if effects.flush {
            self.0.deliver(buffer.flush().1)?;
        }
        self.0.changed.notify_one();
        Ok(())
    }
    pub fn initial_sync_complete(&self) -> Result<(), Error> {
        let mut buffer = self.0.buffer.lock().expect("event buffer mutex");
        self.0
            .initial
            .lock()
            .expect("initial sync mutex")
            .sync_completed();
        self.0.deliver(buffer.flush().1)?;
        self.0.changed.notify_one();
        Ok(())
    }
    pub fn history_notification(
        &self,
        notification: &crate::proto::message::HistorySyncNotification,
        eligible: bool,
    ) {
        let mut history = self.0.history.lock().expect("history progress mutex");
        self.0
            .status(history.observe(notification, eligible, self.0.origin.elapsed()));
        self.0.changed.notify_one();
    }
    pub fn is_buffering(&self) -> bool {
        self.0
            .buffer
            .lock()
            .expect("event buffer mutex")
            .is_buffering()
    }
    pub fn finish(&self, success: bool) {
        self.0
            .buffer
            .lock()
            .expect("event buffer mutex")
            .finish_function(self.0.origin.elapsed(), success);
        self.0.changed.notify_one();
    }
    pub fn flush(&self) -> Result<bool, Error> {
        let mut buffer = self.0.buffer.lock().expect("event buffer mutex");
        let (flushed, batches) = buffer.flush();
        let result = self.0.deliver(batches);
        self.0.changed.notify_one();
        result.map(|_| flushed)
    }
}
/// Unlike createBufferedFunction, processNodeWithBuffer calls flush immediately.
/// Native cancellation also releases the buffer through this guard.
pub(super) struct NodeScope(Option<Emitter>);
impl NodeScope {
    pub fn finish(mut self) -> Result<bool, Error> {
        self.0.take().expect("online node scope").flush()
    }
}
impl Drop for NodeScope {
    fn drop(&mut self) {
        if let Some(emitter) = self.0.take()
            && let Err(error) = emitter.flush()
        {
            tracing::warn!(?error, "online node buffer release failed");
        }
    }
}

/// A cancelled buffered operation follows the source's error/finally path.
pub(super) struct Scope(pub Option<Emitter>);
impl Scope {
    pub fn finish(mut self, success: bool) {
        if let Some(emitter) = self.0.take() {
            emitter.finish(success);
        }
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(emitter) = &self.0 {
            emitter.finish(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        auth::Contact,
        history,
        messages::{self, group_effects, own},
        proto,
        proto_json::JsonField,
        receipts,
    };
    use serde_json::json;
    use std::time::Duration;
    fn key(id: &str) -> proto::MessageKey {
        proto::MessageKey {
            remote_jid: Some("200@s.whatsapp.net".into()),
            id: Some(id.into()),
            from_me: Some(false),
            participant: None,
        }
    }
    fn message(id: &str) -> proto::WebMessageInfo {
        proto::WebMessageInfo {
            key: Some(key(id)),
            message_timestamp: Some(10),
            message: Some(proto::Message {
                conversation: Some(format!("body {id}")),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn upsert(
        id: &str,
        kind: own::UpsertKind,
        metadata: Option<messages::EnvelopeMetadata>,
    ) -> Event {
        Event::MessagesUpserted(own::Upsert {
            messages: vec![message(id)],
            kind,
            request_id: Some("request".into()),
            metadata: metadata.into_iter().collect(),
            metadata_indices: vec![],
            runtime_fields: vec![],
        })
    }
    fn metadata() -> messages::EnvelopeMetadata {
        messages::EnvelopeMetadata {
            key_metadata: messages::KeyMetadata {
                remote_jid_alt: Some("201@lid".into()),
                addressing_mode: "pn".into(),
                ..Default::default()
            },
            category: Some("peer".into()),
            retry_count: Some(2),
            author: "200@s.whatsapp.net".into(),
            sender: "200@s.whatsapp.net".into(),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn history_pause_timer_orders_shared_deadlines_and_close_cancels_it() {
        use crate::history::ProgressState;
        let (emitter, mut receiver) = Emitter::new(32);
        emitter.send(Event::PendingNotifications).unwrap();
        receiver.try_recv().unwrap();
        let recent = proto::message::HistorySyncNotification {
            sync_type: Some(3),
            progress: Some(50),
            ..Default::default()
        };
        emitter.history_notification(&recent, true);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(90)).await;
        emitter.buffer();
        emitter
            .send(Event::ContactsUpserted(vec![Contact {
                id: "200@s.whatsapp.net".into(),
                ..Default::default()
            }]))
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        let Event::MessagingHistoryStatus(status) = receiver.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(status.status, ProgressState::Paused);
        assert!(!status.explicit);
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Event::ContactsUpserted(_)
        ));
        emitter.send(Event::PendingNotifications).unwrap();
        receiver.try_recv().unwrap();
        emitter.history_notification(
            &proto::message::HistorySyncNotification {
                sync_type: Some(3),
                progress: Some(100),
                ..Default::default()
            },
            true,
        );
        assert!(receiver.try_recv().is_err());
        emitter
            .send(Event::Connection(super::super::ConnectionState::Closed {
                code: 428,
            }))
            .unwrap();
        receiver.try_recv().unwrap();
        emitter.send(Event::PendingNotifications).unwrap();
        receiver.try_recv().unwrap();
        emitter.history_notification(&recent, true);
        emitter
            .send(Event::Connection(super::super::ConnectionState::Closed {
                code: 428,
            }))
            .unwrap();
        receiver.try_recv().unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(120)).await;
        tokio::task::yield_now().await;
        assert!(receiver.try_recv().is_err());
        emitter.history_notification(&recent, true);
        drop(emitter);
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn completion_auto_flush_and_last_handle_drop_drive_timers_without_leaks() {
        let (emitter, mut receiver) = Emitter::new(64);
        emitter.buffer();
        emitter
            .send(Event::ContactsUpserted(vec![Contact {
                id: "200@s.whatsapp.net".into(),
                name: Some("first".into()),
                ..Default::default()
            }]))
            .unwrap();
        emitter
            .send(Event::ContactsUpdated(vec![Contact {
                id: "200@s.whatsapp.net".into(),
                name: Some("latest".into()),
                ..Default::default()
            }]))
            .unwrap();
        emitter.send(Event::PendingNotifications).unwrap();
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Event::PendingNotifications
        ));
        emitter.finish(true);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(receiver.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        let Event::ContactsUpserted(contacts) = receiver.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(contacts[0].name.as_deref(), Some("latest"));
        assert!(!emitter.is_buffering());
        emitter.buffer();
        emitter
            .send(upsert("M", own::UpsertKind::Notify, None))
            .unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Event::MessagesUpserted(_)
        ));
        assert!(!emitter.is_buffering());
        drop(emitter);
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn failed_and_cancelled_scopes_flush_and_unbuffered_events_keep_native_data() {
        let (emitter, mut receiver) = Emitter::new(32);
        emitter
            .send(upsert("M", own::UpsertKind::Notify, Some(metadata())))
            .unwrap();
        let Event::MessagesUpserted(first) = receiver.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(first.request_id.as_deref(), Some("request"));
        assert_eq!(first.metadata_for(0), Some(&metadata()));
        emitter.buffer();
        emitter.buffer();
        emitter
            .send(upsert("M", own::UpsertKind::Notify, None))
            .unwrap();
        Scope(Some(emitter.clone())).finish(false);
        assert_eq!(emitter.0.buffer.lock().unwrap().buffer_count(), 1);
        drop(Scope(Some(emitter.clone())));
        assert_eq!(emitter.0.buffer.lock().unwrap().buffer_count(), 0);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Event::MessagesUpserted(_)
        ));
        assert!(!emitter.is_buffering());
    }
    #[tokio::test]
    async fn message_absorption_keeps_sparse_metadata_complete_payload_and_read_counters() {
        let (emitter, mut receiver) = Emitter::new(64);
        emitter.buffer();
        emitter
            .send(Event::ChatsUpdated(vec![own::ChatUpdate {
                id: "200@s.whatsapp.net".into(),
                unread_count: Some(2),
                ..Default::default()
            }]))
            .unwrap();
        emitter
            .send(upsert("OWN", own::UpsertKind::Notify, None))
            .unwrap();
        emitter
            .send(upsert("M", own::UpsertKind::Notify, Some(metadata())))
            .unwrap();
        emitter
            .send(Event::MessagesStarred(vec![crate::app_state::StarUpdate {
                key: key("M"),
                starred: true,
            }]))
            .unwrap();
        emitter
            .send(Event::MessagesUpdated(vec![receipts::MessageUpdate {
                key: key("M"),
                status: proto::web_message_info::Status::Read,
                message_timestamp: 25.0,
            }]))
            .unwrap();
        emitter
            .send(Event::MessageReactions(vec![own::ReactionUpdate {
                key: Some(key("M")),
                key_metadata: None,
                reaction: proto::Reaction {
                    key: Some(key("R")),
                    text: Some("👍".into()),
                    ..Default::default()
                },
            }]))
            .unwrap();
        emitter
            .send(Event::MessageReceiptsUpdated(vec![receipts::UserUpdate {
                key: key("M"),
                user_jid: "300@s.whatsapp.net".into(),
                receipt_timestamp: Some(20.0),
                read_timestamp: None,
            }]))
            .unwrap();
        emitter
            .send(Event::MessageReceiptsUpdated(vec![receipts::UserUpdate {
                key: key("M"),
                user_jid: "300@s.whatsapp.net".into(),
                receipt_timestamp: None,
                read_timestamp: Some(24.0),
            }]))
            .unwrap();
        assert!(receiver.try_recv().is_err());
        emitter.flush().unwrap();
        let mut received = None;
        let mut chat = None;
        while let Ok(event) = receiver.try_recv() {
            match event {
                Event::MessagesUpserted(v) => received = Some(v),
                Event::ChatsUpdated(v) => chat = Some(v),
                _ => panic!("unexpected output"),
            }
        }
        let upsert = received.unwrap();
        assert_eq!(upsert.messages.len(), 2);
        assert_eq!(upsert.request_id, None);
        assert_eq!(upsert.metadata_for(0), None);
        assert_eq!(upsert.metadata_for(1), Some(&metadata()));
        let message = &upsert.messages[1];
        assert_eq!(
            message.message.as_ref().unwrap().conversation.as_deref(),
            Some("body M")
        );
        assert_eq!(message.status, Some(4));
        assert_eq!(message.starred, Some(true));
        assert_eq!(message.message_timestamp, Some(25));
        assert_eq!(message.reactions[0].text.as_deref(), Some("👍"));
        assert_eq!(message.user_receipt[0].receipt_timestamp, Some(20));
        assert_eq!(message.user_receipt[0].read_timestamp, Some(24));
        assert_eq!(chat.unwrap()[0].unread_count, Some(1));
    }
    #[tokio::test]
    async fn all_update_variants_consolidate_and_type_mismatch_releases_previous_messages() {
        let (emitter, mut receiver) = Emitter::new(64);
        emitter.buffer();
        emitter
            .send(upsert("A", own::UpsertKind::Append, None))
            .unwrap();
        emitter
            .send(upsert("N", own::UpsertKind::Notify, None))
            .unwrap();
        let Event::MessagesUpserted(append) = receiver.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(append.kind, own::UpsertKind::Append);
        emitter
            .send(Event::MessagesDeleted(vec![key("N")]))
            .unwrap();
        emitter
            .send(Event::MessagesStarred(vec![crate::app_state::StarUpdate {
                key: key("M"),
                starred: true,
            }]))
            .unwrap();
        emitter
            .send(Event::MessagesUpdated(vec![receipts::MessageUpdate {
                key: key("M"),
                status: proto::web_message_info::Status::Read,
                message_timestamp: 20.0,
            }]))
            .unwrap();
        emitter
            .send(Event::MessageStubsUpdated(vec![
                messages::incoming::StubUpdate {
                    key: key("M"),
                    parameters: vec!["new".into()],
                },
            ]))
            .unwrap();
        let body = Box::new(proto::Message {
            conversation: Some("edited".into()),
            ..Default::default()
        });
        emitter
            .send(Event::MessageContentsUpdated(vec![
                crate::media::ContentUpdate {
                    key: key("M"),
                    message: Some(body.clone()),
                },
            ]))
            .unwrap();
        emitter
            .send(Event::MessageMutations(vec![
                own::Mutation::Edit {
                    key: key("M"),
                    message: body.clone(),
                    timestamp: 30.0,
                },
                own::Mutation::Revoke {
                    key: key("R"),
                    original_key: key("REVOKE"),
                },
            ]))
            .unwrap();
        emitter
            .send(Event::MessageReactions(vec![own::ReactionUpdate {
                key: Some(key("M")),
                key_metadata: None,
                reaction: proto::Reaction {
                    key: Some(key("USER")),
                    text: Some("".into()),
                    ..Default::default()
                },
            }]))
            .unwrap();
        emitter
            .send(Event::MessageReceiptsUpdated(vec![receipts::UserUpdate {
                key: key("M"),
                user_jid: "300@s.whatsapp.net".into(),
                receipt_timestamp: Some(20.0),
                read_timestamp: None,
            }]))
            .unwrap();
        emitter
            .send(Event::ChatsDeleted(vec![Some(
                "deleted@s.whatsapp.net".into(),
            )]))
            .unwrap();
        emitter.flush().unwrap();
        let mut observed = 0;
        while let Ok(event) = receiver.try_recv() {
            match event {
                Event::MessagesDeleted(v) => {
                    assert_eq!(v, vec![key("N")]);
                    observed += 1;
                }
                Event::MessageUpdates(v) => {
                    assert_eq!(v.len(), 2);
                    assert_eq!(v[0].key, key("M"));
                    assert_eq!(v[0].info.starred, Some(true));
                    assert_eq!(v[0].info.status, Some(4));
                    assert_eq!(v[0].info.message_timestamp, Some(30));
                    assert_eq!(v[0].info.message_stub_parameters, vec!["new"]);
                    assert_eq!(v[0].info.message.as_ref(), Some(body.as_ref()));
                    assert_eq!(v[0].update_value()["messageTimestamp"], json!(30.0));
                    assert_eq!(v[1].key, key("R"));
                    assert_eq!(v[1].info.key, Some(key("REVOKE")));
                    assert_eq!(v[1].update_value()["message"], serde_json::Value::Null);
                    observed += 1;
                }
                Event::MessageReactions(v) => {
                    assert_eq!(v[0].reaction.text.as_deref(), Some(""));
                    observed += 1;
                }
                Event::MessageReceiptsUpdated(v) => {
                    assert_eq!(v[0].receipt_timestamp, Some(20.0));
                    observed += 1;
                }
                Event::ChatsDeleted(v) => {
                    assert_eq!(v, vec![Some("deleted@s.whatsapp.net".into())]);
                    observed += 1;
                }
                _ => panic!("unexpected output {event:?}"),
            }
        }
        assert_eq!(observed, 5);
    }
    #[tokio::test]
    async fn reactions_from_pn_and_lid_keys_replace_the_same_author() {
        let (emitter, mut receiver) = Emitter::new(32);
        emitter.buffer();
        emitter
            .send(upsert("M", own::UpsertKind::Notify, None))
            .unwrap();
        for (remote, text, key_metadata) in [
            ("200@s.whatsapp.net", "first", Some(metadata().key_metadata)),
            ("201@lid", "latest", None),
        ] {
            let mut reactor = key("R");
            reactor.remote_jid = Some(remote.into());
            emitter
                .send(Event::MessageReactions(vec![own::ReactionUpdate {
                    key: Some(key("M")),
                    reaction: proto::Reaction {
                        key: Some(reactor),
                        text: Some(text.into()),
                        ..Default::default()
                    },
                    key_metadata,
                }]))
                .unwrap();
        }
        emitter.flush().unwrap();
        let Event::MessagesUpserted(upsert) = receiver.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(upsert.messages[0].reactions.len(), 1);
        assert_eq!(
            upsert.messages[0].reactions[0].text.as_deref(),
            Some("latest")
        );
    }
    #[tokio::test]
    async fn history_and_group_projections_preserve_conditions_metadata_and_first_group_update() {
        let (emitter, mut receiver) = Emitter::new(64);
        emitter.buffer();
        emitter
            .send(Event::ChatsUpdated(vec![own::ChatUpdate {
                id: "200@s.whatsapp.net".into(),
                pinned: Some(Some(1_791_799_695_000.0)),
                conditional: Some(crate::app_state::ChatCondition {
                    id: "200@s.whatsapp.net".into(),
                    message_range: None,
                }),
                ..Default::default()
            }]))
            .unwrap();
        let mut sync = proto::HistorySync {
            sync_type: 0,
            conversations: vec![proto::Conversation {
                id: "200@s.whatsapp.net".into(),
                messages: vec![proto::HistorySyncMsg {
                    message: Some(message("M")),
                    msg_order_id: None,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let set = history::Set {
            data: history::process(&mut sync).unwrap(),
            is_latest: Some(true),
            chunk_order: Some(0),
            peer_data_request_session_id: Some("".into()),
        };
        emitter.send(Event::MessagingHistorySet(set)).unwrap();
        emitter
            .send(upsert("M", own::UpsertKind::Notify, Some(metadata())))
            .unwrap();
        // Source absorbs only matching history messages; a new one stays an upsert.
        emitter
            .send(upsert("NEW", own::UpsertKind::Notify, None))
            .unwrap();
        let author = group_effects::Author {
            jid: Some("300@s.whatsapp.net".into()),
            pn: None,
            username: Some("alice".into()),
        };
        let first = group_effects::GroupEvent::Changed {
            id: "group@g.us".into(),
            author: author.clone(),
            change: group_effects::Change::Announce(true),
        };
        emitter
            .send(Event::GroupEffects(vec![
                first.clone(),
                group_effects::GroupEvent::Changed {
                    id: "group@g.us".into(),
                    author,
                    change: group_effects::Change::Restrict(true),
                },
            ]))
            .unwrap();
        emitter.flush().unwrap();
        let mut history_seen = false;
        let mut group_seen = false;
        while let Ok(event) = receiver.try_recv() {
            match event {
                Event::MessagingHistorySet(set) => {
                    assert_eq!(set.data.messages.len(), 1);
                    assert_eq!(set.data.message_metadata[0], Some(metadata()));
                    assert_eq!(
                        set.data.message_value(0).unwrap()["key"]["remoteJidAlt"],
                        json!("201@lid")
                    );
                    assert_eq!(set.is_latest, Some(true));
                    assert_eq!(set.chunk_order, Some(0));
                    assert_eq!(set.peer_data_request_session_id.as_deref(), Some(""));
                    assert_eq!(
                        set.data.chats[0].to_value()["pinned"],
                        json!(1_791_799_695_000.0)
                    );
                    history_seen = true;
                }
                Event::GroupEffects(v) => {
                    assert_eq!(v, vec![first.clone()]);
                    group_seen = true;
                }
                Event::MessagesUpserted(v) => {
                    assert_eq!(
                        v.messages[0].key.as_ref().unwrap().id.as_deref(),
                        Some("NEW")
                    );
                    assert_eq!(v.metadata_for(0), None);
                }
                _ => (),
            }
        }
        assert!(history_seen && group_seen);
    }
}
