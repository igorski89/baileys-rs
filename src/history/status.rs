//! History progress signals emitted before payload download or self-only guards.
use crate::proto;
use std::time::Duration;
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProgressState {
    Complete,
    Paused,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusUpdate {
    pub sync_type: i32,
    pub status: ProgressState,
    pub explicit: bool,
}
#[derive(Debug, Default)]
pub struct StatusTracker {
    pub initial_bootstrap_complete: bool,
    pub recent_sync_complete: bool,
    pending_received: bool,
    deadline: Option<Duration>,
}
impl StatusTracker {
    pub fn next_deadline(&self) -> Option<Duration> {
        self.deadline
    }
    /// Only the first pending-notifications transition resets progress.
    pub fn pending_notifications(&mut self) {
        if !self.pending_received {
            self.pending_received = true;
            self.initial_bootstrap_complete = false;
            self.recent_sync_complete = false;
            self.deadline = None;
        }
    }
    /// Connection close cancels the pause timer; completion flags survive until
    /// the next pending-notifications transition, matching the socket lifecycle.
    pub fn close(&mut self) {
        self.deadline = None;
        self.pending_received = false;
    }
    pub fn observe(
        &mut self,
        notification: &proto::message::HistorySyncNotification,
        eligible: bool,
        now: Duration,
    ) -> Vec<StatusUpdate> {
        if !eligible {
            return vec![];
        }
        use proto::history_sync::HistorySyncType;
        let mut updates = vec![];
        if notification.sync_type == Some(HistorySyncType::InitialBootstrap as i32)
            && !self.initial_bootstrap_complete
        {
            self.initial_bootstrap_complete = true;
            updates.push(StatusUpdate {
                sync_type: HistorySyncType::InitialBootstrap as i32,
                status: ProgressState::Complete,
                explicit: true,
            });
        }
        if notification.sync_type == Some(HistorySyncType::Recent as i32) {
            if notification.progress == Some(100) && !self.recent_sync_complete {
                self.recent_sync_complete = true;
                self.deadline = None;
                updates.push(StatusUpdate {
                    sync_type: HistorySyncType::Recent as i32,
                    status: ProgressState::Complete,
                    explicit: true,
                });
            }
            if !self.recent_sync_complete {
                self.deadline = Some(now + Duration::from_secs(120));
            }
        }
        updates
    }
    pub fn tick(&mut self, now: Duration) -> Vec<StatusUpdate> {
        if self.deadline.is_some_and(|deadline| deadline <= now) {
            self.deadline = None;
            if !self.recent_sync_complete {
                self.recent_sync_complete = true;
                return vec![StatusUpdate {
                    sync_type: proto::history_sync::HistorySyncType::Recent as i32,
                    status: ProgressState::Paused,
                    explicit: false,
                }];
            }
        }
        vec![]
    }
}
