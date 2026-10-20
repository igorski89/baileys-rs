//! Clock-driven initial synchronization phases from the upstream chat socket.
use std::time::Duration;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Phase {
    #[default]
    Connecting,
    AwaitingInitialSync,
    Syncing,
    Online,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Effects {
    pub buffer: bool,
    pub flush: bool,
    pub increment_counter: bool,
}
#[derive(Debug, Default)]
pub struct Coordinator {
    phase: Phase,
    awaiting: Option<Duration>,
    deferred: Vec<Duration>,
}
impl Coordinator {
    pub fn phase(&self) -> Phase {
        self.phase
    }
    fn next_timer(&self) -> Option<(Duration, Duration, bool)> {
        self.awaiting
            .map(|at| (at, at.saturating_sub(Duration::from_secs(20)), true))
            .into_iter()
            .chain(self.deferred.iter().map(|at| (*at, *at, false)))
            .min()
    }
    pub fn next_deadline(&self) -> Option<Duration> {
        self.next_timer().map(|t| t.0)
    }
    pub fn registration_time(&self) -> Option<Duration> {
        self.next_timer().map(|t| t.1)
    }
    pub fn pending_notifications(
        &mut self,
        will_sync_history: bool,
        account_sync_counter: u32,
        now: Duration,
    ) -> Effects {
        let effects = self.begin_pending_notifications();
        if effects.buffer {
            self.select_history(will_sync_history, account_sync_counter, now);
        }
        effects
    }
    pub fn begin_pending_notifications(&mut self) -> Effects {
        if self.phase != Phase::Connecting {
            return Effects::default();
        }
        self.phase = Phase::AwaitingInitialSync;
        tracing::info!("Connection is now AwaitingInitialSync, buffering events");
        Effects {
            buffer: true,
            ..Default::default()
        }
    }
    pub fn select_history(
        &mut self,
        will_sync_history: bool,
        account_sync_counter: u32,
        now: Duration,
    ) {
        if !will_sync_history || account_sync_counter > 0 {
            if !will_sync_history {
                tracing::info!(
                    "History sync is disabled by config, not waiting for notification. Transitioning to Online."
                );
            } else {
                tracing::info!(
                    "Reconnection with existing sync data, skipping history sync wait. Transitioning to Online."
                );
            }
            self.phase = Phase::Online;
            self.deferred.push(now);
        } else {
            tracing::info!(
                "First connection, awaiting history sync notification with a 20s timeout."
            );
            self.awaiting = Some(now + Duration::from_secs(20));
        }
    }
    pub fn history_notification(&mut self, eligible: bool) -> Effects {
        if self.phase != Phase::AwaitingInitialSync {
            return Effects::default();
        }
        self.awaiting = None;
        if eligible {
            self.phase = Phase::Syncing;
            tracing::info!("Transitioned to Syncing state");
            Effects::default()
        } else {
            self.phase = Phase::Online;
            tracing::info!(
                "History sync skipped, transitioning to Online state and flushing buffer"
            );
            Effects {
                flush: true,
                ..Default::default()
            }
        }
    }
    /// Each sync admitted while Syncing completes independently, as Promise.all
    /// work can overlap; completion does not recheck the phase after the await.
    pub fn sync_completed(&mut self) -> Effects {
        self.phase = Phase::Online;
        Effects {
            flush: true,
            increment_counter: true,
            ..Default::default()
        }
    }
    pub fn tick(&mut self, now: Duration) -> Effects {
        let Some((at, _, wait)) = self.next_timer() else {
            return Effects::default();
        };
        if at > now {
            return Effects::default();
        }
        if wait {
            self.awaiting = None;
        } else {
            let position = self
                .deferred
                .iter()
                .position(|d| *d == at)
                .expect("deferred timer");
            self.deferred.remove(position);
        }
        if wait {
            if self.phase != Phase::AwaitingInitialSync {
                return Effects::default();
            }
            self.phase = Phase::Online;
        }
        Effects {
            flush: true,
            increment_counter: wait,
            ..Default::default()
        }
    }
    pub fn close(&mut self) {
        self.awaiting = None;
        self.phase = Phase::Connecting;
    }
}
