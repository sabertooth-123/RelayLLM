//! In-memory fan-out of [`DashboardEvent`]s.
//!
//! The store publishes after each committed change. Sequence assignment,
//! the recent-events ring and the broadcast all happen under one lock, so
//! subscribers see events in `seq` order with no holes. A subscriber that
//! falls more than [`CHANNEL_CAPACITY`] events behind is told it lagged and
//! must re-fetch the snapshot; nothing is persisted.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use relay_core::clock::Timestamp;
use relay_core::events::{DashboardEvent, EventKind};
use relay_core::job::JobId;
use tokio::sync::broadcast;

pub const CHANNEL_CAPACITY: usize = 1_024;
pub const RECENT_EVENTS: usize = 200;

#[derive(Clone)]
pub struct EventBus {
    epoch: String,
    state: Arc<Mutex<BusState>>,
    sender: broadcast::Sender<DashboardEvent>,
}

struct BusState {
    seq: u64,
    recent: VecDeque<DashboardEvent>,
}

impl EventBus {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            epoch: JobId::generate().to_string(),
            state: Arc::new(Mutex::new(BusState {
                seq: 0,
                recent: VecDeque::with_capacity(RECENT_EVENTS),
            })),
            sender,
        }
    }

    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BusState> {
        // A poisoned lock means a publisher panicked mid-update; the counter
        // and ring are still usable.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn publish(&self, at_ms: Timestamp, kind: EventKind) {
        let mut state = self.lock();
        state.seq += 1;
        let event = DashboardEvent {
            seq: state.seq,
            epoch: self.epoch.clone(),
            at_ms,
            kind,
        };
        if state.recent.len() == RECENT_EVENTS {
            state.recent.pop_front();
        }
        state.recent.push_back(event.clone());
        // No subscribers is fine: send only fails when nobody listens.
        let _ = self.sender.send(event);
    }

    /// The `seq` of the last published event (0 if none).
    pub fn current_seq(&self) -> u64 {
        self.lock().seq
    }

    pub fn recent(&self) -> Vec<DashboardEvent> {
        self.lock().recent.iter().cloned().collect()
    }

    /// Subscribe, also returning the `seq` of the last event published
    /// before the subscription. Taken under the publish lock, so the
    /// receiver's first event is exactly `seq + 1`.
    pub fn subscribe(&self) -> (u64, broadcast::Receiver<DashboardEvent>) {
        let state = self.lock();
        (state.seq, self.sender.subscribe())
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_core::events::EventKind;

    fn heartbeat(n: u32) -> EventKind {
        EventKind::WorkerHeartbeat {
            worker_id: "w".into(),
            job_id: JobId::generate(),
            attempt: n,
            lease_expires_at_ms: 0,
        }
    }

    #[test]
    fn seq_is_contiguous_and_the_ring_is_bounded() {
        let bus = EventBus::new();
        let (start, mut rx) = bus.subscribe();
        assert_eq!(start, 0);
        for n in 0..(RECENT_EVENTS as u32 + 5) {
            bus.publish(0, heartbeat(n));
        }
        assert_eq!(bus.current_seq(), RECENT_EVENTS as u64 + 5);
        let recent = bus.recent();
        assert_eq!(recent.len(), RECENT_EVENTS);
        assert_eq!(recent[0].seq, 6, "oldest events fall off the ring");
        for expected in 1..=3 {
            assert_eq!(rx.try_recv().unwrap().seq, expected);
        }
    }

    #[test]
    fn each_bus_has_its_own_epoch() {
        assert_ne!(EventBus::new().epoch(), EventBus::new().epoch());
    }
}
