//! Background task that advances workflow runs (docs/WORKFLOWS_DESIGN.md §4.2).
//!
//! One task for all runs: no task per run and none per timer, so a run
//! sleeping for a week costs one indexed row, not a parked future. The task
//! decides every due run, then sleeps until the earliest `wake_at` (capped
//! by `max_sleep`), or until a commit nudges it. Restart recovery is the
//! first pass, which runs immediately on spawn: overdue timers fire and runs
//! made due before a crash are decided.

use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::store::Store;

#[derive(Debug, Clone)]
pub struct DeciderConfig {
    /// Longest sleep between passes. Bounds timer lateness if the wall clock
    /// jumps, and a nudge that was somehow lost.
    pub max_sleep: Duration,
    /// Runs decided per `decide_due` call. A full batch triggers another
    /// call at once.
    pub batch: u32,
}

impl Default for DeciderConfig {
    fn default() -> Self {
        Self {
            max_sleep: Duration::from_secs(1),
            batch: 100,
        }
    }
}

/// Spawn the decider. Abort the handle to stop it. Aborting mid-decision is
/// safe: the transaction rolls back and the run is still due.
pub fn spawn_decider(store: Store, config: DeciderConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let wakeups = store.decider_wakeups();
        loop {
            decide_all(&store, config.batch).await;
            let sleep = next_sleep(&store, config.max_sleep).await;
            tokio::select! {
                () = tokio::time::sleep(sleep) => {}
                // `notify_one` stores a permit, so a nudge that arrived
                // during the pass above is not lost.
                () = wakeups.notified() => {}
            }
        }
    })
}

async fn decide_all(store: &Store, batch: u32) {
    loop {
        match store.decide_due(batch).await {
            Ok(pass) => {
                if pass.broken > 0 {
                    warn!(broken = pass.broken, "workflow runs quarantined this pass");
                }
                if pass.deferred > 0 {
                    info!(
                        deferred = pass.deferred,
                        "queue full; workflow scheduling deferred"
                    );
                }
                let handled = pass.decided + pass.deferred + pass.broken;
                if handled < usize::try_from(batch).unwrap_or(usize::MAX) {
                    return;
                }
            }
            Err(err) => {
                // Transient (e.g. SQLITE_BUSY past the timeout): next tick.
                error!(error = %err, "workflow decision pass failed");
                return;
            }
        }
    }
}

/// Until the earliest due time, capped; `max_sleep` if nothing is waiting.
async fn next_sleep(store: &Store, max_sleep: Duration) -> Duration {
    match store.next_wake_at().await {
        Ok(Some(at)) => {
            let delta = u64::try_from(at.saturating_sub(store.now_ms())).unwrap_or(0);
            Duration::from_millis(delta).min(max_sleep)
        }
        Ok(None) => max_sleep,
        Err(err) => {
            error!(error = %err, "reading the next workflow wake time failed");
            max_sleep
        }
    }
}
