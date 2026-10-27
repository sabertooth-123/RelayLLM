//! Background task that recovers jobs whose lease expired.

use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::store::{Store, wall_clock_jumped};

#[derive(Debug, Clone)]
pub struct ReaperConfig {
    pub lease: Duration,
    /// Time between reap passes.
    pub interval: Duration,
    /// Max jobs expired per transaction (keeps the write lock short). A full
    /// batch triggers another pass immediately.
    pub batch: u32,
    /// Forward wall-clock jumps larger than this between ticks grant grace
    /// instead of reaping (§5.1 #6). `None` disables detection; tests that
    /// drive a manual clock need that.
    pub jump_tolerance: Option<Duration>,
}

impl ReaperConfig {
    /// Sensible defaults for a lease: about 4 passes per lease period, so a
    /// dead worker's job is recovered within roughly 1.25 leases.
    pub fn for_lease(lease: Duration) -> Self {
        Self {
            lease,
            interval: (lease / 4).clamp(Duration::from_millis(50), Duration::from_secs(5)),
            batch: 500,
            jump_tolerance: Some(lease.max(Duration::from_secs(5))),
        }
    }
}

/// Spawn the reaper. Abort the handle to stop it. Aborting mid-transaction is
/// safe: sqlx rolls back a dropped transaction, and every reap is idempotent.
///
/// The first pass runs immediately: on startup, jobs that are genuinely
/// abandoned are recovered at once. Grant the startup lease grace *before*
/// spawning, or that first pass will reap jobs whose workers are alive but
/// haven't reconnected yet.
pub fn spawn_reaper(store: Store, config: ReaperConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        reap_all(&store, config.batch).await;
        let mut last_wall = store.now_ms();
        let mut last_mono = Instant::now();
        loop {
            tokio::time::sleep(config.interval).await;
            let (wall, mono) = (store.now_ms(), Instant::now());
            let (wall_elapsed_ms, mono_elapsed) = (wall - last_wall, mono - last_mono);
            (last_wall, last_mono) = (wall, mono);
            let jumped = config.jump_tolerance.is_some_and(|tolerance| {
                wall_clock_jumped(wall_elapsed_ms, mono_elapsed, tolerance)
            });

            if jumped {
                warn!(
                    wall_elapsed_ms,
                    mono_elapsed_ms = mono_elapsed.as_millis(),
                    "wall clock jumped forward; granting lease grace instead of reaping"
                );
                if let Err(err) = store.grant_lease_grace(config.lease).await {
                    error!(error = %err, "lease grace after clock jump failed");
                }
                continue;
            }
            reap_all(&store, config.batch).await;
        }
    })
}

async fn reap_all(store: &Store, batch: u32) {
    loop {
        match store.reap_expired(batch).await {
            Ok(reaped) => {
                for job in &reaped {
                    info!(
                        job_id = %job.job_id, attempt = job.attempt, worker_id = %job.worker_id,
                        new_state = %job.new_state, reason = %job.reason,
                        "lease expired; job recovered"
                    );
                }
                if reaped.len() < usize::try_from(batch).unwrap_or(usize::MAX) {
                    return;
                }
            }
            Err(err) => {
                // Transient (e.g. SQLITE_BUSY past the timeout): try next tick.
                error!(error = %err, "reap pass failed");
                return;
            }
        }
    }
}
