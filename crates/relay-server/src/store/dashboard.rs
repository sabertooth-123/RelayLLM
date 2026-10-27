//! Read model for the live dashboard, and the dev-only failure trigger.

use relay_core::api::WorkerView;
use relay_core::events::{JobSummary, StateCount};
use relay_core::job::JobState;
use sqlx::Row;

use super::workers::{SELECT_WORKERS, worker_from_row};
use super::{ACTIVE_STATES, Store, StoreError, job_from_row};
use crate::events::EventBus;

/// Everything the dashboard snapshot reads, from one database snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct DashboardView {
    /// Exact, zeros included, in [`JobState::ALL`] order.
    pub counts: Vec<StateCount>,
    pub workers: Vec<WorkerView>,
    /// Active jobs, oldest first, then recent terminal jobs, newest first.
    pub jobs: Vec<JobSummary>,
    /// More active jobs exist than `max_active` allowed.
    pub active_truncated: bool,
}

impl Store {
    pub fn events(&self) -> &EventBus {
        &self.events
    }

    pub fn max_active_jobs(&self) -> u64 {
        self.options.max_active_jobs
    }

    /// Counts, workers, up to `max_active` active jobs and the
    /// `recent_terminal` most recently finished ones. All reads share one
    /// read transaction, so they describe the same instant: a job counted as
    /// `running` is listed as `running`. Summaries only: no payloads,
    /// results or idempotency keys.
    pub async fn dashboard_view(
        &self,
        max_active: u32,
        recent_terminal: u32,
    ) -> Result<DashboardView, StoreError> {
        let mut tx = self.reader.begin().await?;

        let rows = sqlx::query("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state")
            .fetch_all(&mut *tx)
            .await?;
        let mut counts: Vec<StateCount> = JobState::ALL
            .into_iter()
            .map(|state| StateCount { state, count: 0 })
            .collect();
        for row in &rows {
            let raw: String = row.try_get("state")?;
            let n: i64 = row.try_get("n")?;
            if let Some(slot) = counts.iter_mut().find(|c| c.state.as_str() == raw) {
                slot.count = u64::try_from(n).unwrap_or(0);
            }
        }

        let workers = sqlx::query(&format!("{SELECT_WORKERS} ORDER BY w.registered_at, w.id"))
            .fetch_all(&mut *tx)
            .await?
            .iter()
            .map(worker_from_row)
            .collect::<Result<_, _>>()?;

        let active = sqlx::query(
            "SELECT * FROM jobs WHERE state IN (?, ?, ?) ORDER BY created_at, id LIMIT ?",
        )
        .bind(ACTIVE_STATES[0].as_str())
        .bind(ACTIVE_STATES[1].as_str())
        .bind(ACTIVE_STATES[2].as_str())
        .bind(i64::from(max_active) + 1)
        .fetch_all(&mut *tx)
        .await?;
        let active_truncated = active.len() > max_active as usize;

        // Every terminal job has `finished_at`; nothing else does.
        let terminal = sqlx::query(
            "SELECT * FROM jobs WHERE finished_at IS NOT NULL
             ORDER BY finished_at DESC, id DESC LIMIT ?",
        )
        .bind(i64::from(recent_terminal))
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        let jobs = active
            .iter()
            .take(max_active as usize)
            .chain(terminal.iter())
            .map(|row| job_from_row(row).map(|job| job.summary()))
            .collect::<Result<_, _>>()?;
        Ok(DashboardView {
            counts,
            workers,
            jobs,
            active_truncated,
        })
    }

    /// Development only: end the lease of every job `worker_id` is running,
    /// as if it had stopped heartbeating, and run the reaper immediately.
    /// The jobs then follow the normal expiry path (`lease_expired` attempt,
    /// retry or fail). The worker itself is untouched; its next report or
    /// heartbeat for those attempts gets `409 stale_attempt`.
    pub async fn expire_worker_leases(&self, worker_id: &str) -> Result<usize, StoreError> {
        let now = self.clock.now_ms();
        sqlx::query(
            "UPDATE jobs SET lease_expires_at = ?
             WHERE state = 'running' AND worker_id = ?",
        )
        .bind(now.saturating_sub(1))
        .bind(worker_id)
        .execute(&self.writer)
        .await?;
        // Other workers' already-expired leases may be reaped too; that's
        // what the background reaper would do on its next tick anyway.
        let reaped = self.reap_expired(u32::MAX).await?;
        Ok(reaped.iter().filter(|r| r.worker_id == worker_id).count())
    }
}
