//! Worker registry, attempt history and metrics queries.

use std::time::Duration;

use relay_core::api::{AttemptView, RegisterWorkerRequest, WorkerView};
use relay_core::events::EventKind;
use relay_core::job::{AttemptOutcome, JobId, JobState};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use super::{Store, StoreError, duration_ms};

pub(super) const SELECT_WORKERS: &str = "
    SELECT w.*,
           (SELECT COUNT(*) FROM jobs j WHERE j.worker_id = w.id AND j.state = 'running')
               AS running_jobs
    FROM workers w";

impl Store {
    /// Idempotent upsert keyed by the worker-chosen id. Re-registering
    /// updates name, concurrency and kinds and keeps `registered_at`.
    pub async fn register_worker(
        &self,
        id: &str,
        request: &RegisterWorkerRequest,
    ) -> Result<WorkerView, StoreError> {
        let now = self.clock.now_ms();
        let kinds = serde_json::to_string(&request.kinds).map_err(|e| StoreError::Corrupt {
            job_id: "<worker>".into(),
            reason: format!("kinds: {e}"),
        })?;
        sqlx::query(
            "INSERT INTO workers (id, name, concurrency, kinds, registered_at, last_seen_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (id) DO UPDATE SET
                 name = excluded.name, concurrency = excluded.concurrency,
                 kinds = excluded.kinds, last_seen_at = excluded.last_seen_at",
        )
        .bind(id)
        .bind(&request.name)
        .bind(i64::from(request.concurrency))
        .bind(kinds)
        .bind(now)
        .bind(now)
        .execute(&self.writer)
        .await?;

        // The upsert has committed, so a reader sees it.
        let row = sqlx::query(&format!("{SELECT_WORKERS} WHERE w.id = ?"))
            .bind(id)
            .fetch_one(&self.reader)
            .await?;
        let worker = worker_from_row(&row)?;
        self.events.publish(
            now,
            EventKind::WorkerRegistered {
                worker: worker.clone(),
            },
        );
        Ok(worker)
    }

    pub async fn list_workers(&self) -> Result<Vec<WorkerView>, StoreError> {
        let rows = sqlx::query(&format!("{SELECT_WORKERS} ORDER BY w.registered_at, w.id"))
            .fetch_all(&self.reader)
            .await?;
        rows.iter().map(worker_from_row).collect()
    }

    /// Attempt history, oldest first.
    pub async fn attempts(&self, job_id: JobId) -> Result<Vec<AttemptView>, StoreError> {
        let rows = sqlx::query(
            "SELECT attempt, worker_id, started_at, finished_at, outcome, error
             FROM job_attempts WHERE job_id = ? ORDER BY attempt",
        )
        .bind(job_id.to_string())
        .fetch_all(&self.reader)
        .await?;
        let corrupt = |reason: String| StoreError::Corrupt {
            job_id: job_id.to_string(),
            reason,
        };
        rows.iter()
            .map(|row| {
                let outcome = row
                    .try_get::<Option<String>, _>("outcome")?
                    .map(|raw| raw.parse::<AttemptOutcome>())
                    .transpose()
                    .map_err(|e| corrupt(e.to_string()))?;
                Ok(AttemptView {
                    attempt: u32::try_from(row.try_get::<i64, _>("attempt")?)
                        .map_err(|e| corrupt(format!("attempt: {e}")))?,
                    worker_id: row.try_get("worker_id")?,
                    started_at_ms: row.try_get("started_at")?,
                    finished_at_ms: row.try_get("finished_at")?,
                    outcome,
                    error: row.try_get("error")?,
                })
            })
            .collect()
    }
}

/// Everything `/metrics` reads from the database, in one consistent pass.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    pub jobs_by_state: Vec<(JobState, u64)>,
    pub attempts_by_outcome: Vec<(AttemptOutcome, u64)>,
    pub capacity: u64,
    /// 0 when nothing is waiting.
    pub oldest_eligible_age_ms: i64,
    pub workers_total: u64,
    pub workers_live: u64,
}

impl Store {
    pub async fn metrics_snapshot(
        &self,
        live_window: Duration,
    ) -> Result<MetricsSnapshot, StoreError> {
        let now = self.clock.now_ms();
        let corrupt = |reason: String| StoreError::Corrupt {
            job_id: "<metrics>".into(),
            reason,
        };
        let jobs_by_state = self.count_by_state().await?;

        let rows = sqlx::query(
            "SELECT outcome, COUNT(*) AS n FROM job_attempts
             WHERE outcome IS NOT NULL GROUP BY outcome",
        )
        .fetch_all(&self.reader)
        .await?;
        let mut attempts_by_outcome = Vec::with_capacity(rows.len());
        for row in &rows {
            let outcome = row
                .try_get::<String, _>("outcome")?
                .parse::<AttemptOutcome>()
                .map_err(|e| corrupt(e.to_string()))?;
            let n: i64 = row.try_get("n")?;
            attempts_by_outcome.push((outcome, u64::try_from(n).unwrap_or(0)));
        }

        // Eligible = claimable right now; a job in backoff isn't waiting on
        // workers, it's waiting on its timer.
        let oldest: Option<i64> = sqlx::query_scalar(
            "SELECT MIN(available_at) FROM jobs
             WHERE state IN ('queued', 'retry_waiting') AND available_at <= ?",
        )
        .bind(now)
        .fetch_one(&self.reader)
        .await?;

        let (workers_total, workers_live): (i64, i64) =
            sqlx::query_as("SELECT COUNT(*), COALESCE(SUM(last_seen_at >= ?), 0) FROM workers")
                .bind(now.saturating_sub(duration_ms(live_window)))
                .fetch_one(&self.reader)
                .await?;

        Ok(MetricsSnapshot {
            jobs_by_state,
            attempts_by_outcome,
            capacity: self.options.max_active_jobs,
            oldest_eligible_age_ms: oldest.map_or(0, |at| (now - at).max(0)),
            workers_total: u64::try_from(workers_total).unwrap_or(0),
            workers_live: u64::try_from(workers_live).unwrap_or(0),
        })
    }
}

pub(super) fn worker_from_row(row: &SqliteRow) -> Result<WorkerView, StoreError> {
    let id: String = row.try_get("id")?;
    let corrupt = |reason: String| StoreError::Corrupt {
        job_id: format!("<worker {id}>"),
        reason,
    };
    let kinds: Vec<String> = serde_json::from_str(&row.try_get::<String, _>("kinds")?)
        .map_err(|e| corrupt(format!("kinds: {e}")))?;
    Ok(WorkerView {
        name: row.try_get("name")?,
        concurrency: u32::try_from(row.try_get::<i64, _>("concurrency")?)
            .map_err(|e| corrupt(format!("concurrency: {e}")))?,
        kinds,
        registered_at_ms: row.try_get("registered_at")?,
        last_seen_at_ms: row.try_get("last_seen_at")?,
        running_jobs: u64::try_from(row.try_get::<i64, _>("running_jobs")?).unwrap_or(0),
        id,
    })
}
