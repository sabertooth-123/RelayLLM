//! Claiming jobs and recording attempt outcomes.
//!
//! Every operation here touches two rows (`jobs` and `job_attempts`), so each
//! runs as one `BEGIN IMMEDIATE` transaction. If the coordinator crashes, or
//! the request future is dropped, between the two writes, neither is
//! applied. `IMMEDIATE` takes the write lock up front: a `DEFERRED`
//! transaction that reads and then writes can fail with
//! `SQLITE_BUSY_SNAPSHOT`, which the busy timeout does not retry.
//!
//! Ownership checks never consult the lease: a report is accepted iff the job
//! is `running` at exactly the reported attempt, owned by the reporting
//! worker. The attempt number is the fencing token (ADR 0002).

use std::time::Duration;

use relay_core::job::{AttemptOutcome, Job, JobId, JobState};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction};

use super::{Store, StoreError, duration_ms, job_from_row, retry_at};

/// Claim the oldest eligible job whose kind the worker registered for.
/// Inside an `IMMEDIATE` transaction we already hold the write lock, so the
/// subquery's choice can't be stolen; the outer `state IN` is belt and braces.
///
/// `attempt_deadline = now + timeout + grace` is fixed for the attempt; the
/// lease starts at `min(now + lease, deadline)` so it can never outlive it.
const CLAIM_NEXT: &str = "
    UPDATE jobs
    SET state = 'running', attempt = attempt + 1, worker_id = ?,
        attempt_deadline = ? + timeout_ms,           -- now + grace + timeout
        lease_expires_at = MIN(?, ? + timeout_ms),   -- now + lease, capped
        updated_at = ?
    WHERE id = (
        SELECT id FROM jobs
        WHERE state IN ('queued', 'retry_waiting')
          AND available_at <= ?
          AND kind IN (SELECT value FROM json_each(?))
        ORDER BY available_at, id
        LIMIT 1)
      AND state IN ('queued', 'retry_waiting')
    RETURNING *";

/// The attempt (if any) previously created by this exact claim request.
const FIND_CLAIM: &str = "
    SELECT j.*, a.attempt AS claimed_attempt
    FROM job_attempts a JOIN jobs j ON j.id = a.job_id
    WHERE a.worker_id = ? AND a.claim_id = ?";

const COMPLETE: &str = "
    UPDATE jobs
    SET state = 'succeeded', result = ?, last_error = NULL,
        worker_id = NULL, lease_expires_at = NULL, attempt_deadline = NULL,
        finished_at = ?, updated_at = ?
    WHERE id = ? AND attempt = ? AND state = 'running' AND worker_id = ?
    RETURNING *";

/// Retryable failures with attempts left go back to `retry_waiting`;
/// everything else is terminal. SET expressions see the *old* row, so
/// `attempt < max_attempts` is evaluated before this update. `available_at`
/// is the backoff deadline (ignored once the job is terminal).
const FAIL: &str = "
    UPDATE jobs
    SET state = CASE WHEN ? AND attempt < max_attempts
                     THEN 'retry_waiting' ELSE 'failed' END,
        finished_at = CASE WHEN ? AND attempt < max_attempts
                           THEN NULL ELSE ? END,
        available_at = ?, last_error = ?,
        worker_id = NULL, lease_expires_at = NULL, attempt_deadline = NULL,
        updated_at = ?
    WHERE id = ? AND attempt = ? AND state = 'running' AND worker_id = ?
    RETURNING *";

/// Result of a complete/fail report.
#[derive(Debug, Clone, PartialEq)]
pub enum ReportOutcome {
    /// This call changed the job.
    Applied(Job),
    /// An identical report for this attempt was already recorded (the
    /// worker is retrying after a lost response). Nothing changed.
    AlreadyApplied(Job),
}

impl ReportOutcome {
    pub fn job(&self) -> &Job {
        match self {
            ReportOutcome::Applied(job) | ReportOutcome::AlreadyApplied(job) => job,
        }
    }
}

impl Store {
    /// Claim one job for `worker_id`, or return `None` if nothing is eligible.
    ///
    /// Idempotent per `(worker_id, claim_id)`: if this claim request already
    /// created an attempt (its response was lost), the same assignment is
    /// returned while it is still current, and *no other job is claimed*.
    /// If that assignment has since ended, `None` is returned and the worker
    /// should use a fresh `claim_id`.
    pub async fn claim(
        &self,
        worker_id: &str,
        claim_id: &str,
        lease: Duration,
    ) -> Result<Option<Job>, StoreError> {
        JobState::Queued.transition_to(JobState::Running)?;
        JobState::RetryWaiting.transition_to(JobState::Running)?;

        let now = self.clock.now_ms();
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;

        let kinds: Option<String> =
            sqlx::query_scalar("UPDATE workers SET last_seen_at = ? WHERE id = ? RETURNING kinds")
                .bind(now)
                .bind(worker_id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(kinds) = kinds else {
            return Err(StoreError::WorkerNotRegistered(worker_id.to_owned()));
        };

        if let Some(row) = sqlx::query(FIND_CLAIM)
            .bind(worker_id)
            .bind(claim_id)
            .fetch_optional(&mut *tx)
            .await?
        {
            let job = job_from_row(&row)?;
            let claimed_attempt: i64 = row.try_get("claimed_attempt")?;
            tx.commit().await?;
            let still_current = job.state == JobState::Running
                && i64::from(job.attempt) == claimed_attempt
                && job.worker_id.as_deref() == Some(worker_id);
            return Ok(still_current.then_some(job));
        }

        let deadline_base = now.saturating_add(duration_ms(self.options.deadline_grace));
        let lease_end = now.saturating_add(duration_ms(lease));
        let Some(row) = sqlx::query(CLAIM_NEXT)
            .bind(worker_id)
            .bind(deadline_base) // attempt_deadline = this + timeout_ms
            .bind(lease_end) // lease = MIN(this, ...
            .bind(deadline_base) // ... deadline)
            .bind(now) // updated_at
            .bind(now) // available_at <= now
            .bind(&kinds)
            .fetch_optional(&mut *tx)
            .await?
        else {
            tx.commit().await?;
            return Ok(None);
        };
        let job = job_from_row(&row)?;

        sqlx::query(
            "INSERT INTO job_attempts (job_id, attempt, worker_id, claim_id, started_at)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(job.id.to_string())
        .bind(i64::from(job.attempt))
        .bind(worker_id)
        .bind(claim_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(job))
    }

    /// Record a successful result for `attempt`. Fenced: only the worker
    /// that currently owns the job at exactly this attempt can complete it.
    pub async fn complete(
        &self,
        job_id: JobId,
        attempt: u32,
        worker_id: &str,
        result: &Value,
    ) -> Result<ReportOutcome, StoreError> {
        JobState::Running.transition_to(JobState::Succeeded)?;
        let now = self.clock.now_ms();
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;

        let updated = sqlx::query(COMPLETE)
            .bind(result.to_string())
            .bind(now)
            .bind(now)
            .bind(job_id.to_string())
            .bind(i64::from(attempt))
            .bind(worker_id)
            .fetch_optional(&mut *tx)
            .await?;

        let outcome = AttemptOutcome::Succeeded;
        match updated {
            Some(row) => {
                let job = job_from_row(&row)?;
                finish_attempt(&mut tx, job_id, attempt, now, outcome, None).await?;
                tx.commit().await?;
                Ok(ReportOutcome::Applied(job))
            }
            None => rejected_report(&mut tx, job_id, attempt, worker_id, outcome).await,
        }
    }

    /// Record a failed attempt. `retryable` failures go to `retry_waiting`
    /// while attempts remain; otherwise the job becomes `failed`.
    pub async fn fail(
        &self,
        job_id: JobId,
        attempt: u32,
        worker_id: &str,
        error: &str,
        retryable: bool,
    ) -> Result<ReportOutcome, StoreError> {
        JobState::Running.transition_to(JobState::RetryWaiting)?;
        JobState::Running.transition_to(JobState::Failed)?;
        let now = self.clock.now_ms();
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;

        let updated = sqlx::query(FAIL)
            .bind(retryable)
            .bind(retryable)
            .bind(now) // finished_at when terminal
            .bind(retry_at(&self.options.retry, now, attempt)) // available_at
            .bind(error)
            .bind(now)
            .bind(job_id.to_string())
            .bind(i64::from(attempt))
            .bind(worker_id)
            .fetch_optional(&mut *tx)
            .await?;

        let outcome = if retryable {
            AttemptOutcome::RetryableError
        } else {
            AttemptOutcome::PermanentError
        };
        match updated {
            Some(row) => {
                let job = job_from_row(&row)?;
                finish_attempt(&mut tx, job_id, attempt, now, outcome, Some(error)).await?;
                tx.commit().await?;
                Ok(ReportOutcome::Applied(job))
            }
            None => rejected_report(&mut tx, job_id, attempt, worker_id, outcome).await,
        }
    }
}

/// Close the attempt row in the same transaction as the job update. Exactly
/// one open row must exist; anything else means jobs and history disagree,
/// and the whole transaction is rolled back.
pub(super) async fn finish_attempt(
    tx: &mut Transaction<'static, Sqlite>,
    job_id: JobId,
    attempt: u32,
    now: i64,
    outcome: AttemptOutcome,
    error: Option<&str>,
) -> Result<(), StoreError> {
    let updated = sqlx::query(
        "UPDATE job_attempts SET finished_at = ?, outcome = ?, error = ?
         WHERE job_id = ? AND attempt = ? AND finished_at IS NULL",
    )
    .bind(now)
    .bind(outcome.as_str())
    .bind(error)
    .bind(job_id.to_string())
    .bind(i64::from(attempt))
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if updated == 1 {
        Ok(())
    } else {
        Err(StoreError::Corrupt {
            job_id: job_id.to_string(),
            reason: format!("expected one open attempt row for attempt {attempt}, found {updated}"),
        })
    }
}

/// The fenced update matched nothing. Distinguish a harmless retry of a
/// report that already landed from a genuinely stale attempt.
///
/// The check is on the *attempt history*, not the job's current state: if
/// this worker's `fail(attempt 1)` landed, the job may already be running
/// attempt 2 elsewhere, yet a retried `fail(attempt 1)` must still be
/// answered `200`, not `409`.
async fn rejected_report(
    tx: &mut Transaction<'static, Sqlite>,
    job_id: JobId,
    attempt: u32,
    worker_id: &str,
    intended: AttemptOutcome,
) -> Result<ReportOutcome, StoreError> {
    let job = sqlx::query("SELECT * FROM jobs WHERE id = ?")
        .bind(job_id.to_string())
        .fetch_optional(&mut **tx)
        .await?
        .as_ref()
        .map(job_from_row)
        .transpose()?
        .ok_or(StoreError::JobNotFound(job_id))?;

    let recorded: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT worker_id, outcome FROM job_attempts WHERE job_id = ? AND attempt = ?",
    )
    .bind(job_id.to_string())
    .bind(i64::from(attempt))
    .fetch_optional(&mut **tx)
    .await?;

    let already_applied = matches!(
        &recorded,
        Some((owner, Some(outcome))) if owner == worker_id && outcome == intended.as_str()
    );
    if already_applied {
        Ok(ReportOutcome::AlreadyApplied(job))
    } else {
        Err(StoreError::StaleAttempt {
            job_id,
            attempt,
            current_attempt: job.attempt,
            state: job.state,
        })
    }
}
