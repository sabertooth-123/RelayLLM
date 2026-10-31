//! Client-initiated cancellation.
//!
//! Cancel and the worker's complete/fail both take the write lock
//! (`BEGIN IMMEDIATE`) and are fenced on the job's current state, so a race
//! between them has exactly one winner:
//!
//! * cancel commits first: the worker's report gets `409 stale_attempt`, and
//!   its next heartbeat gets `409`, which makes it abort the handler;
//! * the report commits first: cancel gets `409 not_cancellable`, and the
//!   result stands.

use relay_core::clock::Timestamp;
use relay_core::events::EventKind;
use relay_core::job::{AttemptOutcome, Job, JobId, JobState};
use sqlx::{Sqlite, Transaction};

use super::claims::finish_attempt;
use super::workflows::{ActivityOutcome, on_activity_terminal};
use super::{Store, StoreError, job_from_row};

pub const CANCELLED_MESSAGE: &str = "cancelled by client";

#[derive(Debug, Clone, PartialEq)]
pub enum CancelOutcome {
    Cancelled(Job),
    /// The job was already cancelled; cancelling is idempotent.
    AlreadyCancelled(Job),
}

impl CancelOutcome {
    pub fn job(&self) -> &Job {
        match self {
            CancelOutcome::Cancelled(job) | CancelOutcome::AlreadyCancelled(job) => job,
        }
    }
}

impl Store {
    pub async fn cancel(&self, job_id: JobId) -> Result<CancelOutcome, StoreError> {
        let now = self.clock.now_ms();
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;
        let (outcome, from) = cancel_in_tx(&mut tx, job_id, now, CANCELLED_MESSAGE).await?;
        let mut woke = false;
        if let CancelOutcome::Cancelled(_) = &outcome {
            // A workflow activity cancelled directly: the run must learn of it
            // in this same transaction (design §5, §6 #13).
            woke = on_activity_terminal(
                &mut tx,
                self.clock.as_ref(),
                job_id,
                ActivityOutcome::Cancelled,
            )
            .await?;
        }
        tx.commit().await?;
        if woke {
            self.nudge_decider();
        }
        if let (CancelOutcome::Cancelled(cancelled), Some(from)) = (&outcome, from) {
            self.events.publish(
                now,
                EventKind::JobCancelled {
                    from: Some(from),
                    job: cancelled.summary(),
                },
            );
        }
        Ok(outcome)
    }
}

/// Cancel `job_id` inside the caller's `BEGIN IMMEDIATE` transaction, with
/// the fencing described in the module docs. Returns the outcome and, if it
/// changed the job, the state it left. Does **not** touch workflow history:
/// [`Store::cancel`] adds the activity event itself, and the workflow decider
/// appends its own `activity_cancelled` events in its own sequence.
pub(super) async fn cancel_in_tx(
    tx: &mut Transaction<'static, Sqlite>,
    job_id: JobId,
    now: Timestamp,
    message: &str,
) -> Result<(CancelOutcome, Option<JobState>), StoreError> {
    let job = sqlx::query("SELECT * FROM jobs WHERE id = ?")
        .bind(job_id.to_string())
        .fetch_optional(&mut **tx)
        .await?
        .as_ref()
        .map(job_from_row)
        .transpose()?
        .ok_or(StoreError::JobNotFound(job_id))?;

    match job.state {
        JobState::Cancelled => return Ok((CancelOutcome::AlreadyCancelled(job), None)),
        JobState::Succeeded | JobState::Failed => {
            return Err(StoreError::NotCancellable {
                job_id,
                state: job.state,
            });
        }
        JobState::Queued | JobState::Running | JobState::RetryWaiting => {}
    }
    let from = job.state;
    from.transition_to(JobState::Cancelled)?;

    // We hold the write lock, so the row can't have changed since the
    // SELECT; the fence is defence in depth.
    let row = sqlx::query(
        "UPDATE jobs
         SET state = 'cancelled', last_error = ?, worker_id = NULL,
             lease_expires_at = NULL, attempt_deadline = NULL,
             finished_at = ?, updated_at = ?
         WHERE id = ? AND attempt = ? AND state = ?
         RETURNING *",
    )
    .bind(message)
    .bind(now)
    .bind(now)
    .bind(job_id.to_string())
    .bind(i64::from(job.attempt))
    .bind(from.as_str())
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| StoreError::Corrupt {
        job_id: job_id.to_string(),
        reason: "job changed under the write lock".into(),
    })?;
    let cancelled = job_from_row(&row)?;

    if from == JobState::Running {
        finish_attempt(
            tx,
            job_id,
            job.attempt,
            now,
            AttemptOutcome::Cancelled,
            Some(message),
        )
        .await?;
    }
    Ok((CancelOutcome::Cancelled(cancelled), Some(from)))
}
