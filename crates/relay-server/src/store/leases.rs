//! Heartbeats, lease expiry ("reaping") and the startup grace period.
//!
//! The lease is the coordinator's *trigger* for recovery. It is not the
//! ownership check: ownership is `state = 'running' AND attempt = :token`, as
//! for every other worker write. A heartbeat that arrives after the lease
//! expired, but before the reaper acted, is still accepted, because nobody
//! else has the job yet.

use std::time::Duration;

use relay_core::clock::Timestamp;
use relay_core::events::EventKind;
use relay_core::job::{AttemptOutcome, Job, JobId, JobState};

use super::claims::finish_attempt;
use super::workflows::{ActivityOutcome, on_activity_terminal};
use super::{Store, StoreError, duration_ms, job_from_row, retry_at};

/// `MAX(current, ...)`: a heartbeat never *shortens* a lease (the startup
/// grace may have pushed it past the deadline). `MIN(now + lease, deadline)`:
/// it never extends one past the attempt deadline (§5.1 #3).
const HEARTBEAT: &str = "
    UPDATE jobs
    SET lease_expires_at = MAX(lease_expires_at, MIN(?, attempt_deadline)),
        updated_at = ?
    WHERE id = ? AND attempt = ? AND state = 'running' AND worker_id = ?
    RETURNING *";

/// A job whose lease the reaper expired.
#[derive(Debug, Clone, PartialEq)]
pub struct ReapedJob {
    pub job_id: JobId,
    pub attempt: u32,
    pub worker_id: String,
    /// `retry_waiting` if attempts remain, else `failed`.
    pub new_state: JobState,
    pub reason: String,
}

impl Store {
    /// Extend the lease of `attempt`, fenced exactly like complete/fail.
    /// `StaleAttempt` tells the worker it lost the job (reaped, or a newer
    /// attempt took over) and should abort the handler.
    pub async fn heartbeat(
        &self,
        job_id: JobId,
        attempt: u32,
        worker_id: &str,
        lease: Duration,
    ) -> Result<Job, StoreError> {
        let now = self.clock.now_ms();
        let row = sqlx::query(HEARTBEAT)
            .bind(now.saturating_add(duration_ms(lease)))
            .bind(now)
            .bind(job_id.to_string())
            .bind(i64::from(attempt))
            .bind(worker_id)
            .fetch_optional(&self.writer)
            .await?;

        // Liveness bookkeeping only; not part of the ownership decision.
        // The previous statement has returned the writer connection, so
        // acquiring it again here can't deadlock.
        sqlx::query("UPDATE workers SET last_seen_at = ? WHERE id = ?")
            .bind(now)
            .bind(worker_id)
            .execute(&self.writer)
            .await?;

        if let Some(row) = row {
            let job = job_from_row(&row)?;
            self.events.publish(
                now,
                EventKind::WorkerHeartbeat {
                    worker_id: worker_id.to_owned(),
                    job_id,
                    attempt,
                    lease_expires_at_ms: job.lease_expires_at.unwrap_or(now),
                },
            );
            return Ok(job);
        }
        let current = self
            .get(job_id)
            .await?
            .ok_or(StoreError::JobNotFound(job_id))?;
        Err(StoreError::StaleAttempt {
            job_id,
            attempt,
            current_attempt: current.attempt,
            state: current.state,
        })
    }

    /// Expire up to `limit` running jobs whose lease ended before now.
    ///
    /// Each job goes to `retry_waiting` (with backoff) if attempts remain,
    /// otherwise `failed`. Its attempt row is closed as `lease_expired`. Each
    /// row is fenced on `(attempt, state, lease_expires_at < now)` inside one
    /// `IMMEDIATE` transaction, so concurrent reap passes can't double-expire
    /// a job, and a heartbeat that won the race keeps its job.
    pub async fn reap_expired(&self, limit: u32) -> Result<Vec<ReapedJob>, StoreError> {
        JobState::Running.transition_to(JobState::RetryWaiting)?;
        JobState::Running.transition_to(JobState::Failed)?;
        let now = self.clock.now_ms();
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;

        let rows = sqlx::query(
            "SELECT * FROM jobs WHERE state = 'running' AND lease_expires_at < ?
             ORDER BY lease_expires_at LIMIT ?",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?;

        let mut reaped = Vec::with_capacity(rows.len());
        let mut events = Vec::with_capacity(rows.len());
        let mut woke = false;
        for row in &rows {
            let job = job_from_row(row)?;
            let worker_id = job.worker_id.clone().unwrap_or_default();
            let reason = expiry_reason(&job, &worker_id);
            let retry = job.attempt < job.max_attempts;
            let new_state = if retry {
                JobState::RetryWaiting
            } else {
                JobState::Failed
            };

            // Computed once: the backoff is jittered.
            let available_at = retry_at(&self.options.retry, now, job.attempt);
            let updated = sqlx::query(
                "UPDATE jobs
                 SET state = ?, available_at = ?, finished_at = ?, last_error = ?,
                     worker_id = NULL, lease_expires_at = NULL, attempt_deadline = NULL,
                     updated_at = ?
                 WHERE id = ? AND attempt = ? AND state = 'running' AND lease_expires_at < ?",
            )
            .bind(new_state.as_str())
            .bind(available_at)
            .bind((!retry).then_some(now))
            .bind(&reason)
            .bind(now)
            .bind(job.id.to_string())
            .bind(i64::from(job.attempt))
            .bind(now)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if updated == 0 {
                continue; // can't happen under the write lock; stay safe anyway
            }
            finish_attempt(
                &mut tx,
                job.id,
                job.attempt,
                now,
                AttemptOutcome::LeaseExpired,
                Some(&reason),
            )
            .await?;
            // Last attempt gone: the activity failed, in this transaction.
            if !retry {
                woke |= on_activity_terminal(
                    &mut tx,
                    self.clock.as_ref(),
                    job.id,
                    ActivityOutcome::Failed(reason.clone()),
                )
                .await?;
            }
            // The row as the UPDATE above left it, for the event.
            let after = Job {
                state: new_state,
                available_at,
                finished_at: (!retry).then_some(now),
                last_error: Some(reason.clone()),
                worker_id: None,
                lease_expires_at: None,
                attempt_deadline: None,
                updated_at: now,
                ..job
            };
            events.push(EventKind::LeaseExpired {
                from: Some(JobState::Running),
                job: after.summary(),
                worker_id: worker_id.clone(),
                reason: reason.clone(),
            });
            reaped.push(ReapedJob {
                job_id: after.id,
                attempt: after.attempt,
                worker_id,
                new_state,
                reason,
            });
        }
        tx.commit().await?;
        if woke {
            self.nudge_decider();
        }
        for event in events {
            self.events.publish(now, event);
        }
        Ok(reaped)
    }

    /// Give every running job one full `lease` from now (§5.1 #5).
    ///
    /// Run on startup *before* the first reap, and whenever the wall clock
    /// jumps forward. After a coordinator outage longer than the lease, every
    /// lease looks expired, yet the workers were healthy and simply couldn't
    /// heartbeat. Reaping them would re-run every in-flight job. The grace
    /// lease may exceed the attempt deadline, so a worker that finished during
    /// the outage can still deliver its result. Heartbeats then can't extend
    /// it further.
    pub async fn grant_lease_grace(&self, lease: Duration) -> Result<u64, StoreError> {
        let now = self.clock.now_ms();
        let extended = sqlx::query(
            "UPDATE jobs SET lease_expires_at = MAX(lease_expires_at, ?)
             WHERE state = 'running'",
        )
        .bind(now.saturating_add(duration_ms(lease)))
        .execute(&self.writer)
        .await?
        .rows_affected();
        Ok(extended)
    }
}

fn expiry_reason(job: &Job, worker_id: &str) -> String {
    let past_deadline = matches!(
        (job.lease_expires_at, job.attempt_deadline),
        (Some(lease), Some(deadline)) if lease >= deadline
    );
    if past_deadline {
        format!(
            "attempt deadline exceeded on worker {worker_id} (timeout {} ms + grace)",
            job.timeout_ms
        )
    } else {
        format!("lease expired: worker {worker_id} stopped heartbeating")
    }
}

/// Did the wall clock jump forward between two reaper ticks?
///
/// `wall_elapsed_ms` comes from the coordinator clock (which follows NTP);
/// `mono_elapsed` comes from the OS monotonic clock (which doesn't). If wall
/// time ran ahead by more than `tolerance`, leases "expired" without real time
/// passing, so the reaper should grant grace instead of reaping.
pub fn wall_clock_jumped(
    wall_elapsed_ms: Timestamp,
    mono_elapsed: Duration,
    tolerance: Duration,
) -> bool {
    wall_elapsed_ms.saturating_sub(duration_ms(mono_elapsed)) > duration_ms(tolerance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_forward_jumps_only_beyond_tolerance() {
        let tol = Duration::from_secs(5);
        let tick = Duration::from_millis(250);
        assert!(!wall_clock_jumped(250, tick, tol), "normal tick");
        assert!(
            !wall_clock_jumped(3_000, tick, tol),
            "scheduler hiccup within tolerance"
        );
        assert!(
            wall_clock_jumped(60_250, tick, tol),
            "NTP stepped forward a minute"
        );
        assert!(
            !wall_clock_jumped(0, tick, tol),
            "clamped backwards step is not a forward jump"
        );
    }
}
