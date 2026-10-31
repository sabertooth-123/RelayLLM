//! Durable workflow runs (docs/WORKFLOWS_DESIGN.md).
//!
//! There is no in-memory workflow state. Every decision runs in one
//! `BEGIN IMMEDIATE` transaction that loads the definition and history, folds
//! it, runs the pure `decide`, and appends the resulting events (and activity
//! jobs) before committing. Recovery after a restart is the first decision
//! pass; nothing else is special about it.
//!
//! Clock rule (design §10a): every transaction that appends workflow events
//! reads the clock *after* taking the write lock, so event timestamps follow
//! commit order.

use std::collections::BTreeMap;

use relay_core::clock::{Clock, Timestamp};
use relay_core::events::EventKind;
use relay_core::job::{AttemptOutcome, JobId};
use relay_core::workflow::{
    Command, DecideContext, JobProgress, NewWorkflow, RunMeta, RunState, Workflow,
    WorkflowDefinition, WorkflowEvent, WorkflowEventKind, WorkflowId, WorkflowLimits,
    WorkflowState, WorkflowView, build_view, decide, fold,
};
use serde_json::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, Sqlite, SqliteConnection, Transaction};
use tracing::{error, warn};

use super::cancel::{CancelOutcome, cancel_in_tx};
use super::{Store, StoreError, insert_job};

/// A full queue postpones scheduling by this much (backpressure, §4.3).
const QUEUE_FULL_RETRY_MS: i64 = 1_000;
/// A run whose history can't be folded is retried this much later, so a bug
/// shows up in the log once a minute instead of on every tick.
const BROKEN_RUN_RETRY_MS: i64 = 60_000;
/// Guards the decide → apply loop against a decider bug.
const MAX_ROUNDS: usize = 1_000;

#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowRun {
    pub id: WorkflowId,
    pub name: String,
    pub state: WorkflowState,
    pub definition: WorkflowDefinition,
    pub input: Value,
    pub idempotency_key: Option<String>,
    /// Due for a decision once `wake_at <= now`.
    pub wake_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub finished_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StartOutcome {
    Created(WorkflowRun),
    /// The idempotency key matched a run started with the same request.
    Existing(WorkflowRun),
}

impl StartOutcome {
    pub fn run(&self) -> &WorkflowRun {
        match self {
            StartOutcome::Created(run) | StartOutcome::Existing(run) => run,
        }
    }
}

/// What one [`Store::decide_due`] pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecidePass {
    /// Runs whose decision committed (possibly with no new events).
    pub decided: usize,
    /// Runs that couldn't schedule every activity because the queue is
    /// full. What fit was committed; the rest is retried in 1 s.
    pub deferred: usize,
    /// Runs whose history or decision is inconsistent (a bug; logged).
    pub broken: usize,
}

/// How an activity's job ended, for the hook.
#[derive(Debug, Clone)]
pub(super) enum ActivityOutcome {
    Completed,
    /// Terminal failure: permanent error, attempts exhausted by `fail()`, or
    /// the last attempt's lease expired.
    Failed(String),
    /// The job itself was cancelled through `/v1/jobs/{id}/cancel`.
    Cancelled,
}

/// Result of [`Store::signal_workflow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    /// First delivery: `signal_received` committed and the run made due.
    Recorded,
    /// The latch was already set with an equal payload (a retried request).
    /// Nothing changed.
    AlreadyReceived,
}

/// Result of [`Store::cancel_workflow`].
#[derive(Debug, Clone, PartialEq)]
pub enum CancelWorkflowOutcome {
    Cancelled(WorkflowRun),
    /// Already cancelled; cancelling is idempotent.
    AlreadyCancelled(WorkflowRun),
}

impl CancelWorkflowOutcome {
    pub fn run(&self) -> &WorkflowRun {
        match self {
            CancelWorkflowOutcome::Cancelled(run)
            | CancelWorkflowOutcome::AlreadyCancelled(run) => run,
        }
    }
}

/// `last_error` of activity jobs cancelled with their workflow.
pub const WORKFLOW_CANCELLED_MESSAGE: &str = "cancelled: its workflow was cancelled";

/// `last_error` of activity jobs the decider cancels (fail fast, §4.1).
pub const SIBLING_CANCELLED_MESSAGE: &str = "cancelled: another activity of its workflow failed";

enum RunDecision {
    NotDue,
    Decided,
    QueueFull,
    Broken(String),
}

impl Store {
    /// Create a run and make it due immediately. Idempotent per key, with
    /// the same rules as job submission.
    pub async fn start_workflow(&self, new: NewWorkflow) -> Result<StartOutcome, StoreError> {
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;
        let now = self.clock.now_ms();

        if let Some(key) = new.idempotency_key.as_deref() {
            if let Some(row) = sqlx::query("SELECT * FROM workflow_runs WHERE idempotency_key = ?")
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?
            {
                let fingerprint: String = row.try_get("request_fingerprint")?;
                let existing = run_from_row(&row)?;
                tx.commit().await?;
                return if fingerprint == new.request_fingerprint {
                    Ok(StartOutcome::Existing(existing))
                } else {
                    Err(StoreError::WorkflowIdempotencyConflict {
                        key: key.to_owned(),
                        existing: existing.id,
                    })
                };
            }
        }

        let id = WorkflowId::generate();
        let definition = serde_json::to_string(&new.definition).map_err(|e| corrupt(id, e))?;
        sqlx::query(
            "INSERT INTO workflow_runs (id, name, definition, input, idempotency_key,
                                        request_fingerprint, state, wake_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(&new.workflow.name)
        .bind(definition)
        .bind(new.input.to_string())
        .bind(new.idempotency_key.as_deref())
        .bind(&new.request_fingerprint)
        .bind(now) // due now: the first decision schedules the first step
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        append_event(
            &mut tx,
            id,
            &WorkflowEvent {
                seq: 1,
                at_ms: now,
                kind: WorkflowEventKind::WorkflowStarted { input: new.input },
            },
        )
        .await?;
        let run = load_run(&mut tx, id)
            .await?
            .ok_or_else(|| corrupt(id, "vanished"))?;
        tx.commit().await?;
        self.nudge_decider();
        Ok(StartOutcome::Created(run))
    }

    /// Deliver signal `name` to a run (design §2, §6 #6 and #8).
    ///
    /// `wait_signal` is a one-shot durable latch. The first delivery appends
    /// `signal_received` and makes the run due, in one transaction, before
    /// the caller is acknowledged. It may arrive before the run reaches the
    /// step; it then waits in the history. A repeat with an equal payload
    /// (JSON equality, so key order doesn't matter) is
    /// [`SignalOutcome::AlreadyReceived`], which makes a retry after a lost
    /// response safe. A repeat with a different payload is
    /// [`StoreError::SignalConflict`].
    pub async fn signal_workflow(
        &self,
        id: WorkflowId,
        name: &str,
        payload: Value,
    ) -> Result<SignalOutcome, StoreError> {
        let max = self.options.workflow.submission.max_payload_bytes;
        let bytes = payload.to_string().len();
        if bytes > max {
            return Err(StoreError::SignalTooLarge { actual: bytes, max });
        }

        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;
        let now = self.clock.now_ms();
        let run = load_run(&mut tx, id)
            .await?
            .ok_or(StoreError::WorkflowNotFound(id))?;
        let workflow = Workflow::from_definition(&run.definition, &self.options.workflow)
            .map_err(|e| corrupt(id, e))?;
        if !workflow.declares_signal(name) {
            return Err(StoreError::UnknownSignal {
                workflow: id,
                name: name.to_owned(),
            });
        }

        let existing: Option<String> = sqlx::query_scalar(
            "SELECT data FROM workflow_events
             WHERE run_id = ? AND type = 'signal_received' AND json_extract(data, '$.name') = ?",
        )
        .bind(id.to_string())
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(raw) = existing {
            let data: Value = serde_json::from_str(&raw).map_err(|e| corrupt(id, e))?;
            let previous = data.get("payload").cloned().unwrap_or(Value::Null);
            return if previous == payload {
                Ok(SignalOutcome::AlreadyReceived)
            } else {
                Err(StoreError::SignalConflict {
                    workflow: id,
                    name: name.to_owned(),
                })
            };
        }
        // Checked after the duplicate test: a retried delivery to a run that
        // has since finished is still "already received", not an error.
        if run.state.is_terminal() {
            return Err(StoreError::WorkflowFinished {
                workflow: id,
                state: run.state,
            });
        }

        let last_seq: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(seq), 0) FROM workflow_events WHERE run_id = ?",
        )
        .bind(id.to_string())
        .fetch_one(&mut *tx)
        .await?;
        let event = WorkflowEvent {
            seq: u64::try_from(last_seq).unwrap_or(0) + 1,
            at_ms: now,
            kind: WorkflowEventKind::SignalReceived {
                name: name.to_owned(),
                payload,
            },
        };
        append_event(&mut tx, id, &event).await?;
        sqlx::query("UPDATE workflow_runs SET wake_at = ?, updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(now)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.nudge_decider();
        Ok(SignalOutcome::Recorded)
    }

    /// Cancel a run (design §6 #10, #11). One `BEGIN IMMEDIATE` transaction
    /// cancels every outstanding activity job through the ordinary fenced job
    /// cancel (its worker gets `409` and aborts), appends one
    /// `activity_cancelled` per job and then `workflow_cancelled`, and makes
    /// the run terminal. A completion racing this takes the same write lock,
    /// so exactly one of them wins for each job.
    ///
    /// Idempotent on a cancelled run; `WorkflowFinished` on a succeeded or
    /// failed one. A pending timer or signal wait just stops mattering.
    pub async fn cancel_workflow(
        &self,
        id: WorkflowId,
        reason: &str,
    ) -> Result<CancelWorkflowOutcome, StoreError> {
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;
        let now = self.clock.now_ms();
        let run = load_run(&mut tx, id)
            .await?
            .ok_or(StoreError::WorkflowNotFound(id))?;
        match run.state {
            WorkflowState::Running => {}
            WorkflowState::Cancelled => return Ok(CancelWorkflowOutcome::AlreadyCancelled(run)),
            state @ (WorkflowState::Succeeded | WorkflowState::Failed) => {
                return Err(StoreError::WorkflowFinished {
                    workflow: id,
                    state,
                });
            }
        }
        let workflow = Workflow::from_definition(&run.definition, &self.options.workflow)
            .map_err(|e| corrupt(id, e))?;
        let events = load_events(&mut tx, id).await?;
        let mut state = fold(&workflow, &events).map_err(|e| corrupt(id, e))?;

        let outstanding: Vec<(String, JobId)> = state
            .activities
            .iter()
            .filter(|(_, a)| !a.status.is_terminal())
            .map(|(addr, a)| (addr.clone(), a.job_id))
            .collect();
        let mut cancelled = Vec::with_capacity(outstanding.len());
        let mut kinds = Vec::with_capacity(outstanding.len() + 1);
        for (activity, job_id) in outstanding {
            match cancel_in_tx(&mut tx, job_id, now, WORKFLOW_CANCELLED_MESSAGE).await {
                Ok((CancelOutcome::Cancelled(job), Some(from))) => cancelled.push((from, job)),
                // The history says outstanding, but the job already ended:
                // the hook would have recorded that. Refuse rather than guess.
                Ok((other, _)) => {
                    return Err(corrupt(
                        id,
                        format!(
                            "activity {activity:?} is outstanding but its job is {}",
                            other.job().state
                        ),
                    ));
                }
                Err(StoreError::NotCancellable { state, .. }) => {
                    return Err(corrupt(
                        id,
                        format!("activity {activity:?} is outstanding but its job is {state}"),
                    ));
                }
                Err(e) => return Err(e),
            }
            kinds.push(WorkflowEventKind::ActivityCancelled { activity, job_id });
        }
        kinds.push(WorkflowEventKind::WorkflowCancelled {
            reason: reason.to_owned(),
        });
        for kind in kinds {
            let event = WorkflowEvent {
                seq: state.last_seq + 1,
                at_ms: now,
                kind,
            };
            state.apply(&workflow, &event).map_err(|e| corrupt(id, e))?;
            append_event(&mut tx, id, &event).await?;
        }
        sqlx::query(
            "UPDATE workflow_runs SET state = 'cancelled', wake_at = NULL, finished_at = ?, updated_at = ?
             WHERE id = ? AND state = 'running'",
        )
        .bind(now)
        .bind(now)
        .bind(id.to_string())
        .execute(&mut *tx)
        .await?;
        let run = load_run(&mut tx, id)
            .await?
            .ok_or_else(|| corrupt(id, "vanished"))?;
        tx.commit().await?;

        for (from, job) in cancelled {
            self.events.publish(
                now,
                EventKind::JobCancelled {
                    from: Some(from),
                    job: job.summary(),
                },
            );
        }
        Ok(CancelWorkflowOutcome::Cancelled(run))
    }

    pub async fn workflow_run(&self, id: WorkflowId) -> Result<Option<WorkflowRun>, StoreError> {
        let mut conn = self.reader.acquire().await?;
        load_run(&mut conn, id).await
    }

    /// The run's events in `seq` order (empty for an unknown run).
    pub async fn workflow_history(&self, id: WorkflowId) -> Result<Vec<WorkflowEvent>, StoreError> {
        let mut conn = self.reader.acquire().await?;
        load_events(&mut conn, id).await
    }

    /// Decide up to `limit` due runs, each in its own transaction. Safe to
    /// call concurrently and repeatedly: a run that is no longer due when
    /// its transaction starts is skipped, and the unique indexes reject any
    /// double schedule.
    pub async fn decide_due(&self, limit: u32) -> Result<DecidePass, StoreError> {
        let now = self.clock.now_ms();
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM workflow_runs WHERE wake_at IS NOT NULL AND wake_at <= ?
             ORDER BY wake_at, id LIMIT ?",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&self.reader)
        .await?;

        let mut pass = DecidePass::default();
        for raw in ids {
            let id: WorkflowId = raw.parse().map_err(|e| StoreError::Corrupt {
                job_id: format!("<workflow {raw}>"),
                reason: format!("{e}"),
            })?;
            match self.decide_run(id).await? {
                RunDecision::NotDue => {}
                RunDecision::Decided => pass.decided += 1,
                // Already committed what fit, with wake_at = now + 1 s.
                RunDecision::QueueFull => pass.deferred += 1,
                RunDecision::Broken(reason) => {
                    pass.broken += 1;
                    error!(workflow_id = %id, reason, "workflow run can't be decided; retrying later");
                    self.postpone(id, BROKEN_RUN_RETRY_MS).await?;
                }
            }
        }
        Ok(pass)
    }

    /// Push a running run's next decision `delay_ms` into the future. Used
    /// after a rolled-back decision, outside its transaction.
    async fn postpone(&self, id: WorkflowId, delay_ms: i64) -> Result<(), StoreError> {
        let mut conn = self.writer.acquire().await?;
        let now = self.clock.now_ms();
        sqlx::query("UPDATE workflow_runs SET wake_at = ? WHERE id = ? AND state = 'running'")
            .bind(now.saturating_add(delay_ms))
            .bind(id.to_string())
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    async fn decide_run(&self, id: WorkflowId) -> Result<RunDecision, StoreError> {
        let mut tx = self.writer.begin_with("BEGIN IMMEDIATE").await?;
        let now = self.clock.now_ms();

        let Some(run) = load_run(&mut tx, id).await? else {
            return Ok(RunDecision::NotDue);
        };
        if run.state != WorkflowState::Running || !run.wake_at.is_some_and(|at| at <= now) {
            return Ok(RunDecision::NotDue);
        }
        let workflow = match Workflow::from_definition(&run.definition, &self.options.workflow) {
            Ok(w) => w,
            Err(e) => {
                return Ok(RunDecision::Broken(format!(
                    "stored definition is invalid: {e}"
                )));
            }
        };
        let events = load_events(&mut tx, id).await?;
        let mut state = match fold(&workflow, &events) {
            Ok(s) => s,
            Err(e) => return Ok(RunDecision::Broken(format!("history: {e}"))),
        };
        let results = load_results(&mut tx, id).await?;

        let mut created = Vec::new();
        let mut cancelled = Vec::new();
        let mut wake_at = None;
        let mut finished = None;
        let mut queue_full = false;
        'rounds: for _ in 0..MAX_ROUNDS {
            let ctx = DecideContext {
                now_ms: now,
                results: &results,
                max_payload_bytes: self.options.workflow.submission.max_payload_bytes,
            };
            let decision = match decide(&workflow, &state, &ctx) {
                Ok(d) => d,
                Err(e) => return Ok(RunDecision::Broken(format!("decide: {e}"))),
            };
            wake_at = decision.wake_at_ms;
            if decision.commands.is_empty() {
                break;
            }
            for command in decision.commands {
                // Most commands become one event; CancelActivities one per job.
                let kinds = match command {
                    Command::ScheduleActivity { activity, request } => {
                        let new_job = match request.validate(&self.options.workflow.submission) {
                            Ok(j) => j,
                            Err(e) => {
                                return Ok(RunDecision::Broken(format!(
                                    "activity {activity:?}: {e}"
                                )));
                            }
                        };
                        // Queue full: keep the branches already scheduled
                        // (partial fan-out) and retry the rest shortly. All-
                        // or-nothing would livelock a step whose branch count
                        // exceeds the free capacity (design §10d).
                        let Some(job) =
                            insert_job(&mut tx, &new_job, now, self.options.max_active_jobs)
                                .await?
                        else {
                            queue_full = true;
                            break 'rounds;
                        };
                        let kind = WorkflowEventKind::ActivityScheduled {
                            activity,
                            job_id: job.id,
                        };
                        created.push(job);
                        vec![kind]
                    }
                    Command::ScheduleTimer { step, fire_at_ms } => {
                        vec![WorkflowEventKind::TimerScheduled { step, fire_at_ms }]
                    }
                    Command::FireTimer { step } => vec![WorkflowEventKind::TimerFired { step }],
                    Command::CancelActivities { activities } => {
                        let mut kinds = Vec::with_capacity(activities.len());
                        for (activity, job_id) in activities {
                            // Through the ordinary fenced cancel, in this
                            // transaction; its worker gets 409 and aborts.
                            // The event is appended here (not by the hook) so
                            // it takes the decision's own seq.
                            match cancel_in_tx(&mut tx, job_id, now, SIBLING_CANCELLED_MESSAGE)
                                .await
                            {
                                Ok((CancelOutcome::Cancelled(job), Some(from))) => {
                                    cancelled.push((from, job));
                                }
                                Ok((other, _)) => {
                                    return Ok(RunDecision::Broken(format!(
                                        "activity {activity:?} is outstanding in the history but its job is {}",
                                        other.job().state
                                    )));
                                }
                                Err(StoreError::NotCancellable { state, .. }) => {
                                    return Ok(RunDecision::Broken(format!(
                                        "activity {activity:?} is outstanding in the history but its job is {state}"
                                    )));
                                }
                                Err(e) => return Err(e),
                            }
                            kinds.push(WorkflowEventKind::ActivityCancelled { activity, job_id });
                        }
                        kinds
                    }
                    Command::CompleteRun => {
                        finished = Some(WorkflowState::Succeeded);
                        vec![WorkflowEventKind::WorkflowCompleted]
                    }
                    Command::FailRun { reason } => {
                        finished = Some(WorkflowState::Failed);
                        vec![WorkflowEventKind::WorkflowFailed { reason }]
                    }
                };
                for kind in kinds {
                    let event = WorkflowEvent {
                        seq: state.last_seq + 1,
                        at_ms: now,
                        kind,
                    };
                    if let Err(e) = state.apply(&workflow, &event) {
                        return Ok(RunDecision::Broken(format!(
                            "decider produced an impossible event: {e}"
                        )));
                    }
                    append_event(&mut tx, id, &event).await?;
                }
            }
        }

        if let Some(final_state) = finished {
            sqlx::query(
                "UPDATE workflow_runs SET state = ?, wake_at = NULL, finished_at = ?, updated_at = ?
                 WHERE id = ? AND state = 'running'",
            )
            .bind(final_state.as_str())
            .bind(now)
            .bind(now)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        } else {
            if queue_full {
                wake_at = Some(now.saturating_add(QUEUE_FULL_RETRY_MS));
            }
            sqlx::query("UPDATE workflow_runs SET wake_at = ?, updated_at = ? WHERE id = ?")
                .bind(wake_at)
                .bind(now)
                .bind(id.to_string())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;

        for job in created {
            self.events.publish(
                now,
                EventKind::JobSubmitted {
                    from: None,
                    job: job.summary(),
                },
            );
        }
        for (from, job) in cancelled {
            self.events.publish(
                now,
                EventKind::JobCancelled {
                    from: Some(from),
                    job: job.summary(),
                },
            );
        }
        Ok(if queue_full {
            RunDecision::QueueFull
        } else {
            RunDecision::Decided
        })
    }
}

/// Runs inside a terminal job transition, right before its `COMMIT` (§5).
/// If the job is a workflow activity, append its terminal event and make the
/// run due, so the two can only commit together. A plain job costs one
/// indexed lookup that finds nothing.
///
/// Returns whether a run was made due; the caller then nudges the decider
/// *after* its commit ([`Store::nudge_decider`]).
pub(super) async fn on_activity_terminal(
    tx: &mut Transaction<'static, Sqlite>,
    clock: &dyn Clock,
    job_id: JobId,
    outcome: ActivityOutcome,
) -> Result<bool, StoreError> {
    let link: Option<(String, String)> = sqlx::query_as(
        "SELECT run_id, step FROM workflow_events WHERE job_id = ? AND type = 'activity_scheduled'",
    )
    .bind(job_id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    let Some((raw_run, activity)) = link else {
        return Ok(false);
    };
    let run_id: WorkflowId = raw_run.parse().map_err(|e| StoreError::Corrupt {
        job_id: job_id.to_string(),
        reason: format!("activity link: {e}"),
    })?;
    // Re-read under the write lock: the caller's `now` predates it.
    let now = clock.now_ms();

    let (state, last_seq): (String, i64) = sqlx::query_as(
        "SELECT r.state, (SELECT COALESCE(MAX(seq), 0) FROM workflow_events WHERE run_id = r.id)
         FROM workflow_runs r WHERE r.id = ?",
    )
    .bind(run_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    if state != WorkflowState::Running.as_str() {
        // A finished run has no outstanding activities (they are cancelled
        // first, in the same transaction), so this would be a bug. Appending
        // would make the history unfoldable; record the job outcome alone.
        warn!(workflow_id = %run_id, job_id = %job_id, state, "activity finished after its run ended");
        return Ok(false);
    }

    let kind = match outcome {
        ActivityOutcome::Completed => WorkflowEventKind::ActivityCompleted { activity, job_id },
        ActivityOutcome::Failed(error) => WorkflowEventKind::ActivityFailed {
            activity,
            job_id,
            error,
        },
        ActivityOutcome::Cancelled => WorkflowEventKind::ActivityCancelled { activity, job_id },
    };
    let seq = u64::try_from(last_seq).unwrap_or(0) + 1;
    append_event(
        tx,
        run_id,
        &WorkflowEvent {
            seq,
            at_ms: now,
            kind,
        },
    )
    .await?;
    sqlx::query("UPDATE workflow_runs SET wake_at = ?, updated_at = ? WHERE id = ?")
        .bind(now)
        .bind(now)
        .bind(run_id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(true)
}

async fn append_event(
    conn: &mut SqliteConnection,
    run_id: WorkflowId,
    event: &WorkflowEvent,
) -> Result<(), StoreError> {
    let (step, job_id, data) = event.kind.columns();
    sqlx::query(
        "INSERT INTO workflow_events (run_id, seq, type, step, job_id, data, at)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(run_id.to_string())
    .bind(i64::try_from(event.seq).unwrap_or(i64::MAX))
    .bind(event.kind.type_name())
    .bind(step)
    .bind(job_id.map(|j| j.to_string()))
    .bind(data.to_string())
    .bind(event.at_ms)
    .execute(conn)
    .await?;
    Ok(())
}

async fn load_run(
    conn: &mut SqliteConnection,
    id: WorkflowId,
) -> Result<Option<WorkflowRun>, StoreError> {
    sqlx::query("SELECT * FROM workflow_runs WHERE id = ?")
        .bind(id.to_string())
        .fetch_optional(conn)
        .await?
        .as_ref()
        .map(run_from_row)
        .transpose()
}

async fn load_events(
    conn: &mut SqliteConnection,
    id: WorkflowId,
) -> Result<Vec<WorkflowEvent>, StoreError> {
    let rows = sqlx::query(
        "SELECT seq, type, step, job_id, data, at FROM workflow_events WHERE run_id = ? ORDER BY seq",
    )
    .bind(id.to_string())
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| {
            let job_id = row
                .try_get::<Option<String>, _>("job_id")?
                .map(|raw| raw.parse::<JobId>())
                .transpose()
                .map_err(|e| corrupt(id, e))?;
            let data: Value = serde_json::from_str(&row.try_get::<String, _>("data")?)
                .map_err(|e| corrupt(id, e))?;
            let kind = WorkflowEventKind::from_columns(
                &row.try_get::<String, _>("type")?,
                row.try_get("step")?,
                job_id,
                &data,
            )
            .map_err(|e| corrupt(id, e))?;
            Ok(WorkflowEvent {
                seq: u64::try_from(row.try_get::<i64, _>("seq")?).map_err(|e| corrupt(id, e))?,
                at_ms: row.try_get("at")?,
                kind,
            })
        })
        .collect()
}

/// Results of the run's completed activities, by address. Immutable once
/// written: a succeeded job's result can't change (0001 CHECK + absorbing
/// terminal states).
async fn load_results(
    conn: &mut SqliteConnection,
    id: WorkflowId,
) -> Result<BTreeMap<String, Value>, StoreError> {
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT e.step, j.result FROM workflow_events e JOIN jobs j ON j.id = e.job_id
         WHERE e.run_id = ? AND e.type = 'activity_completed'",
    )
    .bind(id.to_string())
    .fetch_all(conn)
    .await?;
    let mut results = BTreeMap::new();
    for (address, raw) in rows {
        let raw = raw
            .ok_or_else(|| corrupt(id, format!("completed activity {address:?} has no result")))?;
        results.insert(
            address,
            serde_json::from_str(&raw).map_err(|e| corrupt(id, e))?,
        );
    }
    Ok(results)
}

fn run_from_row(row: &SqliteRow) -> Result<WorkflowRun, StoreError> {
    let raw: String = row.try_get("id")?;
    let id: WorkflowId = raw.parse().map_err(|e| StoreError::Corrupt {
        job_id: format!("<workflow {raw}>"),
        reason: format!("{e}"),
    })?;
    Ok(WorkflowRun {
        id,
        name: row.try_get("name")?,
        state: row
            .try_get::<String, _>("state")?
            .parse()
            .map_err(|e| corrupt(id, e))?,
        definition: serde_json::from_str(&row.try_get::<String, _>("definition")?)
            .map_err(|e| corrupt(id, e))?,
        input: serde_json::from_str(&row.try_get::<String, _>("input")?)
            .map_err(|e| corrupt(id, e))?,
        idempotency_key: row.try_get("idempotency_key")?,
        wake_at: row.try_get("wake_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn corrupt(id: WorkflowId, reason: impl std::fmt::Display) -> StoreError {
    StoreError::Corrupt {
        job_id: format!("<workflow {id}>"),
        reason: reason.to_string(),
    }
}

/// What `/metrics` reports about workflows. All of it is read from the
/// database, so it is exact and survives restarts (design §9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowMetrics {
    pub runs_by_state: Vec<(WorkflowState, u64)>,
    /// Finished attempts of jobs that belong to a workflow, by outcome:
    /// `retryable_error` and `lease_expired` are the activity retries.
    pub activity_attempts_by_outcome: Vec<(AttemptOutcome, u64)>,
    /// Runs due for a decision right now. Growing means the decider is
    /// falling behind.
    pub due_runs: u64,
}

impl Store {
    pub async fn workflow_metrics(&self) -> Result<WorkflowMetrics, StoreError> {
        let now = self.clock.now_ms();
        let mut tx = self.reader.begin().await?;
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT state, COUNT(*) FROM workflow_runs GROUP BY state")
                .fetch_all(&mut *tx)
                .await?;
        let runs_by_state = WorkflowState::ALL
            .into_iter()
            .map(|s| {
                let n = rows
                    .iter()
                    .find(|(r, _)| r == s.as_str())
                    .map_or(0, |(_, n)| *n);
                (s, u64::try_from(n).unwrap_or(0))
            })
            .collect();
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT a.outcome, COUNT(*) FROM job_attempts a
             JOIN workflow_events e ON e.job_id = a.job_id AND e.type = 'activity_scheduled'
             WHERE a.outcome IS NOT NULL GROUP BY a.outcome",
        )
        .fetch_all(&mut *tx)
        .await?;
        let activity_attempts_by_outcome = AttemptOutcome::ALL
            .into_iter()
            .map(|o| {
                let n = rows
                    .iter()
                    .find(|(r, _)| r == o.as_str())
                    .map_or(0, |(_, n)| *n);
                (o, u64::try_from(n).unwrap_or(0))
            })
            .collect();
        let due: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_runs WHERE wake_at IS NOT NULL AND wake_at <= ?",
        )
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(WorkflowMetrics {
            runs_by_state,
            activity_attempts_by_outcome,
            due_runs: u64::try_from(due).unwrap_or(0),
        })
    }

    /// Definition and activity limits; the API validates start requests
    /// with these so the decider never meets a definition it would reject.
    pub fn workflow_limits(&self) -> &WorkflowLimits {
        &self.options.workflow
    }

    /// The per-step view of a run (`GET /v1/workflows/{id}`). Run, history
    /// and job progress are read in one read transaction, so they agree.
    pub async fn workflow_view(&self, id: WorkflowId) -> Result<Option<WorkflowView>, StoreError> {
        let mut tx = self.reader.begin().await?;
        let Some(run) = load_run(&mut tx, id).await? else {
            return Ok(None);
        };
        let workflow = Workflow::from_definition(&run.definition, &self.options.workflow)
            .map_err(|e| corrupt(id, e))?;
        let events = load_events(&mut tx, id).await?;
        let state = fold(&workflow, &events).map_err(|e| corrupt(id, e))?;
        let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
            "SELECT j.id, j.state, j.attempt, j.max_attempts
             FROM workflow_events e JOIN jobs j ON j.id = e.job_id
             WHERE e.run_id = ? AND e.type = 'activity_scheduled'",
        )
        .bind(id.to_string())
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;

        let mut jobs = BTreeMap::new();
        for (job_id, state, attempt, max_attempts) in rows {
            jobs.insert(
                job_id.parse::<JobId>().map_err(|e| corrupt(id, e))?,
                JobProgress {
                    state: state.parse().map_err(|e| corrupt(id, e))?,
                    attempt: u32::try_from(attempt).unwrap_or(0),
                    max_attempts: u32::try_from(max_attempts).unwrap_or(0),
                },
            );
        }
        let meta = RunMeta {
            id: run.id,
            name: run.name,
            state: run.state,
            created_at_ms: run.created_at,
            updated_at_ms: run.updated_at,
            finished_at_ms: run.finished_at,
            wake_at_ms: run.wake_at,
        };
        Ok(Some(build_view(meta, &workflow, &state, &jobs)))
    }

    /// Wake the decider task now instead of at its next tick. Only a hint:
    /// `wake_at` in the database is the truth, so a lost nudge costs one
    /// tick, never a decision. Call it after the commit that set `wake_at`.
    pub(crate) fn nudge_decider(&self) {
        self.decider_wake.notify_one();
    }

    /// The nudges [`Store::nudge_decider`] sends, for the decider task.
    pub fn decider_wakeups(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.decider_wake.clone()
    }

    /// The earliest `wake_at` of any running run, so the decider can sleep
    /// exactly until a timer is due.
    pub async fn next_wake_at(&self) -> Result<Option<Timestamp>, StoreError> {
        Ok(
            sqlx::query_scalar("SELECT MIN(wake_at) FROM workflow_runs WHERE wake_at IS NOT NULL")
                .fetch_one(&self.reader)
                .await?,
        )
    }

    /// The folded state of a run, for tests and the API's step view.
    pub async fn workflow_state(
        &self,
        id: WorkflowId,
    ) -> Result<Option<(Workflow, RunState)>, StoreError> {
        let Some(run) = self.workflow_run(id).await? else {
            return Ok(None);
        };
        let workflow = Workflow::from_definition(&run.definition, &self.options.workflow)
            .map_err(|e| corrupt(id, e))?;
        let events = self.workflow_history(id).await?;
        let state = fold(&workflow, &events).map_err(|e| corrupt(id, e))?;
        Ok(Some((workflow, state)))
    }
}
