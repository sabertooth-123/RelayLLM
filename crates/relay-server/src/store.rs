//! SQLite persistence for jobs.
//!
//! Each state change is a single conditional SQL statement, or an explicit
//! `BEGIN IMMEDIATE` transaction when one statement isn't enough. Correctness
//! therefore never depends on in-process locks: when concurrent tasks race,
//! the database decides, and the loser sees 0 affected rows.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use relay_core::clock::{Clock, Timestamp};
use relay_core::events::EventKind;
use relay_core::job::{InvalidTransition, Job, JobId, JobState};
use relay_core::retry::RetryPolicy;
use relay_core::submission::NewJob;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// States that count against queue capacity. Kept as a fixed-size array so the
/// SQL below can use a fixed number of placeholders; a unit test checks it
/// against [`JobState::is_active`].
const ACTIVE_STATES: [JobState; 3] = [JobState::Queued, JobState::Running, JobState::RetryWaiting];

/// Insert a job unless the queue is full or the idempotency key already exists.
/// Capacity check and insert run in one write transaction, so concurrent
/// submits can't both take the last slot.
const INSERT_JOB: &str = "
    INSERT INTO jobs (id, kind, payload, request_fingerprint, idempotency_key, state,
                      attempt, max_attempts, timeout_ms, available_at, created_at, updated_at)
    SELECT ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?
    WHERE (SELECT COUNT(*) FROM jobs WHERE state IN (?, ?, ?)) < ?
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING *";

mod cancel;
mod claims;
mod dashboard;
mod leases;
mod workers;
mod workflows;

use crate::events::EventBus;
use relay_core::workflow::{WorkflowId, WorkflowLimits, WorkflowState};

pub use cancel::{CANCELLED_MESSAGE, CancelOutcome};
pub use claims::ReportOutcome;
pub use dashboard::DashboardView;
pub use leases::{ReapedJob, wall_clock_jumped};
pub use workers::MetricsSnapshot;
pub use workflows::{
    CancelWorkflowOutcome, DecidePass, SignalOutcome, StartOutcome, WORKFLOW_CANCELLED_MESSAGE,
    WorkflowMetrics, WorkflowRun,
};

// `Database` and `Migrate` don't repeat their source in the message: error
// reporters that walk the `source()` chain (anyhow's `{:#}`) print it anyway,
// and repeating it printed every cause twice.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error")]
    Database(#[from] sqlx::Error),
    #[error("database migration failed")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    InvalidTransition(#[from] InvalidTransition),
    #[error("worker {0:?} is not registered")]
    WorkerNotRegistered(String),
    #[error("job {0} not found")]
    JobNotFound(JobId),
    /// The report is for an attempt that no longer owns the job: a newer
    /// attempt took over, or the job already finished another way.
    #[error(
        "attempt {attempt} of job {job_id} is stale (job is {state} at attempt {current_attempt})"
    )]
    StaleAttempt {
        job_id: JobId,
        attempt: u32,
        current_attempt: u32,
        state: JobState,
    },
    #[error("job {job_id} is already {state} and can't be cancelled")]
    NotCancellable { job_id: JobId, state: JobState },
    #[error("corrupt row for job {job_id}: {reason}")]
    Corrupt { job_id: String, reason: String },
    #[error("queue is full ({capacity} active jobs)")]
    QueueFull { capacity: u64 },
    #[error("idempotency key {key:?} is already bound to job {existing} with a different request")]
    IdempotencyConflict { key: String, existing: JobId },
    #[error(
        "idempotency key {key:?} is already bound to workflow {existing} with a different request"
    )]
    WorkflowIdempotencyConflict { key: String, existing: WorkflowId },
    #[error("workflow {0} not found")]
    WorkflowNotFound(WorkflowId),
    #[error("workflow {workflow} is already {state}")]
    WorkflowFinished {
        workflow: WorkflowId,
        state: WorkflowState,
    },
    #[error("workflow {workflow} has no wait_signal step for {name:?}")]
    UnknownSignal { workflow: WorkflowId, name: String },
    /// `wait_signal` is a one-shot latch: this name was already delivered
    /// with a different payload.
    #[error(
        "signal {name:?} was already delivered to workflow {workflow} with a different payload"
    )]
    SignalConflict { workflow: WorkflowId, name: String },
    #[error("signal payload is {actual} bytes; the limit is {max}")]
    SignalTooLarge { actual: usize, max: usize },
}

#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// Maximum jobs in `queued`, `running` or `retry_waiting` at once.
    pub max_active_jobs: u64,
    /// Size of the read-only *reader* pool. Writes always go through a
    /// separate pool of exactly one connection; see [`Store`].
    pub max_connections: u32,
    /// Backoff before a failed or abandoned job becomes claimable again.
    pub retry: RetryPolicy,
    /// Added to a job's timeout to form the absolute attempt deadline. Covers
    /// the time a worker needs to notice its own timeout and report it.
    pub deadline_grace: Duration,
    pub durability: Durability,
    /// Definition limits, and the job limits activities are validated
    /// against. The coordinator keeps `submission` equal to its API limits.
    pub workflow: WorkflowLimits,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            max_active_jobs: 10_000,
            max_connections: 8,
            retry: RetryPolicy::default(),
            deadline_grace: Duration::from_secs(30),
            durability: Durability::Full,
            workflow: WorkflowLimits::default(),
        }
    }
}

/// SQLite `synchronous` level, i.e. what a commit (and so an HTTP `2xx`)
/// promises. Benchmarks show this setting dominates throughput, because
/// SQLite has one writer and FULL fsyncs on every commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// fsync on every commit: an acknowledged write survives power loss.
    Full,
    /// WAL is fsynced at checkpoints only: an acknowledged write survives a
    /// coordinator *process* crash, but the last few may be lost if the
    /// machine loses power or the OS crashes.
    Normal,
}

impl Durability {
    /// The value `PRAGMA synchronous` reports for this level.
    pub fn pragma_value(self) -> i64 {
        match self {
            Durability::Full => 2,
            Durability::Normal => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubmitOutcome {
    Created(Job),
    /// The idempotency key matched an existing job with the same request.
    Existing(Job),
}

impl SubmitOutcome {
    pub fn job(&self) -> &Job {
        match self {
            SubmitOutcome::Created(job) | SubmitOutcome::Existing(job) => job,
        }
    }
}

/// The coordinator's database handle.
///
/// SQLite allows one writer at a time. With several pooled connections,
/// colliding writers wait inside SQLite's *busy handler*, which sleeps and
/// re-polls with growing delays (1, 2, 5, 10, 15, 20, 25… ms) instead of
/// waking when the lock frees. Under load that cost tens of ms per
/// transaction (see `bench/README.md`). So there are two pools:
///
/// * `writer`: exactly **one** connection, used for every write and every
///   `BEGIN IMMEDIATE` transaction. Writers queue in sqlx's async FIFO and
///   are woken the moment it frees. Nothing may acquire it while already
///   holding it; multi-statement writes go through the transaction handle.
/// * `reader`: `max_connections` **read-only** connections for every query
///   that doesn't write. In WAL mode readers never wait for the writer, and
///   they see every commit that finished before their read began, so a status
///   read after an acknowledged write is consistent. Read-only mode makes an
///   accidental write here fail loudly instead of reintroducing contention.
#[derive(Clone)]
pub struct Store {
    writer: SqlitePool,
    reader: SqlitePool,
    clock: Arc<dyn Clock>,
    options: StoreOptions,
    /// Published to after every committed state change (never before commit,
    /// never for a rolled-back transaction).
    events: EventBus,
    /// Nudges the workflow decider after a commit made a run due.
    decider_wake: Arc<tokio::sync::Notify>,
}

impl Store {
    /// Open (creating if needed) the database at `path` and apply migrations.
    pub async fn open(
        path: &Path,
        clock: Arc<dyn Clock>,
        options: StoreOptions,
    ) -> Result<Self, StoreError> {
        let base = SqliteConnectOptions::new()
            .filename(path)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(match options.durability {
                Durability::Full => SqliteSynchronous::Full,
                Durability::Normal => SqliteSynchronous::Normal,
            })
            // Still needed: another process or a WAL checkpoint can hold the lock.
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);

        // The writer creates and migrates the file before any reader opens
        // it; a read-only connection can do neither.
        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(base.clone().create_if_missing(true))
            .await?;
        MIGRATOR.run(&writer).await?;
        let reader = SqlitePoolOptions::new()
            .max_connections(options.max_connections.max(1))
            .connect_with(base.read_only(true))
            .await?;
        Ok(Self {
            writer,
            reader,
            events: EventBus::new(),
            decider_wake: Arc::default(),
            clock,
            options,
        })
    }

    /// The store's clock. Everything time-related on the coordinator reads it.
    pub fn now_ms(&self) -> Timestamp {
        self.clock.now_ms()
    }

    /// The newest timestamp written by any previous run. Used at startup as a
    /// floor for the clock, so a wall clock that went backwards while the
    /// coordinator was down can't stamp rows "before" existing ones.
    pub async fn latest_timestamp(&self) -> Result<Option<Timestamp>, StoreError> {
        // Only timestamps that were *written* as "now". Future deadlines
        // (lease expiry, workflow wake_at, timer fire_at) must not raise the
        // floor, or a long sleep would push the clock days ahead.
        Ok(sqlx::query_scalar(
            "SELECT MAX(m) FROM (SELECT MAX(updated_at) AS m FROM jobs
                                 UNION ALL SELECT MAX(last_seen_at) FROM workers
                                 UNION ALL SELECT MAX(updated_at) FROM workflow_runs
                                 UNION ALL SELECT MAX(at) FROM workflow_events)",
        )
        .fetch_one(&self.reader)
        .await?)
    }

    /// Wait for in-flight queries and close every connection.
    pub async fn close(&self) {
        self.writer.close().await;
        self.reader.close().await;
    }

    /// The journal mode and `synchronous` level actually in effect on the
    /// connection that commits, so the durability claim ("a 201 survives
    /// power loss") can be checked and logged at startup rather than
    /// assumed. `synchronous` 2 = FULL.
    pub async fn durability(&self) -> Result<(String, i64), StoreError> {
        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&self.writer)
            .await?;
        let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&self.writer)
            .await?;
        Ok((journal, synchronous))
    }

    /// Health check. Uses a reader on purpose: a long write must not make
    /// the coordinator look dead.
    pub async fn ping(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.reader).await?;
        Ok(())
    }

    pub async fn submit(&self, job: NewJob) -> Result<SubmitOutcome, StoreError> {
        let now = self.clock.now_ms();
        let inserted = {
            let mut conn = self.writer.acquire().await?;
            insert_job(&mut conn, &job, now, self.options.max_active_jobs).await?
        };

        if let Some(created) = inserted {
            self.events.publish(
                now,
                EventKind::JobSubmitted {
                    from: None,
                    job: created.summary(),
                },
            );
            return Ok(SubmitOutcome::Created(created));
        }

        // Nothing inserted: either the key is taken or the queue is full. Keys
        // are never deleted, so this lookup can't race with a removal. A replay
        // of an existing job succeeds even when the queue is full.
        if let Some(key) = job.idempotency_key.as_deref() {
            if let Some(existing) = self.get_by_idempotency_key(key).await? {
                return if existing.request_fingerprint == job.request_fingerprint {
                    Ok(SubmitOutcome::Existing(existing))
                } else {
                    Err(StoreError::IdempotencyConflict {
                        key: key.to_owned(),
                        existing: existing.id,
                    })
                };
            }
        }
        Err(StoreError::QueueFull {
            capacity: self.options.max_active_jobs,
        })
    }

    pub async fn get(&self, id: JobId) -> Result<Option<Job>, StoreError> {
        let row = sqlx::query("SELECT * FROM jobs WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.reader)
            .await?;
        row.as_ref().map(job_from_row).transpose()
    }

    pub async fn get_by_idempotency_key(&self, key: &str) -> Result<Option<Job>, StoreError> {
        let row = sqlx::query("SELECT * FROM jobs WHERE idempotency_key = ?")
            .bind(key)
            .fetch_optional(&self.reader)
            .await?;
        row.as_ref().map(job_from_row).transpose()
    }

    /// Number of jobs per state, for status endpoints and tests.
    pub async fn count_by_state(&self) -> Result<Vec<(JobState, u64)>, StoreError> {
        let rows =
            sqlx::query("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state ORDER BY state")
                .fetch_all(&self.reader)
                .await?;
        rows.iter()
            .map(|row| {
                let raw: String = row.try_get("state")?;
                let state = raw.parse::<JobState>().map_err(|e| StoreError::Corrupt {
                    job_id: "<aggregate>".into(),
                    reason: e.to_string(),
                })?;
                let n: i64 = row.try_get("n")?;
                Ok((state, u64::try_from(n).unwrap_or(0)))
            })
            .collect()
    }
}

/// Insert a new `queued` job unless the queue is at capacity (`None`) or the
/// idempotency key exists (`None` too; the caller tells them apart). Takes a
/// connection so it can run inside an outer transaction (the workflow
/// decider schedules activities this way).
pub(crate) async fn insert_job(
    conn: &mut SqliteConnection,
    job: &NewJob,
    now: Timestamp,
    max_active_jobs: u64,
) -> Result<Option<Job>, StoreError> {
    let id = JobId::generate();
    let row = sqlx::query(INSERT_JOB)
        .bind(id.to_string())
        .bind(job.kind.as_str())
        .bind(job.payload.to_string())
        .bind(job.request_fingerprint.as_str())
        .bind(job.idempotency_key.as_deref())
        .bind(JobState::Queued.as_str())
        .bind(i64::from(job.max_attempts))
        .bind(saturating_i64(job.timeout_ms))
        .bind(now) // available_at
        .bind(now) // created_at
        .bind(now) // updated_at
        .bind(ACTIVE_STATES[0].as_str())
        .bind(ACTIVE_STATES[1].as_str())
        .bind(ACTIVE_STATES[2].as_str())
        .bind(saturating_i64(max_active_jobs))
        .fetch_optional(conn)
        .await?;
    row.as_ref().map(job_from_row).transpose()
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// When a job that just failed attempt `attempt` may be claimed again.
fn retry_at(policy: &RetryPolicy, now: Timestamp, attempt: u32) -> Timestamp {
    now.saturating_add(duration_ms(policy.delay(attempt, fastrand::f64())))
}

fn job_from_row(row: &SqliteRow) -> Result<Job, StoreError> {
    let raw_id: String = row.try_get("id")?;
    let corrupt = |reason: String| StoreError::Corrupt {
        job_id: raw_id.clone(),
        reason,
    };

    let id = raw_id
        .parse::<JobId>()
        .map_err(|e| corrupt(e.to_string()))?;
    let state = row
        .try_get::<String, _>("state")?
        .parse::<JobState>()
        .map_err(|e| corrupt(e.to_string()))?;
    let payload = serde_json::from_str(&row.try_get::<String, _>("payload")?)
        .map_err(|e| corrupt(format!("payload: {e}")))?;
    let result = row
        .try_get::<Option<String>, _>("result")?
        .map(|raw| serde_json::from_str(&raw))
        .transpose()
        .map_err(|e| corrupt(format!("result: {e}")))?;
    let attempt = u32::try_from(row.try_get::<i64, _>("attempt")?)
        .map_err(|e| corrupt(format!("attempt: {e}")))?;
    let max_attempts = u32::try_from(row.try_get::<i64, _>("max_attempts")?)
        .map_err(|e| corrupt(format!("max_attempts: {e}")))?;
    let timeout_ms = u64::try_from(row.try_get::<i64, _>("timeout_ms")?)
        .map_err(|e| corrupt(format!("timeout_ms: {e}")))?;

    Ok(Job {
        id,
        kind: row.try_get("kind")?,
        payload,
        request_fingerprint: row.try_get("request_fingerprint")?,
        idempotency_key: row.try_get("idempotency_key")?,
        state,
        attempt,
        max_attempts,
        timeout_ms,
        available_at: row.try_get("available_at")?,
        worker_id: row.try_get("worker_id")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        attempt_deadline: row.try_get("attempt_deadline")?,
        result,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_states_match_the_state_machine() {
        let expected: Vec<_> = JobState::ALL
            .into_iter()
            .filter(|s| s.is_active())
            .collect();
        assert_eq!(ACTIVE_STATES.to_vec(), expected);
    }

    #[tokio::test]
    async fn one_writer_connection_and_a_read_only_reader_pool() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            &dir.path().join("r.db"),
            Arc::new(relay_core::clock::ManualClock::new(0)),
            StoreOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(store.writer.options().get_max_connections(), 1);
        assert_eq!(store.reader.options().get_max_connections(), 8);

        // A write through the reader must fail, not silently contend.
        let err = sqlx::query("DELETE FROM jobs")
            .execute(&store.reader)
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("readonly"), "{message}");
        store.close().await;
    }
}
