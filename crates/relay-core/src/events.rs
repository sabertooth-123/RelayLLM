//! Live coordinator events for observers (the dashboard).
//!
//! Every event describes a *committed* change. Job events carry the
//! transition (`from` → `job.state`), so a client can tell whether an event
//! applies to the state it holds, is a duplicate, or reveals that it has
//! fallen out of sync. The stream is a notification channel, not a log: it
//! lives in memory, restarts with the coordinator (new `epoch`), and can drop
//! events for a slow subscriber. Clients must treat
//! [`DashboardSnapshot`] as the source of truth and re-fetch it whenever the
//! stream can't be applied cleanly. See `docs/api.md`.

use serde::{Deserialize, Serialize};

use crate::api::WorkerView;
use crate::clock::Timestamp;
use crate::job::{JobId, JobState};

/// A job as observers see it: no payload, no result, no idempotency key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: JobId,
    pub kind: String,
    pub state: JobState,
    /// The current attempt number, which is also the fencing token.
    pub attempt: u32,
    pub max_attempts: u32,
    pub worker_id: Option<String>,
    pub has_idempotency_key: bool,
    pub last_error: Option<String>,
    pub created_at_ms: Timestamp,
    pub updated_at_ms: Timestamp,
    pub finished_at_ms: Option<Timestamp>,
    /// Earliest claim time (backoff deadline for `retry_waiting`).
    pub available_at_ms: Timestamp,
    pub lease_expires_at_ms: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DashboardEvent {
    /// Increases by exactly 1 per event within one `epoch`.
    pub seq: u64,
    /// Identifies one coordinator run; changes on restart, resetting `seq`.
    pub epoch: String,
    pub at_ms: Timestamp,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    /// `from` is always `None`: the job is new.
    JobSubmitted {
        from: Option<JobState>,
        job: JobSummary,
    },
    /// `queued` or `retry_waiting` → `running`, attempt incremented.
    JobClaimed {
        from: Option<JobState>,
        job: JobSummary,
    },
    JobSucceeded {
        from: Option<JobState>,
        job: JobSummary,
    },
    /// A retryable failure with attempts left: `running` → `retry_waiting`.
    JobRetryScheduled {
        from: Option<JobState>,
        job: JobSummary,
    },
    /// Terminal failure: permanent error or attempts exhausted.
    JobFailed {
        from: Option<JobState>,
        job: JobSummary,
    },
    /// The reaper expired a lease: `running` → `retry_waiting` or `failed`.
    LeaseExpired {
        from: Option<JobState>,
        job: JobSummary,
        /// The worker whose lease expired.
        worker_id: String,
        reason: String,
    },
    JobCancelled {
        from: Option<JobState>,
        job: JobSummary,
    },
    WorkerRegistered {
        worker: WorkerView,
    },
    /// A worker renewed the lease of a running job. No state change.
    WorkerHeartbeat {
        worker_id: String,
        job_id: JobId,
        attempt: u32,
        lease_expires_at_ms: Timestamp,
    },
}

impl EventKind {
    /// The job and its previous state, for job transition events.
    pub fn transition(&self) -> Option<(Option<JobState>, &JobSummary)> {
        match self {
            EventKind::JobSubmitted { from, job }
            | EventKind::JobClaimed { from, job }
            | EventKind::JobSucceeded { from, job }
            | EventKind::JobRetryScheduled { from, job }
            | EventKind::JobFailed { from, job }
            | EventKind::LeaseExpired { from, job, .. }
            | EventKind::JobCancelled { from, job } => Some((*from, job)),
            EventKind::WorkerRegistered { .. } | EventKind::WorkerHeartbeat { .. } => None,
        }
    }
}

/// Everything a dashboard needs to (re)build its view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    pub epoch: String,
    /// Taken *before* the reads below. Events with a higher `seq` may already
    /// be reflected here; clients must apply them idempotently (by checking
    /// each event's `from` against the state they hold).
    pub seq: u64,
    pub server_time_ms: Timestamp,
    /// The coordinator's lease. A worker not seen for longer than this has
    /// missed its heartbeats (or claim polls) and is shown as unavailable.
    pub lease_ms: u64,
    pub max_active_jobs: u64,
    /// Exact counts per state, from the database.
    pub counts: Vec<StateCount>,
    pub workers: Vec<WorkerView>,
    /// All active jobs (up to a cap) followed by the most recent terminal ones.
    pub jobs: Vec<JobSummary>,
    /// True when more active jobs exist than are included above.
    pub active_jobs_truncated: bool,
    /// The most recent events of this epoch, oldest first.
    pub recent_events: Vec<DashboardEvent>,
    /// Whether development-only controls are enabled on this coordinator.
    pub dev_controls: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateCount {
    pub state: JobState,
    pub count: u64,
}
