//! HTTP request/response bodies. The wire contract is documented in
//! `docs/api.md`; these types are its Rust mirror.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::clock::Timestamp;
use crate::job::{AttemptOutcome, JobId, JobState};

/// Body of `POST /v1/jobs`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitJobRequest {
    /// Selects the worker handler, e.g. `"mock"`.
    pub kind: String,
    /// Handler-specific JSON object, e.g. `{"prompt": "..."}`.
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmitJobResponse {
    /// `false` when an idempotency key matched an existing job.
    pub created: bool,
    pub job: JobStatusView,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobStatusView {
    pub id: JobId,
    pub kind: String,
    pub state: JobState,
    pub attempt: u32,
    pub max_attempts: u32,
    pub idempotency_key: Option<String>,
    pub worker_id: Option<String>,
    pub last_error: Option<String>,
    pub created_at_ms: Timestamp,
    pub updated_at_ms: Timestamp,
    pub finished_at_ms: Option<Timestamp>,
}

/// Body of `GET /v1/jobs/{id}/result`. Served with `200` once the job is
/// terminal and `202` while it is still in progress.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobResultView {
    pub id: JobId,
    pub state: JobState,
    pub result: Option<Value>,
    pub error: Option<String>,
}

/// Every non-2xx response carries this body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// Stable machine-readable code, e.g. `queue_full`.
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

/// Body of `PUT /v1/workers/{worker_id}`. Registration is an idempotent
/// upsert keyed by the worker-chosen id, so a retried registration never
/// creates a ghost worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterWorkerRequest {
    /// Human-readable label; several processes may share it.
    pub name: String,
    pub concurrency: u32,
    /// Job kinds this worker has handlers for. Claims only return these.
    pub kinds: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerView {
    pub id: String,
    pub name: String,
    pub concurrency: u32,
    pub kinds: Vec<String>,
    pub registered_at_ms: Timestamp,
    /// Last registration or claim. Heartbeats update it from Phase 3.
    pub last_seen_at_ms: Timestamp,
    pub running_jobs: u64,
}

/// Body of `POST /v1/workers/{worker_id}/claims`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRequest {
    /// Worker-generated id for this claim. Retrying a claim whose response
    /// was lost with the *same* `claim_id` returns the same assignment
    /// instead of taking a second job.
    pub claim_id: String,
}

/// A job handed to a worker. `attempt` is the fencing token that every
/// later report for this assignment must carry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub job_id: JobId,
    pub attempt: u32,
    pub claim_id: String,
    pub kind: String,
    pub payload: Value,
    pub timeout_ms: u64,
    pub lease_expires_at_ms: Timestamp,
    /// Coordinator-chosen heartbeat cadence (a fraction of the lease).
    /// Workers never compute lease deadlines from their own clocks.
    pub heartbeat_interval_ms: u64,
}

/// Body of `POST /v1/jobs/{id}/attempts/{attempt}/heartbeat`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatRequest {
    pub worker_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub lease_expires_at_ms: Timestamp,
    /// Heartbeats can't extend the lease past this.
    pub attempt_deadline_ms: Timestamp,
}

/// Body of `POST /v1/jobs/{id}/attempts/{attempt}/complete`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteRequest {
    pub worker_id: String,
    pub result: Value,
}

/// Body of `POST /v1/jobs/{id}/attempts/{attempt}/fail`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailRequest {
    pub worker_id: String,
    pub error: String,
    /// `true`: retry if attempts remain. `false`: fail the job now.
    pub retryable: bool,
}

/// One row of `GET /v1/jobs/{id}/attempts`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptView {
    pub attempt: u32,
    pub worker_id: String,
    pub started_at_ms: Timestamp,
    pub finished_at_ms: Option<Timestamp>,
    /// `None` while the attempt is in progress.
    pub outcome: Option<AttemptOutcome>,
    pub error: Option<String>,
}
