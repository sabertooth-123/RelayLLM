//! Worker-facing routes: registration, claims, and attempt reports.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use relay_core::api::{
    Assignment, AttemptView, ClaimRequest, CompleteRequest, FailRequest, HeartbeatRequest,
    HeartbeatResponse, JobStatusView, RegisterWorkerRequest, WorkerView,
};
use relay_core::job::{Job, JobId};
use relay_core::worker::{parse_attempt, validate_worker_id};
use tracing::{info, warn};

use crate::api::{AppState, load_job};
use crate::error::ApiError;
use crate::store::{ReportOutcome, StoreError};

pub(crate) async fn register(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
    body: Result<Json<RegisterWorkerRequest>, JsonRejection>,
) -> Result<Json<WorkerView>, ApiError> {
    let Json(request) = body?;
    validate_worker_id(&worker_id)?;
    request.validate(state.limits.submission.max_kind_len)?;
    let worker = state.store.register_worker(&worker_id, &request).await?;
    info!(
        worker_id = %worker.id, name = %worker.name,
        concurrency = worker.concurrency, kinds = ?worker.kinds,
        "worker registered"
    );
    Ok(Json(worker))
}

pub(crate) async fn list_workers(
    State(state): State<AppState>,
) -> Result<Json<Vec<WorkerView>>, ApiError> {
    Ok(Json(state.store.list_workers().await?))
}

/// `200` with an [`Assignment`], or `204` when no eligible job exists.
pub(crate) async fn claim(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
    body: Result<Json<ClaimRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) = body?;
    validate_worker_id(&worker_id)?;
    request.validate()?;

    match state
        .store
        .claim(&worker_id, &request.claim_id, state.lease)
        .await?
    {
        Some(job) => {
            info!(job_id = %job.id, attempt = job.attempt, worker_id = %worker_id, "job claimed");
            Ok(Json(assignment(
                job,
                request.claim_id,
                heartbeat_interval(&state),
            )?)
            .into_response())
        }
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

/// A third of the lease: two heartbeats can be lost before the lease ends.
fn heartbeat_interval(state: &AppState) -> u64 {
    u64::try_from((state.lease / 3).as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

/// `200` with the extended lease, or `409 stale_attempt` if this attempt no
/// longer owns the job. The worker should then abort the handler.
pub(crate) async fn heartbeat(
    State(state): State<AppState>,
    Path((raw_id, raw_attempt)): Path<(String, String)>,
    body: Result<Json<HeartbeatRequest>, JsonRejection>,
) -> Result<Json<HeartbeatResponse>, ApiError> {
    let Json(request) = body?;
    let job_id: JobId = raw_id.parse()?;
    let attempt = parse_attempt(&raw_attempt)?;
    validate_worker_id(&request.worker_id)?;

    let job = state
        .store
        .heartbeat(job_id, attempt, &request.worker_id, state.lease)
        .await
        .inspect_err(|e| log_rejection(e, &request.worker_id))?;
    let corrupt = |what: &str| {
        ApiError::from(StoreError::Corrupt {
            job_id: job.id.to_string(),
            reason: format!("running job without {what}"),
        })
    };
    Ok(Json(HeartbeatResponse {
        lease_expires_at_ms: job.lease_expires_at.ok_or_else(|| corrupt("a lease"))?,
        attempt_deadline_ms: job.attempt_deadline.ok_or_else(|| corrupt("a deadline"))?,
    }))
}

fn assignment(
    job: Job,
    claim_id: String,
    heartbeat_interval_ms: u64,
) -> Result<Assignment, ApiError> {
    // A running job always has a lease (DB CHECK); its absence is corruption.
    let lease_expires_at_ms = job.lease_expires_at.ok_or_else(|| {
        ApiError::from(StoreError::Corrupt {
            job_id: job.id.to_string(),
            reason: "running job without a lease".into(),
        })
    })?;
    Ok(Assignment {
        job_id: job.id,
        attempt: job.attempt,
        claim_id,
        kind: job.kind,
        payload: job.payload,
        timeout_ms: job.timeout_ms,
        lease_expires_at_ms,
        heartbeat_interval_ms,
    })
}

pub(crate) async fn complete(
    State(state): State<AppState>,
    Path((raw_id, raw_attempt)): Path<(String, String)>,
    body: Result<Json<CompleteRequest>, JsonRejection>,
) -> Result<Json<JobStatusView>, ApiError> {
    let Json(request) = body?;
    let job_id: JobId = raw_id.parse()?;
    let attempt = parse_attempt(&raw_attempt)?;
    request.validate(state.limits.max_result_bytes)?;

    let outcome = state
        .store
        .complete(job_id, attempt, &request.worker_id, &request.result)
        .await
        .inspect_err(|e| log_rejection(e, &request.worker_id))?;
    log_report(&outcome, attempt, "completed");
    Ok(Json(outcome.job().status_view()))
}

pub(crate) async fn fail(
    State(state): State<AppState>,
    Path((raw_id, raw_attempt)): Path<(String, String)>,
    body: Result<Json<FailRequest>, JsonRejection>,
) -> Result<Json<JobStatusView>, ApiError> {
    let Json(request) = body?;
    let job_id: JobId = raw_id.parse()?;
    let attempt = parse_attempt(&raw_attempt)?;
    let request = request.validate()?;

    let outcome = state
        .store
        .fail(
            job_id,
            attempt,
            &request.worker_id,
            &request.error,
            request.retryable,
        )
        .await
        .inspect_err(|e| log_rejection(e, &request.worker_id))?;
    log_report(&outcome, attempt, "failure recorded");
    Ok(Json(outcome.job().status_view()))
}

pub(crate) async fn list_attempts(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<Vec<AttemptView>>, ApiError> {
    let job = load_job(&state, &raw_id).await?;
    Ok(Json(state.store.attempts(job.id).await?))
}

fn log_report(outcome: &ReportOutcome, attempt: u32, what: &str) {
    let job = outcome.job();
    match outcome {
        ReportOutcome::Applied(_) => {
            info!(job_id = %job.id, attempt, state = %job.state, "attempt {what}");
        }
        ReportOutcome::AlreadyApplied(_) => {
            info!(job_id = %job.id, attempt, state = %job.state, "duplicate report ignored ({what} earlier)");
        }
    }
}

fn log_rejection(error: &StoreError, worker_id: &str) {
    if let StoreError::StaleAttempt {
        job_id,
        attempt,
        current_attempt,
        state,
    } = error
    {
        warn!(
            %job_id, attempt, current_attempt, %state, worker_id,
            "rejected report from stale attempt"
        );
    }
}
