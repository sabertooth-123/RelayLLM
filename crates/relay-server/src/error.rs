//! Mapping of internal errors to HTTP responses.
//!
//! Every error response has the body `{"error": {"code", "message"}}`. The
//! `code` values are part of the API contract (`docs/api.md`).

use std::error::Error as _;

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use relay_core::api::{ErrorBody, ErrorDetail};
use relay_core::job::{JobId, JobState, ParseJobIdError};
use relay_core::submission::ValidationError;
use relay_core::worker::WorkerValidationError;

use crate::store::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    WorkerValidation(#[from] WorkerValidationError),
    #[error("{message}")]
    BadBody { status: StatusCode, message: String },
    #[error(transparent)]
    InvalidJobId(#[from] ParseJobIdError),
    #[error("job {0} not found")]
    JobNotFound(JobId),
    #[error("worker {0:?} is not registered; register it with PUT /v1/workers/{{id}}")]
    WorkerNotRegistered(String),
    #[error("queue is full ({capacity} active jobs); retry later")]
    QueueFull { capacity: u64 },
    #[error("idempotency key {key:?} is already bound to job {existing} with a different request")]
    IdempotencyConflict { key: String, existing: JobId },
    #[error(
        "attempt {attempt} no longer owns job {job_id} (job is {state} at attempt {current_attempt}); \
         discard this result"
    )]
    StaleAttempt {
        job_id: JobId,
        attempt: u32,
        current_attempt: u32,
        state: JobState,
    },
    #[error("job {job_id} is already {state} and can't be cancelled")]
    NotCancellable { job_id: JobId, state: JobState },
    /// The handler didn't finish in time. Deliberately a 504, which clients
    /// treat as *ambiguous*: a statement may still commit after the handler
    /// was dropped, so a retry must reuse the same idempotency key.
    #[error("request timed out after {0} ms; the operation may or may not have been applied")]
    RequestTimeout(u128),
    #[error("internal error")]
    Internal,
}

impl ApiError {
    fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            ApiError::Validation(_) | ApiError::WorkerValidation(_) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "invalid_request")
            }
            ApiError::BadBody { status, .. } => (*status, "bad_body"),
            ApiError::InvalidJobId(_) => (StatusCode::BAD_REQUEST, "invalid_job_id"),
            ApiError::JobNotFound(_) => (StatusCode::NOT_FOUND, "job_not_found"),
            ApiError::WorkerNotRegistered(_) => (StatusCode::NOT_FOUND, "worker_not_registered"),
            ApiError::QueueFull { .. } => (StatusCode::SERVICE_UNAVAILABLE, "queue_full"),
            ApiError::IdempotencyConflict { .. } => (StatusCode::CONFLICT, "idempotency_conflict"),
            ApiError::StaleAttempt { .. } => (StatusCode::CONFLICT, "stale_attempt"),
            ApiError::NotCancellable { .. } => (StatusCode::CONFLICT, "not_cancellable"),
            ApiError::RequestTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "request_timeout"),
            ApiError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        }
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        ApiError::BadBody {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::QueueFull { capacity } => ApiError::QueueFull { capacity },
            StoreError::IdempotencyConflict { key, existing } => {
                ApiError::IdempotencyConflict { key, existing }
            }
            StoreError::WorkerNotRegistered(id) => ApiError::WorkerNotRegistered(id),
            StoreError::NotCancellable { job_id, state } => {
                ApiError::NotCancellable { job_id, state }
            }
            StoreError::JobNotFound(id) => ApiError::JobNotFound(id),
            StoreError::StaleAttempt {
                job_id,
                attempt,
                current_attempt,
                state,
            } => ApiError::StaleAttempt {
                job_id,
                attempt,
                current_attempt,
                state,
            },
            other => {
                // Details stay in the server log; clients get a generic 500.
                tracing::error!(error = %error_chain(&other), "store error");
                ApiError::Internal
            }
        }
    }
}

/// "outer: cause: root cause" for log lines.
fn error_chain(err: &StoreError) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code) = self.status_and_code();
        let body = ErrorBody {
            error: ErrorDetail {
                code: code.to_owned(),
                message: self.to_string(),
            },
        };
        let mut response = (status, Json(body)).into_response();
        if matches!(self, ApiError::QueueFull { .. }) {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}
