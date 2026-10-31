//! `/v1/workflows`: start, inspect, signal and cancel durable workflow runs
//! (docs/api.md, docs/WORKFLOWS_DESIGN.md §8).

use axum::Json;
use axum::body::Bytes;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use relay_core::workflow::{
    CancelWorkflowRequest, SignalRequest, SignalResponse, StartWorkflowRequest,
    StartWorkflowResponse, WorkflowHistoryView, WorkflowId, WorkflowView,
};
use serde::de::DeserializeOwned;
use tracing::info;

use crate::api::AppState;
use crate::error::ApiError;
use crate::store::{CancelWorkflowOutcome, SignalOutcome, StartOutcome};

/// `201` for a new run, `200` when an idempotency key matched an existing
/// one with the same request.
pub(crate) async fn start(
    State(state): State<AppState>,
    body: Result<Json<StartWorkflowRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<StartWorkflowResponse>), ApiError> {
    let Json(request) = body?;
    let new = request.validate(state.store.workflow_limits())?;
    let steps = new.workflow.steps.len();
    let outcome = state.store.start_workflow(new).await?;
    let (status, created) = match &outcome {
        StartOutcome::Created(run) => {
            info!(workflow_id = %run.id, name = %run.name, steps, "workflow started");
            (StatusCode::CREATED, true)
        }
        StartOutcome::Existing(run) => {
            info!(workflow_id = %run.id, "idempotent replay returned existing workflow");
            (StatusCode::OK, false)
        }
    };
    let workflow = view(&state, outcome.run().id).await?;
    Ok((status, Json(StartWorkflowResponse { created, workflow })))
}

pub(crate) async fn get(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<WorkflowView>, ApiError> {
    let id: WorkflowId = raw_id.parse()?;
    Ok(Json(view(&state, id).await?))
}

pub(crate) async fn history(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<WorkflowHistoryView>, ApiError> {
    let id: WorkflowId = raw_id.parse()?;
    // An unknown run has no history *and* no row; say which.
    if state.store.workflow_run(id).await?.is_none() {
        return Err(ApiError::WorkflowNotFound(id));
    }
    let events = state.store.workflow_history(id).await?;
    Ok(Json(WorkflowHistoryView { id, events }))
}

/// The body is optional: an empty body is a `null` payload.
pub(crate) async fn signal(
    State(state): State<AppState>,
    Path((raw_id, name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<SignalResponse>, ApiError> {
    let id: WorkflowId = raw_id.parse()?;
    let request: SignalRequest = optional_json(&body)?;
    let recorded = match state
        .store
        .signal_workflow(id, &name, request.payload)
        .await?
    {
        SignalOutcome::Recorded => {
            info!(workflow_id = %id, signal = %name, "signal recorded");
            true
        }
        SignalOutcome::AlreadyReceived => false,
    };
    Ok(Json(SignalResponse { recorded }))
}

/// The body is optional: `{"reason": "..."}`.
pub(crate) async fn cancel(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
    body: Bytes,
) -> Result<Json<WorkflowView>, ApiError> {
    let id: WorkflowId = raw_id.parse()?;
    let request: CancelWorkflowRequest = optional_json(&body)?;
    let reason = request
        .reason
        .unwrap_or_else(|| "cancelled by client".into());
    if let CancelWorkflowOutcome::Cancelled(_) = state.store.cancel_workflow(id, &reason).await? {
        info!(workflow_id = %id, reason, "workflow cancelled");
    }
    Ok(Json(view(&state, id).await?))
}

async fn view(state: &AppState, id: WorkflowId) -> Result<WorkflowView, ApiError> {
    state
        .store
        .workflow_view(id)
        .await?
        .ok_or(ApiError::WorkflowNotFound(id))
}

/// Parse an optional JSON body: empty means `T::default()`. Unknown fields
/// are rejected like everywhere else.
fn optional_json<T: DeserializeOwned + Default>(body: &Bytes) -> Result<T, ApiError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|e| ApiError::BadBody {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        message: format!("invalid JSON body: {e}"),
    })
}
