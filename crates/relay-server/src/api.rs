//! Axum routes for the coordinator HTTP API.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use relay_core::api::{
    HealthResponse, JobResultView, JobStatusView, SubmitJobRequest, SubmitJobResponse,
};
use relay_core::job::{Job, JobId};
use relay_core::submission::SubmissionLimits;
use tokio::net::TcpListener;
use tokio::sync::{Notify, watch};
use tracing::{info, warn};

use crate::dashboard_api;
use crate::error::ApiError;
use crate::observe::{HttpMetrics, observe};
use crate::store::{CancelOutcome, Store, SubmitOutcome};
use crate::worker_api;

#[derive(Debug, Clone)]
pub struct Limits {
    pub submission: SubmissionLimits,
    /// Hard cap on request body size, enforced before JSON parsing.
    pub max_request_bytes: usize,
    /// Largest job result a worker may report, as compact JSON.
    pub max_result_bytes: usize,
    /// Every request, including reading its body, must finish within this,
    /// or it gets `504 request_timeout` (an *ambiguous* outcome).
    pub request_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            submission: SubmissionLimits::default(),
            max_request_bytes: 256 * 1024,
            max_result_bytes: 128 * 1024,
            // Well under the default 30 s lease; SQLite's busy timeout is 5 s.
            request_timeout: Duration::from_secs(10),
        }
    }
}

pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct AppState {
    pub(crate) store: Store,
    pub(crate) limits: Arc<Limits>,
    pub(crate) lease: Duration,
    pub(crate) http: Arc<HttpMetrics>,
    /// Registers `/v1/dev/...` routes. Off by default; see `with_dev_controls`.
    pub(crate) dev_controls: bool,
    /// Flipped to `true` when shutdown starts, ending long-lived event streams
    /// so they don't hold up the drain.
    pub(crate) closing: Arc<watch::Sender<bool>>,
}

impl AppState {
    pub fn new(store: Store, limits: Limits) -> Self {
        Self {
            store,
            limits: Arc::new(limits),
            lease: DEFAULT_LEASE,
            http: Arc::default(),
            dev_controls: false,
            closing: Arc::new(watch::channel(false).0),
        }
    }

    /// Enable development-only controls that can disrupt running jobs
    /// (`POST /v1/dev/workers/{id}/expire-leases`). There is no
    /// authentication: the caller must only enable this on loopback.
    pub fn with_dev_controls(mut self, enabled: bool) -> Self {
        self.dev_controls = enabled;
        self
    }

    /// How long a claim owns its job without a heartbeat.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }
}

pub fn router(state: AppState) -> Router {
    let body_limit = state.limits.max_request_bytes;
    let mut router = Router::new();
    if state.dev_controls {
        router = router.route(
            "/v1/dev/workers/{worker_id}/expire-leases",
            post(dashboard_api::expire_leases),
        );
    }
    router
        .route("/healthz", get(health))
        .route("/metrics", get(crate::metrics::metrics))
        .route("/v1/jobs", post(submit_job))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/jobs/{id}/result", get(get_result))
        .route("/v1/jobs/{id}/cancel", post(cancel_job))
        .route("/v1/jobs/{id}/attempts", get(worker_api::list_attempts))
        .route(
            "/v1/jobs/{id}/attempts/{attempt}/complete",
            post(worker_api::complete),
        )
        .route(
            "/v1/jobs/{id}/attempts/{attempt}/fail",
            post(worker_api::fail),
        )
        .route(
            "/v1/jobs/{id}/attempts/{attempt}/heartbeat",
            post(worker_api::heartbeat),
        )
        .route("/v1/workers", get(worker_api::list_workers))
        .route("/v1/workers/{worker_id}", put(worker_api::register))
        .route("/v1/workers/{worker_id}/claims", post(worker_api::claim))
        .route("/v1/dashboard/snapshot", get(dashboard_api::snapshot))
        .route("/v1/events", get(dashboard_api::events))
        // route_layer: runs after routing, so the route template is known.
        .route_layer(middleware::from_fn_with_state(state.clone(), observe))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state)
}

/// Serve the API on `listener` until `shutdown` resolves. Then stop
/// accepting connections and let in-flight requests finish, for at most
/// `drain`: a stalled client can't hold up shutdown forever. Cutting a
/// request off is safe; every write is atomic and clients retry ambiguous
/// failures.
pub async fn serve<F>(
    listener: TcpListener,
    state: AppState,
    shutdown: F,
    drain: Duration,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let started_draining = Arc::new(Notify::new());
    let notify = started_draining.clone();
    let closing = state.closing.clone();
    let server = axum::serve(listener, router(state)).with_graceful_shutdown(async move {
        shutdown.await;
        closing.send_replace(true);
        // notify_one stores a permit, so the deadline below starts even if
        // it isn't polled yet.
        notify.notify_one();
    });
    tokio::select! {
        served = server => served,
        () = async {
            started_draining.notified().await;
            tokio::time::sleep(drain).await;
        } => {
            warn!(drain_secs = drain.as_secs_f64(), "drain deadline passed; closing remaining connections");
            Ok(())
        }
    }
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    state.store.ping().await?;
    Ok(Json(HealthResponse {
        status: "ok".into(),
    }))
}

async fn submit_job(
    State(state): State<AppState>,
    body: Result<Json<SubmitJobRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<SubmitJobResponse>), ApiError> {
    let Json(request) = body?;
    let new_job = request.validate(&state.limits.submission)?;
    let payload_bytes = new_job.payload.to_string().len();

    let (status, created, job) = match state.store.submit(new_job).await? {
        SubmitOutcome::Created(job) => (StatusCode::CREATED, true, job),
        SubmitOutcome::Existing(job) => (StatusCode::OK, false, job),
    };
    // Never log the payload itself: prompts may contain sensitive data.
    if created {
        info!(
            job_id = %job.id, kind = %job.kind, payload_bytes,
            has_idempotency_key = job.idempotency_key.is_some(),
            "job accepted"
        );
    } else {
        info!(job_id = %job.id, state = %job.state, "idempotent replay returned existing job");
    }

    Ok((
        status,
        Json(SubmitJobResponse {
            created,
            job: job.status_view(),
        }),
    ))
}

async fn get_job(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<JobStatusView>, ApiError> {
    let job = load_job(&state, &raw_id).await?;
    Ok(Json(job.status_view()))
}

async fn get_result(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<(StatusCode, Json<JobResultView>), ApiError> {
    let job = load_job(&state, &raw_id).await?;
    let status = if job.state.is_terminal() {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    Ok((status, Json(job.result_view())))
}

/// `200` with the cancelled job (also when it was already cancelled), or
/// `409 not_cancellable` if it already succeeded or failed.
async fn cancel_job(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<JobStatusView>, ApiError> {
    let id: JobId = raw_id.parse()?;
    let outcome = state.store.cancel(id).await?;
    if let CancelOutcome::Cancelled(job) = &outcome {
        info!(job_id = %job.id, attempt = job.attempt, "job cancelled");
    }
    Ok(Json(outcome.job().status_view()))
}

pub(crate) async fn load_job(state: &AppState, raw_id: &str) -> Result<Job, ApiError> {
    let id: JobId = raw_id.parse()?;
    state.store.get(id).await?.ok_or(ApiError::JobNotFound(id))
}
