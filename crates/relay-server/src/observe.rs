//! Per-request middleware: timeout, structured access log, HTTP counters.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::extract::{MatchedPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tracing::{debug, info, warn};

use crate::api::AppState;
use crate::error::ApiError;

/// In-process HTTP counters. They reset when the coordinator restarts;
/// durable facts (jobs, attempts) are read from the database instead.
#[derive(Debug, Default)]
pub struct HttpMetrics {
    /// (route template, status class like "2xx") -> count. Route templates
    /// such as `/v1/jobs/{id}` keep label cardinality bounded.
    requests: Mutex<BTreeMap<(String, &'static str), u64>>,
    pub timeouts: AtomicU64,
}

impl HttpMetrics {
    fn record(&self, route: &str, status: StatusCode) {
        let class = match status.as_u16() {
            100..=199 => "1xx",
            200..=299 => "2xx",
            300..=399 => "3xx",
            400..=499 => "4xx",
            _ => "5xx",
        };
        // A poisoned lock only means another request panicked mid-update;
        // the counts are still usable.
        let mut requests = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        *requests.entry((route.to_owned(), class)).or_insert(0) += 1;
    }

    pub fn requests_snapshot(&self) -> Vec<((String, &'static str), u64)> {
        let requests = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        requests.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }
}

/// Bounds every request by `limits.request_timeout`, then logs and counts
/// it. Never logs bodies. Successful requests log at `debug` because
/// workers poll constantly; 4xx at `info`; 5xx at `warn`.
pub(crate) async fn observe(
    State(state): State<AppState>,
    matched: Option<MatchedPath>,
    request: Request,
    next: Next,
) -> Response {
    let route = matched.map_or_else(|| "unmatched".to_owned(), |m| m.as_str().to_owned());
    let method = request.method().clone();
    let started = Instant::now();
    let timeout = state.limits.request_timeout;

    let response = match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => {
            state.http.timeouts.fetch_add(1, Ordering::Relaxed);
            ApiError::RequestTimeout(timeout.as_millis()).into_response()
        }
    };

    let status = response.status();
    state.http.record(&route, status);
    let elapsed_ms = started.elapsed().as_millis();
    let code = status.as_u16();
    if status.is_server_error() {
        warn!(%method, route, status = code, elapsed_ms, "request failed");
    } else if status.is_client_error() {
        info!(%method, route, status = code, elapsed_ms, "request rejected");
    } else {
        debug!(%method, route, status = code, elapsed_ms, "request");
    }
    response
}
