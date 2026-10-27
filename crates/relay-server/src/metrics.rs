//! `GET /metrics` in the Prometheus text exposition format.
//!
//! Job and attempt counts are read from the database, so they are correct
//! across restarts and count every coordinator's work on this file.
//! `job_attempts` rows are never deleted, so `relay_attempts_total` is a true
//! monotonic counter. HTTP counters are in-process and reset on restart; their
//! HELP text says so.

use std::fmt::Write as _;
use std::sync::atomic::Ordering;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use relay_core::job::{AttemptOutcome, JobState};

use crate::api::AppState;
use crate::error::ApiError;

pub(crate) async fn metrics(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    // A worker counts as live if it was seen within three leases.
    let live_window = state.lease.saturating_mul(3);
    let snapshot = state.store.metrics_snapshot(live_window).await?;
    let mut out = String::new();

    gauge_header(&mut out, "relay_jobs", "Jobs by current state.");
    for job_state in JobState::ALL {
        let n = snapshot
            .jobs_by_state
            .iter()
            .find(|(s, _)| *s == job_state)
            .map_or(0, |(_, n)| *n);
        let _ = writeln!(out, "relay_jobs{{state=\"{job_state}\"}} {n}");
    }

    gauge_header(
        &mut out,
        "relay_queue_capacity",
        "Maximum active (queued, running, retry_waiting) jobs.",
    );
    let _ = writeln!(out, "relay_queue_capacity {}", snapshot.capacity);

    gauge_header(
        &mut out,
        "relay_oldest_eligible_job_age_seconds",
        "How long the oldest claimable job has been waiting. Growing values mean workers can't keep up.",
    );
    let _ = writeln!(
        out,
        "relay_oldest_eligible_job_age_seconds {:.3}",
        snapshot.oldest_eligible_age_ms as f64 / 1000.0
    );

    let _ = writeln!(
        out,
        "# HELP relay_attempts_total Finished attempts by outcome (durable)."
    );
    let _ = writeln!(out, "# TYPE relay_attempts_total counter");
    for outcome in AttemptOutcome::ALL {
        let n = snapshot
            .attempts_by_outcome
            .iter()
            .find(|(o, _)| *o == outcome)
            .map_or(0, |(_, n)| *n);
        let _ = writeln!(out, "relay_attempts_total{{outcome=\"{outcome}\"}} {n}");
    }

    gauge_header(
        &mut out,
        "relay_workers",
        "Registered workers, by whether they were seen within 3 leases.",
    );
    let _ = writeln!(
        out,
        "relay_workers{{status=\"live\"}} {}",
        snapshot.workers_live
    );
    let _ = writeln!(
        out,
        "relay_workers{{status=\"stale\"}} {}",
        snapshot.workers_total - snapshot.workers_live
    );

    let _ = writeln!(
        out,
        "# HELP relay_http_requests_total HTTP requests by route and status class (resets on restart)."
    );
    let _ = writeln!(out, "# TYPE relay_http_requests_total counter");
    for ((route, class), n) in state.http.requests_snapshot() {
        let _ = writeln!(
            out,
            "relay_http_requests_total{{route=\"{}\",status=\"{class}\"}} {n}",
            escape(&route)
        );
    }
    let _ = writeln!(
        out,
        "# HELP relay_http_request_timeouts_total Requests cut off by the request timeout (resets on restart)."
    );
    let _ = writeln!(out, "# TYPE relay_http_request_timeouts_total counter");
    let _ = writeln!(
        out,
        "relay_http_request_timeouts_total {}",
        state.http.timeouts.load(Ordering::Relaxed)
    );

    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    ))
}

fn gauge_header(out: &mut String, name: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
}

/// Label values escape backslash, double quote and newline.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
