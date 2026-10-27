//! Read-only endpoints for the live dashboard, plus one dev-only control.
//!
//! Protocol (see `docs/api.md`):
//!
//! 1. Open `GET /v1/events`. The first message is `hello` with the epoch and
//!    the `seq` of the last event before the subscription; `job`/`worker`
//!    events follow with consecutive `seq`s.
//! 2. After `hello`, fetch `GET /v1/dashboard/snapshot`. Its `seq` is taken
//!    before its reads, so every event up to it is already reflected; later
//!    events may be too, and are applied by checking each transition's
//!    `from` against the state held.
//! 3. A `resync` message, a gap in `seq`, a new epoch, or a transition that
//!    doesn't fit means: discard and start again at 1.

use std::convert::Infallible;
use std::time::Duration;

use axum::Json;
use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::{self, Stream};
use relay_core::events::{DashboardEvent, DashboardSnapshot};
use serde::Serialize;
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

use crate::api::AppState;
use crate::error::ApiError;

/// Active jobs included in a snapshot. More than this and the snapshot says
/// it was truncated; the counts stay exact.
pub const SNAPSHOT_ACTIVE_JOBS: u32 = 500;
/// Most recently finished jobs included in a snapshot.
pub const SNAPSHOT_RECENT_JOBS: u32 = 100;

pub(crate) async fn snapshot(
    State(state): State<AppState>,
) -> Result<Json<DashboardSnapshot>, ApiError> {
    let events = state.store.events();
    // Before any read: see the module docs.
    let seq = events.current_seq();
    let view = state
        .store
        .dashboard_view(SNAPSHOT_ACTIVE_JOBS, SNAPSHOT_RECENT_JOBS)
        .await?;
    let recent_events = events
        .recent()
        .into_iter()
        .filter(|e| e.seq <= seq)
        .collect();
    Ok(Json(DashboardSnapshot {
        epoch: events.epoch().to_owned(),
        seq,
        server_time_ms: state.store.now_ms(),
        lease_ms: u64::try_from(state.lease.as_millis()).unwrap_or(u64::MAX),
        max_active_jobs: state.store.max_active_jobs(),
        counts: view.counts,
        workers: view.workers,
        jobs: view.jobs,
        active_jobs_truncated: view.active_truncated,
        recent_events,
        dev_controls: state.dev_controls,
    }))
}

#[derive(Serialize)]
struct Hello<'a> {
    epoch: &'a str,
    seq: u64,
}

enum Phase {
    Hello(u64),
    Streaming,
    Done,
}

struct StreamState {
    phase: Phase,
    epoch: String,
    events: broadcast::Receiver<DashboardEvent>,
    closing: watch::Receiver<bool>,
    ping: tokio::time::Interval,
}

/// A named `ping` message this often. SSE comments (axum's keep-alive) are
/// invisible to browser `EventSource`s, and a proxy that loses the upstream
/// can leave the client's connection open forever; a visible ping lets the
/// client notice silence and reconnect.
pub const PING_INTERVAL: Duration = Duration::from_secs(5);

/// Server-sent events. The request timeout doesn't apply to the body, so
/// the stream lives until the client leaves, it lags, or the coordinator
/// starts shutting down.
pub(crate) async fn events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let bus = state.store.events();
    let (seq, events) = bus.subscribe();
    let initial = StreamState {
        phase: Phase::Hello(seq),
        epoch: bus.epoch().to_owned(),
        events,
        closing: state.closing.subscribe(),
        ping: tokio::time::interval_at(tokio::time::Instant::now() + PING_INTERVAL, PING_INTERVAL),
    };
    let stream = stream::unfold(initial, |mut s| async move {
        match s.phase {
            Phase::Done => None,
            Phase::Hello(seq) => {
                s.phase = Phase::Streaming;
                let event = json_event(
                    "hello",
                    &Hello {
                        epoch: &s.epoch,
                        seq,
                    },
                );
                Some((Ok(event), s))
            }
            Phase::Streaming => {
                let next = tokio::select! {
                    next = s.events.recv() => Some(next),
                    _ = s.ping.tick() => None,
                    _ = s.closing.wait_for(|closing| *closing) => return None,
                };
                let Some(next) = next else {
                    return Some((Ok(Event::default().event("ping").data("{}")), s));
                };
                match next {
                    Ok(event) => {
                        let name = if event.kind.transition().is_some() {
                            "job"
                        } else {
                            "worker"
                        };
                        Some((Ok(json_event(name, &event)), s))
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        // Events were dropped: the client must rebuild from a
                        // snapshot. End the stream so the reconnect starts
                        // the protocol from scratch.
                        warn!(missed, "dashboard subscriber lagged; asking it to resync");
                        s.phase = Phase::Done;
                        Some((Ok(Event::default().event("resync").data("lagged")), s))
                    }
                    Err(broadcast::error::RecvError::Closed) => None,
                }
            }
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn json_event<T: Serialize>(name: &str, value: &T) -> Event {
    // Serializing our own plain structs can't fail; fall back to a resync
    // rather than panicking if it somehow does.
    match serde_json::to_string(value) {
        Ok(json) => Event::default().event(name).data(json),
        Err(_) => Event::default().event("resync").data("serialization"),
    }
}

#[derive(Serialize)]
pub(crate) struct ExpireLeasesResponse {
    worker_id: String,
    expired_jobs: usize,
}

/// `POST /v1/dev/workers/{worker_id}/expire-leases`. Registered only with
/// `--dev-controls`, which the coordinator refuses on a non-loopback bind.
pub(crate) async fn expire_leases(
    State(state): State<AppState>,
    Path(worker_id): Path<String>,
) -> Result<Json<ExpireLeasesResponse>, ApiError> {
    let expired_jobs = state.store.expire_worker_leases(&worker_id).await?;
    info!(
        worker_id,
        expired_jobs, "dev control: expired worker leases"
    );
    Ok(Json(ExpireLeasesResponse {
        worker_id,
        expired_jobs,
    }))
}
