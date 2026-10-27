//! Dashboard event stream and snapshot: events follow committed transitions
//! only, carry contiguous `seq`s, and the HTTP endpoints serve them.
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::{T0, mock_request, open_store, prompt, register, submit_job};
use relay_core::clock::ManualClock;
use relay_core::events::{DashboardEvent, DashboardSnapshot, EventKind};
use relay_core::job::JobState;
use relay_core::retry::RetryPolicy;
use relay_core::submission::SubmissionLimits;
use relay_server::{AppState, Limits, Store, StoreOptions, router};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

const LEASE: Duration = Duration::from_secs(30);

async fn store(clock: &ManualClock) -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let options = StoreOptions {
        retry: RetryPolicy::IMMEDIATE,
        ..StoreOptions::default()
    };
    let store = open_store(&dir.path().join("r.db"), clock, options).await;
    (dir, store)
}

fn drain(rx: &mut broadcast::Receiver<DashboardEvent>) -> Vec<DashboardEvent> {
    let mut out = Vec::new();
    while let Ok(event) = rx.try_recv() {
        out.push(event);
    }
    out
}

/// `(event type, from, to)` for job events; `(type, None, None)` otherwise.
fn shape(event: &DashboardEvent) -> (&'static str, Option<JobState>, Option<JobState>) {
    let name = match &event.kind {
        EventKind::JobSubmitted { .. } => "submitted",
        EventKind::JobClaimed { .. } => "claimed",
        EventKind::JobSucceeded { .. } => "succeeded",
        EventKind::JobRetryScheduled { .. } => "retry_scheduled",
        EventKind::JobFailed { .. } => "failed",
        EventKind::LeaseExpired { .. } => "lease_expired",
        EventKind::JobCancelled { .. } => "cancelled",
        EventKind::WorkerRegistered { .. } => "worker_registered",
        EventKind::WorkerHeartbeat { .. } => "heartbeat",
    };
    match event.kind.transition() {
        Some((from, job)) => (name, from, Some(job.state)),
        None => (name, None, None),
    }
}

#[tokio::test]
async fn every_committed_transition_emits_one_event_in_order() {
    use JobState::*;
    let clock = ManualClock::new(T0);
    let (_dir, store) = store(&clock).await;
    let (start, mut rx) = store.events().subscribe();
    assert_eq!(start, 0);

    register(&store, "w1", &["mock"]).await;
    let a = submit_job(&store, prompt("a"), Some(2)).await;
    store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    store.heartbeat(a.id, 1, "w1", LEASE).await.unwrap();
    store.fail(a.id, 1, "w1", "boom", true).await.unwrap();
    store.claim("w1", "c2", LEASE).await.unwrap().unwrap();
    store
        .complete(a.id, 2, "w1", &json!({"ok": 1}))
        .await
        .unwrap();

    let b = submit_job(&store, prompt("b"), Some(1)).await;
    store.claim("w1", "c3", LEASE).await.unwrap().unwrap();
    store.fail(b.id, 1, "w1", "bad input", false).await.unwrap();

    let c = submit_job(&store, prompt("c"), None).await;
    store.cancel(c.id).await.unwrap();

    let events = drain(&mut rx);
    let shapes: Vec<_> = events.iter().map(shape).collect();
    assert_eq!(
        shapes,
        vec![
            ("worker_registered", None, None),
            ("submitted", None, Some(Queued)),
            ("claimed", Some(Queued), Some(Running)),
            ("heartbeat", None, None),
            ("retry_scheduled", Some(Running), Some(RetryWaiting)),
            ("claimed", Some(RetryWaiting), Some(Running)),
            ("succeeded", Some(Running), Some(Succeeded)),
            ("submitted", None, Some(Queued)),
            ("claimed", Some(Queued), Some(Running)),
            ("failed", Some(Running), Some(Failed)),
            ("submitted", None, Some(Queued)),
            ("cancelled", Some(Queued), Some(Cancelled)),
        ]
    );
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.seq, i as u64 + 1, "seq must be contiguous");
        assert_eq!(event.epoch, store.events().epoch());
    }
    // The claimed event carries the new attempt (the fencing token) and owner.
    let (_, claimed) = events[5].kind.transition().unwrap();
    assert_eq!(
        (claimed.attempt, claimed.worker_id.as_deref()),
        (2, Some("w1"))
    );
}

#[tokio::test]
async fn rejected_and_replayed_operations_emit_nothing() {
    let clock = ManualClock::new(T0);
    let (_dir, store) = store(&clock).await;
    register(&store, "w1", &["mock"]).await;
    register(&store, "w2", &["mock"]).await;
    let keyed = mock_request(prompt("k"), Some("key-1"))
        .validate(&SubmissionLimits::default())
        .unwrap();
    let job = store.submit(keyed.clone()).await.unwrap().job().clone();
    store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    store.complete(job.id, 1, "w1", &json!(1)).await.unwrap();
    let (_, mut rx) = store.events().subscribe();

    // Idempotent replays, stale reports, a claim replay and an empty claim.
    store.submit(keyed).await.unwrap();
    store.complete(job.id, 1, "w1", &json!(1)).await.unwrap();
    assert!(store.complete(job.id, 1, "w2", &json!(2)).await.is_err());
    assert!(store.heartbeat(job.id, 1, "w1", LEASE).await.is_err());
    assert!(store.claim("w1", "c1", LEASE).await.unwrap().is_none());
    assert!(store.claim("w2", "c9", LEASE).await.unwrap().is_none());
    assert!(store.cancel(job.id).await.is_err());

    assert_eq!(drain(&mut rx), vec![]);
}

#[tokio::test]
async fn lease_expiry_names_the_worker_that_lost_the_job() {
    let clock = ManualClock::new(T0);
    let (_dir, store) = store(&clock).await;
    register(&store, "w1", &["mock"]).await;
    let job = submit_job(&store, prompt("x"), Some(1)).await;
    store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    let (_, mut rx) = store.events().subscribe();

    clock.advance_ms(30_001);
    assert_eq!(store.reap_expired(10).await.unwrap().len(), 1);

    let events = drain(&mut rx);
    assert_eq!(events.len(), 1);
    let EventKind::LeaseExpired {
        from,
        job: summary,
        worker_id,
        reason,
    } = &events[0].kind
    else {
        panic!("expected lease_expired, got {:?}", events[0].kind);
    };
    assert_eq!(*from, Some(JobState::Running));
    assert_eq!(summary.id, job.id);
    assert_eq!(summary.state, JobState::Failed, "last attempt: terminal");
    assert_eq!(summary.worker_id, None);
    assert_eq!(worker_id, "w1");
    assert!(reason.contains("w1"), "{reason}");
    // The event matches what was committed.
    let stored = store.get(job.id).await.unwrap().unwrap().summary();
    assert_eq!(*summary, stored);
}

#[tokio::test]
async fn dev_expiry_only_touches_that_workers_jobs() {
    let clock = ManualClock::new(T0);
    let (_dir, store) = store(&clock).await;
    register(&store, "w1", &["mock"]).await;
    register(&store, "w2", &["mock"]).await;
    let a = submit_job(&store, prompt("a"), Some(3)).await;
    let b = submit_job(&store, prompt("b"), Some(3)).await;
    store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    store.claim("w2", "c2", LEASE).await.unwrap().unwrap();

    assert_eq!(store.expire_worker_leases("w1").await.unwrap(), 1);
    assert_eq!(store.expire_worker_leases("nobody").await.unwrap(), 0);

    let a = store.get(a.id).await.unwrap().unwrap();
    let b = store.get(b.id).await.unwrap().unwrap();
    assert_eq!(a.state, JobState::RetryWaiting);
    assert_eq!(b.state, JobState::Running);
    // w1 is fenced out exactly as after a real expiry.
    assert!(store.complete(a.id, 1, "w1", &json!(1)).await.is_err());
}

#[tokio::test]
async fn dashboard_jobs_lists_active_then_recent_and_hides_secrets() {
    let clock = ManualClock::new(T0);
    let (_dir, store) = store(&clock).await;
    register(&store, "w1", &["mock"]).await;
    let keyed = mock_request(prompt("secret prompt"), Some("secret-key"))
        .validate(&SubmissionLimits::default())
        .unwrap();
    let done = store.submit(keyed).await.unwrap().job().clone();
    store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    store
        .complete(done.id, 1, "w1", &json!({"text": "secret result"}))
        .await
        .unwrap();
    for i in 0..3 {
        submit_job(&store, prompt(&format!("{i}")), None).await;
    }

    let page = store.dashboard_view(2, 10).await.unwrap();
    assert!(page.active_truncated);
    let states: Vec<_> = page.jobs.iter().map(|j| j.state).collect();
    assert_eq!(
        states,
        vec![JobState::Queued, JobState::Queued, JobState::Succeeded]
    );
    assert!(page.jobs[2].has_idempotency_key);

    let json = serde_json::to_string(&page.jobs).unwrap();
    for secret in ["secret-key", "secret prompt", "secret result"] {
        assert!(!json.contains(secret), "{secret} leaked: {json}");
    }

    let counts = &page.counts;
    assert_eq!(counts.len(), JobState::ALL.len(), "zeros included");
    let get = |s| counts.iter().find(|c| c.state == s).unwrap().count;
    assert_eq!(
        (
            get(JobState::Queued),
            get(JobState::Succeeded),
            get(JobState::Failed)
        ),
        (3, 1, 0)
    );
    assert_eq!(page.workers.len(), 1);
}

struct Http {
    url: String,
    client: reqwest::Client,
    store: Store,
    _dir: tempfile::TempDir,
}

async fn serve(dev_controls: bool) -> Http {
    let clock = ManualClock::new(T0);
    let (dir, store) = store(&clock).await;
    let state = AppState::new(store.clone(), Limits::default()).with_dev_controls(dev_controls);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router(state)).await });
    Http {
        url,
        client: reqwest::Client::new(),
        store,
        _dir: dir,
    }
}

/// Read SSE messages until `n` non-comment messages arrived.
async fn read_sse(response: &mut reqwest::Response, n: usize) -> Vec<(String, Value)> {
    let mut buffer = String::new();
    let mut out = Vec::new();
    while out.len() < n {
        let chunk = tokio::time::timeout(Duration::from_secs(8), response.chunk())
            .await
            .expect("SSE message within 8 s")
            .unwrap()
            .expect("stream still open");
        buffer.push_str(std::str::from_utf8(&chunk).unwrap());
        while let Some(end) = buffer.find("\n\n") {
            let message: String = buffer.drain(..end + 2).collect();
            let mut name = String::new();
            let mut data = String::new();
            for line in message.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    name = v.trim().to_owned();
                } else if let Some(v) = line.strip_prefix("data:") {
                    data.push_str(v.trim_start());
                }
            }
            if !name.is_empty() {
                out.push((name, serde_json::from_str(&data).unwrap()));
            }
        }
    }
    out
}

#[tokio::test]
async fn sse_says_hello_then_streams_job_events() {
    let http = serve(false).await;
    submit_job(&http.store, prompt("before"), None).await;

    let mut stream = http
        .client
        .get(format!("{}/v1/events", http.url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        stream.headers()["content-type"].to_str().unwrap(),
        "text/event-stream"
    );
    let hello = read_sse(&mut stream, 1).await;
    assert_eq!(hello[0].0, "hello");
    assert_eq!(hello[0].1["seq"], 1);
    assert_eq!(hello[0].1["epoch"], http.store.events().epoch());

    let job = submit_job(&http.store, prompt("after"), None).await;
    let next = read_sse(&mut stream, 1).await;
    assert_eq!(next[0].0, "job");
    assert_eq!(next[0].1["type"], "job_submitted");
    assert_eq!(next[0].1["seq"], 2);
    assert_eq!(next[0].1["job"]["id"], job.id.to_string());

    // The snapshot taken now covers both events.
    let snapshot: DashboardSnapshot = http
        .client
        .get(format!("{}/v1/dashboard/snapshot", http.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snapshot.seq, 2);
    assert_eq!(snapshot.jobs.len(), 2);
    assert_eq!(snapshot.recent_events.len(), 2);
    assert_eq!(snapshot.lease_ms, 30_000);
    assert!(!snapshot.dev_controls);
}

#[tokio::test]
async fn an_idle_stream_sends_visible_pings() {
    let http = serve(false).await;
    let mut stream = http
        .client
        .get(format!("{}/v1/events", http.url))
        .send()
        .await
        .unwrap();
    // hello, then (with nothing happening) a named ping within ~5 s; SSE
    // comments wouldn't count, browsers can't see them.
    let messages = read_sse(&mut stream, 2).await;
    assert_eq!(messages[0].0, "hello");
    assert_eq!(messages[1].0, "ping");
}

#[tokio::test]
async fn dev_controls_are_absent_unless_enabled() {
    let off = serve(false).await;
    let status = off
        .client
        .post(format!("{}/v1/dev/workers/w1/expire-leases", off.url))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, 404);

    let on = serve(true).await;
    register(&on.store, "w1", &["mock"]).await;
    submit_job(&on.store, prompt("x"), None).await;
    on.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    let body: Value = on
        .client
        .post(format!("{}/v1/dev/workers/w1/expire-leases", on.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body, json!({"worker_id": "w1", "expired_jobs": 1}));
}
