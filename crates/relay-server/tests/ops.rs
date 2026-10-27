//! Operational behaviour: request timeouts, the shutdown drain deadline,
//! and the metrics endpoint.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use common::{T0, TestServer, mock_request, prompt, raw_pool, register};
use relay_client::RelayClient;
use relay_core::clock::ManualClock;
use relay_server::{AppState, Limits, StoreOptions};
use reqwest::StatusCode;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Hold SQLite's write lock from another connection for `hold`, simulating a
/// long checkpoint or a stuck writer. Resolves once the lock is held.
async fn hold_write_lock(db: &std::path::Path, hold: Duration) -> tokio::task::JoinHandle<()> {
    let raw = raw_pool(db).await;
    let (locked_tx, locked_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let tx = raw.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let _ = locked_tx.send(());
        tokio::time::sleep(hold).await;
        tx.rollback().await.unwrap();
    });
    locked_rx.await.unwrap();
    handle
}

/// Reads have their own pool, so they stay fast even when writes pile up.
/// Under the old single shared pool (8 connections), a dozen writes stuck
/// behind a held lock occupied every connection, and a status read queued
/// behind them for the whole lock duration.
#[tokio::test]
async fn reads_stay_fast_while_writes_queue_behind_a_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("r.db");
    let store = common::open_store(&db, &ManualClock::new(T0), StoreOptions::default()).await;
    let existing = common::submit_job(&store, prompt("already here"), None).await;

    let lock = hold_write_lock(&db, Duration::from_millis(1_500)).await;
    let writes: Vec<_> = (0..12)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                common::submit_job(&store, prompt(&format!("queued {i}")), None).await
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await; // let them all queue up

    let started = Instant::now();
    let job = store.get(existing.id).await.unwrap().unwrap();
    store.ping().await.unwrap();
    let counted: u64 = store
        .count_by_state()
        .await
        .unwrap()
        .iter()
        .map(|(_, n)| n)
        .sum();
    let read_time = started.elapsed();
    assert_eq!(job.id, existing.id);
    assert_eq!(
        counted, 1,
        "queued writes must not be visible before they commit"
    );
    assert!(
        read_time < Duration::from_millis(300),
        "reads waited {read_time:?} behind queued writes"
    );

    lock.await.unwrap();
    for write in writes {
        write.await.unwrap();
    }
    let total: u64 = store
        .count_by_state()
        .await
        .unwrap()
        .iter()
        .map(|(_, n)| n)
        .sum();
    assert_eq!(total, 13, "every queued write commits once the lock frees");
    store.close().await;
}

async fn total_jobs(server: &TestServer) -> u64 {
    server
        .store
        .count_by_state()
        .await
        .unwrap()
        .iter()
        .map(|(_, n)| n)
        .sum()
}

/// A request stuck behind a database lock is cut off with `504
/// request_timeout`. The test then shows *why* that is a 504 and not a 503:
/// the abandoned INSERT can still commit once the lock is released, so a
/// timeout is "outcome unknown". The client's idempotent retry ends with
/// exactly one job anyway.
#[tokio::test]
async fn request_timeouts_are_504_and_ambiguous() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("r.db");
    let clock = ManualClock::new(T0);
    let limits = Limits {
        request_timeout: Duration::from_millis(200),
        ..Limits::default()
    };
    let server = TestServer::start_with_limits(&db, &clock, StoreOptions::default(), limits).await;

    // 1. A raw request times out with a typed, *ambiguous* error.
    let lock = hold_write_lock(&db, Duration::from_millis(600)).await;
    let started = Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{}/v1/jobs", server.url()))
        .json(&json!({"kind": "mock", "payload": {"prompt": "stuck"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(
        started.elapsed() < Duration::from_millis(550),
        "timeout not enforced"
    );
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["error"]["code"], "request_timeout");
    lock.await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_timeout = total_jobs(&server).await;
    println!("jobs created by the request that got a 504: {after_timeout}");
    assert!(after_timeout <= 1);

    // 2. RelayClient treats 504 as ambiguous and retries under one
    //    generated idempotency key: exactly one job, however many 504s.
    let lock = hold_write_lock(&db, Duration::from_millis(500)).await;
    let client = RelayClient::new(server.url()).unwrap();
    let submitted = client
        .submit(&mock_request(prompt("retry me"), None))
        .await
        .unwrap();
    assert!(submitted.created);
    lock.await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        total_jobs(&server).await,
        after_timeout + 1,
        "retries must not duplicate the job"
    );

    let metrics = scrape(&server.url()).await;
    assert!(metrics["relay_http_request_timeouts_total"] >= 2.0);
    server.stop().await;
}

/// On shutdown, in-flight requests get `drain` to finish; a request stuck
/// behind a lock can't hold `serve` up beyond that.
///
/// `serve` returning doesn't close the stuck connection: axum runs each
/// connection in its own task. In the real binary the process exits right
/// after (with `store.close()` also bounded), which ends it. So this test
/// asserts on `serve`'s return time only.
#[tokio::test]
async fn shutdown_drain_deadline_bounds_a_stuck_request() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("r.db");
    let store = common::open_store(&db, &ManualClock::new(T0), StoreOptions::default()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(relay_server::serve(
        listener,
        AppState::new(store.clone(), Limits::default()),
        async {
            let _ = stop_rx.await;
        },
        Duration::from_millis(300),
    ));

    let lock = hold_write_lock(&db, Duration::from_secs(4)).await;
    let stuck = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{url}/v1/jobs"))
            .json(&json!({"kind": "mock", "payload": {"prompt": "x"}}))
            .send()
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await; // request is in flight

    let started = Instant::now();
    stop_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
    let took = started.elapsed();
    assert!(
        took < Duration::from_secs(2),
        "shutdown waited {took:?} for a stuck request"
    );
    assert!(
        took >= Duration::from_millis(250),
        "drained too early: {took:?}"
    );
    assert!(
        !stuck.is_finished(),
        "the request really was still stuck at shutdown"
    );
    stuck.abort();
    lock.abort();
}

/// Parse Prometheus text into `"name{labels}" -> value`.
async fn scrape(base: &str) -> HashMap<String, f64> {
    let response = reqwest::get(format!("{base}/metrics")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response.headers()["content-type"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        content_type.starts_with("text/plain; version=0.0.4"),
        "{content_type}"
    );
    response
        .text()
        .await
        .unwrap()
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|l| {
            let (name, value) = l.rsplit_once(' ').unwrap();
            (name.to_owned(), value.parse().unwrap())
        })
        .collect()
}

#[tokio::test]
async fn metrics_report_jobs_attempts_workers_and_http() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();
    let store = &server.store;
    register(store, "w1", &["mock"]).await;
    let lease = Duration::from_secs(30);

    let ok = client
        .submit(&mock_request(prompt("ok"), None))
        .await
        .unwrap()
        .job
        .id;
    let bad = client
        .submit(&mock_request(prompt("bad"), None))
        .await
        .unwrap()
        .job
        .id;
    let cancelled = client
        .submit(&mock_request(prompt("c"), None))
        .await
        .unwrap()
        .job
        .id;
    store.claim("w1", "c1", lease).await.unwrap().unwrap();
    store.complete(ok, 1, "w1", &json!({})).await.unwrap();
    store.claim("w1", "c2", lease).await.unwrap().unwrap();
    store.fail(bad, 1, "w1", "nope", false).await.unwrap();
    client.cancel(cancelled).await.unwrap();
    let _waiting = client
        .submit(&mock_request(prompt("w"), None))
        .await
        .unwrap();
    let _ = client.status(relay_core::job::JobId::generate()).await; // a 404
    clock.advance_ms(5_000);

    let m = scrape(&server.url()).await;
    for (state, n) in [
        ("queued", 1.0),
        ("running", 0.0),
        ("succeeded", 1.0),
        ("failed", 1.0),
        ("cancelled", 1.0),
    ] {
        assert_eq!(m[&format!("relay_jobs{{state=\"{state}\"}}")], n, "{state}");
    }
    assert_eq!(m["relay_attempts_total{outcome=\"succeeded\"}"], 1.0);
    assert_eq!(m["relay_attempts_total{outcome=\"permanent_error\"}"], 1.0);
    assert_eq!(m["relay_attempts_total{outcome=\"lease_expired\"}"], 0.0);
    assert_eq!(m["relay_queue_capacity"], 10_000.0);
    assert_eq!(m["relay_oldest_eligible_job_age_seconds"], 5.0);
    assert_eq!(m["relay_workers{status=\"live\"}"], 1.0);
    assert_eq!(
        m["relay_http_requests_total{route=\"/v1/jobs\",status=\"2xx\"}"],
        4.0
    );
    assert_eq!(
        m["relay_http_requests_total{route=\"/v1/jobs/{id}\",status=\"4xx\"}"],
        1.0
    );
    assert_eq!(m["relay_http_request_timeouts_total"], 0.0);

    // Workers unseen for three leases are stale.
    clock.advance_ms(91_000);
    let m = scrape(&server.url()).await;
    assert_eq!(m["relay_workers{status=\"stale\"}"], 1.0);
    server.stop().await;
}
