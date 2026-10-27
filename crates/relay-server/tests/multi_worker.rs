//! End-to-end: real `Worker` loops against an in-process coordinator.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::{
    RunningWorker, T0, TestServer, fast_worker_config, lose_first_response, prompt,
    wait_until_terminal,
};
use relay_client::RelayClient;
use relay_core::api::SubmitJobRequest;
use relay_core::clock::ManualClock;
use relay_core::job::{AttemptOutcome, JobId, JobState};
use relay_core::retry::RetryPolicy;
use relay_server::{Limits, StoreOptions};
use relay_worker::{
    BoxFuture, HandlerError, JobContext, JobHandler, MockLlmHandler, Worker, WorkerStats,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

/// Wraps a handler and records how many runs overlap, across all workers
/// sharing the same counters.
#[derive(Clone)]
struct Tracking {
    inner: Arc<MockLlmHandler>,
    current: Arc<AtomicU64>,
    max: Arc<AtomicU64>,
}

impl JobHandler for Tracking {
    fn run(&self, ctx: JobContext) -> BoxFuture<'_, Result<Value, HandlerError>> {
        Box::pin(async move {
            let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.max.fetch_max(now, Ordering::SeqCst);
            let outcome = self.inner.run(ctx).await;
            self.current.fetch_sub(1, Ordering::SeqCst);
            outcome
        })
    }
}

fn mock_worker(url: &str, name: &str, concurrency: u32) -> Worker {
    Worker::new(
        RelayClient::new(url).unwrap(),
        fast_worker_config(name, concurrency),
    )
    .with_handler("mock", MockLlmHandler::new(Duration::ZERO))
}

async fn submit(client: &RelayClient, payload: Value, max_attempts: Option<u32>) -> JobId {
    client
        .submit(&SubmitJobRequest {
            kind: "mock".into(),
            payload,
            idempotency_key: None,
            max_attempts,
            timeout_secs: None,
        })
        .await
        .unwrap()
        .job
        .id
}

/// Requirement 4: multiple workers process different jobs concurrently, and
/// each stays within its own concurrency bound.
#[tokio::test]
async fn three_workers_share_the_queue_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();

    let mut ids = Vec::new();
    for i in 0..24 {
        ids.push(
            submit(
                &client,
                json!({"prompt": format!("job {i}"), "delay_ms": 60}),
                None,
            )
            .await,
        );
    }

    let tracking = Tracking {
        inner: Arc::new(MockLlmHandler::new(Duration::ZERO)),
        current: Arc::default(),
        max: Arc::default(),
    };
    let workers: Vec<_> = (0..3)
        .map(|i| {
            RunningWorker::spawn(
                Worker::new(
                    RelayClient::new(server.url()).unwrap(),
                    fast_worker_config(&format!("w{i}"), 2),
                )
                .with_handler("mock", tracking.clone()),
            )
        })
        .collect();

    let jobs = wait_until_terminal(&server.store, &ids, Duration::from_secs(20)).await;
    let mut who_ran = HashSet::new();
    for job in &jobs {
        assert_eq!(job.state, JobState::Succeeded, "{job:?}");
        assert_eq!(job.attempt, 1, "no job should need a second attempt");
        let expected = MockLlmHandler::new(Duration::ZERO)
            .run(JobContext {
                job_id: job.id,
                attempt: 1,
                kind: "mock".into(),
                payload: job.payload.clone(),
            })
            .await
            .unwrap();
        assert_eq!(job.result.as_ref(), Some(&expected));
        who_ran.insert(
            server.store.attempts(job.id).await.unwrap()[0]
                .worker_id
                .clone(),
        );
    }

    assert!(who_ran.len() >= 2, "work was not spread: {who_ran:?}");
    let overall = tracking.max.load(Ordering::SeqCst);
    assert!(overall >= 2, "jobs never overlapped (max {overall})");
    assert!(
        overall <= 6,
        "more jobs than total slots ran at once: {overall}"
    );
    for worker in &workers {
        let max = WorkerStats::get(&worker.stats.max_in_flight);
        assert!(
            max <= 2,
            "worker {} exceeded its concurrency: {max}",
            worker.id
        );
    }
    let done: u64 = workers
        .iter()
        .map(|w| WorkerStats::get(&w.stats.jobs_succeeded))
        .sum();
    assert_eq!(done, 24);

    for worker in workers {
        worker.stop().await;
    }
    server.stop().await;
}

/// Requirements 9 and 10, end to end: the *coordinator* decides retries.
#[tokio::test]
async fn handler_failures_follow_the_coordinator_retry_policy() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    // This test is about *which* outcome each failure leads to, not timing;
    // the frozen manual clock would otherwise never end the backoff.
    let options = StoreOptions {
        retry: RetryPolicy::IMMEDIATE,
        ..StoreOptions::default()
    };
    let server = TestServer::start(&dir.path().join("r.db"), &clock, options).await;
    let client = RelayClient::new(server.url()).unwrap();

    let recovers = submit(&client, json!({"prompt": "a", "fail_attempts": 1}), Some(3)).await;
    let exhausts = submit(&client, json!({"prompt": "b", "fail_attempts": 9}), Some(2)).await;
    let permanent = submit(
        &client,
        json!({"prompt": "c", "fail_attempts": 1, "fail_kind": "permanent"}),
        Some(5),
    )
    .await;
    let invalid = submit(&client, json!({"no_prompt": true}), Some(5)).await;

    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 2));
    let ids = [recovers, exhausts, permanent, invalid];
    let jobs = wait_until_terminal(&server.store, &ids, Duration::from_secs(20)).await;

    let summary: Vec<_> = jobs.iter().map(|j| (j.state, j.attempt)).collect();
    assert_eq!(
        summary,
        vec![
            (JobState::Succeeded, 2),
            (JobState::Failed, 2),
            (JobState::Failed, 1),
            (JobState::Failed, 1),
        ]
    );
    let history = server.store.attempts(recovers).await.unwrap();
    assert_eq!(history[0].outcome, Some(AttemptOutcome::RetryableError));
    assert_eq!(history[1].outcome, Some(AttemptOutcome::Succeeded));
    assert!(
        jobs[3]
            .last_error
            .as_deref()
            .unwrap()
            .contains("invalid mock payload")
    );

    worker.stop().await;
    server.stop().await;
}

/// §5.1 #1 end to end: the coordinator commits a claim, the response is
/// lost. The worker retries with the same claim id and runs the job as
/// attempt 1. Nothing is stranded and no attempt is burned.
#[tokio::test]
async fn lost_claim_response_does_not_strand_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = TestServer::start_custom(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        Limits::default(),
        listener,
        |router| lose_first_response(router, "/claims"),
    )
    .await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = submit(&client, prompt("only job"), Some(1)).await;

    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 1));
    let job = wait_until_terminal(&server.store, &[id], Duration::from_secs(10)).await;
    assert_eq!((job[0].state, job[0].attempt), (JobState::Succeeded, 1));
    assert_eq!(server.store.attempts(id).await.unwrap().len(), 1);
    assert!(
        WorkerStats::get(&worker.stats.coordinator_errors) >= 1,
        "fault was not injected"
    );

    worker.stop().await;
    server.stop().await;
}

/// §5.1 #2 end to end: the completion commits, the response is lost, the
/// worker retries and gets a 200, not a false "stale" rejection.
#[tokio::test]
async fn lost_completion_response_is_not_reported_as_stale() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = TestServer::start_custom(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        Limits::default(),
        listener,
        |router| lose_first_response(router, "/complete"),
    )
    .await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = submit(&client, prompt("x"), None).await;

    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 1));
    wait_until_terminal(&server.store, &[id], Duration::from_secs(10)).await;
    // The retry lands after the job is already terminal; wait for it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while WorkerStats::get(&worker.stats.jobs_succeeded) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "retried report never landed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(WorkerStats::get(&worker.stats.reports_stale), 0);
    assert_eq!(WorkerStats::get(&worker.stats.reports_abandoned), 0);

    worker.stop().await;
    server.stop().await;
}

/// Requirement 13: with the coordinator down, the worker backs off in a
/// single loop. It claims nothing, runs nothing, spawns no tasks, and still
/// shuts down promptly.
#[tokio::test]
async fn unavailable_coordinator_causes_backoff_not_task_growth() {
    // A port with nothing listening.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);

    let worker = RunningWorker::spawn(mock_worker(&url, "w", 8));
    // Long enough for at least one refused connection on Windows, where
    // connecting to a closed localhost port takes about 2 s (Linux: instant).
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    let tries = WorkerStats::get(&worker.stats.registrations);
    assert!(tries >= 1, "worker never retried registration");
    // Backoff starts at 20 ms and caps at 100 ms with jitter in [d/2, d],
    // so 2.5 s allows at most ~50 tries even when refusal is instant.
    assert!(
        tries <= 60,
        "worker hammered the dead coordinator: {tries} tries"
    );
    assert_eq!(WorkerStats::get(&worker.stats.claims), 0);
    assert_eq!(WorkerStats::get(&worker.stats.max_in_flight), 0);

    let stop_started = std::time::Instant::now();
    worker.stop().await;
    assert!(stop_started.elapsed() < Duration::from_secs(1));
}

/// The worker starts before the coordinator, then picks up work once the
/// coordinator appears.
#[tokio::test]
async fn worker_connects_once_the_coordinator_comes_up() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let worker = RunningWorker::spawn(mock_worker(&format!("http://{addr}"), "early", 1));
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_custom(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        Limits::default(),
        TcpListener::bind(addr).await.unwrap(),
        |router| router,
    )
    .await;
    let id = submit(
        &RelayClient::new(server.url()).unwrap(),
        prompt("late"),
        None,
    )
    .await;
    let job = wait_until_terminal(&server.store, &[id], Duration::from_secs(10)).await;
    assert_eq!(job[0].state, JobState::Succeeded);

    worker.stop().await;
    server.stop().await;
}

/// Graceful shutdown: stop claiming, but finish and report in-flight jobs.
#[tokio::test]
async fn shutdown_drains_in_flight_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();
    let a = submit(&client, json!({"prompt": "a", "delay_ms": 400}), None).await;
    let b = submit(&client, json!({"prompt": "b", "delay_ms": 400}), None).await;

    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 2));
    while WorkerStats::get(&worker.stats.claims) < 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    worker.stop().await; // returns only after both reports

    for id in [a, b] {
        let job = server.store.get(id).await.unwrap().unwrap();
        assert_eq!(
            job.state,
            JobState::Succeeded,
            "in-flight job was abandoned"
        );
    }
    server.stop().await;
}

/// Advance the manual clock in step with real time and run the reaper, like
/// a production coordinator would. Stops when the returned handle is aborted.
fn drive_clock_and_reaper(server: &TestServer, clock: &ManualClock) -> tokio::task::JoinHandle<()> {
    let (store, clock) = (server.store.clone(), clock.clone());
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            clock.advance_ms(25);
            store.reap_expired(100).await.unwrap();
        }
    })
}

/// Heartbeats keep a job alive for longer than its lease. The job takes
/// 900 ms; the lease is 300 ms; the reaper runs every 25 ms.
#[tokio::test]
async fn heartbeats_keep_a_long_job_alive() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let lease = Duration::from_millis(300);
    let server = TestServer::start_with_lease(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        lease,
    )
    .await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = submit(&client, json!({"prompt": "long", "delay_ms": 900}), Some(1)).await;

    let time = drive_clock_and_reaper(&server, &clock);
    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 1));
    let job = wait_until_terminal(&server.store, &[id], Duration::from_secs(10)).await;
    time.abort();

    assert_eq!(
        (job[0].state, job[0].attempt),
        (JobState::Succeeded, 1),
        "{job:?}"
    );
    assert!(
        WorkerStats::get(&worker.stats.heartbeats) >= 3,
        "expected several heartbeats, got {}",
        WorkerStats::get(&worker.stats.heartbeats)
    );
    worker.stop().await;
    server.stop().await;
}

/// When the coordinator reaps a job, the worker learns it from its next
/// heartbeat (`409`), aborts the handler at once instead of finishing the
/// 10 s "inference", and sends no report.
#[tokio::test]
async fn worker_aborts_the_handler_when_its_lease_is_lost() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start_with_lease(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        Duration::from_millis(300),
    )
    .await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = submit(
        &client,
        json!({"prompt": "doomed", "delay_ms": 10_000}),
        Some(1),
    )
    .await;

    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 1));
    while WorkerStats::get(&worker.stats.claims) == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Simulate the worker going silent long enough to be reaped.
    clock.advance_ms(1_000);
    let reaped = server.store.reap_expired(100).await.unwrap();
    assert_eq!(reaped.len(), 1);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while WorkerStats::get(&worker.stats.leases_lost) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker never noticed it lost the lease"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Aborted, not left running for 10 s.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(WorkerStats::get(&worker.stats.in_flight), 0);
    assert_eq!(
        WorkerStats::get(&worker.stats.reports_stale),
        0,
        "no pointless report"
    );

    let job = server.store.get(id).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Failed);
    assert!(job.last_error.unwrap().contains("lease expired"));
    worker.stop().await;
    server.stop().await;
}

struct Oversized(usize);

impl JobHandler for Oversized {
    fn run(&self, _ctx: JobContext) -> BoxFuture<'_, Result<Value, HandlerError>> {
        let size = self.0;
        Box::pin(async move { Ok(json!({"completion": "x".repeat(size)})) })
    }
}

/// A result the coordinator refuses is a definite rejection: over the 128 KiB
/// result limit (422) or over the 256 KiB body limit (413). Re-running would
/// produce it again, so the worker fails the job permanently instead of
/// leaving it to lease expiry and pointless retries.
#[tokio::test]
async fn an_oversized_result_fails_the_job_instead_of_looping() {
    for size in [200 * 1024, 400 * 1024] {
        let dir = tempfile::tempdir().unwrap();
        let clock = ManualClock::new(T0);
        let server =
            TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
        let client = RelayClient::new(server.url()).unwrap();
        let id = submit(&client, json!({"prompt": "write a novel"}), Some(3)).await;

        let worker = RunningWorker::spawn(
            Worker::new(
                RelayClient::new(server.url()).unwrap(),
                fast_worker_config("w", 1),
            )
            .with_handler("mock", Oversized(size)),
        );
        let job = wait_until_terminal(&server.store, &[id], Duration::from_secs(10)).await;
        assert_eq!(
            (job[0].state, job[0].attempt),
            (JobState::Failed, 1),
            "size {size}"
        );
        assert!(
            job[0]
                .last_error
                .as_deref()
                .unwrap()
                .contains("coordinator rejected the result"),
            "size {size}: {:?}",
            job[0].last_error
        );
        assert_eq!(WorkerStats::get(&worker.stats.reports_abandoned), 0);
        worker.stop().await;
        server.stop().await;
    }
}

/// Cancelling a running job stops the worker's handler at its next
/// heartbeat. No result is reported, and the job stays cancelled.
#[tokio::test]
async fn cancelling_a_running_job_aborts_the_worker_handler() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start_with_lease(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        Duration::from_millis(300),
    )
    .await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = submit(
        &client,
        json!({"prompt": "never mind", "delay_ms": 10_000}),
        None,
    )
    .await;

    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 1));
    while WorkerStats::get(&worker.stats.claims) == 0 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    client.cancel(id).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while WorkerStats::get(&worker.stats.leases_lost) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker kept running a cancelled job"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(WorkerStats::get(&worker.stats.in_flight), 0);
    assert_eq!(WorkerStats::get(&worker.stats.reports_stale), 0);
    assert_eq!(
        server.store.get(id).await.unwrap().unwrap().state,
        JobState::Cancelled
    );
    worker.stop().await;
    server.stop().await;
}

/// The worker enforces `timeout_secs` and aborts the handler; the timeout is
/// a retryable failure, judged by the coordinator's attempt budget.
#[tokio::test]
async fn handler_timeout_fails_the_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = client
        .submit(&SubmitJobRequest {
            kind: "mock".into(),
            payload: json!({"prompt": "slow", "delay_ms": 10_000}),
            idempotency_key: None,
            max_attempts: Some(1),
            timeout_secs: Some(1),
        })
        .await
        .unwrap()
        .job
        .id;

    let worker = RunningWorker::spawn(mock_worker(&server.url(), "w", 1));
    let job = wait_until_terminal(&server.store, &[id], Duration::from_secs(5)).await;
    assert_eq!(job[0].state, JobState::Failed);
    assert!(job[0].last_error.as_deref().unwrap().contains("timed out"));

    worker.stop().await;
    server.stop().await;
}
