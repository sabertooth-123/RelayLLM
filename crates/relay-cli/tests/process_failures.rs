//! Process-level failure tests: the real `relay` binary, run as separate OS
//! processes, hard-killed (TerminateProcess / SIGKILL: no cleanup code runs)
//! at the worst moment.
//!
//! Timings use real clocks, so the coordinator runs with a 1 s lease and
//! 100–200 ms retry backoff to keep each test to a few seconds.
#![allow(clippy::unwrap_used)]

use std::fs::File;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use relay_client::RelayClient;
use relay_core::api::{JobStatusView, SubmitJobRequest};
use relay_core::job::{AttemptOutcome, JobId, JobState};
use serde_json::{Value, json};

/// Child processes that are killed when the test ends, pass or fail. On a
/// failure their logs are printed, because they are the only evidence.
struct Cluster {
    dir: PathBuf,
    port: u16,
    procs: Vec<(String, Child)>,
}

impl Cluster {
    fn new(dir: &Path) -> Self {
        // Reserve a free port, then release it for the coordinator to bind.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Self {
            dir: dir.to_owned(),
            port,
            procs: Vec::new(),
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn spawn(&mut self, label: &str, args: &[&str]) {
        let log = |ext: &str| File::create(self.dir.join(format!("{label}.{ext}"))).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_relay"))
            .args(args)
            .env("RELAY_LOG", "info")
            .stdout(log("out"))
            .stderr(log("err"))
            .spawn()
            .unwrap();
        self.procs.push((label.to_owned(), child));
    }

    /// Start a coordinator with test-speed timings and wait until it serves.
    async fn start_coordinator(&mut self, label: &str) {
        let db = self.dir.join("relay.db").display().to_string();
        let bind = format!("127.0.0.1:{}", self.port);
        self.spawn(
            label,
            &[
                "coordinator",
                "--db",
                &db,
                "--bind",
                &bind,
                "--lease-secs",
                "1",
                "--retry-base-ms",
                "100",
                "--retry-max-ms",
                "200",
            ],
        );
        let client = RelayClient::with_timeout(self.url(), Duration::from_secs(2)).unwrap();
        poll(
            Duration::from_secs(15),
            "coordinator to become healthy",
            || async { client.health().await.is_ok().then_some(()) },
        )
        .await;
    }

    fn start_worker(&mut self, name: &str) {
        let url = self.url();
        self.spawn(
            name,
            &[
                "--server",
                &url,
                "worker",
                "--name",
                name,
                "--concurrency",
                "1",
            ],
        );
    }

    /// Hard kill: no graceful shutdown, no final report, no cleanup.
    fn kill(&mut self, label: &str) {
        let (_, child) = self.procs.iter_mut().find(|(l, _)| l == label).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, child) in &mut self.procs {
            let _ = child.kill();
            let _ = child.wait();
        }
        if std::thread::panicking() {
            for (label, _) in &self.procs {
                for ext in ["out", "err"] {
                    let path = self.dir.join(format!("{label}.{ext}"));
                    let text = std::fs::read_to_string(&path).unwrap_or_default();
                    let tail: Vec<_> = text.lines().rev().take(25).collect();
                    eprintln!("---- {label}.{ext} (last 25 lines) ----");
                    for line in tail.into_iter().rev() {
                        eprintln!("{line}");
                    }
                }
            }
        }
    }
}

/// Retry `check` until it yields `Some`, or panic after `timeout`.
async fn poll<T, F, Fut>(timeout: Duration, what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn submit(client: &RelayClient, payload: Value) -> JobId {
    client
        .submit(&SubmitJobRequest {
            kind: "mock".into(),
            payload,
            idempotency_key: None,
            max_attempts: Some(3),
            timeout_secs: None,
        })
        .await
        .unwrap()
        .job
        .id
}

async fn wait_for_state(
    client: &RelayClient,
    id: JobId,
    state: JobState,
    within: Duration,
) -> JobStatusView {
    poll(within, &format!("job {id} to be {state}"), || async {
        client
            .status(id)
            .await
            .ok()
            .filter(|job| job.state == state)
    })
    .await
}

fn worker_name(worker_id: &str) -> &str {
    worker_id.split('-').next().unwrap_or_default()
}

/// Requirement 6 + demo steps 4–5: kill a worker while it is processing a
/// job. Its lease expires, and another worker recovers and finishes the job.
#[tokio::test]
async fn killed_worker_job_is_recovered_by_another_worker() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path());
    cluster.start_coordinator("coordinator").await;
    let client = RelayClient::new(cluster.url()).unwrap();

    let id = submit(
        &client,
        json!({"prompt": "survive a crash", "delay_ms": 1_500}),
    )
    .await;
    cluster.start_worker("doomed");
    poll(
        Duration::from_secs(10),
        "worker 'doomed' to start the job",
        || async {
            let job = client.status(id).await.ok()?;
            (job.state == JobState::Running
                && job.worker_id.as_deref().map(worker_name) == Some("doomed"))
            .then_some(())
        },
    )
    .await;

    cluster.kill("doomed");
    let killed_at = Instant::now();
    cluster.start_worker("rescuer");

    let done = wait_for_state(&client, id, JobState::Succeeded, Duration::from_secs(20)).await;
    let recovery = killed_at.elapsed();
    println!("job recovered and finished {recovery:?} after the worker was killed");

    assert_eq!(done.attempt, 2);
    let history = client.attempts(id).await.unwrap();
    let summary: Vec<_> = history
        .iter()
        .map(|a| (worker_name(&a.worker_id), a.outcome))
        .collect();
    assert_eq!(
        summary,
        vec![
            ("doomed", Some(AttemptOutcome::LeaseExpired)),
            ("rescuer", Some(AttemptOutcome::Succeeded)),
        ]
    );
    let result = client.result(id).await.unwrap().result.unwrap();
    assert_eq!(result["completion"], "[mock-llm] crash a survive");
}

/// Requirement 8 + §5.1 #5: hard-kill the coordinator while a job is
/// running, keep it down longer than the lease, restart it. Completed
/// results survive, and the in-flight job finishes as attempt 1 because the
/// startup grace period let its still-running worker reconnect. Without the
/// grace period it would be reaped and run twice.
#[tokio::test]
async fn coordinator_crash_keeps_results_and_in_flight_work() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path());
    cluster.start_coordinator("coordinator-1").await;
    let client = RelayClient::new(cluster.url()).unwrap();
    cluster.start_worker("steady");

    let quick = submit(
        &client,
        json!({"prompt": "done before the crash", "delay_ms": 50}),
    )
    .await;
    let finished =
        wait_for_state(&client, quick, JobState::Succeeded, Duration::from_secs(10)).await;
    let quick_result = client.result(quick).await.unwrap();

    let long = submit(
        &client,
        json!({"prompt": "spans the outage", "delay_ms": 4_000}),
    )
    .await;
    wait_for_state(&client, long, JobState::Running, Duration::from_secs(10)).await;

    cluster.kill("coordinator-1");
    // Down for 2.5 leases: every lease in the database is expired by the
    // time the coordinator returns.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    cluster.start_coordinator("coordinator-2").await;

    // Completed work survived the crash, byte for byte.
    assert_eq!(client.status(quick).await.unwrap(), finished);
    assert_eq!(client.result(quick).await.unwrap(), quick_result);

    let done = wait_for_state(&client, long, JobState::Succeeded, Duration::from_secs(20)).await;
    assert_eq!(
        done.attempt, 1,
        "the in-flight job must not have been re-run"
    );
    let history = client.attempts(long).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].outcome, Some(AttemptOutcome::Succeeded));

    let log = std::fs::read_to_string(dir.path().join("coordinator-2.out")).unwrap();
    assert!(
        log.contains("recovered running jobs from the previous run"),
        "restart did not report grace for in-flight jobs"
    );
}

/// A job that runs 3.5× longer than its lease completes in one attempt
/// because the worker heartbeats.
#[tokio::test]
async fn heartbeats_carry_a_job_past_its_lease() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path());
    cluster.start_coordinator("coordinator").await;
    let client = RelayClient::new(cluster.url()).unwrap();
    cluster.start_worker("patient");

    let id = submit(
        &client,
        json!({"prompt": "slow inference", "delay_ms": 3_500}),
    )
    .await;
    let done = wait_for_state(&client, id, JobState::Succeeded, Duration::from_secs(20)).await;
    assert_eq!(done.attempt, 1);
}
