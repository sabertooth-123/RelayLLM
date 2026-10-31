//! `relay demo`: the end-to-end failure-recovery demonstration.
//!
//! Spawns a real cluster (1 coordinator + 3 workers, separate OS processes),
//! then walks through the ten required steps. Every claim the demo makes is a
//! *check*; any failed check makes the command exit 1. So the demo is also an
//! end-to-end test (`tests/demo.rs` runs it).

use std::collections::BTreeMap;
use std::fmt::Display;
use std::future::Future;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Args;
use relay_client::RelayClient;
use relay_core::api::{CompleteRequest, HeartbeatRequest, RegisterWorkerRequest, SubmitJobRequest};
use relay_core::job::{AttemptOutcome, JobId, JobState};
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::cluster::Cluster;

#[derive(Debug, Args)]
pub struct DemoArgs {
    /// Number of mock jobs to push through the cluster.
    #[arg(long, default_value_t = 100)]
    jobs: usize,

    /// Directory for the demo database and process logs (default: a new
    /// directory under the system temp dir).
    #[arg(long)]
    workdir: Option<PathBuf>,
}

const WORKERS: [&str; 3] = ["w1", "w2", "w3"];

/// Every claim a demo makes is a check; any failure makes it exit 1.
pub(crate) struct Report {
    steps: u32,
    pub(crate) passed: usize,
    pub(crate) failed: Vec<String>,
}

impl Report {
    pub(crate) fn new(steps: u32) -> Self {
        Self {
            steps,
            passed: 0,
            failed: Vec::new(),
        }
    }

    pub(crate) fn step(&self, n: u32, title: &str) {
        println!("\n[{n}/{}] {title}", self.steps);
    }

    pub(crate) fn check(&mut self, ok: bool, what: impl Display) {
        if ok {
            println!("  [ok]   {what}");
            self.passed += 1;
        } else {
            println!("  [FAIL] {what}");
            self.failed.push(what.to_string());
        }
    }

    pub(crate) fn note(&self, what: impl Display) {
        println!("         {what}");
    }
}

/// Retry `probe` every 100 ms until it yields `Some`, or give up.
pub(crate) async fn wait_for<T, F, Fut>(timeout: Duration, mut probe: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe().await {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One value from the coordinator's Prometheus text, e.g.
/// `relay_jobs{state="succeeded"}`.
async fn metric(client: &RelayClient, name: &str) -> Option<f64> {
    let text = reqwest_get(&format!("{}/metrics", client.base_url())).await?;
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
}

async fn reqwest_get(url: &str) -> Option<String> {
    let response = reqwest::get(url).await.ok()?;
    response.text().await.ok()
}

fn mock(prompt: impl Into<String>, extra: Value) -> SubmitJobRequest {
    let mut payload = json!({ "prompt": prompt.into() });
    if let (Some(payload), Value::Object(extra)) = (payload.as_object_mut(), extra) {
        payload.extend(extra);
    }
    SubmitJobRequest {
        kind: "mock".into(),
        payload,
        idempotency_key: None,
        max_attempts: Some(5),
        timeout_secs: None,
    }
}

fn worker_name(worker_id: &str) -> &str {
    worker_id.split('-').next().unwrap_or(worker_id)
}

pub async fn run(args: DemoArgs) -> anyhow::Result<ExitCode> {
    let dir = args
        .workdir
        .unwrap_or_else(|| std::env::temp_dir().join(format!("relay-demo-{}", JobId::generate())));
    println!("RelayLLM end-to-end demo");
    println!(
        "working directory (database + process logs): {}",
        dir.display()
    );
    let started = Instant::now();
    let mut r = Report::new(10);

    // Short lease and backoff so failures recover in seconds, not minutes.
    let mut cluster = Cluster::new(
        &dir,
        &[
            "--lease-secs",
            "2",
            "--retry-base-ms",
            "200",
            "--retry-max-ms",
            "1000",
            "--request-timeout-secs",
            "1",
        ],
    )?;

    // ---------------------------------------------------------------- 1
    r.step(
        1,
        "Start one coordinator and three workers (separate processes)",
    );
    cluster.start_coordinator().await?;
    for name in WORKERS {
        cluster.start_worker(name, &["--concurrency", "2", "--mock-delay-ms", "300"])?;
    }
    let client = RelayClient::new(cluster.url())?;
    let registered = wait_for(Duration::from_secs(15), || async {
        let workers = client.list_workers().await.ok()?;
        (workers.len() == WORKERS.len()).then_some(workers.len())
    })
    .await;
    r.check(
        registered == Some(3),
        format!(
            "coordinator at {} with 3 registered workers (lease 2 s)",
            cluster.url()
        ),
    );

    // ---------------------------------------------------------------- 2
    r.step(2, &format!("Submit {} mock LLM jobs", args.jobs));
    // The marker goes first so it is claimed right away; step 4 kills the
    // worker that runs it.
    let marker = client
        .submit(&mock(
            "the job whose worker we will kill",
            json!({"delay_ms": 4000}),
        ))
        .await?
        .job
        .id;
    let mut submits = JoinSet::new();
    for i in 0..args.jobs {
        let client = client.clone();
        submits.spawn(async move {
            client
                .submit(&mock(format!("demo job {i}"), json!({})))
                .await
        });
    }
    let mut ids = Vec::with_capacity(args.jobs);
    while let Some(done) = submits.join_next().await {
        ids.push(done??.job.id);
    }
    r.check(
        ids.len() == args.jobs,
        format!("{} jobs accepted (plus 1 long marker job)", ids.len()),
    );

    // ---------------------------------------------------------------- 3
    r.step(3, "Workers process jobs concurrently");
    let (mut max_running, mut max_busy, mut example) = (0, 0, String::new());
    let sample_until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < sample_until {
        if let Ok(workers) = client.list_workers().await {
            let running: u64 = workers.iter().map(|w| w.running_jobs).sum();
            let busy = workers.iter().filter(|w| w.running_jobs > 0).count();
            if busy > max_busy || (busy == max_busy && running > max_running) {
                example = workers
                    .iter()
                    .map(|w| format!("{}: {} running", worker_name(&w.id), w.running_jobs))
                    .collect::<Vec<_>>()
                    .join(", ");
            }
            max_running = max_running.max(running);
            max_busy = max_busy.max(busy);
            if max_busy == WORKERS.len() && max_running >= 4 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    r.check(
        max_busy >= 2 && max_running >= 2,
        format!("up to {max_running} jobs running at once across {max_busy} workers"),
    );
    r.note(format!("sample: {example}"));

    // ---------------------------------------------------------------- 4
    r.step(4, "Kill a worker while it is processing a job");
    let owner = wait_for(Duration::from_secs(10), || async {
        let job = client.status(marker).await.ok()?;
        (job.state == JobState::Running).then_some(job.worker_id?)
    })
    .await
    .context("marker job never started running")?;
    let victim = worker_name(&owner).to_owned();
    cluster.kill_worker(&victim)?;
    let killed_at = Instant::now();
    r.check(
        true,
        format!("hard-killed worker {victim} while it was running job {marker}"),
    );

    // ---------------------------------------------------------------- 5
    r.step(5, "The lease expires and another worker recovers the job");
    let recovered = wait_for(Duration::from_secs(30), || async {
        let job = client.status(marker).await.ok()?;
        (job.state == JobState::Succeeded).then_some(job)
    })
    .await;
    let recovery = killed_at.elapsed();
    let history = client.attempts(marker).await.unwrap_or_default();
    for a in &history {
        r.note(format!(
            "attempt {} on {} -> {}",
            a.attempt,
            worker_name(&a.worker_id),
            a.outcome.map_or("in progress".into(), |o| o.to_string())
        ));
    }
    let shape_ok = history.len() == 2
        && worker_name(&history[0].worker_id) == victim
        && history[0].outcome == Some(AttemptOutcome::LeaseExpired)
        && worker_name(&history[1].worker_id) != victim
        && history[1].outcome == Some(AttemptOutcome::Succeeded);
    r.check(
        recovered.is_some() && shape_ok,
        format!(
            "marker job recovered and finished as attempt 2, {:.1} s after the kill",
            recovery.as_secs_f64()
        ),
    );
    r.note(
        "(= 2 s lease + reaper tick + retry backoff + its turn behind still-queued jobs + 4 s re-run; \
         retries rejoin the queue in FIFO order by availability)",
    );

    let total = args.jobs + 1;
    let all_done = wait_for(Duration::from_secs(120), || async {
        let done = metric(&client, "relay_jobs{state=\"succeeded\"}").await?;
        (done as usize == total).then_some(())
    })
    .await;
    let mut per_worker: BTreeMap<String, usize> = BTreeMap::new();
    let mut recovered_jobs = 0;
    for id in &ids {
        let attempts = client.attempts(*id).await.unwrap_or_default();
        if attempts
            .iter()
            .any(|a| a.outcome == Some(AttemptOutcome::LeaseExpired))
        {
            recovered_jobs += 1;
        }
        if let Some(last) = attempts.last() {
            *per_worker
                .entry(worker_name(&last.worker_id).to_owned())
                .or_default() += 1;
        }
    }
    r.check(all_done.is_some(), format!("all {total} jobs succeeded"));
    r.note(format!(
        "completed per worker: {} ({recovered_jobs} other job(s) were also orphaned by the kill and recovered)",
        per_worker.iter().map(|(w, n)| format!("{w}={n}")).collect::<Vec<_>>().join(", ")
    ));

    // ---------------------------------------------------------------- 6
    r.step(6, "Restart the coordinator (hard kill, then start again)");
    let sample: Vec<JobId> = ids.iter().take(5).copied().collect();
    let mut before = Vec::new();
    for id in &sample {
        before.push(client.result(*id).await?);
    }
    cluster.kill_coordinator()?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    cluster.start_coordinator().await?;
    r.check(
        true,
        "coordinator killed and restarted on the same database",
    );

    // ---------------------------------------------------------------- 7
    r.step(7, "Persisted jobs and completed results are still there");
    let succeeded_after = metric(&client, "relay_jobs{state=\"succeeded\"}").await;
    r.check(
        succeeded_after == Some(total as f64),
        format!("{total} succeeded jobs after restart"),
    );
    let mut same = true;
    for (id, old) in sample.iter().zip(&before) {
        same &= client.result(*id).await.ok().as_ref() == Some(old);
    }
    r.check(
        same,
        "sampled results are byte-for-byte identical to before the crash",
    );
    let mut fresh = Vec::new();
    for i in 0..5 {
        fresh.push(
            client
                .submit(&mock(format!("after restart {i}"), json!({})))
                .await?
                .job
                .id,
        );
    }
    let reconnected = wait_for(Duration::from_secs(30), || async {
        for id in &fresh {
            if client.status(*id).await.ok()?.state != JobState::Succeeded {
                return None;
            }
        }
        Some(())
    })
    .await;
    r.check(
        reconnected.is_some(),
        "surviving workers reconnected and processed new jobs",
    );

    // ---------------------------------------------------------------- 8
    r.step(8, "Submit the same idempotency key twice");
    let key = format!("demo-key-{}", JobId::generate());
    let keyed = |prompt: &str| {
        let mut request = mock(prompt, json!({}));
        request.idempotency_key = Some(key.clone());
        request
    };
    let first = client.submit(&keyed("idempotent request")).await?;
    let second = client.submit(&keyed("idempotent request")).await?;
    r.check(
        first.created && !second.created && first.job.id == second.job.id,
        format!(
            "one logical job {}: created={}, then created={}",
            first.job.id, first.created, second.created
        ),
    );
    let conflict = client.submit(&keyed("a different request")).await;
    r.check(
        conflict.as_ref().err().and_then(|e| e.api_code()) == Some("idempotency_conflict"),
        "same key with a different request is rejected (409 idempotency_conflict)",
    );

    // ---------------------------------------------------------------- 9
    r.step(9, "A stale attempt cannot overwrite a newer result");
    stale_attempt_demo(&client, &mut r).await?;

    // ---------------------------------------------------------------- 10
    r.step(10, "Run the automated tests");
    r.note("cargo test --workspace     (scripts/demo.ps1 runs this after the demo)");

    println!(
        "\n{} checks passed, {} failed, in {:.1} s. Logs: {}",
        r.passed,
        r.failed.len(),
        started.elapsed().as_secs_f64(),
        cluster.dir().display()
    );
    Ok(if r.failed.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// Drive the fencing scenario by hand over the worker API: a "zombie"
/// worker claims a job and goes silent; its lease expires; a "rescuer"
/// claims attempt 2 and completes it; then the zombie wakes up and tries to
/// complete attempt 1.
async fn stale_attempt_demo(client: &RelayClient, r: &mut Report) -> anyhow::Result<()> {
    const KIND: &str = "demo-stale";
    let suffix = JobId::generate().to_string();
    let (zombie, rescuer) = (format!("zombie-{suffix}"), format!("rescuer-{suffix}"));
    for worker in [&zombie, &rescuer] {
        client
            .register_worker(
                worker,
                &RegisterWorkerRequest {
                    name: worker_name(worker).into(),
                    concurrency: 1,
                    kinds: vec![KIND.into()],
                },
            )
            .await?;
    }
    let job = client
        .submit(&SubmitJobRequest {
            kind: KIND.into(),
            payload: json!({"prompt": "who answers last?"}),
            idempotency_key: None,
            max_attempts: Some(3),
            timeout_secs: None,
        })
        .await?
        .job
        .id;

    let first = client
        .claim(&zombie, "c-zombie")
        .await?
        .context("zombie got no job")?;
    r.note(format!(
        "zombie claimed attempt {} and then went silent (no heartbeats)",
        first.attempt
    ));
    let expired = wait_for(Duration::from_secs(15), || async {
        (client.status(job).await.ok()?.state == JobState::RetryWaiting).then_some(())
    })
    .await;
    r.check(
        expired.is_some(),
        "the zombie's lease expired and the job was requeued",
    );

    let mut tries = 0;
    let rescuer_ref = &rescuer;
    let second = wait_for(Duration::from_secs(15), || {
        // A fresh claim id per definite "nothing yet" (204), per the protocol.
        tries += 1;
        let claim_id = format!("c-rescuer-{tries}");
        async move { client.claim(rescuer_ref, &claim_id).await.ok().flatten() }
    })
    .await
    .context("rescuer never got the job")?;
    client
        .complete(
            job,
            second.attempt,
            &CompleteRequest {
                worker_id: rescuer.clone(),
                result: json!({"answer": "fresh, from attempt 2"}),
            },
        )
        .await?;
    r.note(format!(
        "rescuer claimed attempt {} and completed it",
        second.attempt
    ));

    let late = client
        .complete(
            job,
            first.attempt,
            &CompleteRequest {
                worker_id: zombie.clone(),
                result: json!({"answer": "stale, from attempt 1"}),
            },
        )
        .await;
    r.check(
        late.as_ref().err().and_then(|e| e.api_code()) == Some("stale_attempt"),
        "zombie's late completion of attempt 1 was rejected (409 stale_attempt)",
    );
    let late_beat = client
        .heartbeat(
            job,
            first.attempt,
            &HeartbeatRequest {
                worker_id: zombie.clone(),
            },
        )
        .await;
    r.check(
        late_beat.as_ref().err().and_then(|e| e.api_code()) == Some("stale_attempt"),
        "zombie's heartbeat was rejected too (it would abort its handler)",
    );
    let result = client.result(job).await?.result;
    r.check(
        result == Some(json!({"answer": "fresh, from attempt 2"})),
        "the stored result is still attempt 2's",
    );
    Ok(())
}
