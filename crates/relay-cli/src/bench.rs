//! `relay bench`: reproducible throughput, latency and recovery benchmark.
//!
//! Every scenario runs on a fresh local cluster (new database, separate
//! coordinator and worker processes) using the deterministic `mock` handler.
//! **These are queue-overhead numbers, not LLM inference numbers**: the mock
//! "inference" is a sleep of `delay_ms`.
//!
//! Workload per scenario: all jobs are submitted as a burst by concurrent
//! submitters, then drained by the workers. So "queue wait" measures how
//! long a burst takes to drain (backlog), not idle pick-up latency.
//!
//! All latencies come from coordinator timestamps (`created_at`, attempt
//! `started_at`/`finished_at`), so client-side delays don't skew them.
//! Output files:
//!
//! * `environment.json`: machine, OS, build profile, parameters
//! * `scenarios.csv` / `scenarios.json`: one row per scenario
//! * `jobs.csv`: raw per-job measurements, so every summary can be recomputed
//! * `recovery.csv` / `recovery.json`: worker-kill recovery trials

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::Args;
use relay_client::RelayClient;
use relay_core::api::SubmitJobRequest;
use relay_core::job::{AttemptOutcome, JobId, JobState};
use serde::Serialize;
use serde_json::json;
use tokio::task::JoinSet;

use crate::cluster::Cluster;

#[derive(Debug, Args)]
pub struct BenchArgs {
    /// Jobs per scenario.
    #[arg(long, default_value_t = 200)]
    jobs: usize,

    /// Worker counts to sweep (comma-separated).
    #[arg(long, value_delimiter = ',', default_value = "1,2,4")]
    workers: Vec<u32>,

    /// Mock handler delays to sweep, in ms (comma-separated).
    #[arg(long, value_delimiter = ',', default_value = "0,20,100")]
    delay_ms: Vec<u64>,

    /// Concurrent jobs per worker.
    #[arg(long, default_value_t = 4)]
    concurrency: u32,

    /// Concurrent client tasks submitting the burst.
    #[arg(long, default_value_t = 16)]
    submitters: usize,

    /// Fraction of jobs scripted to fail once (retryably) before succeeding,
    /// to exercise the retry path. 0 disables.
    #[arg(long, default_value_t = 0.05)]
    failure_rate: f64,

    /// Coordinator durability (`full` or `normal`); see
    /// `relay coordinator --help`. The dominant throughput factor.
    #[arg(long, default_value = "full")]
    sqlite_synchronous: String,

    /// Run every scenario this many times (fresh cluster each time). One
    /// run on a shared machine is noisy; report medians and spread.
    #[arg(long, default_value_t = 1)]
    repeat: u32,

    /// Worker-kill recovery trials (0 skips).
    #[arg(long, default_value_t = 3)]
    recovery_trials: u32,

    /// Results go to `<out>/<UTC timestamp>/`.
    #[arg(long, default_value = "bench/results")]
    out: PathBuf,
}

#[derive(Debug, Serialize)]
struct Environment {
    relay_version: &'static str,
    build_profile: &'static str,
    os: &'static str,
    arch: &'static str,
    logical_cpus: usize,
    cpu_model: Option<String>,
    started_at_unix_ms: i64,
    jobs_per_scenario: usize,
    concurrency_per_worker: u32,
    submitters: usize,
    failure_rate: f64,
    sqlite_synchronous: String,
    handler: &'static str,
    note: &'static str,
}

#[derive(Debug, Serialize)]
struct Scenario {
    scenario: String,
    repetition: u32,
    workers: u32,
    concurrency_per_worker: u32,
    total_slots: u32,
    delay_ms: u64,
    jobs: usize,
    submit_secs: f64,
    submitted_per_sec: f64,
    makespan_secs: f64,
    completed_per_sec: f64,
    /// Ideal makespan with perfect parallelism: jobs × delay / slots.
    ideal_makespan_secs: f64,
    queue_wait_p50_ms: i64,
    queue_wait_p95_ms: i64,
    queue_wait_max_ms: i64,
    exec_p50_ms: i64,
    exec_p95_ms: i64,
    retries: usize,
    failed: usize,
    coordinator_peak_memory_mb: Option<f64>,
}

#[derive(Debug, Serialize)]
struct JobRow {
    scenario: String,
    repetition: u32,
    job_id: JobId,
    state: JobState,
    attempts: usize,
    queue_wait_ms: i64,
    exec_ms: i64,
}

#[derive(Debug, Serialize)]
struct RecoveryTrial {
    trial: u32,
    lease_secs: u64,
    /// Kill → coordinator marks the attempt `lease_expired`.
    detect_ms: i64,
    /// Kill → another worker starts attempt 2.
    reclaim_ms: i64,
    /// Kill → job succeeded (includes re-running the 2 s job).
    complete_ms: i64,
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Nearest-rank percentile of an unsorted sample.
fn percentile(values: &[i64], p: f64) -> i64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn cpu_model() -> Option<String> {
    if cfg!(windows) {
        std::env::var("PROCESSOR_IDENTIFIER").ok()
    } else {
        let info = std::fs::read_to_string("/proc/cpuinfo").ok()?;
        info.lines()
            .find(|l| l.starts_with("model name"))
            .and_then(|l| l.split(':').nth(1))
            .map(|s| s.trim().to_owned())
    }
}

/// Coordinator `relay_jobs{state="..."}` value.
async fn jobs_in_state(client: &RelayClient, state: &str) -> f64 {
    let Ok(response) = reqwest::get(format!("{}/metrics", client.base_url())).await else {
        return 0.0;
    };
    let text = response.text().await.unwrap_or_default();
    let prefix = format!("relay_jobs{{state=\"{state}\"}} ");
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix)?.parse().ok())
        .unwrap_or(0.0)
}

async fn wait_for_workers(client: &RelayClient, n: usize) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    while client.list_workers().await.map(|w| w.len()).unwrap_or(0) < n {
        anyhow::ensure!(
            Instant::now() < deadline,
            "workers did not register within 20 s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

pub async fn run(args: BenchArgs) -> anyhow::Result<ExitCode> {
    anyhow::ensure!(args.jobs > 0, "--jobs must be positive");
    anyhow::ensure!(
        (0.0..=1.0).contains(&args.failure_rate),
        "--failure-rate must be in [0, 1]"
    );
    anyhow::ensure!(
        matches!(args.sqlite_synchronous.as_str(), "full" | "normal"),
        "--sqlite-synchronous must be full or normal"
    );
    let debug_build = cfg!(debug_assertions);
    let out = args.out.join(format!("{}", unix_ms()));
    std::fs::create_dir_all(&out).with_context(|| format!("creating {}", out.display()))?;

    let environment = Environment {
        relay_version: env!("CARGO_PKG_VERSION"),
        build_profile: if debug_build { "debug" } else { "release" },
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        logical_cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
        cpu_model: cpu_model(),
        started_at_unix_ms: unix_ms(),
        jobs_per_scenario: args.jobs,
        concurrency_per_worker: args.concurrency,
        submitters: args.submitters,
        failure_rate: args.failure_rate,
        sqlite_synchronous: args.sqlite_synchronous.clone(),
        handler: "mock (sleep delay_ms, deterministic output)",
        note: "Queue overhead on one machine, not LLM inference throughput. \
               Coordinator, workers and client share the same CPUs.",
    };
    std::fs::write(
        out.join("environment.json"),
        serde_json::to_string_pretty(&environment)?,
    )?;
    if debug_build {
        println!("WARNING: debug build; build with --release for representative numbers.");
    }
    println!("RelayLLM benchmark -> {}", out.display());
    println!(
        "{:<18} {:>6} {:>10} {:>11} {:>11} {:>9} {:>9} {:>8} {:>8} {:>7} {:>8}",
        "scenario",
        "slots",
        "submit/s",
        "complete/s",
        "makespan s",
        "ideal s",
        "wait p50",
        "wait p95",
        "exec p95",
        "retries",
        "mem MB"
    );

    let mut scenarios = Vec::new();
    let mut job_rows = Vec::new();
    for repetition in 1..=args.repeat.max(1) {
        for &workers in &args.workers {
            for &delay in &args.delay_ms {
                let name = format!("{}-w{workers}-d{delay}", args.sqlite_synchronous);
                let (mut scenario, rows) =
                    run_scenario(&args, &out, &name, repetition, workers, delay).await?;
                scenario.repetition = repetition;
                println!(
                    "{:<18} {:>6} {:>10.0} {:>11.1} {:>11.2} {:>9.2} {:>9} {:>9} {:>8} {:>7} {:>8}",
                    scenario.scenario,
                    scenario.total_slots,
                    scenario.submitted_per_sec,
                    scenario.completed_per_sec,
                    scenario.makespan_secs,
                    scenario.ideal_makespan_secs,
                    scenario.queue_wait_p50_ms,
                    scenario.queue_wait_p95_ms,
                    scenario.exec_p95_ms,
                    scenario.retries,
                    scenario
                        .coordinator_peak_memory_mb
                        .map_or("n/a".into(), |m| format!("{m:.1}"))
                );
                scenarios.push(scenario);
                job_rows.extend(rows);
            }
        }
    }
    write_csv(&out.join("scenarios.csv"), &scenarios)?;
    std::fs::write(
        out.join("scenarios.json"),
        serde_json::to_string_pretty(&scenarios)?,
    )?;
    write_csv(&out.join("jobs.csv"), &job_rows)?;

    if args.recovery_trials > 0 {
        println!("\nworker-kill recovery (lease 2 s, reaper every 0.5 s):");
        let mut trials = Vec::new();
        for trial in 1..=args.recovery_trials {
            let t = recovery_trial(&out, trial).await?;
            println!(
                "  trial {}: detected {:.2} s, re-claimed {:.2} s, completed {:.2} s after the kill",
                t.trial,
                t.detect_ms as f64 / 1000.0,
                t.reclaim_ms as f64 / 1000.0,
                t.complete_ms as f64 / 1000.0
            );
            trials.push(t);
        }
        write_csv(&out.join("recovery.csv"), &trials)?;
        std::fs::write(
            out.join("recovery.json"),
            serde_json::to_string_pretty(&trials)?,
        )?;
    }
    println!("\nraw results: {}", out.display());
    Ok(ExitCode::SUCCESS)
}

async fn run_scenario(
    args: &BenchArgs,
    out: &Path,
    name: &str,
    repetition: u32,
    workers: u32,
    delay: u64,
) -> anyhow::Result<(Scenario, Vec<JobRow>)> {
    let capacity = (args.jobs + 10).to_string();
    let mut cluster = Cluster::new(
        &out.join("clusters").join(format!("{name}-r{repetition}")),
        &[
            "--lease-secs",
            "10",
            "--retry-base-ms",
            "50",
            "--retry-max-ms",
            "200",
            "--max-active-jobs",
            &capacity,
            "--request-timeout-secs",
            "5",
            "--sqlite-synchronous",
            &args.sqlite_synchronous,
        ],
    )?;
    cluster.start_coordinator().await?;
    let concurrency = args.concurrency.to_string();
    for i in 0..workers {
        cluster.start_worker(&format!("bench{i}"), &["--concurrency", &concurrency])?;
    }
    let client = RelayClient::new(cluster.url())?;
    wait_for_workers(&client, workers as usize).await?;

    // Every Nth job is scripted to fail once, deterministically.
    let fail_every = if args.failure_rate > 0.0 {
        (1.0 / args.failure_rate).round().max(1.0) as usize
    } else {
        usize::MAX
    };

    // Burst-submit with a bounded number of concurrent submitters.
    let started = Instant::now();
    let mut ids = Vec::with_capacity(args.jobs);
    let mut tasks = JoinSet::new();
    let mut next = 0;
    while next < args.jobs || !tasks.is_empty() {
        while next < args.jobs && tasks.len() < args.submitters {
            let client = client.clone();
            let fail = usize::from(next % fail_every == fail_every - 1);
            let payload = json!({"prompt": format!("bench job {next}"), "delay_ms": delay, "fail_attempts": fail});
            tasks.spawn(async move {
                client
                    .submit(&SubmitJobRequest {
                        kind: "mock".into(),
                        payload,
                        idempotency_key: None,
                        max_attempts: Some(3),
                        timeout_secs: None,
                    })
                    .await
            });
            next += 1;
        }
        if let Some(done) = tasks.join_next().await {
            ids.push(done??.job.id);
        }
    }
    let submit_secs = started.elapsed().as_secs_f64();

    let target = args.jobs as f64;
    loop {
        let finished =
            jobs_in_state(&client, "succeeded").await + jobs_in_state(&client, "failed").await;
        if finished >= target {
            break;
        }
        anyhow::ensure!(
            started.elapsed() < Duration::from_secs(600),
            "scenario {name} did not finish in 10 min"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let makespan_secs = started.elapsed().as_secs_f64();
    let peak_memory = cluster.coordinator_peak_memory_bytes();

    let mut rows = Vec::with_capacity(ids.len());
    for id in &ids {
        let status = client.status(*id).await?;
        let attempts = client.attempts(*id).await?;
        let first_start = attempts
            .first()
            .map_or(status.created_at_ms, |a| a.started_at_ms);
        let exec_ms = attempts
            .iter()
            .rev()
            .find(|a| a.outcome == Some(AttemptOutcome::Succeeded))
            .and_then(|a| Some(a.finished_at_ms? - a.started_at_ms))
            .unwrap_or(0);
        rows.push(JobRow {
            scenario: name.to_owned(),
            repetition,
            job_id: *id,
            state: status.state,
            attempts: attempts.len(),
            queue_wait_ms: first_start - status.created_at_ms,
            exec_ms,
        });
    }
    drop(cluster);

    let waits: Vec<i64> = rows.iter().map(|r| r.queue_wait_ms).collect();
    let execs: Vec<i64> = rows
        .iter()
        .filter(|r| r.state == JobState::Succeeded)
        .map(|r| r.exec_ms)
        .collect();
    let slots = workers * args.concurrency;
    let scenario = Scenario {
        scenario: name.to_owned(),
        repetition,
        workers,
        concurrency_per_worker: args.concurrency,
        total_slots: slots,
        delay_ms: delay,
        jobs: args.jobs,
        submit_secs,
        submitted_per_sec: args.jobs as f64 / submit_secs,
        makespan_secs,
        completed_per_sec: args.jobs as f64 / makespan_secs,
        ideal_makespan_secs: args.jobs as f64 * delay as f64 / 1000.0 / f64::from(slots),
        queue_wait_p50_ms: percentile(&waits, 50.0),
        queue_wait_p95_ms: percentile(&waits, 95.0),
        queue_wait_max_ms: waits.iter().copied().max().unwrap_or(0),
        exec_p50_ms: percentile(&execs, 50.0),
        exec_p95_ms: percentile(&execs, 95.0),
        retries: rows.iter().map(|r| r.attempts.saturating_sub(1)).sum(),
        failed: rows.iter().filter(|r| r.state == JobState::Failed).count(),
        coordinator_peak_memory_mb: peak_memory.map(|b| b as f64 / (1024.0 * 1024.0)),
    };
    Ok((scenario, rows))
}

/// Kill the worker running a 2 s job and time each recovery stage against
/// the coordinator's own timestamps. The bench and the coordinator share
/// this machine's clock, so kill time and coordinator times are comparable.
async fn recovery_trial(out: &Path, trial: u32) -> anyhow::Result<RecoveryTrial> {
    let mut cluster = Cluster::new(
        &out.join("clusters").join(format!("recovery-{trial}")),
        &[
            "--lease-secs",
            "2",
            "--retry-base-ms",
            "50",
            "--retry-max-ms",
            "100",
            "--request-timeout-secs",
            "1",
        ],
    )?;
    cluster.start_coordinator().await?;
    cluster.start_worker("victim", &["--concurrency", "1"])?;
    let client = RelayClient::new(cluster.url())?;
    wait_for_workers(&client, 1).await?;

    let job = client
        .submit(&SubmitJobRequest {
            kind: "mock".into(),
            payload: json!({"prompt": "recovery probe", "delay_ms": 2000}),
            idempotency_key: None,
            max_attempts: Some(3),
            timeout_secs: None,
        })
        .await?
        .job
        .id;
    let deadline = Instant::now() + Duration::from_secs(10);
    while client.status(job).await?.state != JobState::Running {
        anyhow::ensure!(Instant::now() < deadline, "probe job never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Kill at a random point in the heartbeat cycle, like a real crash.
    tokio::time::sleep(Duration::from_millis(fastrand_ms(100, 600))).await;
    cluster.kill_worker("victim")?;
    let killed_at = unix_ms();
    cluster.start_worker("rescuer", &["--concurrency", "1"])?;

    let deadline = Instant::now() + Duration::from_secs(30);
    while client.status(job).await?.state != JobState::Succeeded {
        anyhow::ensure!(Instant::now() < deadline, "probe job never recovered");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let attempts = client.attempts(job).await?;
    anyhow::ensure!(
        attempts.len() == 2,
        "expected exactly 2 attempts, got {}",
        attempts.len()
    );
    Ok(RecoveryTrial {
        trial,
        lease_secs: 2,
        detect_ms: attempts[0].finished_at_ms.unwrap_or(killed_at) - killed_at,
        reclaim_ms: attempts[1].started_at_ms - killed_at,
        complete_ms: attempts[1].finished_at_ms.unwrap_or(killed_at) - killed_at,
    })
}

fn fastrand_ms(min: u64, max: u64) -> u64 {
    // Avoid a dependency: the sub-second part of the clock is random enough
    // to pick a kill point.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    min + nanos % (max - min)
}

/// Minimal CSV writer: header from the JSON field names, then one line per
/// row. Values here are numbers, enums and ids, so none need quoting.
fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> anyhow::Result<()> {
    let mut text = String::new();
    for (i, row) in rows.iter().enumerate() {
        let serde_json::Value::Object(fields) = serde_json::to_value(row)? else {
            anyhow::bail!("CSV rows must serialize to objects");
        };
        if i == 0 {
            let _ = writeln!(
                text,
                "{}",
                fields.keys().cloned().collect::<Vec<_>>().join(",")
            );
        }
        let values: Vec<String> = fields
            .values()
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            })
            .collect();
        let _ = writeln!(text, "{}", values.join(","));
    }
    std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles() {
        let v: Vec<i64> = (1..=100).collect();
        assert_eq!(percentile(&v, 50.0), 50);
        assert_eq!(percentile(&v, 95.0), 95);
        assert_eq!(percentile(&v, 100.0), 100);
        assert_eq!(percentile(&[7], 95.0), 7);
        assert_eq!(percentile(&[], 50.0), 0);
        assert_eq!(percentile(&[3, 1, 2], 50.0), 2, "unsorted input");
    }
}
