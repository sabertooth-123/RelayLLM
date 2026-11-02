//! One repetition against a real local cluster: spawn processes, submit the
//! planned workflows, inject the planned failures with real process kills,
//! wait, collect everything through the public API, tear down.
//!
//! The harness observes RelayLLM only the way a user can (HTTP API and
//! `/metrics`); it never reads the database or calls internals.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use relay_client::RelayClient;
use relay_core::job::{JobId, JobState};
use relay_core::workflow::{
    ActivityViewStatus, StartWorkflowRequest, StepKind, StepStatus, WorkflowEventKind, WorkflowId,
    WorkflowView,
};
use serde_json::json;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use super::analysis::{Collected, CollectedWorkflow, FailureKind, FailureRecord, Sample};
use super::config::{CrashPoint, Experiment};
use super::plan::{self, RunPlan};
use crate::bench::unix_ms;
use crate::cluster::Cluster;

const POLL: Duration = Duration::from_millis(50);
const SAMPLE_EVERY: Duration = Duration::from_millis(250);

type Shared<T> = Arc<Mutex<T>>;

/// Run one repetition. `dir` receives the cluster's database and logs.
pub async fn run(
    exp: &Experiment,
    run_id: &str,
    plan: &RunPlan,
    dir: &Path,
) -> anyhow::Result<Collected> {
    let c = &exp.cluster;
    let coordinator_args = [
        "--lease-secs".to_owned(),
        c.lease_secs.to_string(),
        "--retry-base-ms".to_owned(),
        c.retry_base_ms.to_string(),
        "--retry-max-ms".to_owned(),
        c.retry_max_ms.to_string(),
        "--max-active-jobs".to_owned(),
        c.max_active_jobs.to_string(),
        "--sqlite-synchronous".to_owned(),
        c.sqlite_synchronous.clone(),
    ];
    let args: Vec<&str> = coordinator_args.iter().map(String::as_str).collect();
    let mut cluster = Cluster::new(dir, &args)?;
    cluster.start_coordinator().await?;
    let concurrency = c.concurrency.to_string();
    for w in 1..=c.workers {
        cluster.start_worker(&format!("w{w}"), &["--concurrency", &concurrency])?;
    }
    let client = RelayClient::with_timeout(cluster.url(), Duration::from_secs(5))?;
    wait_for_workers(&client, c.workers as usize).await?;
    let cluster: Shared<Cluster> = Arc::new(Mutex::new(cluster));

    let ids: Shared<Vec<Option<WorkflowId>>> =
        Arc::new(Mutex::new(vec![None; plan.workflows.len()]));
    let failures: Shared<Vec<FailureRecord>> = Arc::default();
    let (done_tx, done_rx) = watch::channel(false);

    let sampler = tokio::spawn(sample(client.clone(), done_rx.clone()));
    let worker_injector = exp.failures.worker_crash.as_ref().map(|wc| {
        tokio::spawn(inject_worker_crashes(
            client.clone(),
            Arc::clone(&cluster),
            plan.clone(),
            Arc::clone(&ids),
            Arc::clone(&failures),
            wc.restart_worker,
            concurrency.clone(),
            done_rx.clone(),
        ))
    });
    let coordinator_injector = exp.failures.coordinator_crash.as_ref().map(|cc| {
        tokio::spawn(inject_coordinator_crashes(
            client.clone(),
            Arc::clone(&cluster),
            Arc::clone(&ids),
            Arc::clone(&failures),
            cc.points.clone(),
            Duration::from_millis(cc.downtime_ms),
            Duration::from_secs(cc.observe_timeout_secs),
            done_rx.clone(),
        ))
    });

    // Submit, in plan order, at the configured interval.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(exp.timeout_secs);
    let mut workflows: Vec<CollectedWorkflow> = Vec::with_capacity(plan.workflows.len());
    for wf in &plan.workflows {
        let request = StartWorkflowRequest {
            definition: plan::definition(exp, wf),
            input: json!({ "experiment": exp.name, "run": run_id, "workflow": wf.index }),
            idempotency_key: Some(format!("exp-{run_id}-{}", wf.index)),
        };
        let submitted_at_ms = unix_ms();
        let mut collected = CollectedWorkflow {
            index: wf.index,
            submitted_at_ms,
            ..Default::default()
        };
        match start_with_retry(&client, &request, deadline).await {
            Ok(id) => {
                collected.id = Some(id);
                ids.lock().await[wf.index as usize] = Some(id);
            }
            Err(e) => collected.submit_error = Some(format!("{e:#}")),
        }
        workflows.push(collected);
        if exp.workload.submit_interval_ms > 0 {
            tokio::time::sleep(Duration::from_millis(exp.workload.submit_interval_ms)).await;
        }
    }

    // Wait for every started run to finish, or the timeout.
    let mut pending: BTreeSet<WorkflowId> = workflows.iter().filter_map(|w| w.id).collect();
    while !pending.is_empty() && tokio::time::Instant::now() < deadline {
        let mut finished = Vec::new();
        for id in &pending {
            if let Ok(view) = client.workflow(*id).await {
                if view.state.is_terminal() {
                    finished.push(*id);
                }
            }
        }
        for id in finished {
            pending.remove(&id);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let timed_out = !pending.is_empty();

    // Stop injecting. The coordinator injector finishes an in-progress
    // restart before it returns, so collection sees a live coordinator.
    let _ = done_tx.send(true);
    if let Some(h) = coordinator_injector {
        h.await.context("coordinator injector panicked")?;
    }
    if let Some(h) = worker_injector {
        h.await.context("worker injector panicked")?;
    }
    let samples = sampler.await.context("sampler panicked")?;

    let collect_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    for w in &mut workflows {
        let Some(id) = w.id else { continue };
        w.view = Some(retry(collect_deadline, || client.workflow(id)).await?);
        w.history = retry(collect_deadline, || client.workflow_history(id))
            .await?
            .events;
        let jobs: BTreeSet<JobId> = w
            .history
            .iter()
            .filter_map(|e| match &e.kind {
                WorkflowEventKind::ActivityScheduled { job_id, .. } => Some(*job_id),
                _ => None,
            })
            .collect();
        for job in jobs {
            w.attempts
                .insert(job, retry(collect_deadline, || client.attempts(job)).await?);
        }
    }
    let mut failures = std::mem::take(&mut *failures.lock().await);
    failures.sort_by_key(|f| (f.requested_at_ms, f.failure_id));
    for (i, f) in failures.iter_mut().enumerate() {
        f.failure_id = i as u32;
    }
    drop(cluster); // kills every process (Cluster::drop)
    Ok(Collected {
        workflows,
        failures,
        samples,
        timed_out,
        slots: c.workers * c.concurrency,
    })
}

async fn wait_for_workers(client: &RelayClient, n: usize) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while client.list_workers().await.map(|w| w.len()).unwrap_or(0) < n {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "workers did not register within 20 s"
        );
        tokio::time::sleep(POLL).await;
    }
    Ok(())
}

/// Retry a read until it succeeds or the deadline passes.
async fn retry<T, F, Fut>(deadline: tokio::time::Instant, mut f: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, relay_client::ClientError>>,
{
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if tokio::time::Instant::now() >= deadline => return Err(e.into()),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

/// Start a run, riding out a coordinator restart (the idempotency key makes
/// retries safe: a lost response never starts a second run).
async fn start_with_retry(
    client: &RelayClient,
    request: &StartWorkflowRequest,
    deadline: tokio::time::Instant,
) -> anyhow::Result<WorkflowId> {
    loop {
        match client.start_workflow(request).await {
            Ok(r) => return Ok(r.workflow.id),
            Err(e) if e.is_ambiguous() && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// `/metrics` gauges every 250 ms until `done`. Unreachable → `None`s.
async fn sample(client: RelayClient, mut done: watch::Receiver<bool>) -> Vec<Sample> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("reqwest client");
    let url = format!("{}/metrics", client.base_url());
    let mut out = Vec::new();
    loop {
        let t_ms = unix_ms();
        let text = match http.get(&url).send().await {
            Ok(r) => r.text().await.ok(),
            Err(_) => None,
        };
        let gauge = |name: &str| text.as_deref().and_then(|t| metric(t, name));
        out.push(Sample {
            t_ms,
            queued: gauge("relay_jobs{state=\"queued\"}"),
            running: gauge("relay_jobs{state=\"running\"}"),
            retry_waiting: gauge("relay_jobs{state=\"retry_waiting\"}"),
            due_runs: gauge("relay_workflow_due_runs"),
        });
        tokio::select! {
            _ = tokio::time::sleep(SAMPLE_EVERY) => {}
            _ = done.wait_for(|d| *d) => return out,
        }
    }
}

pub(crate) fn metric(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
}

/// The worker's name is everything before the first `-` of its id
/// (`w3-0192...`); the harness names workers `w<N>` and `w<N>r<K>`.
pub(crate) fn worker_name(worker_id: &str) -> &str {
    worker_id.split_once('-').map_or(worker_id, |(n, _)| n)
}

struct Target {
    workflow: u32,
    address: String,
    after_ms: u64,
}

/// For every planned crash: watch for attempt 1 of the target activity to
/// start, then — `after_ms` after its coordinator `started_at` — check it's
/// still running on the same worker and SIGKILL that worker's process.
#[allow(clippy::too_many_arguments)]
async fn inject_worker_crashes(
    client: RelayClient,
    cluster: Shared<Cluster>,
    plan: RunPlan,
    ids: Shared<Vec<Option<WorkflowId>>>,
    failures: Shared<Vec<FailureRecord>>,
    restart: bool,
    concurrency: String,
    mut done: watch::Receiver<bool>,
) {
    let mut targets: Vec<Target> = plan
        .workflows
        .iter()
        .flat_map(|w| {
            w.activities.iter().filter_map(move |a| {
                a.crash.map(|c| Target {
                    workflow: w.index,
                    address: a.address.clone(),
                    after_ms: c.after_ms,
                })
            })
        })
        .collect();
    let restarts: Shared<BTreeMap<String, u32>> = Arc::default();
    let mut kills: Vec<JoinHandle<()>> = Vec::new();

    while !targets.is_empty() && !*done.borrow() {
        let known = ids.lock().await.clone();
        let mut views: BTreeMap<u32, WorkflowView> = BTreeMap::new();
        for wf in targets.iter().map(|t| t.workflow).collect::<BTreeSet<_>>() {
            if let Some(id) = known[wf as usize] {
                if let Ok(v) = client.workflow(id).await {
                    views.insert(wf, v);
                }
            }
        }
        let mut remaining = Vec::new();
        for t in targets {
            let activity = views.get(&t.workflow).and_then(|v| {
                v.steps
                    .iter()
                    .flat_map(|s| &s.activities)
                    .find(|a| a.address == t.address)
            });
            let Some(a) = activity else {
                remaining.push(t);
                continue;
            };
            let (Some(job), Some(attempt)) = (a.job_id, a.attempt) else {
                remaining.push(t); // not scheduled yet
                continue;
            };
            let record = |outcome: &str, detail: String| FailureRecord {
                failure_id: 0,
                kind: FailureKind::WorkerSigkill,
                outcome: outcome.into(),
                detail,
                workflow_index: Some(t.workflow),
                address: Some(t.address.clone()),
                job_id: Some(job),
                attempt: Some(1),
                worker_id: None,
                planned_after_ms: Some(t.after_ms),
                crash_point: None,
                downtime_planned_ms: None,
                requested_at_ms: unix_ms(),
                executed_at_ms: None,
                restarted_at_ms: None,
            };
            match (a.status, attempt) {
                (ActivityViewStatus::Running, 1) => {
                    // Its worker and coordinator start time.
                    let first = match client.attempts(job).await {
                        Ok(list) => list
                            .into_iter()
                            .find(|x| x.attempt == 1 && x.outcome.is_none()),
                        Err(_) => {
                            remaining.push(t);
                            continue;
                        }
                    };
                    let Some(first) = first else {
                        failures.lock().await.push(record(
                            "skipped_attempt_finished",
                            "attempt 1 ended before it was seen running".into(),
                        ));
                        continue;
                    };
                    let mut r = record("pending", String::new());
                    r.worker_id = Some(first.worker_id.clone());
                    kills.push(tokio::spawn(kill_at(
                        client.clone(),
                        Arc::clone(&cluster),
                        Arc::clone(&failures),
                        Arc::clone(&restarts),
                        r,
                        first.started_at_ms + t.after_ms as i64,
                        restart,
                        concurrency.clone(),
                    )));
                }
                (ActivityViewStatus::Pending | ActivityViewStatus::Queued, 0 | 1) => {
                    remaining.push(t)
                }
                (status, n) => {
                    failures.lock().await.push(record(
                        "skipped_attempt_finished",
                        format!(
                            "first seen as {status:?} at attempt {n}; attempt 1 ended between polls"
                        ),
                    ));
                }
            }
        }
        targets = remaining;
        tokio::select! {
            _ = tokio::time::sleep(POLL) => {}
            _ = done.changed() => {}
        }
    }
    for t in targets {
        failures.lock().await.push(FailureRecord {
            failure_id: 0,
            kind: FailureKind::WorkerSigkill,
            outcome: "skipped_attempt_never_ran".into(),
            detail: "run ended (or timed out) before attempt 1 was seen running".into(),
            workflow_index: Some(t.workflow),
            address: Some(t.address),
            job_id: None,
            attempt: Some(1),
            worker_id: None,
            planned_after_ms: Some(t.after_ms),
            crash_point: None,
            downtime_planned_ms: None,
            requested_at_ms: unix_ms(),
            executed_at_ms: None,
            restarted_at_ms: None,
        });
    }
    for k in kills {
        let _ = k.await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn kill_at(
    client: RelayClient,
    cluster: Shared<Cluster>,
    failures: Shared<Vec<FailureRecord>>,
    restarts: Shared<BTreeMap<String, u32>>,
    mut r: FailureRecord,
    at_ms: i64,
    restart: bool,
    concurrency: String,
) {
    let wait = (at_ms - unix_ms()).max(0);
    tokio::time::sleep(Duration::from_millis(wait as u64)).await;
    let job = r.job_id.expect("worker targets carry a job");
    let worker_id = r.worker_id.clone().expect("worker targets carry a worker");
    r.requested_at_ms = unix_ms();
    // Re-check just before the kill: still attempt 1, still on that worker.
    match client.status(job).await {
        Ok(s)
            if s.state == JobState::Running
                && s.attempt == 1
                && s.worker_id.as_deref() == Some(&worker_id) => {}
        Ok(s) => {
            r.outcome = "skipped_attempt_finished".into();
            r.detail = format!(
                "at the crash point the job was {:?} at attempt {}",
                s.state, s.attempt
            );
            failures.lock().await.push(r);
            return;
        }
        Err(e) => {
            r.outcome = "error".into();
            r.detail = format!("status check failed: {e}");
            failures.lock().await.push(r);
            return;
        }
    }
    let name = worker_name(&worker_id).to_owned();
    let mut cluster = cluster.lock().await;
    match cluster.kill_worker(&name) {
        Ok(()) => {
            r.executed_at_ms = Some(unix_ms());
            r.outcome = "executed".into();
            r.detail = format!("SIGKILL worker process {name}");
            if restart {
                let base = name
                    .split_once('r')
                    .map_or(name.as_str(), |(b, _)| b)
                    .to_owned();
                let mut restarts = restarts.lock().await;
                let k = restarts.entry(base.clone()).or_default();
                *k += 1;
                let replacement = format!("{base}r{k}");
                match cluster.start_worker(&replacement, &["--concurrency", &concurrency]) {
                    Ok(()) => r.restarted_at_ms = Some(unix_ms()),
                    Err(e) => r.detail.push_str(&format!("; restart failed: {e}")),
                }
            }
        }
        Err(e) => {
            r.outcome = "skipped_worker_unknown".into();
            r.detail = format!("{e:#} (already killed by an earlier injection?)");
        }
    }
    failures.lock().await.push(r);
}

/// Is `point` observable right now in any of these runs?
pub(crate) fn observed(point: CrashPoint, views: &[WorkflowView], now_ms: i64) -> bool {
    let running = views.iter().filter(|v| !v.state.is_terminal());
    match point {
        CrashPoint::ActivitiesRunning => running
            .flat_map(|v| &v.steps)
            .flat_map(|s| &s.activities)
            .any(|a| a.status == ActivityViewStatus::Running),
        CrashPoint::BranchesPartiallyDone => running.flat_map(|v| &v.steps).any(|s| {
            s.kind == StepKind::Parallel
                && s.status == StepStatus::Active
                && s.activities
                    .iter()
                    .any(|a| a.status == ActivityViewStatus::Succeeded)
                && s.activities
                    .iter()
                    .any(|a| a.status != ActivityViewStatus::Succeeded)
        }),
        CrashPoint::TimerPending => running.flat_map(|v| &v.steps).any(|s| {
            s.kind == StepKind::Sleep
                && s.status == StepStatus::Active
                && s.fire_at_ms.is_some_and(|f| f >= now_ms + 1000)
        }),
        CrashPoint::DecisionPending => running
            .into_iter()
            .any(|v| v.wake_at_ms.is_some_and(|w| w <= now_ms)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn inject_coordinator_crashes(
    client: RelayClient,
    cluster: Shared<Cluster>,
    ids: Shared<Vec<Option<WorkflowId>>>,
    failures: Shared<Vec<FailureRecord>>,
    points: Vec<CrashPoint>,
    downtime: Duration,
    observe_timeout: Duration,
    done: watch::Receiver<bool>,
) {
    for point in points {
        let record = |outcome: &str, detail: String| FailureRecord {
            failure_id: 0,
            kind: FailureKind::CoordinatorSigkill,
            outcome: outcome.into(),
            detail,
            workflow_index: None,
            address: None,
            job_id: None,
            attempt: None,
            worker_id: None,
            planned_after_ms: None,
            crash_point: Some(point.as_str().into()),
            downtime_planned_ms: Some(downtime.as_millis() as u64),
            requested_at_ms: unix_ms(),
            executed_at_ms: None,
            restarted_at_ms: None,
        };
        let give_up = tokio::time::Instant::now() + observe_timeout;
        let mut seen = false;
        while !*done.borrow() && tokio::time::Instant::now() < give_up {
            let known: Vec<WorkflowId> = ids.lock().await.iter().flatten().copied().collect();
            let mut views = Vec::with_capacity(known.len());
            for id in known {
                if let Ok(v) = client.workflow(id).await {
                    views.push(v);
                }
            }
            if observed(point, &views, unix_ms()) {
                seen = true;
                break;
            }
            tokio::time::sleep(POLL).await;
        }
        if !seen {
            let why = if *done.borrow() {
                "every run finished first"
            } else {
                "observe timeout"
            };
            failures.lock().await.push(record(
                "not_observed",
                format!("condition never seen ({why})"),
            ));
            continue;
        }
        let mut r = record(
            "executed",
            format!("SIGKILL coordinator at {}", point.as_str()),
        );
        let mut cluster = cluster.lock().await;
        if let Err(e) = cluster.kill_coordinator() {
            r.outcome = "error".into();
            r.detail = format!("kill failed: {e:#}");
            failures.lock().await.push(r);
            continue;
        }
        r.executed_at_ms = Some(unix_ms());
        tokio::time::sleep(downtime).await;
        match cluster.start_coordinator().await {
            Ok(()) => r.restarted_at_ms = Some(unix_ms()),
            Err(e) => r.detail.push_str(&format!("; restart failed: {e:#}")),
        }
        failures.lock().await.push(r);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_core::workflow::{ActivityView, StepView, WorkflowState};

    #[test]
    fn worker_names_come_from_ids() {
        assert_eq!(worker_name("w3-0192aa-bb"), "w3");
        assert_eq!(worker_name("w3r2-0192aa"), "w3r2");
        assert_eq!(worker_name("solo"), "solo");
    }

    #[test]
    fn metrics_lines_parse_exactly() {
        let text = "relay_jobs{state=\"queued\"} 7\nrelay_jobs{state=\"queued_x\"} 9\nrelay_workflow_due_runs 2\n";
        assert_eq!(metric(text, "relay_jobs{state=\"queued\"}"), Some(7.0));
        assert_eq!(metric(text, "relay_workflow_due_runs"), Some(2.0));
        assert_eq!(metric(text, "relay_jobs{state=\"running\"}"), None);
    }

    fn view(steps: Vec<StepView>, wake_at_ms: Option<i64>) -> WorkflowView {
        WorkflowView {
            id: WorkflowId::generate(),
            name: "x".into(),
            state: WorkflowState::Running,
            reason: None,
            created_at_ms: 0,
            updated_at_ms: 0,
            finished_at_ms: None,
            wake_at_ms,
            steps,
        }
    }

    fn step(
        kind: StepKind,
        status: StepStatus,
        acts: &[ActivityViewStatus],
        fire_at_ms: Option<i64>,
    ) -> StepView {
        StepView {
            name: "s".into(),
            kind,
            status,
            activities: acts
                .iter()
                .map(|s| ActivityView {
                    address: "s.b".into(),
                    kind: "mock".into(),
                    status: *s,
                    job_id: None,
                    attempt: None,
                    max_attempts: None,
                })
                .collect(),
            fire_at_ms,
            signal: None,
            signal_received: None,
        }
    }

    #[test]
    fn crash_point_conditions() {
        use ActivityViewStatus as A;
        let now = 10_000;
        let fan = |acts: &[A]| {
            view(
                vec![step(StepKind::Parallel, StepStatus::Active, acts, None)],
                None,
            )
        };
        assert!(observed(
            CrashPoint::ActivitiesRunning,
            &[fan(&[A::Running, A::Queued])],
            now
        ));
        assert!(!observed(
            CrashPoint::ActivitiesRunning,
            &[fan(&[A::Queued])],
            now
        ));
        assert!(observed(
            CrashPoint::BranchesPartiallyDone,
            &[fan(&[A::Succeeded, A::Running])],
            now
        ));
        assert!(!observed(
            CrashPoint::BranchesPartiallyDone,
            &[fan(&[A::Succeeded, A::Succeeded])],
            now
        ));
        let timer = |fire| {
            view(
                vec![step(StepKind::Sleep, StepStatus::Active, &[], Some(fire))],
                Some(fire),
            )
        };
        assert!(observed(
            CrashPoint::TimerPending,
            &[timer(now + 5_000)],
            now
        ));
        assert!(
            !observed(CrashPoint::TimerPending, &[timer(now + 500)], now),
            "too close to fire to be 'pending'"
        );
        assert!(observed(
            CrashPoint::DecisionPending,
            &[view(vec![], Some(now - 1))],
            now
        ));
        assert!(!observed(
            CrashPoint::DecisionPending,
            &[timer(now + 5_000)],
            now
        ));
        let mut finished = fan(&[A::Running]);
        finished.state = WorkflowState::Succeeded;
        assert!(
            !observed(CrashPoint::ActivitiesRunning, &[finished], now),
            "finished runs don't count"
        );
    }
}
