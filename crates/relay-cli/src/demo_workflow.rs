//! `relay demo-workflow`: durable workflows surviving coordinator crashes.
//!
//! Runs `workflows/research.json` (plan → parallel a, b, c → aggregate →
//! 8 s durable timer → final) on a real cluster of separate processes, and
//! hard-kills the coordinator twice:
//!
//! 1. mid fan-out, with `a` done, `b` running a 6 s job and `c` retrying;
//! 2. while the run sleeps on its timer.
//!
//! Every claim is a check; the command exits 1 if any fails, so the demo is
//! also an end-to-end test (`tests/demo_workflow.rs`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Args;
use relay_client::RelayClient;
use relay_core::job::{AttemptOutcome, JobId};
use relay_core::workflow::{
    ActivityViewStatus, StartWorkflowRequest, StepStatus, WorkflowDefinition, WorkflowEventKind,
    WorkflowId, WorkflowState, WorkflowView,
};
use serde_json::json;

use crate::cluster::Cluster;
use crate::demo::{Report, wait_for};

/// The demo definition, also the documented example (`relay workflow start
/// --file crates/relay-cli/workflows/research.json`).
pub const RESEARCH: &str = include_str!("../workflows/research.json");

#[derive(Debug, Args)]
pub struct DemoWorkflowArgs {
    /// Directory for the database and process logs (default: a new
    /// directory under the system temp dir).
    #[arg(long)]
    workdir: Option<PathBuf>,
}

const WORKERS: [&str; 3] = ["w1", "w2", "w3"];
const STEPS: u32 = 8;

fn activity<'v>(
    view: &'v WorkflowView,
    address: &str,
) -> Option<&'v relay_core::workflow::ActivityView> {
    view.steps
        .iter()
        .flat_map(|s| &s.activities)
        .find(|a| a.address == address)
}

fn status(view: &WorkflowView, address: &str) -> Option<ActivityViewStatus> {
    activity(view, address).map(|a| a.status)
}

async fn attempts(client: &RelayClient, job: Option<JobId>) -> Vec<Option<AttemptOutcome>> {
    match job {
        Some(id) => client
            .attempts(id)
            .await
            .map(|a| a.into_iter().map(|x| x.outcome).collect())
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

/// Poll the run's view until `ready` holds (ambiguous errors while the
/// coordinator restarts are expected and retried).
async fn until(
    client: &RelayClient,
    id: WorkflowId,
    timeout: Duration,
    ready: impl Fn(&WorkflowView) -> bool,
) -> Option<WorkflowView> {
    wait_for(timeout, || async {
        let view = client.workflow(id).await.ok()?;
        ready(&view).then_some(view)
    })
    .await
}

pub async fn run(args: DemoWorkflowArgs) -> anyhow::Result<ExitCode> {
    let dir = args.workdir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("relay-workflow-demo-{}", JobId::generate()))
    });
    println!("RelayLLM durable workflow demo");
    println!(
        "working directory (database + process logs): {}",
        dir.display()
    );
    let started = Instant::now();
    let mut r = Report::new(STEPS);

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
        cluster.start_worker(name, &["--concurrency", "2"])?;
    }
    let client = RelayClient::new(cluster.url())?;
    let ready = wait_for(Duration::from_secs(15), || async {
        (client.list_workers().await.ok()?.len() == WORKERS.len()).then_some(())
    })
    .await;
    r.check(
        ready.is_some(),
        format!(
            "coordinator at {} with 3 workers (lease 2 s)",
            cluster.url()
        ),
    );

    // ---------------------------------------------------------------- 2
    r.step(2, "Start the research workflow");
    let definition: WorkflowDefinition = serde_json::from_str(RESEARCH)?;
    r.note("plan -> parallel(a 0.3 s, b 6 s, c fails once) -> aggregate -> sleep 8 s -> final");
    let start = client
        .start_workflow(&StartWorkflowRequest {
            definition,
            input: json!({ "topic": "durable execution" }),
            idempotency_key: Some(format!("demo-{}", JobId::generate())),
        })
        .await?;
    let id = start.workflow.id;
    r.check(
        start.created && start.workflow.state == WorkflowState::Running,
        format!("workflow {id} started"),
    );

    // ---------------------------------------------------------------- 3
    r.step(3, "Wait for the fan-out: a done, b running, c started");
    let before = until(&client, id, Duration::from_secs(20), |v| {
        status(v, "plan") == Some(ActivityViewStatus::Succeeded)
            && status(v, "research.a") == Some(ActivityViewStatus::Succeeded)
            && status(v, "research.b") == Some(ActivityViewStatus::Running)
            && activity(v, "research.c")
                .and_then(|a| a.attempt)
                .unwrap_or(0)
                >= 1
    })
    .await;
    r.check(
        before.is_some(),
        "plan and a succeeded; b is mid-way through its 6 s job",
    );
    let Some(before) = before else {
        return finish(r, started, &cluster);
    };
    let job = |v: &WorkflowView, a: &str| activity(v, a).and_then(|x| x.job_id);
    let (plan_job, a_job, b_job, c_job) = (
        job(&before, "plan"),
        job(&before, "research.a"),
        job(&before, "research.b"),
        job(&before, "research.c"),
    );
    let history_before = client.workflow_history(id).await?.events.len();

    // ---------------------------------------------------------------- 4
    r.step(
        4,
        "Kill the coordinator (hard) mid fan-out, then restart it",
    );
    cluster.kill_coordinator()?;
    let crash1 = now_ms();
    r.note(format!("coordinator killed at {crash1} ms; down for 1 s"));
    tokio::time::sleep(Duration::from_secs(1)).await;
    cluster.start_coordinator().await?;
    let after = client.workflow(id).await?;
    r.check(
        after.state == WorkflowState::Running,
        "the run survived the crash (still running)",
    );
    r.check(
        status(&after, "plan") == Some(ActivityViewStatus::Succeeded)
            && status(&after, "research.a") == Some(ActivityViewStatus::Succeeded),
        "completed activities are still completed (results durable)",
    );
    let history_after = client.workflow_history(id).await?.events.len();
    r.check(
        history_after >= history_before,
        format!("history intact: {history_before} events before, {history_after} after"),
    );

    // ---------------------------------------------------------------- 5
    r.step(
        5,
        "Recovery: in-flight branches finish under lease semantics; nothing re-runs",
    );
    let fanned_in = until(&client, id, Duration::from_secs(30), |v| {
        v.steps
            .iter()
            .find(|s| s.name == "aggregate")
            .is_some_and(|s| s.status == StepStatus::Done)
    })
    .await;
    r.check(
        fanned_in.is_some(),
        "all three branches done and aggregate finished after the restart",
    );
    r.check(
        attempts(&client, plan_job).await.len() == 1,
        "plan ran exactly once (not re-executed)",
    );
    r.check(
        attempts(&client, a_job).await.len() == 1,
        "research.a ran exactly once (not re-executed)",
    );
    let b = attempts(&client, b_job).await;
    r.check(
        b.last() == Some(&Some(AttemptOutcome::Succeeded)),
        format!(
            "research.b, running during the crash, recovered: {} attempt(s), last succeeded",
            b.len()
        ),
    );
    let c = attempts(&client, c_job).await;
    r.check(
        c.first() == Some(&Some(AttemptOutcome::RetryableError))
            && c.last() == Some(&Some(AttemptOutcome::Succeeded)),
        format!("research.c retried inside its own job: {c:?}"),
    );

    // ---------------------------------------------------------------- 6
    r.step(
        6,
        "Kill the coordinator while the run sleeps on its durable timer",
    );
    let sleeping = until(&client, id, Duration::from_secs(15), |v| {
        v.steps.iter().any(|s| {
            s.name == "cooldown" && s.status == StepStatus::Active && s.fire_at_ms.is_some()
        })
    })
    .await;
    let fire_at = sleeping
        .as_ref()
        .and_then(|v| v.steps.iter().find(|s| s.name == "cooldown")?.fire_at_ms);
    r.check(
        fire_at.is_some(),
        format!("timer armed: fires at {} ms", fire_at.unwrap_or_default()),
    );
    cluster.kill_coordinator()?;
    let crash2 = now_ms();
    r.note(format!("coordinator killed at {crash2} ms; down for 2 s"));
    tokio::time::sleep(Duration::from_secs(2)).await;
    cluster.start_coordinator().await?;
    let view = client.workflow(id).await?;
    let fire_after = view
        .steps
        .iter()
        .find(|s| s.name == "cooldown")
        .and_then(|s| s.fire_at_ms);
    r.check(
        fire_after == fire_at,
        "the deadline survived the restart unchanged (not re-armed from 'now')",
    );

    // ---------------------------------------------------------------- 7
    r.step(
        7,
        "The timer fires once, the final step runs, the workflow succeeds",
    );
    let done = client
        .wait_for_workflow(id, Duration::from_secs(40))
        .await?;
    r.check(
        done.state == WorkflowState::Succeeded,
        format!("workflow {}", done.state),
    );
    let history = client.workflow_history(id).await?;
    let fired: Vec<_> = history
        .events
        .iter()
        .filter(|e| matches!(e.kind, WorkflowEventKind::TimerFired { .. }))
        .collect();
    r.check(
        fired.len() == 1 && fire_at.is_some_and(|f| fired[0].at_ms >= f),
        "timer fired exactly once, not before its deadline",
    );
    if let Some(final_job) = activity(&done, "final").and_then(|a| a.job_id) {
        let result = client.result(final_job).await?.result.unwrap_or_default();
        r.check(
            result["inputs_seen"] == json!(["aggregate"]),
            "final saw aggregate's result",
        );
    }

    // ---------------------------------------------------------------- 8
    r.step(8, "The event history shows exactly how recovery happened");
    let mut scheduled: BTreeMap<String, usize> = BTreeMap::new();
    for e in &history.events {
        if let WorkflowEventKind::ActivityScheduled { activity, .. } = &e.kind {
            *scheduled.entry(activity.clone()).or_default() += 1;
        }
        let mark = if e.at_ms >= crash2 {
            " (after crash 2)"
        } else if e.at_ms >= crash1 {
            " (after crash 1)"
        } else {
            ""
        };
        let (step, _, _) = e.kind.columns();
        r.note(format!(
            "#{:<2} {:<20} {}{mark}",
            e.seq,
            e.kind.type_name(),
            step.unwrap_or("")
        ));
    }
    r.check(
        scheduled.len() == 6 && scheduled.values().all(|n| *n == 1),
        "each of the 6 activities was scheduled exactly once, across both crashes",
    );
    r.check(
        history
            .events
            .iter()
            .enumerate()
            .all(|(i, e)| e.seq == i as u64 + 1)
            && history.events.last().map(|e| e.kind.type_name()) == Some("workflow_completed"),
        "seq is contiguous and the history ends in workflow_completed",
    );

    finish(r, started, &cluster)
}

fn finish(r: Report, started: Instant, cluster: &Cluster) -> anyhow::Result<ExitCode> {
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

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
