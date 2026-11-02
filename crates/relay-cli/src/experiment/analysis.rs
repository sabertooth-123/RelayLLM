//! From collected observations to raw rows, per-run metrics and invariant
//! checks. Pure: no I/O, so every definition here is unit-tested.
//!
//! Every timestamp is milliseconds since the Unix epoch. Coordinator-side
//! times (`*_at_ms` from workflow events and job attempts) come from the
//! coordinator's clock; harness-side times (`requested_at_ms`,
//! `executed_at_ms`, `restarted_at_ms`, `submitted_at_ms`) from the
//! harness's. Both read the same machine's system clock, so differences
//! between them are meaningful to about a millisecond.
//! docs/EXPERIMENTS.md §Metrics restates every definition below.

use std::collections::BTreeMap;

use relay_core::api::AttemptView;
use relay_core::job::{AttemptOutcome, JobId};
use relay_core::workflow::{
    WorkflowEvent, WorkflowEventKind, WorkflowId, WorkflowState, WorkflowView,
};
use serde::Serialize;

use super::plan::RunPlan;
use super::stats::summarize;

// ---- what the runner collects ---------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct CollectedWorkflow {
    pub index: u32,
    pub id: Option<WorkflowId>,
    pub submitted_at_ms: i64,
    pub submit_error: Option<String>,
    pub view: Option<WorkflowView>,
    pub history: Vec<WorkflowEvent>,
    pub attempts: BTreeMap<JobId, Vec<AttemptView>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    WorkerSigkill,
    CoordinatorSigkill,
}

/// One injection the harness *intended*, and what actually happened.
/// Requested and executed are distinct on purpose: an injection that did
/// not happen is recorded with its reason, never silently dropped.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FailureRecord {
    pub failure_id: u32,
    pub kind: FailureKind,
    /// `executed`, `skipped_attempt_finished` (the attempt ended before the
    /// crash point), `skipped_attempt_never_ran`, `skipped_worker_unknown`,
    /// `not_observed` (coordinator crash condition never seen) or `error`.
    pub outcome: String,
    pub detail: String,
    // Worker crashes.
    pub workflow_index: Option<u32>,
    pub address: Option<String>,
    pub job_id: Option<JobId>,
    pub attempt: Option<u32>,
    pub worker_id: Option<String>,
    pub planned_after_ms: Option<u64>,
    // Coordinator crashes.
    pub crash_point: Option<String>,
    pub downtime_planned_ms: Option<u64>,
    /// When the harness decided to inject (target observed / condition met).
    pub requested_at_ms: i64,
    /// When the process was actually dead (after `kill` + `wait` returned).
    pub executed_at_ms: Option<i64>,
    /// Replacement worker spawned / coordinator healthy again.
    pub restarted_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Sample {
    pub t_ms: i64,
    /// `None` while the coordinator is unreachable.
    pub queued: Option<f64>,
    pub running: Option<f64>,
    pub retry_waiting: Option<f64>,
    pub due_runs: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct Collected {
    pub workflows: Vec<CollectedWorkflow>,
    pub failures: Vec<FailureRecord>,
    pub samples: Vec<Sample>,
    pub timed_out: bool,
    /// Worker slots: workers × concurrency.
    pub slots: u32,
}

/// Columns every row carries.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Key {
    pub variant: String,
    pub repetition: u32,
}

// ---- raw rows ----------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowRow {
    #[serde(flatten)]
    pub key: Key,
    pub workflow_index: u32,
    pub workflow_id: Option<WorkflowId>,
    /// `succeeded` / `failed` / `cancelled`, `running` (unfinished at the
    /// timeout) or `not_started` (submission failed).
    pub state: String,
    pub reason: Option<String>,
    pub submitted_at_ms: i64,
    /// `workflow_started` event.
    pub started_at_ms: Option<i64>,
    /// Terminal workflow event.
    pub finished_at_ms: Option<i64>,
    /// `finished_at − started_at` (coordinator clock).
    pub latency_ms: Option<i64>,
    /// Fan-out step: last branch `activity_completed` − first
    /// `activity_scheduled`. The tail-amplification measure.
    pub fanout_ms: Option<i64>,
    /// Sleep step: `timer_fired` event time − the persisted `fire_at`.
    /// Never negative if the timer invariant holds; coordinator downtime
    /// across the deadline shows up here.
    pub timer_lateness_ms: Option<i64>,
    pub activities: usize,
    pub attempts: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ActivityRow {
    #[serde(flatten)]
    pub key: Key,
    pub workflow_index: u32,
    pub address: String,
    pub job_id: Option<JobId>,
    pub planned_duration_ms: u64,
    pub crash_planned: bool,
    /// `completed` / `failed` / `cancelled`, `unfinished`, `unscheduled`.
    pub outcome: String,
    /// `activity_scheduled` event.
    pub scheduled_at_ms: Option<i64>,
    /// Attempt 1 `started_at`.
    pub first_started_at_ms: Option<i64>,
    /// The activity's terminal event.
    pub finished_at_ms: Option<i64>,
    /// `first_started_at − scheduled_at`: queue wait plus claim polling.
    pub scheduling_delay_ms: Option<i64>,
    /// `finished_at − scheduled_at`, for completed activities.
    pub latency_ms: Option<i64>,
    pub attempts: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AttemptRow {
    #[serde(flatten)]
    pub key: Key,
    pub workflow_index: u32,
    pub address: String,
    pub job_id: JobId,
    pub attempt: u32,
    pub worker_id: String,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    /// Attempt outcome, or `unfinished`.
    pub outcome: String,
    /// The harness killed this attempt's worker while it ran it.
    pub killed_by_injection: bool,
    /// Execution time spent: until the kill for killed attempts, else
    /// `finished_at − started_at`.
    pub execution_ms: Option<i64>,
    /// `execution_ms` if the attempt didn't produce the activity's result.
    pub wasted_execution_ms: Option<i64>,
    /// True when the end used is the reaper's `finished_at` rather than an
    /// observed kill: then execution ended *at or before* it.
    pub wasted_is_upper_bound: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FailureRow {
    #[serde(flatten)]
    pub key: Key,
    #[serde(flatten)]
    pub record: FailureRecord,
    /// Worker kill: killed attempt's `finished_at` (reaper expired it) − kill.
    pub detection_latency_ms: Option<i64>,
    /// Worker kill: next attempt's `started_at` − killed attempt's `finished_at`.
    pub reschedule_latency_ms: Option<i64>,
    /// Worker kill: activity's `activity_completed` − kill.
    pub recovery_latency_ms: Option<i64>,
    /// Worker kill: kill − killed attempt's `started_at`.
    pub lost_execution_ms: Option<i64>,
    /// Coordinator kill: healthy again − kill.
    pub downtime_ms: Option<i64>,
    /// Coordinator kill: first workflow event or attempt start at or after
    /// the restart − restart.
    pub first_progress_after_restart_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InvariantRow {
    #[serde(flatten)]
    pub key: Key,
    pub workflow_index: u32,
    pub invariant: &'static str,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunRow {
    #[serde(flatten)]
    pub key: Key,
    pub seed: u64,
    pub workflows: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub unfinished: usize,
    pub not_started: usize,
    pub timed_out: bool,
    /// `succeeded / workflows`.
    pub completion_rate: f64,
    /// Last terminal workflow event − first `workflow_started` (all runs).
    pub makespan_ms: Option<i64>,
    /// `succeeded / makespan`.
    pub throughput_wf_per_s: Option<f64>,
    pub activities_completed: usize,
    pub activities_failed: usize,
    pub attempts: usize,
    /// `attempts − activities with ≥ 1 attempt`: re-executions.
    pub extra_attempts: usize,
    pub wasted_execution_ms: i64,
    pub busy_execution_ms: i64,
    pub slots: u32,
    /// `busy_execution / (slots × makespan)`.
    pub utilization: Option<f64>,
    /// `slots × makespan − busy_execution`: slot time spent not executing.
    pub idle_slot_ms: Option<i64>,
    pub worker_kills_requested: usize,
    pub worker_kills_executed: usize,
    pub coordinator_kills_requested: usize,
    pub coordinator_kills_executed: usize,
    pub invariant_violations: usize,
    pub workflow_latency_p50_ms: Option<f64>,
    pub workflow_latency_p95_ms: Option<f64>,
    pub workflow_latency_p99_ms: Option<f64>,
    /// Over coordinator `/metrics` samples (`relay_jobs{state="queued"}`).
    pub queue_depth_max: Option<f64>,
    pub queue_depth_mean: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct Rows {
    pub runs: Vec<RunRow>,
    pub workflows: Vec<WorkflowRow>,
    pub activities: Vec<ActivityRow>,
    pub attempts: Vec<AttemptRow>,
    pub failures: Vec<FailureRow>,
    pub invariants: Vec<InvariantRow>,
    pub samples: Vec<(Key, Sample)>,
}

fn terminal_state(history: &[WorkflowEvent]) -> Option<(&'static str, Option<&str>, i64)> {
    history.iter().rev().find_map(|e| match &e.kind {
        WorkflowEventKind::WorkflowCompleted => Some(("succeeded", None, e.at_ms)),
        WorkflowEventKind::WorkflowFailed { reason } => {
            Some(("failed", Some(reason.as_str()), e.at_ms))
        }
        WorkflowEventKind::WorkflowCancelled { reason } => {
            Some(("cancelled", Some(reason.as_str()), e.at_ms))
        }
        _ => None,
    })
}

fn timer_lateness(history: &[WorkflowEvent]) -> Option<i64> {
    let fire_at = history.iter().find_map(|e| match e.kind {
        WorkflowEventKind::TimerScheduled { fire_at_ms, .. } => Some(fire_at_ms),
        _ => None,
    })?;
    let fired = history
        .iter()
        .find(|e| matches!(e.kind, WorkflowEventKind::TimerFired { .. }))?;
    Some(fired.at_ms - fire_at)
}

/// Turn one repetition's observations into rows.
pub fn build(key: &Key, plan: &RunPlan, c: &Collected) -> Rows {
    let mut rows = Rows::default();
    // Kills that actually happened, by (job, attempt).
    let kills: BTreeMap<(JobId, u32), i64> = c
        .failures
        .iter()
        .filter(|f| f.kind == FailureKind::WorkerSigkill && f.outcome == "executed")
        .filter_map(|f| Some(((f.job_id?, f.attempt?), f.executed_at_ms?)))
        .collect();

    let mut wf_latencies = Vec::new();
    let mut first_start: Option<i64> = None;
    let mut last_end: Option<i64> = None;
    let (mut busy, mut wasted) = (0_i64, 0_i64);
    let (mut acts_completed, mut acts_failed, mut total_attempts, mut extra) = (0, 0, 0, 0);
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();

    for (wf_plan, w) in plan.workflows.iter().zip(&c.workflows) {
        debug_assert_eq!(wf_plan.index, w.index, "collected in plan order");
        let h = &w.history;
        let started = h
            .first()
            .filter(|e| matches!(e.kind, WorkflowEventKind::WorkflowStarted { .. }))
            .map(|e| e.at_ms);
        let terminal = terminal_state(h);
        let state = match (&w.submit_error, terminal) {
            (Some(_), _) => "not_started",
            (None, Some((s, _, _))) => s,
            (None, None) => "running",
        };
        *counts.entry(state).or_default() += 1;
        if let Some(s) = started {
            first_start = Some(first_start.map_or(s, |f| f.min(s)));
        }
        if let Some((_, _, at)) = terminal {
            last_end = Some(last_end.map_or(at, |l| l.max(at)));
        }
        let latency = match (started, terminal) {
            (Some(s), Some((_, _, end))) => Some(end - s),
            _ => None,
        };
        if state == "succeeded" {
            wf_latencies.extend(latency.map(|l| l as f64));
        }

        // Per-activity events.
        let mut scheduled: BTreeMap<&str, (JobId, i64)> = BTreeMap::new();
        let mut ended: BTreeMap<&str, (&'static str, i64)> = BTreeMap::new();
        for e in h {
            match &e.kind {
                WorkflowEventKind::ActivityScheduled { activity, job_id } => {
                    scheduled
                        .entry(activity.as_str())
                        .or_insert((*job_id, e.at_ms));
                }
                WorkflowEventKind::ActivityCompleted { activity, .. } => {
                    ended.insert(activity.as_str(), ("completed", e.at_ms));
                }
                WorkflowEventKind::ActivityFailed { activity, .. } => {
                    ended.insert(activity.as_str(), ("failed", e.at_ms));
                }
                WorkflowEventKind::ActivityCancelled { activity, .. } => {
                    ended.insert(activity.as_str(), ("cancelled", e.at_ms));
                }
                _ => {}
            }
        }
        let fanout_first = scheduled
            .iter()
            .filter(|(a, _)| a.starts_with("fanout."))
            .map(|(_, (_, at))| *at)
            .min();
        let fanout_branches = wf_plan
            .activities
            .iter()
            .filter(|a| a.address.starts_with("fanout."))
            .count();
        let fanout_done: Vec<i64> = ended
            .iter()
            .filter(|(a, (o, _))| a.starts_with("fanout.") && *o == "completed")
            .map(|(_, (_, at))| *at)
            .collect();
        let fanout_ms = match fanout_first {
            Some(first) if fanout_done.len() == fanout_branches => {
                fanout_done.iter().max().map(|last| last - first)
            }
            _ => None,
        };

        let mut wf_attempts = 0;
        for a in &wf_plan.activities {
            let sched = scheduled.get(a.address.as_str()).copied();
            let attempts: &[AttemptView] = sched
                .and_then(|(j, _)| w.attempts.get(&j))
                .map_or(&[], Vec::as_slice);
            let end = ended.get(a.address.as_str()).copied();
            let outcome = match (sched, end) {
                (None, _) => "unscheduled",
                (Some(_), None) => "unfinished",
                (Some(_), Some((o, _))) => o,
            };
            match outcome {
                "completed" => acts_completed += 1,
                "failed" => acts_failed += 1,
                _ => {}
            }
            let first_started = attempts
                .iter()
                .find(|x| x.attempt == 1)
                .map(|x| x.started_at_ms);
            rows.activities.push(ActivityRow {
                key: key.clone(),
                workflow_index: wf_plan.index,
                address: a.address.clone(),
                job_id: sched.map(|(j, _)| j),
                planned_duration_ms: a.duration_ms,
                crash_planned: a.crash.is_some(),
                outcome: outcome.into(),
                scheduled_at_ms: sched.map(|(_, at)| at),
                first_started_at_ms: first_started,
                finished_at_ms: end.map(|(_, at)| at),
                scheduling_delay_ms: match (sched, first_started) {
                    (Some((_, s)), Some(f)) => Some(f - s),
                    _ => None,
                },
                latency_ms: match (sched, end) {
                    (Some((_, s)), Some(("completed", e))) => Some(e - s),
                    _ => None,
                },
                attempts: attempts.len(),
            });
            wf_attempts += attempts.len();
            total_attempts += attempts.len();
            extra += attempts.len().saturating_sub(1);

            for x in attempts {
                let killed_at = kills
                    .get(&(sched.map_or_else(JobId::generate, |(j, _)| j), x.attempt))
                    .copied();
                let end_ms = killed_at.or(x.finished_at_ms);
                let execution = end_ms.map(|e| (e - x.started_at_ms).max(0));
                let succeeded = x.outcome == Some(AttemptOutcome::Succeeded);
                let unfinished = x.outcome.is_none();
                let waste = (!succeeded && !unfinished).then_some(execution).flatten();
                busy += execution.unwrap_or(0);
                wasted += waste.unwrap_or(0);
                rows.attempts.push(AttemptRow {
                    key: key.clone(),
                    workflow_index: wf_plan.index,
                    address: a.address.clone(),
                    job_id: sched
                        .map(|(j, _)| j)
                        .expect("attempts imply a scheduled job"),
                    attempt: x.attempt,
                    worker_id: x.worker_id.clone(),
                    started_at_ms: x.started_at_ms,
                    finished_at_ms: x.finished_at_ms,
                    outcome: x
                        .outcome
                        .map_or_else(|| "unfinished".into(), |o| o.to_string()),
                    killed_by_injection: killed_at.is_some(),
                    execution_ms: execution,
                    wasted_execution_ms: waste,
                    wasted_is_upper_bound: waste.is_some()
                        && killed_at.is_none()
                        && x.outcome == Some(AttemptOutcome::LeaseExpired),
                });
            }
        }

        rows.workflows.push(WorkflowRow {
            key: key.clone(),
            workflow_index: wf_plan.index,
            workflow_id: w.id,
            state: state.into(),
            reason: terminal
                .and_then(|(_, r, _)| r.map(str::to_owned))
                .or_else(|| w.submit_error.clone()),
            submitted_at_ms: w.submitted_at_ms,
            started_at_ms: started,
            finished_at_ms: terminal.map(|(_, _, at)| at),
            latency_ms: latency,
            fanout_ms,
            timer_lateness_ms: timer_lateness(h),
            activities: wf_plan.activities.len(),
            attempts: wf_attempts,
        });
        rows.invariants
            .extend(check_invariants(key, wf_plan.index, w));
    }

    // Failures with derived latencies.
    let all_progress: Vec<i64> = c
        .workflows
        .iter()
        .flat_map(|w| {
            w.history
                .iter()
                .map(|e| e.at_ms)
                .chain(w.attempts.values().flatten().map(|a| a.started_at_ms))
        })
        .collect();
    for f in &c.failures {
        let mut row = FailureRow {
            key: key.clone(),
            record: f.clone(),
            detection_latency_ms: None,
            reschedule_latency_ms: None,
            recovery_latency_ms: None,
            lost_execution_ms: None,
            downtime_ms: None,
            first_progress_after_restart_ms: None,
        };
        match (f.kind, f.executed_at_ms) {
            (FailureKind::WorkerSigkill, Some(kill)) => {
                let w = f.workflow_index.and_then(|i| c.workflows.get(i as usize));
                let attempts = w.zip(f.job_id).and_then(|(w, j)| w.attempts.get(&j));
                let lost = attempts.and_then(|a| a.iter().find(|x| Some(x.attempt) == f.attempt));
                let next = attempts.and_then(|a| {
                    a.iter()
                        .find(|x| Some(x.attempt) == f.attempt.map(|n| n + 1))
                });
                if let Some(lost) = lost {
                    row.lost_execution_ms = Some(kill - lost.started_at_ms);
                    if lost.outcome == Some(AttemptOutcome::LeaseExpired) {
                        row.detection_latency_ms = lost.finished_at_ms.map(|t| t - kill);
                        row.reschedule_latency_ms = lost
                            .finished_at_ms
                            .zip(next)
                            .map(|(t, n)| n.started_at_ms - t);
                    }
                }
                row.recovery_latency_ms = w.zip(f.address.as_deref()).and_then(|(w, addr)| {
                    w.history.iter().find_map(|e| match &e.kind {
                        WorkflowEventKind::ActivityCompleted { activity, .. }
                            if activity == addr =>
                        {
                            Some(e.at_ms - kill)
                        }
                        _ => None,
                    })
                });
            }
            (FailureKind::CoordinatorSigkill, Some(kill)) => {
                row.downtime_ms = f.restarted_at_ms.map(|r| r - kill);
                row.first_progress_after_restart_ms = f.restarted_at_ms.and_then(|r| {
                    all_progress
                        .iter()
                        .filter(|t| **t >= r)
                        .min()
                        .map(|t| t - r)
                });
            }
            _ => {}
        }
        rows.failures.push(row);
    }

    let queued: Vec<f64> = c.samples.iter().filter_map(|s| s.queued).collect();
    let makespan = first_start.zip(last_end).map(|(s, e)| e - s);
    let succeeded = counts.get("succeeded").copied().unwrap_or(0);
    let lat = summarize(wf_latencies);
    let kills_of = |k: FailureKind| c.failures.iter().filter(move |f| f.kind == k);
    rows.runs.push(RunRow {
        key: key.clone(),
        seed: plan.seed,
        workflows: c.workflows.len(),
        succeeded,
        failed: counts.get("failed").copied().unwrap_or(0),
        cancelled: counts.get("cancelled").copied().unwrap_or(0),
        unfinished: counts.get("running").copied().unwrap_or(0),
        not_started: counts.get("not_started").copied().unwrap_or(0),
        timed_out: c.timed_out,
        completion_rate: if c.workflows.is_empty() {
            0.0
        } else {
            succeeded as f64 / c.workflows.len() as f64
        },
        makespan_ms: makespan,
        throughput_wf_per_s: makespan
            .filter(|m| *m > 0)
            .map(|m| succeeded as f64 / (m as f64 / 1000.0)),
        activities_completed: acts_completed,
        activities_failed: acts_failed,
        attempts: total_attempts,
        extra_attempts: extra,
        wasted_execution_ms: wasted,
        busy_execution_ms: busy,
        slots: c.slots,
        utilization: makespan
            .filter(|m| *m > 0 && c.slots > 0)
            .map(|m| busy as f64 / (f64::from(c.slots) * m as f64)),
        idle_slot_ms: makespan.map(|m| i64::from(c.slots) * m - busy),
        worker_kills_requested: kills_of(FailureKind::WorkerSigkill).count(),
        worker_kills_executed: kills_of(FailureKind::WorkerSigkill)
            .filter(|f| f.outcome == "executed")
            .count(),
        coordinator_kills_requested: kills_of(FailureKind::CoordinatorSigkill).count(),
        coordinator_kills_executed: kills_of(FailureKind::CoordinatorSigkill)
            .filter(|f| f.outcome == "executed")
            .count(),
        invariant_violations: rows.invariants.iter().filter(|i| !i.ok).count(),
        workflow_latency_p50_ms: lat.p50,
        workflow_latency_p95_ms: lat.p95,
        workflow_latency_p99_ms: lat.p99,
        queue_depth_max: queued.iter().copied().reduce(f64::max),
        queue_depth_mean: (!queued.is_empty())
            .then(|| queued.iter().sum::<f64>() / queued.len() as f64),
    });
    rows.samples = c.samples.iter().map(|s| (key.clone(), s.clone())).collect();
    rows
}

/// Correctness checks on one run's observed history and attempts. These
/// must hold under any failure; a violation fails the experiment.
pub fn check_invariants(key: &Key, index: u32, w: &CollectedWorkflow) -> Vec<InvariantRow> {
    let mut out = Vec::new();
    let mut check = |invariant: &'static str, ok: bool, detail: String| {
        out.push(InvariantRow {
            key: key.clone(),
            workflow_index: index,
            invariant,
            ok,
            detail,
        });
    };
    if w.submit_error.is_some() {
        return out;
    }
    let h = &w.history;

    // I1: seq is 1, 2, 3, ... and time never runs backwards.
    let contiguous = h.iter().enumerate().all(|(i, e)| e.seq == i as u64 + 1);
    let monotone = h.windows(2).all(|p| p[0].at_ms <= p[1].at_ms);
    check(
        "seq_contiguous_and_time_monotone",
        contiguous && monotone,
        format!("{} events", h.len()),
    );

    // I2: each activity scheduled at most once (retries live inside its job).
    let mut scheduled: BTreeMap<&str, usize> = BTreeMap::new();
    let mut job_of: BTreeMap<&str, JobId> = BTreeMap::new();
    for e in h {
        if let WorkflowEventKind::ActivityScheduled { activity, job_id } = &e.kind {
            *scheduled.entry(activity).or_default() += 1;
            job_of.insert(activity, *job_id);
        }
    }
    let twice: Vec<_> = scheduled
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(a, _)| *a)
        .collect();
    check(
        "each_activity_scheduled_once",
        twice.is_empty(),
        format!("{twice:?}"),
    );

    // I3: no attempt of an activity starts after its activity_completed:
    // a completed activity is never executed again.
    let mut reran = Vec::new();
    // I4: a completed activity has exactly one succeeded attempt.
    let mut bad_success = Vec::new();
    for e in h {
        if let WorkflowEventKind::ActivityCompleted { activity, job_id } = &e.kind {
            if let Some(attempts) = w.attempts.get(job_id) {
                if attempts.iter().any(|a| a.started_at_ms > e.at_ms) {
                    reran.push(activity.clone());
                }
                let ok = attempts
                    .iter()
                    .filter(|a| a.outcome == Some(AttemptOutcome::Succeeded))
                    .count();
                if ok != 1 {
                    bad_success.push(format!("{activity}: {ok}"));
                }
            }
        }
    }
    check(
        "completed_activities_never_rerun",
        reran.is_empty(),
        format!("{reran:?}"),
    );
    check(
        "one_success_per_completed_activity",
        bad_success.is_empty(),
        format!("{bad_success:?}"),
    );

    // I5: timers scheduled once, fired once, never early.
    // step → (scheduled, fired, fire_at, fired_at)
    type Timer = (usize, usize, Option<i64>, Option<i64>);
    let mut timers: BTreeMap<&str, Timer> = BTreeMap::new();
    for e in h {
        match &e.kind {
            WorkflowEventKind::TimerScheduled { step, fire_at_ms } => {
                let t = timers.entry(step).or_default();
                t.0 += 1;
                t.2 = Some(*fire_at_ms);
            }
            WorkflowEventKind::TimerFired { step } => {
                let t = timers.entry(step).or_default();
                t.1 += 1;
                t.3 = Some(e.at_ms);
            }
            _ => {}
        }
    }
    let bad_timers: Vec<_> = timers
        .iter()
        .filter(|(_, (s, f, at, fired))| {
            *s > 1 || *f > 1 || matches!((at, fired), (Some(a), Some(fd)) if fd < a)
        })
        .map(|(n, _)| *n)
        .collect();
    check(
        "timers_once_and_never_early",
        bad_timers.is_empty(),
        format!("{bad_timers:?}"),
    );

    // I6: a terminal workflow event, if any, is the last event; the view agrees.
    let terminal_at = h.iter().position(|e| {
        matches!(
            e.kind,
            WorkflowEventKind::WorkflowCompleted
                | WorkflowEventKind::WorkflowFailed { .. }
                | WorkflowEventKind::WorkflowCancelled { .. }
        )
    });
    let view_state = w.view.as_ref().map(|v| v.state);
    let consistent = match (terminal_at, view_state) {
        (Some(i), Some(s)) => i == h.len() - 1 && s.is_terminal(),
        (None, Some(s)) => s == WorkflowState::Running,
        (_, None) => true,
    };
    check(
        "terminal_event_last_and_view_agrees",
        consistent,
        format!("{view_state:?}"),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experiment::plan::{ActivityPlan, PlannedCrash, WorkflowPlan};
    use relay_core::workflow::WorkflowEventKind as K;

    const T: i64 = 1_000_000;

    fn key() -> Key {
        Key {
            variant: "v".into(),
            repetition: 1,
        }
    }

    fn ev(seq: u64, at_ms: i64, kind: K) -> WorkflowEvent {
        WorkflowEvent { seq, at_ms, kind }
    }

    fn attempt(
        n: u32,
        start: i64,
        end: Option<i64>,
        outcome: Option<AttemptOutcome>,
    ) -> AttemptView {
        AttemptView {
            attempt: n,
            worker_id: format!("w{n}-x"),
            started_at_ms: start,
            finished_at_ms: end,
            outcome,
            error: None,
        }
    }

    /// One workflow: a single-branch fan-out whose first attempt was killed
    /// at T+1000, reaped at T+2500, retried from T+2600, done at T+4600.
    fn crashed_run() -> (RunPlan, Collected, JobId) {
        let job = JobId::generate();
        let plan = RunPlan {
            seed: 1,
            workflows: vec![WorkflowPlan {
                index: 0,
                activities: vec![ActivityPlan {
                    address: "fanout.b00".into(),
                    duration_ms: 2000,
                    crash: Some(PlannedCrash {
                        attempt: 1,
                        after_ms: 1000,
                    }),
                }],
            }],
        };
        let history = vec![
            ev(
                1,
                T,
                K::WorkflowStarted {
                    input: serde_json::Value::Null,
                },
            ),
            ev(
                2,
                T,
                K::ActivityScheduled {
                    activity: "fanout.b00".into(),
                    job_id: job,
                },
            ),
            ev(
                3,
                T + 4600,
                K::ActivityCompleted {
                    activity: "fanout.b00".into(),
                    job_id: job,
                },
            ),
            ev(4, T + 4601, K::WorkflowCompleted),
        ];
        let attempts = vec![
            attempt(
                1,
                T + 50,
                Some(T + 2500),
                Some(AttemptOutcome::LeaseExpired),
            ),
            attempt(2, T + 2600, Some(T + 4600), Some(AttemptOutcome::Succeeded)),
        ];
        let c = Collected {
            workflows: vec![CollectedWorkflow {
                index: 0,
                id: Some(WorkflowId::generate()),
                submitted_at_ms: T - 5,
                history,
                attempts: BTreeMap::from([(job, attempts)]),
                ..Default::default()
            }],
            failures: vec![FailureRecord {
                failure_id: 0,
                kind: FailureKind::WorkerSigkill,
                outcome: "executed".into(),
                detail: String::new(),
                workflow_index: Some(0),
                address: Some("fanout.b00".into()),
                job_id: Some(job),
                attempt: Some(1),
                worker_id: Some("w1-x".into()),
                planned_after_ms: Some(1000),
                crash_point: None,
                downtime_planned_ms: None,
                requested_at_ms: T + 1040,
                executed_at_ms: Some(T + 1050),
                restarted_at_ms: Some(T + 1100),
            }],
            samples: vec![Sample {
                t_ms: T,
                queued: Some(3.0),
                running: Some(1.0),
                retry_waiting: None,
                due_runs: Some(0.0),
            }],
            timed_out: false,
            slots: 2,
        };
        (plan, c, job)
    }

    #[test]
    fn a_killed_attempt_yields_every_recovery_metric() {
        let (plan, c, _) = crashed_run();
        let rows = build(&key(), &plan, &c);
        let f = &rows.failures[0];
        assert_eq!(f.lost_execution_ms, Some(1000), "kill − attempt start");
        assert_eq!(f.detection_latency_ms, Some(1450), "reaped − kill");
        assert_eq!(
            f.reschedule_latency_ms,
            Some(100),
            "attempt 2 start − reaped"
        );
        assert_eq!(
            f.recovery_latency_ms,
            Some(3550),
            "activity completed − kill"
        );

        let a = &rows.attempts;
        assert!(a[0].killed_by_injection && !a[0].wasted_is_upper_bound);
        assert_eq!(
            (a[0].execution_ms, a[0].wasted_execution_ms),
            (Some(1000), Some(1000)),
            "until the kill, not the reap"
        );
        assert_eq!(
            (a[1].execution_ms, a[1].wasted_execution_ms),
            (Some(2000), None),
            "the successful attempt isn't waste"
        );

        let r = &rows.runs[0];
        assert_eq!(
            (r.succeeded, r.completion_rate, r.extra_attempts),
            (1, 1.0, 1)
        );
        assert_eq!((r.wasted_execution_ms, r.busy_execution_ms), (1000, 3000));
        assert_eq!(r.makespan_ms, Some(4601));
        assert!((r.utilization.unwrap() - 3000.0 / (2.0 * 4601.0)).abs() < 1e-9);
        assert_eq!((r.worker_kills_requested, r.worker_kills_executed), (1, 1));
        assert_eq!(r.invariant_violations, 0);

        let act = &rows.activities[0];
        assert_eq!(
            (act.scheduling_delay_ms, act.latency_ms, act.attempts),
            (Some(50), Some(4600), 2)
        );
        assert_eq!(rows.workflows[0].fanout_ms, Some(4600));
        assert_eq!(rows.workflows[0].latency_ms, Some(4601));
    }

    #[test]
    fn an_unobserved_lease_expiry_is_an_upper_bound_and_skipped_kills_count_as_requested_only() {
        let (plan, mut c, _) = crashed_run();
        c.failures[0].outcome = "skipped_attempt_finished".into();
        c.failures[0].executed_at_ms = None;
        let rows = build(&key(), &plan, &c);
        let a = &rows.attempts[0];
        assert!(!a.killed_by_injection);
        assert_eq!(
            a.wasted_execution_ms,
            Some(2450),
            "start → reaper's finished_at"
        );
        assert!(a.wasted_is_upper_bound);
        let r = &rows.runs[0];
        assert_eq!((r.worker_kills_requested, r.worker_kills_executed), (1, 0));
        assert_eq!(
            rows.failures[0].detection_latency_ms, None,
            "no kill, no detection latency"
        );
    }

    #[test]
    fn invariants_catch_a_rerun_completed_activity_and_bad_timers() {
        let (_, mut c, job) = crashed_run();
        let w = &mut c.workflows[0];
        w.attempts
            .get_mut(&job)
            .unwrap()
            .push(attempt(3, T + 4700, None, None));
        w.history.insert(
            3,
            ev(
                4,
                T + 4600,
                K::TimerScheduled {
                    step: "pause".into(),
                    fire_at_ms: T + 9000,
                },
            ),
        );
        w.history.insert(
            4,
            ev(
                5,
                T + 4650,
                K::TimerFired {
                    step: "pause".into(),
                },
            ),
        );
        w.history[5].seq = 6; // keep seq contiguous and time monotone for this test
        w.history[5].at_ms = T + 4700;
        let rows = check_invariants(&key(), 0, w);
        let failed: Vec<_> = rows.iter().filter(|r| !r.ok).map(|r| r.invariant).collect();
        assert_eq!(
            failed,
            [
                "completed_activities_never_rerun",
                "timers_once_and_never_early"
            ]
        );
    }

    #[test]
    fn invariants_catch_gaps_and_double_scheduling() {
        let (_, mut c, job) = crashed_run();
        let w = &mut c.workflows[0];
        w.history.push(ev(
            9,
            T + 5000,
            K::ActivityScheduled {
                activity: "fanout.b00".into(),
                job_id: job,
            },
        ));
        let failed: Vec<_> = check_invariants(&key(), 0, w)
            .into_iter()
            .filter(|r| !r.ok)
            .map(|r| r.invariant)
            .collect();
        assert!(failed.contains(&"seq_contiguous_and_time_monotone"));
        assert!(failed.contains(&"each_activity_scheduled_once"));
        assert!(failed.contains(&"terminal_event_last_and_view_agrees") || failed.len() == 2);
    }

    #[test]
    fn unfinished_and_unstarted_workflows_are_counted_not_dropped() {
        let (plan, mut c, _) = crashed_run();
        let mut plan2 = plan.clone();
        plan2.workflows.push(WorkflowPlan {
            index: 1,
            activities: plan.workflows[0].activities.clone(),
        });
        plan2.workflows.push(WorkflowPlan {
            index: 2,
            activities: plan.workflows[0].activities.clone(),
        });
        let mut running = c.workflows[0].clone();
        running.index = 1;
        running.history.truncate(2);
        running.attempts.clear();
        c.workflows.push(running);
        c.workflows.push(CollectedWorkflow {
            index: 2,
            submit_error: Some("coordinator unreachable".into()),
            ..Default::default()
        });
        c.timed_out = true;
        let rows = build(&key(), &plan2, &c);
        let r = &rows.runs[0];
        assert_eq!(
            (r.succeeded, r.unfinished, r.not_started, r.workflows),
            (1, 1, 1, 3)
        );
        assert!((r.completion_rate - 1.0 / 3.0).abs() < 1e-9);
        let states: Vec<_> = rows.workflows.iter().map(|w| w.state.as_str()).collect();
        assert_eq!(states, ["succeeded", "running", "not_started"]);
        assert_eq!(rows.activities[1].outcome, "unfinished");
        assert_eq!(rows.activities[2].outcome, "unscheduled");
    }

    #[test]
    fn coordinator_kills_report_downtime_and_first_progress() {
        let (plan, mut c, _) = crashed_run();
        c.failures = vec![FailureRecord {
            failure_id: 0,
            kind: FailureKind::CoordinatorSigkill,
            outcome: "executed".into(),
            detail: String::new(),
            workflow_index: None,
            address: None,
            job_id: None,
            attempt: None,
            worker_id: None,
            planned_after_ms: None,
            crash_point: Some("activities_running".into()),
            downtime_planned_ms: Some(1000),
            requested_at_ms: T + 1990,
            executed_at_ms: Some(T + 2000),
            restarted_at_ms: Some(T + 2300),
        }];
        let rows = build(&key(), &plan, &c);
        let f = &rows.failures[0];
        assert_eq!(f.downtime_ms, Some(300));
        assert_eq!(
            f.first_progress_after_restart_ms,
            Some(300),
            "attempt 2 started at T+2600"
        );
        assert_eq!(rows.runs[0].coordinator_kills_executed, 1);
    }
}
