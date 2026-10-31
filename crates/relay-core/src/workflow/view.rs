//! HTTP bodies for `/v1/workflows` and the per-step view of a run.
//!
//! The step view is what `GET /v1/workflows/{id}` returns and what a
//! dashboard renders:
//!
//! ```text
//! plan        done        plan            succeeded
//! research    active      research.a      succeeded
//!                         research.b      running
//!                         research.c      retrying
//! aggregate   pending     aggregate       pending
//! ```
//!
//! It's derived, never stored: the folded history says which activities are
//! scheduled and how they ended, and the jobs table says where an
//! outstanding one is right now (queued, running, retrying).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::clock::Timestamp;
use crate::job::{JobId, JobState};
use crate::workflow::definition::{StepBody, Workflow};
use crate::workflow::history::{ActivityStatus, RunState, RunStatus, TimerState, WorkflowEvent};
use crate::workflow::run::{WorkflowId, WorkflowState};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowView {
    pub id: WorkflowId,
    pub name: String,
    pub state: WorkflowState,
    /// Why a failed or cancelled run ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub created_at_ms: Timestamp,
    pub updated_at_ms: Timestamp,
    pub finished_at_ms: Option<Timestamp>,
    /// When the coordinator will next look at the run (a timer deadline, or
    /// "now" while a decision is pending). `None` while it waits on
    /// activities or a signal, or once it's over.
    pub wake_at_ms: Option<Timestamp>,
    pub steps: Vec<StepView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Activity,
    Parallel,
    Sleep,
    WaitSignal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    /// Not reached yet (or never reached, if the run ended earlier).
    Pending,
    /// The step the run is on now.
    Active,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepView {
    pub name: String,
    pub kind: StepKind,
    pub status: StepStatus,
    /// Empty for `sleep` and `wait_signal`.
    pub activities: Vec<ActivityView>,
    /// `sleep`: the persisted deadline, once the timer is armed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fire_at_ms: Option<Timestamp>,
    /// `wait_signal`: the awaited name, and whether it has arrived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_received: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityViewStatus {
    /// No job yet.
    Pending,
    Queued,
    Running,
    /// A retryable failure; waiting out the backoff.
    Retrying,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActivityView {
    /// `step` or `step.branch`.
    pub address: String,
    pub kind: String,
    pub status: ActivityViewStatus,
    /// The activity's job, once scheduled: its attempts are at
    /// `GET /v1/jobs/{job_id}/attempts`.
    pub job_id: Option<JobId>,
    pub attempt: Option<u32>,
    pub max_attempts: Option<u32>,
}

/// What the view needs from an activity's job row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobProgress {
    pub state: JobState,
    pub attempt: u32,
    pub max_attempts: u32,
}

/// The run row's columns the view shows.
#[derive(Debug, Clone, PartialEq)]
pub struct RunMeta {
    pub id: WorkflowId,
    pub name: String,
    pub state: WorkflowState,
    pub created_at_ms: Timestamp,
    pub updated_at_ms: Timestamp,
    pub finished_at_ms: Option<Timestamp>,
    pub wake_at_ms: Option<Timestamp>,
}

/// Pure: run row + definition + folded history + job progress → view.
pub fn build_view(
    meta: RunMeta,
    workflow: &Workflow,
    state: &RunState,
    jobs: &BTreeMap<JobId, JobProgress>,
) -> WorkflowView {
    let cursor = state.cursor(workflow);
    let running = state.status == RunStatus::Running;
    let steps = workflow
        .steps
        .iter()
        .enumerate()
        .map(|(index, step)| {
            let activities: Vec<ActivityView> = step
                .activities()
                .iter()
                .map(|a| {
                    let held = state.activities.get(&a.address);
                    let job = held.and_then(|h| jobs.get(&h.job_id));
                    let status = match held.map(|h| &h.status) {
                        None => ActivityViewStatus::Pending,
                        Some(ActivityStatus::Completed) => ActivityViewStatus::Succeeded,
                        Some(ActivityStatus::Failed { .. }) => ActivityViewStatus::Failed,
                        Some(ActivityStatus::Cancelled) => ActivityViewStatus::Cancelled,
                        Some(ActivityStatus::Scheduled) => match job.map(|j| j.state) {
                            Some(JobState::Running) => ActivityViewStatus::Running,
                            Some(JobState::RetryWaiting) => ActivityViewStatus::Retrying,
                            // Queued, or a job row we couldn't read.
                            _ => ActivityViewStatus::Queued,
                        },
                    };
                    ActivityView {
                        address: a.address.clone(),
                        kind: a.kind.clone(),
                        status,
                        job_id: held.map(|h| h.job_id),
                        attempt: job.map(|j| j.attempt),
                        max_attempts: job.map(|j| j.max_attempts),
                    }
                })
                .collect();

            let has = |s: ActivityViewStatus| activities.iter().any(|a| a.status == s);
            let status = if state.step_done(workflow, index) {
                StepStatus::Done
            } else if has(ActivityViewStatus::Failed) {
                StepStatus::Failed
            } else if has(ActivityViewStatus::Cancelled) {
                StepStatus::Cancelled
            } else if running && cursor == Some(index) {
                StepStatus::Active
            } else {
                StepStatus::Pending
            };

            let (kind, fire_at_ms, signal, signal_received) = match &step.body {
                StepBody::Activity(_) => (StepKind::Activity, None, None, None),
                StepBody::Parallel(_) => (StepKind::Parallel, None, None, None),
                StepBody::Sleep { .. } => {
                    let fire_at = state.timers.get(&step.name).map(|t| match t {
                        TimerState::Scheduled { fire_at_ms } | TimerState::Fired { fire_at_ms } => {
                            *fire_at_ms
                        }
                    });
                    (StepKind::Sleep, fire_at, None, None)
                }
                StepBody::WaitSignal { signal } => (
                    StepKind::WaitSignal,
                    None,
                    Some(signal.clone()),
                    Some(state.signals.contains_key(signal)),
                ),
            };
            StepView {
                name: step.name.clone(),
                kind,
                status,
                activities,
                fire_at_ms,
                signal,
                signal_received,
            }
        })
        .collect();

    let reason = match &state.status {
        RunStatus::Failed { reason } | RunStatus::Cancelled { reason } => Some(reason.clone()),
        RunStatus::Running | RunStatus::Succeeded => None,
    };
    WorkflowView {
        id: meta.id,
        name: meta.name,
        state: meta.state,
        reason,
        created_at_ms: meta.created_at_ms,
        updated_at_ms: meta.updated_at_ms,
        finished_at_ms: meta.finished_at_ms,
        wake_at_ms: meta.wake_at_ms,
        steps,
    }
}

/// Body of `POST /v1/workflows` responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartWorkflowResponse {
    /// `false` when an idempotency key matched an existing run.
    pub created: bool,
    pub workflow: WorkflowView,
}

/// Body of `GET /v1/workflows/{id}/history`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowHistoryView {
    pub id: WorkflowId,
    pub events: Vec<WorkflowEvent>,
}

/// Body of `POST /v1/workflows/{id}/signals/{name}` (optional; empty = null payload).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalRequest {
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalResponse {
    /// `false`: the latch was already set with this payload (a retry).
    pub recorded: bool,
}

/// Body of `POST /v1/workflows/{id}/cancel` (optional).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelWorkflowRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{WorkflowDefinition, WorkflowEventKind, WorkflowLimits, fold};
    use serde_json::json;

    const T0: Timestamp = 1_700_000_000_000;

    fn meta(state: WorkflowState) -> RunMeta {
        RunMeta {
            id: WorkflowId::generate(),
            name: "research".into(),
            state,
            created_at_ms: T0,
            updated_at_ms: T0,
            finished_at_ms: None,
            wake_at_ms: None,
        }
    }

    fn workflow() -> Workflow {
        let def: WorkflowDefinition =
            serde_json::from_value(json!({ "name": "research", "steps": [
            { "name": "plan", "activity": { "kind": "mock", "payload": {} } },
            { "name": "research", "parallel": [
                { "name": "a", "kind": "mock", "payload": {} },
                { "name": "b", "kind": "mock", "payload": {} },
                { "name": "c", "kind": "mock", "payload": {} } ] },
            { "name": "wait", "sleep_ms": 1000 },
            { "name": "gate", "wait_signal": "go" },
        ] }))
            .unwrap();
        Workflow::from_definition(&def, &WorkflowLimits::default()).unwrap()
    }

    fn ev(seq: u64, kind: WorkflowEventKind) -> WorkflowEvent {
        WorkflowEvent {
            seq,
            at_ms: T0,
            kind,
        }
    }

    #[test]
    fn a_run_mid_fan_out_renders_each_step_and_branch() {
        let wf = workflow();
        let ids: Vec<JobId> = (0..4).map(|_| JobId::generate()).collect();
        let history = vec![
            ev(1, WorkflowEventKind::WorkflowStarted { input: Value::Null }),
            ev(
                2,
                WorkflowEventKind::ActivityScheduled {
                    activity: "plan".into(),
                    job_id: ids[0],
                },
            ),
            ev(
                3,
                WorkflowEventKind::ActivityCompleted {
                    activity: "plan".into(),
                    job_id: ids[0],
                },
            ),
            ev(
                4,
                WorkflowEventKind::ActivityScheduled {
                    activity: "research.a".into(),
                    job_id: ids[1],
                },
            ),
            ev(
                5,
                WorkflowEventKind::ActivityScheduled {
                    activity: "research.b".into(),
                    job_id: ids[2],
                },
            ),
            ev(
                6,
                WorkflowEventKind::ActivityScheduled {
                    activity: "research.c".into(),
                    job_id: ids[3],
                },
            ),
            ev(
                7,
                WorkflowEventKind::ActivityCompleted {
                    activity: "research.a".into(),
                    job_id: ids[1],
                },
            ),
        ];
        let state = fold(&wf, &history).unwrap();
        let progress = |state, attempt| JobProgress {
            state,
            attempt,
            max_attempts: 3,
        };
        let jobs = BTreeMap::from([
            (ids[0], progress(JobState::Succeeded, 1)),
            (ids[1], progress(JobState::Succeeded, 1)),
            (ids[2], progress(JobState::Running, 1)),
            (ids[3], progress(JobState::RetryWaiting, 2)),
        ]);
        let view = build_view(meta(WorkflowState::Running), &wf, &state, &jobs);

        let steps: Vec<_> = view
            .steps
            .iter()
            .map(|s| (s.name.as_str(), s.status))
            .collect();
        assert_eq!(
            steps,
            [
                ("plan", StepStatus::Done),
                ("research", StepStatus::Active),
                ("wait", StepStatus::Pending),
                ("gate", StepStatus::Pending)
            ]
        );
        let branches: Vec<_> = view.steps[1]
            .activities
            .iter()
            .map(|a| (a.address.as_str(), a.status, a.attempt))
            .collect();
        assert_eq!(
            branches,
            [
                ("research.a", ActivityViewStatus::Succeeded, Some(1)),
                ("research.b", ActivityViewStatus::Running, Some(1)),
                ("research.c", ActivityViewStatus::Retrying, Some(2)),
            ]
        );
        assert_eq!(
            (
                view.steps[3].signal.as_deref(),
                view.steps[3].signal_received
            ),
            (Some("go"), Some(false))
        );
        assert_eq!(view.reason, None);
    }

    #[test]
    fn a_failed_run_shows_where_and_why() {
        let wf = workflow();
        let j = JobId::generate();
        let history = vec![
            ev(1, WorkflowEventKind::WorkflowStarted { input: Value::Null }),
            ev(
                2,
                WorkflowEventKind::ActivityScheduled {
                    activity: "plan".into(),
                    job_id: j,
                },
            ),
            ev(
                3,
                WorkflowEventKind::ActivityFailed {
                    activity: "plan".into(),
                    job_id: j,
                    error: "boom".into(),
                },
            ),
            ev(
                4,
                WorkflowEventKind::WorkflowFailed {
                    reason: "activity \"plan\" failed: boom".into(),
                },
            ),
        ];
        let state = fold(&wf, &history).unwrap();
        let view = build_view(meta(WorkflowState::Failed), &wf, &state, &BTreeMap::new());
        assert_eq!(view.steps[0].status, StepStatus::Failed);
        assert_eq!(
            view.steps[1].status,
            StepStatus::Pending,
            "never reached; not 'active' on a finished run"
        );
        assert_eq!(
            view.steps[1].activities[0].status,
            ActivityViewStatus::Pending
        );
        assert_eq!(
            view.reason.as_deref(),
            Some("activity \"plan\" failed: boom")
        );
    }

    #[test]
    fn bodies_have_a_stable_wire_shape() {
        let req: SignalRequest = serde_json::from_value(json!({})).unwrap();
        assert_eq!(req.payload, Value::Null);
        assert!(serde_json::from_value::<SignalRequest>(json!({ "payload": 1, "x": 2 })).is_err());
        let cancel: CancelWorkflowRequest =
            serde_json::from_value(json!({ "reason": "r" })).unwrap();
        assert_eq!(cancel.reason.as_deref(), Some("r"));
        let step = StepView {
            name: "s".into(),
            kind: StepKind::WaitSignal,
            status: StepStatus::Active,
            activities: vec![],
            fire_at_ms: None,
            signal: Some("go".into()),
            signal_received: Some(false),
        };
        assert_eq!(
            serde_json::to_value(&step).unwrap(),
            json!({ "name": "s", "kind": "wait_signal", "status": "active", "activities": [],
                    "signal": "go", "signal_received": false })
        );
    }
}
