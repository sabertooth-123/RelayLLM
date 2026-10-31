//! Workflow events and the pure fold from a history to a [`RunState`].
//!
//! `fold` is total over *possible* histories and rejects impossible ones
//! with a typed [`HistoryError`] instead of guessing. The coordinator only
//! ever appends events that the decider (or a terminal job transition)
//! produced, so a `HistoryError` means a bug or a corrupted database, and the
//! run must not advance. Nothing here does I/O or reads a clock.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::clock::Timestamp;
use crate::job::JobId;
use crate::workflow::definition::{StepBody, Workflow};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEvent {
    /// 1, 2, 3, ... per run, in commit order.
    pub seq: u64,
    /// Coordinator clock at commit; non-decreasing within a run.
    pub at_ms: Timestamp,
    #[serde(flatten)]
    pub kind: WorkflowEventKind,
}

/// Mirrors the `type` column of `workflow_events` (migration 0005).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowEventKind {
    WorkflowStarted {
        input: Value,
    },
    ActivityScheduled {
        activity: String,
        job_id: JobId,
    },
    ActivityCompleted {
        activity: String,
        job_id: JobId,
    },
    ActivityFailed {
        activity: String,
        job_id: JobId,
        error: String,
    },
    ActivityCancelled {
        activity: String,
        job_id: JobId,
    },
    TimerScheduled {
        step: String,
        fire_at_ms: Timestamp,
    },
    TimerFired {
        step: String,
    },
    SignalReceived {
        name: String,
        payload: Value,
    },
    WorkflowCompleted,
    WorkflowFailed {
        reason: String,
    },
    WorkflowCancelled {
        reason: String,
    },
}

impl WorkflowEventKind {
    /// The `type` column value.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::WorkflowStarted { .. } => "workflow_started",
            Self::ActivityScheduled { .. } => "activity_scheduled",
            Self::ActivityCompleted { .. } => "activity_completed",
            Self::ActivityFailed { .. } => "activity_failed",
            Self::ActivityCancelled { .. } => "activity_cancelled",
            Self::TimerScheduled { .. } => "timer_scheduled",
            Self::TimerFired { .. } => "timer_fired",
            Self::SignalReceived { .. } => "signal_received",
            Self::WorkflowCompleted => "workflow_completed",
            Self::WorkflowFailed { .. } => "workflow_failed",
            Self::WorkflowCancelled { .. } => "workflow_cancelled",
        }
    }

    /// The `(step, job_id, data)` columns. `step` holds the activity address
    /// for activity events and the step name for timers.
    pub fn columns(&self) -> (Option<&str>, Option<JobId>, Value) {
        let empty = || Value::Object(Map::new());
        match self {
            Self::WorkflowStarted { input } => (None, None, json_obj([("input", input.clone())])),
            Self::ActivityScheduled { activity, job_id }
            | Self::ActivityCompleted { activity, job_id }
            | Self::ActivityCancelled { activity, job_id } => {
                (Some(activity), Some(*job_id), empty())
            }
            Self::ActivityFailed {
                activity,
                job_id,
                error,
            } => (
                Some(activity),
                Some(*job_id),
                json_obj([("error", Value::from(error.clone()))]),
            ),
            Self::TimerScheduled { step, fire_at_ms } => (
                Some(step),
                None,
                json_obj([("fire_at_ms", Value::from(*fire_at_ms))]),
            ),
            Self::TimerFired { step } => (Some(step), None, empty()),
            Self::SignalReceived { name, payload } => (
                None,
                None,
                json_obj([
                    ("name", Value::from(name.clone())),
                    ("payload", payload.clone()),
                ]),
            ),
            Self::WorkflowCompleted => (None, None, empty()),
            Self::WorkflowFailed { reason } | Self::WorkflowCancelled { reason } => (
                None,
                None,
                json_obj([("reason", Value::from(reason.clone()))]),
            ),
        }
    }

    /// The inverse of [`type_name`](Self::type_name) + [`columns`](Self::columns).
    pub fn from_columns(
        type_name: &str,
        step: Option<String>,
        job_id: Option<JobId>,
        data: &Value,
    ) -> Result<Self, ColumnsError> {
        let text = |key: &'static str| -> Result<String, ColumnsError> {
            data.get(key)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or(ColumnsError::Missing(key))
        };
        let step = || step.clone().ok_or(ColumnsError::Missing("step"));
        let job = || job_id.ok_or(ColumnsError::Missing("job_id"));
        Ok(match type_name {
            "workflow_started" => Self::WorkflowStarted {
                input: data
                    .get("input")
                    .cloned()
                    .ok_or(ColumnsError::Missing("input"))?,
            },
            "activity_scheduled" => Self::ActivityScheduled {
                activity: step()?,
                job_id: job()?,
            },
            "activity_completed" => Self::ActivityCompleted {
                activity: step()?,
                job_id: job()?,
            },
            "activity_failed" => Self::ActivityFailed {
                activity: step()?,
                job_id: job()?,
                error: text("error")?,
            },
            "activity_cancelled" => Self::ActivityCancelled {
                activity: step()?,
                job_id: job()?,
            },
            "timer_scheduled" => Self::TimerScheduled {
                step: step()?,
                fire_at_ms: data
                    .get("fire_at_ms")
                    .and_then(Value::as_i64)
                    .ok_or(ColumnsError::Missing("fire_at_ms"))?,
            },
            "timer_fired" => Self::TimerFired { step: step()? },
            "signal_received" => Self::SignalReceived {
                name: text("name")?,
                payload: data.get("payload").cloned().unwrap_or(Value::Null),
            },
            "workflow_completed" => Self::WorkflowCompleted,
            "workflow_failed" => Self::WorkflowFailed {
                reason: text("reason")?,
            },
            "workflow_cancelled" => Self::WorkflowCancelled {
                reason: text("reason")?,
            },
            other => return Err(ColumnsError::UnknownType(other.to_owned())),
        })
    }
}

fn json_obj<const N: usize>(entries: [(&str, Value); N]) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ColumnsError {
    #[error("unknown workflow event type {0:?}")]
    UnknownType(String),
    #[error("workflow event is missing {0}")]
    Missing(&'static str),
}

// ---- state -----------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed { reason: String },
    Cancelled { reason: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ActivityStatus {
    /// The job exists; it may be queued, running or retrying.
    Scheduled,
    Completed,
    Failed {
        error: String,
    },
    Cancelled,
}

impl ActivityStatus {
    pub fn is_terminal(&self) -> bool {
        !matches!(self, ActivityStatus::Scheduled)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ActivityState {
    pub job_id: JobId,
    pub status: ActivityStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerState {
    Scheduled { fire_at_ms: Timestamp },
    Fired { fire_at_ms: Timestamp },
}

/// Everything the decider needs, derived only from the history.
#[derive(Debug, Clone, PartialEq)]
pub struct RunState {
    pub status: RunStatus,
    pub input: Value,
    /// By activity address. Absent = not yet scheduled.
    pub activities: BTreeMap<String, ActivityState>,
    /// By step name.
    pub timers: BTreeMap<String, TimerState>,
    /// One-shot latches: name → payload of the single delivery.
    pub signals: BTreeMap<String, Value>,
    pub last_seq: u64,
    pub last_at_ms: Timestamp,
}

impl RunState {
    /// Is step `index` complete? Activity steps: every activity completed.
    /// Timers: fired. Signal waits: the signal was received (possibly before
    /// the run reached the step).
    pub fn step_done(&self, workflow: &Workflow, index: usize) -> bool {
        let Some(step) = workflow.steps.get(index) else {
            return false;
        };
        match &step.body {
            StepBody::Activity(_) | StepBody::Parallel(_) => step.activities().iter().all(|a| {
                matches!(
                    self.activities.get(&a.address),
                    Some(ActivityState {
                        status: ActivityStatus::Completed,
                        ..
                    })
                )
            }),
            StepBody::Sleep { .. } => {
                matches!(self.timers.get(&step.name), Some(TimerState::Fired { .. }))
            }
            StepBody::WaitSignal { signal } => self.signals.contains_key(signal),
        }
    }

    /// The first incomplete step, or `None` when every step is done.
    pub fn cursor(&self, workflow: &Workflow) -> Option<usize> {
        (0..workflow.steps.len()).find(|&i| !self.step_done(workflow, i))
    }

    /// Apply one event. Errors leave `self` unchanged.
    pub fn apply(
        &mut self,
        workflow: &Workflow,
        event: &WorkflowEvent,
    ) -> Result<(), HistoryError> {
        let seq = event.seq;
        if seq != self.last_seq + 1 {
            return Err(HistoryError::SeqGap {
                expected: self.last_seq + 1,
                found: seq,
            });
        }
        if event.at_ms < self.last_at_ms {
            return Err(HistoryError::TimeWentBackwards { seq });
        }
        if self.status != RunStatus::Running {
            return Err(HistoryError::AfterEnd { seq });
        }
        let impossible = |why: String| HistoryError::Impossible { seq, why };

        match &event.kind {
            WorkflowEventKind::WorkflowStarted { .. } => {
                return Err(impossible("workflow_started after the start".into()));
            }
            WorkflowEventKind::ActivityScheduled { activity, job_id } => {
                let (index, _) = workflow
                    .activity(activity)
                    .ok_or_else(|| impossible(format!("unknown activity {activity:?}")))?;
                if self.activities.contains_key(activity) {
                    return Err(impossible(format!("activity {activity:?} scheduled twice")));
                }
                if self.cursor(workflow) != Some(index) {
                    return Err(impossible(format!(
                        "activity {activity:?} scheduled before its step was reached"
                    )));
                }
                if self.activities.values().any(|a| {
                    matches!(
                        a.status,
                        ActivityStatus::Failed { .. } | ActivityStatus::Cancelled
                    )
                }) {
                    return Err(impossible(format!(
                        "activity {activity:?} scheduled after a failure"
                    )));
                }
                if self.activities.values().any(|a| a.job_id == *job_id) {
                    return Err(impossible(format!("job {job_id} linked to two activities")));
                }
                self.activities.insert(
                    activity.clone(),
                    ActivityState {
                        job_id: *job_id,
                        status: ActivityStatus::Scheduled,
                    },
                );
            }
            WorkflowEventKind::ActivityCompleted { activity, job_id } => {
                self.finish_activity(activity, *job_id, ActivityStatus::Completed, seq)?;
            }
            WorkflowEventKind::ActivityFailed {
                activity,
                job_id,
                error,
            } => {
                self.finish_activity(
                    activity,
                    *job_id,
                    ActivityStatus::Failed {
                        error: error.clone(),
                    },
                    seq,
                )?;
            }
            WorkflowEventKind::ActivityCancelled { activity, job_id } => {
                self.finish_activity(activity, *job_id, ActivityStatus::Cancelled, seq)?;
            }
            WorkflowEventKind::TimerScheduled { step, fire_at_ms } => {
                let index = self.sleep_step(workflow, step, seq)?;
                if self.timers.contains_key(step) {
                    return Err(impossible(format!("timer {step:?} scheduled twice")));
                }
                if self.cursor(workflow) != Some(index) {
                    return Err(impossible(format!(
                        "timer {step:?} scheduled before its step was reached"
                    )));
                }
                if *fire_at_ms <= event.at_ms {
                    return Err(impossible(format!(
                        "timer {step:?} scheduled to fire in the past"
                    )));
                }
                self.timers.insert(
                    step.clone(),
                    TimerState::Scheduled {
                        fire_at_ms: *fire_at_ms,
                    },
                );
            }
            WorkflowEventKind::TimerFired { step } => {
                self.sleep_step(workflow, step, seq)?;
                match self.timers.get(step) {
                    Some(TimerState::Scheduled { fire_at_ms }) => {
                        if event.at_ms < *fire_at_ms {
                            return Err(impossible(format!(
                                "timer {step:?} fired before its deadline"
                            )));
                        }
                        let fire_at_ms = *fire_at_ms;
                        self.timers
                            .insert(step.clone(), TimerState::Fired { fire_at_ms });
                    }
                    Some(TimerState::Fired { .. }) => {
                        return Err(impossible(format!("timer {step:?} fired twice")));
                    }
                    None => {
                        return Err(impossible(format!(
                            "timer {step:?} fired without being scheduled"
                        )));
                    }
                }
            }
            WorkflowEventKind::SignalReceived { name, payload } => {
                if !workflow.declares_signal(name) {
                    return Err(impossible(format!(
                        "signal {name:?} is not awaited by any step"
                    )));
                }
                if self.signals.contains_key(name) {
                    return Err(impossible(format!(
                        "signal {name:?} received twice (one-shot latch)"
                    )));
                }
                self.signals.insert(name.clone(), payload.clone());
            }
            WorkflowEventKind::WorkflowCompleted => {
                if self.cursor(workflow).is_some() {
                    return Err(impossible("workflow completed with steps left".into()));
                }
                self.status = RunStatus::Succeeded;
            }
            WorkflowEventKind::WorkflowFailed { reason } => {
                self.require_no_outstanding(seq)?;
                self.status = RunStatus::Failed {
                    reason: reason.clone(),
                };
            }
            WorkflowEventKind::WorkflowCancelled { reason } => {
                self.require_no_outstanding(seq)?;
                self.status = RunStatus::Cancelled {
                    reason: reason.clone(),
                };
            }
        }
        self.last_seq = seq;
        self.last_at_ms = event.at_ms;
        Ok(())
    }

    fn finish_activity(
        &mut self,
        activity: &str,
        job_id: JobId,
        status: ActivityStatus,
        seq: u64,
    ) -> Result<(), HistoryError> {
        let impossible = |why: String| HistoryError::Impossible { seq, why };
        let state = self.activities.get_mut(activity).ok_or_else(|| {
            impossible(format!(
                "activity {activity:?} finished without being scheduled"
            ))
        })?;
        if state.job_id != job_id {
            return Err(impossible(format!(
                "activity {activity:?} finished by job {job_id}, not its own"
            )));
        }
        if state.status.is_terminal() {
            return Err(impossible(format!("activity {activity:?} finished twice")));
        }
        state.status = status;
        Ok(())
    }

    fn sleep_step(&self, workflow: &Workflow, step: &str, seq: u64) -> Result<usize, HistoryError> {
        workflow
            .step_index(step)
            .filter(|&i| matches!(workflow.steps[i].body, StepBody::Sleep { .. }))
            .ok_or_else(|| HistoryError::Impossible {
                seq,
                why: format!("{step:?} is not a sleep step"),
            })
    }

    /// A run only ends once every scheduled activity reached an outcome
    /// (outstanding ones are cancelled first, in the same transaction).
    fn require_no_outstanding(&self, seq: u64) -> Result<(), HistoryError> {
        match self
            .activities
            .iter()
            .find(|(_, a)| !a.status.is_terminal())
        {
            Some((addr, _)) => Err(HistoryError::Impossible {
                seq,
                why: format!("run ended while activity {addr:?} was still outstanding"),
            }),
            None => Ok(()),
        }
    }
}

/// Fold a complete history. The first event must be `workflow_started`
/// with `seq` 1.
pub fn fold(workflow: &Workflow, events: &[WorkflowEvent]) -> Result<RunState, HistoryError> {
    let Some((first, rest)) = events.split_first() else {
        return Err(HistoryError::Empty);
    };
    let WorkflowEventKind::WorkflowStarted { input } = &first.kind else {
        return Err(HistoryError::NotStarted);
    };
    if first.seq != 1 {
        return Err(HistoryError::SeqGap {
            expected: 1,
            found: first.seq,
        });
    }
    let mut state = RunState {
        status: RunStatus::Running,
        input: input.clone(),
        activities: BTreeMap::new(),
        timers: BTreeMap::new(),
        signals: BTreeMap::new(),
        last_seq: 1,
        last_at_ms: first.at_ms,
    };
    for event in rest {
        state.apply(workflow, event)?;
    }
    Ok(state)
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HistoryError {
    #[error("history is empty")]
    Empty,
    #[error("history does not begin with workflow_started")]
    NotStarted,
    #[error("expected seq {expected}, found {found}")]
    SeqGap { expected: u64, found: u64 },
    #[error("event {seq} is earlier than the event before it")]
    TimeWentBackwards { seq: u64 },
    #[error("event {seq} comes after the run ended")]
    AfterEnd { seq: u64 },
    #[error("event {seq} is impossible: {why}")]
    Impossible { seq: u64, why: String },
}
