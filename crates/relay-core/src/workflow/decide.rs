//! The pure decision function: given a definition and a folded state, what
//! must happen next?
//!
//! The coordinator runs `decide`, turns each [`Command`] into events (and,
//! for activities, a job), applies those events with [`RunState::apply`] and
//! decides again, all in one transaction, until a decision has no commands.
//! The last decision's `wake_at_ms` is stored on the run.
//!
//! `now_ms` is the only input not derived from the history. It is used only
//! to compute a timer's `fire_at_ms`, which is persisted in
//! `timer_scheduled`, so deciding again over the same history never re-arms
//! a timer from a new "now".

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::api::SubmitJobRequest;
use crate::clock::Timestamp;
use crate::job::JobId;
use crate::workflow::definition::{
    Activity, INPUT_ADDRESS, INPUTS_PAYLOAD_KEY, StepBody, Workflow,
};
use crate::workflow::history::{ActivityStatus, RunState, RunStatus, TimerState};

pub struct DecideContext<'a> {
    pub now_ms: Timestamp,
    /// Results of completed activities, by address. The coordinator loads
    /// them from `jobs.result` (immutable once a job succeeded). Only the
    /// ones named in some `inputs` are needed.
    pub results: &'a BTreeMap<String, Value>,
    /// The job payload limit; an activity whose merged inputs exceed it
    /// fails the run instead of being truncated.
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Create the job, then append `activity_scheduled` with its id.
    ScheduleActivity {
        activity: String,
        request: SubmitJobRequest,
    },
    ScheduleTimer {
        step: String,
        fire_at_ms: Timestamp,
    },
    FireTimer {
        step: String,
    },
    /// Cancel these jobs through the ordinary job cancel path, which appends
    /// `activity_cancelled` for each. Always followed by [`Command::FailRun`].
    CancelActivities {
        activities: Vec<(String, JobId)>,
    },
    CompleteRun,
    FailRun {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Decision {
    pub commands: Vec<Command>,
    /// When to decide again if nothing else happens. `None`: wait for an
    /// activity outcome or a signal (those set `wake_at` themselves), or the
    /// run is over.
    pub wake_at_ms: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecideError {
    /// The coordinator failed to supply a completed activity's result. A
    /// bug, not a workflow failure: the run must not advance on it.
    #[error("result of completed activity {activity:?} was not provided")]
    MissingResult { activity: String },
}

pub fn decide(
    workflow: &Workflow,
    state: &RunState,
    ctx: &DecideContext<'_>,
) -> Result<Decision, DecideError> {
    if state.status != RunStatus::Running {
        return Ok(Decision::default());
    }
    let Some(index) = state.cursor(workflow) else {
        return Ok(Decision {
            commands: vec![Command::CompleteRun],
            wake_at_ms: None,
        });
    };
    let step = &workflow.steps[index];

    match &step.body {
        StepBody::Activity(_) | StepBody::Parallel(_) => {
            decide_activities(step.activities(), state, ctx)
        }
        StepBody::Sleep { ms } => Ok(match state.timers.get(&step.name) {
            None => {
                let fire_at_ms = ctx
                    .now_ms
                    .saturating_add(i64::try_from(*ms).unwrap_or(i64::MAX));
                Decision {
                    commands: vec![Command::ScheduleTimer {
                        step: step.name.clone(),
                        fire_at_ms,
                    }],
                    wake_at_ms: Some(fire_at_ms),
                }
            }
            Some(TimerState::Scheduled { fire_at_ms }) if ctx.now_ms >= *fire_at_ms => Decision {
                commands: vec![Command::FireTimer {
                    step: step.name.clone(),
                }],
                wake_at_ms: None,
            },
            Some(TimerState::Scheduled { fire_at_ms }) => Decision {
                commands: Vec::new(),
                wake_at_ms: Some(*fire_at_ms),
            },
            // A fired timer makes the step done, so the cursor is never here.
            Some(TimerState::Fired { .. }) => Decision::default(),
        }),
        // Not done means the signal hasn't arrived; its arrival wakes the run.
        StepBody::WaitSignal { .. } => Ok(Decision::default()),
    }
}

fn decide_activities(
    activities: &[Activity],
    state: &RunState,
    ctx: &DecideContext<'_>,
) -> Result<Decision, DecideError> {
    // Fail fast: the first failed or cancelled activity (in definition
    // order) fails the run; outstanding siblings are cancelled first.
    let failure = activities
        .iter()
        .find_map(|a| match state.activities.get(&a.address) {
            Some(s) => match &s.status {
                ActivityStatus::Failed { error } => {
                    Some(format!("activity {:?} failed: {error}", a.address))
                }
                ActivityStatus::Cancelled => {
                    Some(format!("activity {:?} was cancelled", a.address))
                }
                ActivityStatus::Scheduled | ActivityStatus::Completed => None,
            },
            None => None,
        });
    if let Some(reason) = failure {
        return Ok(fail(activities, state, reason));
    }

    let mut commands = Vec::new();
    for activity in activities {
        if state.activities.contains_key(&activity.address) {
            continue;
        }
        let payload = build_payload(activity, state, ctx)?;
        let bytes = Value::Object(payload.clone()).to_string().len();
        if bytes > ctx.max_payload_bytes {
            let reason = format!(
                "inputs too large for activity {:?}: payload would be {bytes} bytes, the limit is {}",
                activity.address, ctx.max_payload_bytes
            );
            return Ok(fail(activities, state, reason));
        }
        commands.push(Command::ScheduleActivity {
            activity: activity.address.clone(),
            request: activity.job_request(payload),
        });
    }
    // Either we just scheduled the missing ones, or all are in flight.
    Ok(Decision {
        commands,
        wake_at_ms: None,
    })
}

/// Cancel what's still outstanding in this step, then fail the run.
fn fail(activities: &[Activity], state: &RunState, reason: String) -> Decision {
    let outstanding: Vec<_> = activities
        .iter()
        .filter_map(|a| match state.activities.get(&a.address) {
            Some(s) if s.status == ActivityStatus::Scheduled => Some((a.address.clone(), s.job_id)),
            _ => None,
        })
        .collect();
    let mut commands = Vec::new();
    if !outstanding.is_empty() {
        commands.push(Command::CancelActivities {
            activities: outstanding,
        });
    }
    commands.push(Command::FailRun { reason });
    Decision {
        commands,
        wake_at_ms: None,
    }
}

/// The definition's payload plus `"inputs": {address: result, ...}`.
fn build_payload(
    activity: &Activity,
    state: &RunState,
    ctx: &DecideContext<'_>,
) -> Result<Map<String, Value>, DecideError> {
    let mut payload = activity.payload.clone();
    if activity.inputs.is_empty() {
        return Ok(payload);
    }
    let mut inputs = Map::new();
    for address in &activity.inputs {
        let value = if address == INPUT_ADDRESS {
            state.input.clone()
        } else {
            ctx.results
                .get(address)
                .cloned()
                .ok_or_else(|| DecideError::MissingResult {
                    activity: address.clone(),
                })?
        };
        inputs.insert(address.clone(), value);
    }
    payload.insert(INPUTS_PAYLOAD_KEY.to_owned(), Value::Object(inputs));
    Ok(payload)
}
