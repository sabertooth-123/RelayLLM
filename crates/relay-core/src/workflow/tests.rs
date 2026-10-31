//! State-machine tests for `fold` and `decide`.
//!
//! [`Sim`] plays the coordinator's role purely: it runs `decide`, turns
//! commands into events exactly as the store will (§4.2 of the design),
//! applies them and decides again until quiet. Every simulated history is
//! then re-folded from scratch, so each test also checks that replay
//! reproduces the live state.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::*;
use crate::clock::Timestamp;
use crate::job::JobId;

const T0: Timestamp = 1_700_000_000_000;
const MAX_PAYLOAD: usize = 64 * 1024;

fn workflow(v: Value) -> Workflow {
    let def: WorkflowDefinition = serde_json::from_value(v).expect("wire format");
    Workflow::from_definition(&def, &WorkflowLimits::default()).expect("valid definition")
}

fn act(prompt: &str) -> Value {
    json!({ "kind": "mock", "payload": { "prompt": prompt } })
}

/// plan → parallel(a, b, c) → aggregate(inputs) → sleep 30 s → approval → final
fn research() -> Workflow {
    workflow(json!({
        "name": "research",
        "steps": [
            { "name": "plan", "activity": { "kind": "mock", "payload": { "prompt": "plan" }, "inputs": ["input"] } },
            { "name": "research", "parallel": [
                { "name": "a", "kind": "mock", "payload": { "prompt": "a" } },
                { "name": "b", "kind": "mock", "payload": { "prompt": "b" } },
                { "name": "c", "kind": "mock", "payload": { "prompt": "c" } } ] },
            { "name": "aggregate", "activity": { "kind": "mock", "payload": { "prompt": "merge" },
                                                  "inputs": ["research.a", "research.b", "research.c"] } },
            { "name": "cooldown", "sleep_ms": 30000 },
            { "name": "approval", "wait_signal": "approved" },
            { "name": "final", "activity": { "kind": "mock", "payload": { "prompt": "final" }, "inputs": ["aggregate"] } },
        ]
    }))
}

fn sequential() -> Workflow {
    workflow(json!({ "name": "seq", "steps": [
        { "name": "first", "activity": act("1") },
        { "name": "second", "activity": act("2") },
    ] }))
}

/// A pure stand-in for the coordinator's decision transaction.
struct Sim {
    wf: Workflow,
    events: Vec<WorkflowEvent>,
    state: RunState,
    /// Results of completed activities (the coordinator reads `jobs.result`).
    results: BTreeMap<String, Value>,
    /// Jobs "created" for scheduled activities.
    jobs: BTreeMap<String, (JobId, SubmitJobRequestLite)>,
    wake_at: Option<Timestamp>,
}

/// What a test cares about from a scheduled job request.
#[derive(Debug, Clone, PartialEq)]
struct SubmitJobRequestLite {
    payload: Value,
    max_attempts: Option<u32>,
}

impl Sim {
    fn start(wf: Workflow, input: Value, now: Timestamp) -> Self {
        let first = WorkflowEvent {
            seq: 1,
            at_ms: now,
            kind: WorkflowEventKind::WorkflowStarted { input },
        };
        let state = fold(&wf, std::slice::from_ref(&first)).unwrap();
        let mut sim = Sim {
            wf,
            events: vec![first],
            state,
            results: BTreeMap::new(),
            jobs: BTreeMap::new(),
            wake_at: Some(now),
        };
        sim.decide(now);
        sim
    }

    fn append(&mut self, at_ms: Timestamp, kind: WorkflowEventKind) {
        let event = WorkflowEvent {
            seq: self.state.last_seq + 1,
            at_ms,
            kind,
        };
        self.state
            .apply(&self.wf, &event)
            .unwrap_or_else(|e| panic!("{e} applying {event:?}"));
        self.events.push(event);
    }

    /// One decision transaction: decide, apply, repeat until quiet. Returns
    /// the commands that were executed.
    fn decide(&mut self, now: Timestamp) -> Vec<Command> {
        let mut executed = Vec::new();
        for _ in 0..1_000 {
            let ctx = DecideContext {
                now_ms: now,
                results: &self.results,
                max_payload_bytes: MAX_PAYLOAD,
            };
            let decision = decide(&self.wf, &self.state, &ctx).unwrap();
            self.wake_at = decision.wake_at_ms;
            if decision.commands.is_empty() {
                return executed;
            }
            for command in decision.commands {
                match &command {
                    Command::ScheduleActivity { activity, request } => {
                        let job_id = JobId::generate();
                        let lite = SubmitJobRequestLite {
                            payload: request.payload.clone(),
                            max_attempts: request.max_attempts,
                        };
                        self.jobs.insert(activity.clone(), (job_id, lite));
                        self.append(
                            now,
                            WorkflowEventKind::ActivityScheduled {
                                activity: activity.clone(),
                                job_id,
                            },
                        );
                    }
                    Command::ScheduleTimer { step, fire_at_ms } => {
                        self.append(
                            now,
                            WorkflowEventKind::TimerScheduled {
                                step: step.clone(),
                                fire_at_ms: *fire_at_ms,
                            },
                        );
                    }
                    Command::FireTimer { step } => {
                        self.append(now, WorkflowEventKind::TimerFired { step: step.clone() })
                    }
                    Command::CancelActivities { activities } => {
                        for (activity, job_id) in activities {
                            self.append(
                                now,
                                WorkflowEventKind::ActivityCancelled {
                                    activity: activity.clone(),
                                    job_id: *job_id,
                                },
                            );
                        }
                    }
                    Command::CompleteRun => self.append(now, WorkflowEventKind::WorkflowCompleted),
                    Command::FailRun { reason } => self.append(
                        now,
                        WorkflowEventKind::WorkflowFailed {
                            reason: reason.clone(),
                        },
                    ),
                }
                executed.push(command);
            }
        }
        panic!("decide did not quiesce");
    }

    fn job(&self, activity: &str) -> JobId {
        self.jobs[activity].0
    }

    /// The job succeeded (the hook appends the event; then the run is due).
    fn complete(&mut self, activity: &str, now: Timestamp, result: Value) -> Vec<Command> {
        let job_id = self.job(activity);
        self.results.insert(activity.to_owned(), result);
        self.append(
            now,
            WorkflowEventKind::ActivityCompleted {
                activity: activity.into(),
                job_id,
            },
        );
        self.decide(now)
    }

    fn fail(&mut self, activity: &str, now: Timestamp, error: &str) -> Vec<Command> {
        let job_id = self.job(activity);
        self.append(
            now,
            WorkflowEventKind::ActivityFailed {
                activity: activity.into(),
                job_id,
                error: error.into(),
            },
        );
        self.decide(now)
    }

    fn signal(&mut self, name: &str, now: Timestamp, payload: Value) -> Vec<Command> {
        self.append(
            now,
            WorkflowEventKind::SignalReceived {
                name: name.into(),
                payload,
            },
        );
        self.decide(now)
    }

    fn types(&self) -> Vec<&'static str> {
        self.events.iter().map(|e| e.kind.type_name()).collect()
    }

    /// Replaying the persisted history must reproduce the live state.
    fn assert_replay_matches(&self) {
        assert_eq!(
            fold(&self.wf, &self.events).unwrap(),
            self.state,
            "replay diverged"
        );
    }
}

fn scheduled(commands: &[Command]) -> Vec<&str> {
    commands
        .iter()
        .filter_map(|c| match c {
            Command::ScheduleActivity { activity, .. } => Some(activity.as_str()),
            _ => None,
        })
        .collect()
}

// ---- normal execution ------------------------------------------------------

#[test]
fn a_sequential_workflow_schedules_one_step_at_a_time() {
    let mut sim = Sim::start(sequential(), json!(null), T0);
    assert_eq!(sim.types(), ["workflow_started", "activity_scheduled"]);
    assert_eq!(sim.wake_at, None, "waiting on the job, not a timer");

    assert_eq!(
        scheduled(&sim.complete("first", T0 + 10, json!(1))),
        ["second"]
    );
    let done = sim.complete("second", T0 + 20, json!(2));
    assert_eq!(done, [Command::CompleteRun]);
    assert_eq!(sim.state.status, RunStatus::Succeeded);
    assert_eq!(
        sim.types(),
        [
            "workflow_started",
            "activity_scheduled",
            "activity_completed",
            "activity_scheduled",
            "activity_completed",
            "workflow_completed"
        ]
    );
    let seqs: Vec<u64> = sim.events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=6).collect::<Vec<_>>());
    sim.assert_replay_matches();
}

#[test]
fn the_full_research_workflow_runs_to_completion() {
    let mut sim = Sim::start(research(), json!({ "topic": "rust" }), T0);
    assert_eq!(
        scheduled(&sim.decide(T0)),
        Vec::<&str>::new(),
        "deciding again is a no-op"
    );
    let plan = &sim.jobs["plan"].1;
    assert_eq!(
        plan.payload,
        json!({ "prompt": "plan", "inputs": { "input": { "topic": "rust" } } })
    );

    // Fan-out: all three branches in one decision.
    assert_eq!(
        scheduled(&sim.complete("plan", T0 + 1, json!("p"))),
        ["research.a", "research.b", "research.c"]
    );
    // Fan-in: partial completion schedules nothing.
    assert!(sim.complete("research.b", T0 + 2, json!("B")).is_empty());
    assert!(sim.complete("research.a", T0 + 3, json!("A")).is_empty());
    assert_eq!(
        scheduled(&sim.complete("research.c", T0 + 4, json!("C"))),
        ["aggregate"]
    );
    assert_eq!(
        sim.jobs["aggregate"].1.payload,
        json!({ "prompt": "merge", "inputs": { "research.a": "A", "research.b": "B", "research.c": "C" } })
    );

    // Timer: scheduled from "now" once, waiting until its deadline.
    let after_aggregate = sim.complete("aggregate", T0 + 5, json!({ "summary": "s" }));
    assert_eq!(
        after_aggregate,
        [Command::ScheduleTimer {
            step: "cooldown".into(),
            fire_at_ms: T0 + 5 + 30_000
        }]
    );
    assert_eq!(sim.wake_at, Some(T0 + 30_005));
    assert!(sim.decide(T0 + 30_004).is_empty(), "one ms early");
    assert_eq!(sim.wake_at, Some(T0 + 30_005));

    // Fired, then wait for the signal: nothing to schedule, no timer.
    assert_eq!(
        sim.decide(T0 + 30_005),
        [Command::FireTimer {
            step: "cooldown".into()
        }]
    );
    assert_eq!(sim.wake_at, None);
    assert_eq!(
        scheduled(&sim.signal("approved", T0 + 40_000, json!({ "by": "pranav" }))),
        ["final"]
    );
    assert_eq!(
        sim.jobs["final"].1.payload["inputs"],
        json!({ "aggregate": { "summary": "s" } })
    );

    assert_eq!(
        sim.complete("final", T0 + 41_000, json!("done")),
        [Command::CompleteRun]
    );
    assert_eq!(sim.state.status, RunStatus::Succeeded);
    sim.assert_replay_matches();
}

#[test]
fn replay_never_reschedules_completed_activities() {
    // Crash after "plan" and "research.a" completed, with b and c in flight.
    let mut sim = Sim::start(research(), json!(null), T0);
    sim.complete("plan", T0 + 1, json!("p"));
    sim.complete("research.a", T0 + 2, json!("A"));
    let before: Vec<_> = sim.jobs.keys().cloned().collect();

    // "Restart": a fresh fold of the persisted history, then a decision.
    let restored = fold(&sim.wf, &sim.events).unwrap();
    assert_eq!(restored, sim.state);
    let ctx = DecideContext {
        now_ms: T0 + 1_000_000,
        results: &sim.results,
        max_payload_bytes: MAX_PAYLOAD,
    };
    let decision = decide(&sim.wf, &restored, &ctx).unwrap();
    assert!(
        decision.commands.is_empty(),
        "b and c are in flight; nothing else may be scheduled: {decision:?}"
    );

    // And the branches finish without anything running twice.
    sim.complete("research.b", T0 + 3, json!("B"));
    assert_eq!(
        scheduled(&sim.complete("research.c", T0 + 4, json!("C"))),
        ["aggregate"]
    );
    let after: Vec<_> = sim.jobs.keys().cloned().collect();
    assert_eq!(
        after.len(),
        before.len() + 1,
        "exactly one new job: aggregate"
    );
}

#[test]
fn a_timer_deadline_is_never_recomputed() {
    let wf = workflow(json!({ "name": "t", "steps": [
        { "name": "wait", "sleep_ms": 1000 }, { "name": "after", "activity": act("x") } ] }));
    let mut sim = Sim::start(wf, json!(null), T0);
    assert_eq!(sim.wake_at, Some(T0 + 1000));
    // Deciding later (e.g. after a restart) keeps the original deadline.
    for now in [T0 + 1, T0 + 500, T0 + 999] {
        assert!(sim.decide(now).is_empty());
        assert_eq!(sim.wake_at, Some(T0 + 1000));
        assert_eq!(
            fold(&sim.wf, &sim.events).unwrap().timers["wait"],
            TimerState::Scheduled {
                fire_at_ms: T0 + 1000
            }
        );
    }
    // Overdue by an hour (coordinator was down): fires once, at once.
    let fired = sim.decide(T0 + 3_600_000);
    assert_eq!(
        fired.first(),
        Some(&Command::FireTimer {
            step: "wait".into()
        })
    );
    assert_eq!(scheduled(&fired), ["after"]);
    assert_eq!(
        sim.types()
            .iter()
            .filter(|t| **t == "timer_scheduled")
            .count(),
        1
    );
    sim.assert_replay_matches();
}

#[test]
fn a_signal_that_arrives_early_satisfies_the_step_later() {
    let wf = workflow(json!({ "name": "s", "steps": [
        { "name": "work", "activity": act("w") },
        { "name": "gate", "wait_signal": "go" },
        { "name": "publish", "activity": act("p") } ] }));
    let mut sim = Sim::start(wf, json!(null), T0);
    // The signal arrives while "work" is still running: recorded, no effect yet.
    assert!(sim.signal("go", T0 + 1, json!(1)).is_empty());
    assert_eq!(sim.state.signals["go"], json!(1));
    // "work" completes and the run passes the gate without waiting.
    assert_eq!(
        scheduled(&sim.complete("work", T0 + 2, json!("w"))),
        ["publish"]
    );
    sim.assert_replay_matches();
}

#[test]
fn waiting_for_a_signal_issues_no_commands_and_no_wake_time() {
    let wf = workflow(json!({ "name": "s", "steps": [{ "name": "gate", "wait_signal": "go" }] }));
    let mut sim = Sim::start(wf, json!(null), T0);
    for now in [T0, T0 + 1_000_000_000] {
        assert!(sim.decide(now).is_empty());
        assert_eq!(sim.wake_at, None);
    }
    assert_eq!(
        sim.signal("go", T0 + 5, Value::Null),
        [Command::CompleteRun]
    );
}

// ---- failures ----------------------------------------------------------

#[test]
fn a_failed_branch_cancels_outstanding_siblings_then_fails_the_run() {
    let mut sim = Sim::start(research(), json!(null), T0);
    sim.complete("plan", T0 + 1, json!("p"));
    sim.complete("research.a", T0 + 2, json!("A"));
    let commands = sim.fail("research.b", T0 + 3, "upstream 500 after 3 attempts");
    let c_job = sim.job("research.c");
    assert_eq!(
        commands,
        [
            Command::CancelActivities {
                activities: vec![("research.c".into(), c_job)]
            },
            Command::FailRun {
                reason: "activity \"research.b\" failed: upstream 500 after 3 attempts".into()
            },
        ],
        "completed 'a' is left alone; only in-flight 'c' is cancelled"
    );
    assert!(matches!(sim.state.status, RunStatus::Failed { .. }));
    assert_eq!(
        sim.state.activities["research.c"].status,
        ActivityStatus::Cancelled
    );
    assert!(!sim.jobs.contains_key("aggregate"));
    sim.assert_replay_matches();
}

#[test]
fn a_cancelled_activity_fails_the_run() {
    let mut sim = Sim::start(sequential(), json!(null), T0);
    let job_id = sim.job("first");
    sim.append(
        T0 + 1,
        WorkflowEventKind::ActivityCancelled {
            activity: "first".into(),
            job_id,
        },
    );
    assert_eq!(
        sim.decide(T0 + 1),
        [Command::FailRun {
            reason: "activity \"first\" was cancelled".into()
        }]
    );
}

#[test]
fn a_late_sibling_failure_after_the_first_one_fails_only_once() {
    // b and c both fail before the decider runs: one decision, one FailRun.
    let mut sim = Sim::start(research(), json!(null), T0);
    sim.complete("plan", T0 + 1, json!("p"));
    let (b, c) = (sim.job("research.b"), sim.job("research.c"));
    sim.append(
        T0 + 2,
        WorkflowEventKind::ActivityFailed {
            activity: "research.b".into(),
            job_id: b,
            error: "x".into(),
        },
    );
    sim.append(
        T0 + 2,
        WorkflowEventKind::ActivityFailed {
            activity: "research.c".into(),
            job_id: c,
            error: "y".into(),
        },
    );
    let commands = sim.decide(T0 + 3);
    let a = sim.job("research.a");
    assert_eq!(
        commands,
        [
            Command::CancelActivities {
                activities: vec![("research.a".into(), a)]
            },
            Command::FailRun {
                reason: "activity \"research.b\" failed: x".into()
            },
        ],
        "the first failure in definition order is the reason"
    );
    assert!(
        sim.decide(T0 + 4).is_empty(),
        "a failed run decides nothing"
    );
}

#[test]
fn oversized_inputs_fail_the_run_instead_of_truncating() {
    let mut sim = Sim::start(research(), json!(null), T0);
    sim.complete("plan", T0 + 1, json!("p"));
    let big = json!("x".repeat(30_000));
    sim.complete("research.a", T0 + 2, big.clone());
    sim.complete("research.b", T0 + 3, big.clone());
    let commands = sim.complete("research.c", T0 + 4, big);
    assert_eq!(commands.len(), 1, "{commands:?}");
    match &commands[0] {
        Command::FailRun { reason } => assert!(
            reason.contains("inputs too large for activity \"aggregate\""),
            "{reason}"
        ),
        other => panic!("expected FailRun, got {other:?}"),
    }
    assert!(!sim.jobs.contains_key("aggregate"));
}

#[test]
fn a_missing_result_is_a_coordinator_bug_not_a_workflow_failure() {
    let mut sim = Sim::start(research(), json!(null), T0);
    sim.complete("plan", T0 + 1, json!("p"));
    for b in ["research.a", "research.b", "research.c"] {
        let job_id = sim.job(b);
        sim.append(
            T0 + 2,
            WorkflowEventKind::ActivityCompleted {
                activity: b.into(),
                job_id,
            },
        );
    }
    // research.* results were never supplied.
    let ctx = DecideContext {
        now_ms: T0 + 3,
        results: &sim.results,
        max_payload_bytes: MAX_PAYLOAD,
    };
    assert_eq!(
        decide(&sim.wf, &sim.state, &ctx),
        Err(DecideError::MissingResult {
            activity: "research.a".into()
        })
    );
}

#[test]
fn finished_runs_decide_nothing() {
    for end in [
        WorkflowEventKind::WorkflowCompleted,
        WorkflowEventKind::WorkflowFailed { reason: "r".into() },
        WorkflowEventKind::WorkflowCancelled { reason: "r".into() },
    ] {
        let wf =
            workflow(json!({ "name": "s", "steps": [{ "name": "gate", "wait_signal": "go" }] }));
        let mut sim = Sim::start(wf, json!(null), T0);
        if end == WorkflowEventKind::WorkflowCompleted {
            sim.append(
                T0 + 1,
                WorkflowEventKind::SignalReceived {
                    name: "go".into(),
                    payload: Value::Null,
                },
            );
        }
        sim.append(T0 + 2, end.clone());
        let ctx = DecideContext {
            now_ms: T0 + 3,
            results: &sim.results,
            max_payload_bytes: MAX_PAYLOAD,
        };
        assert_eq!(
            decide(&sim.wf, &sim.state, &ctx).unwrap(),
            Decision::default(),
            "{end:?}"
        );
    }
}

#[test]
fn a_cancelled_run_must_cancel_its_activities_first() {
    let mut sim = Sim::start(sequential(), json!(null), T0);
    let job_id = sim.job("first");
    // The cancel transaction: activity first, then the run.
    sim.append(
        T0 + 1,
        WorkflowEventKind::ActivityCancelled {
            activity: "first".into(),
            job_id,
        },
    );
    sim.append(
        T0 + 1,
        WorkflowEventKind::WorkflowCancelled {
            reason: "by client".into(),
        },
    );
    assert_eq!(
        sim.state.status,
        RunStatus::Cancelled {
            reason: "by client".into()
        }
    );
    sim.assert_replay_matches();
}

#[test]
fn decide_is_deterministic() {
    let mut sim = Sim::start(research(), json!({ "k": 1 }), T0);
    sim.complete("plan", T0 + 1, json!("p"));
    let ctx = DecideContext {
        now_ms: T0 + 7,
        results: &sim.results,
        max_payload_bytes: MAX_PAYLOAD,
    };
    let a = decide(&sim.wf, &fold(&sim.wf, &sim.events).unwrap(), &ctx).unwrap();
    let b = decide(&sim.wf, &fold(&sim.wf, &sim.events).unwrap(), &ctx).unwrap();
    assert_eq!(a, b);
}

#[test]
fn two_runs_of_one_definition_are_independent() {
    let mut x = Sim::start(sequential(), json!("x"), T0);
    let mut y = Sim::start(sequential(), json!("y"), T0);
    x.complete("first", T0 + 1, json!(1));
    assert_eq!(
        y.state.activities["first"].status,
        ActivityStatus::Scheduled
    );
    assert_ne!(x.job("first"), y.job("first"));
    y.fail("first", T0 + 2, "boom");
    assert_eq!(x.state.status, RunStatus::Running);
    assert!(matches!(y.state.status, RunStatus::Failed { .. }));
}

#[test]
fn activity_job_options_pass_through() {
    let wf = workflow(json!({ "name": "o", "steps": [{ "name": "s", "activity":
        { "kind": "mock", "payload": { "prompt": "p" }, "max_attempts": 7, "timeout_secs": 9 } }] }));
    let sim = Sim::start(wf, json!(null), T0);
    assert_eq!(sim.jobs["s"].1.max_attempts, Some(7));
    let ctx = DecideContext {
        now_ms: T0,
        results: &BTreeMap::new(),
        max_payload_bytes: MAX_PAYLOAD,
    };
    let fresh = fold(&sim.wf, &sim.events[..1]).unwrap();
    match &decide(&sim.wf, &fresh, &ctx).unwrap().commands[0] {
        Command::ScheduleActivity { request, .. } => {
            assert_eq!(
                (
                    request.kind.as_str(),
                    request.max_attempts,
                    request.timeout_secs
                ),
                ("mock", Some(7), Some(9))
            );
            assert_eq!(
                request.idempotency_key, None,
                "uniqueness comes from the history, not a job key"
            );
        }
        other => panic!("{other:?}"),
    }
}

// ---- impossible histories ----------------------------------------------

/// Build raw histories with automatic seq numbering.
struct H {
    events: Vec<WorkflowEvent>,
}

impl H {
    fn started() -> Self {
        H {
            events: vec![WorkflowEvent {
                seq: 1,
                at_ms: T0,
                kind: WorkflowEventKind::WorkflowStarted { input: Value::Null },
            }],
        }
    }
    fn push(mut self, at_ms: Timestamp, kind: WorkflowEventKind) -> Self {
        let seq = self.events.len() as u64 + 1;
        self.events.push(WorkflowEvent { seq, at_ms, kind });
        self
    }
    fn fold(&self, wf: &Workflow) -> Result<RunState, HistoryError> {
        fold(wf, &self.events)
    }
}

fn sched(activity: &str, job_id: JobId) -> WorkflowEventKind {
    WorkflowEventKind::ActivityScheduled {
        activity: activity.into(),
        job_id,
    }
}
fn done(activity: &str, job_id: JobId) -> WorkflowEventKind {
    WorkflowEventKind::ActivityCompleted {
        activity: activity.into(),
        job_id,
    }
}

fn impossible(r: Result<RunState, HistoryError>, needle: &str) {
    match r {
        Err(HistoryError::Impossible { why, .. }) => {
            assert!(why.contains(needle), "{why:?} lacks {needle:?}")
        }
        other => panic!("expected Impossible({needle}), got {other:?}"),
    }
}

#[test]
fn malformed_history_framing_is_rejected() {
    let wf = sequential();
    assert_eq!(fold(&wf, &[]), Err(HistoryError::Empty));

    let j = JobId::generate();
    let not_started = vec![WorkflowEvent {
        seq: 1,
        at_ms: T0,
        kind: sched("first", j),
    }];
    assert_eq!(fold(&wf, &not_started), Err(HistoryError::NotStarted));

    let mut h = H::started();
    h.events[0].seq = 2;
    assert_eq!(
        h.fold(&wf),
        Err(HistoryError::SeqGap {
            expected: 1,
            found: 2
        })
    );

    let mut h = H::started().push(T0, sched("first", j));
    h.events[1].seq = 3;
    assert_eq!(
        h.fold(&wf),
        Err(HistoryError::SeqGap {
            expected: 2,
            found: 3
        })
    );

    let mut h = H::started().push(T0, sched("first", j));
    h.events[1].seq = 1;
    assert_eq!(
        h.fold(&wf),
        Err(HistoryError::SeqGap {
            expected: 2,
            found: 1
        }),
        "duplicate seq"
    );

    let h = H::started().push(T0 - 1, sched("first", j));
    assert_eq!(h.fold(&wf), Err(HistoryError::TimeWentBackwards { seq: 2 }));

    impossible(
        H::started()
            .push(
                T0,
                WorkflowEventKind::WorkflowStarted { input: Value::Null },
            )
            .fold(&wf),
        "after the start",
    );
}

#[test]
fn nothing_may_follow_the_end_of_a_run() {
    let wf = sequential();
    let j = JobId::generate();
    let h = H::started()
        .push(T0, sched("first", j))
        .push(
            T0,
            WorkflowEventKind::ActivityCancelled {
                activity: "first".into(),
                job_id: j,
            },
        )
        .push(
            T0,
            WorkflowEventKind::WorkflowCancelled { reason: "c".into() },
        )
        .push(T0, sched("second", JobId::generate()));
    assert_eq!(h.fold(&wf), Err(HistoryError::AfterEnd { seq: 5 }));
}

#[test]
fn impossible_activity_events_are_rejected() {
    let wf = research();
    let (j1, j2) = (JobId::generate(), JobId::generate());
    let base = || H::started().push(T0, sched("plan", j1));

    impossible(
        H::started().push(T0, sched("nope", j1)).fold(&wf),
        "unknown activity",
    );
    impossible(
        base().push(T0, sched("plan", j2)).fold(&wf),
        "scheduled twice",
    );
    impossible(
        H::started().push(T0, sched("research.a", j1)).fold(&wf),
        "before its step was reached",
    );
    impossible(
        base().push(T0, sched("aggregate", j2)).fold(&wf),
        "before its step was reached",
    );
    impossible(
        base()
            .push(T0, done("plan", j1))
            .push(T0, sched("research.a", j1))
            .fold(&wf),
        "linked to two activities",
    );
    impossible(
        H::started().push(T0, done("plan", j1)).fold(&wf),
        "without being scheduled",
    );
    impossible(base().push(T0, done("plan", j2)).fold(&wf), "not its own");
    impossible(
        base()
            .push(T0, done("plan", j1))
            .push(T0, done("plan", j1))
            .fold(&wf),
        "finished twice",
    );
    impossible(
        base()
            .push(T0, done("plan", j1))
            .push(
                T0,
                WorkflowEventKind::ActivityFailed {
                    activity: "plan".into(),
                    job_id: j1,
                    error: "e".into(),
                },
            )
            .fold(&wf),
        "finished twice",
    );

    // After a branch failed, its unscheduled siblings may not be scheduled.
    let (a, b) = (JobId::generate(), JobId::generate());
    let h = base()
        .push(T0, done("plan", j1))
        .push(T0, sched("research.a", a))
        .push(
            T0,
            WorkflowEventKind::ActivityFailed {
                activity: "research.a".into(),
                job_id: a,
                error: "e".into(),
            },
        )
        .push(T0, sched("research.b", b));
    impossible(h.fold(&wf), "after a failure");
}

#[test]
fn impossible_timer_events_are_rejected() {
    let wf = workflow(json!({ "name": "t", "steps": [
        { "name": "work", "activity": act("w") },
        { "name": "wait", "sleep_ms": 100 } ] }));
    let j = JobId::generate();
    let reached = || {
        H::started()
            .push(T0, sched("work", j))
            .push(T0, done("work", j))
    };
    let timer = |at: Timestamp| WorkflowEventKind::TimerScheduled {
        step: "wait".into(),
        fire_at_ms: at,
    };
    let fired = || WorkflowEventKind::TimerFired {
        step: "wait".into(),
    };

    impossible(
        H::started()
            .push(
                T0,
                WorkflowEventKind::TimerScheduled {
                    step: "work".into(),
                    fire_at_ms: T0 + 1,
                },
            )
            .fold(&wf),
        "not a sleep step",
    );
    impossible(
        H::started().push(T0, timer(T0 + 100)).fold(&wf),
        "before its step was reached",
    );
    impossible(
        reached().push(T0 + 5, timer(T0 + 5)).fold(&wf),
        "in the past",
    );
    impossible(
        reached()
            .push(T0, timer(T0 + 100))
            .push(T0, timer(T0 + 200))
            .fold(&wf),
        "scheduled twice",
    );
    impossible(
        reached().push(T0, fired()).fold(&wf),
        "without being scheduled",
    );
    impossible(
        reached()
            .push(T0, timer(T0 + 100))
            .push(T0 + 99, fired())
            .fold(&wf),
        "before its deadline",
    );
    impossible(
        reached()
            .push(T0, timer(T0 + 100))
            .push(T0 + 100, fired())
            .push(T0 + 100, fired())
            .fold(&wf),
        "fired twice",
    );
    assert!(
        reached()
            .push(T0, timer(T0 + 100))
            .push(T0 + 100, fired())
            .fold(&wf)
            .is_ok()
    );
}

#[test]
fn impossible_signal_and_completion_events_are_rejected() {
    let wf = workflow(json!({ "name": "s", "steps": [
        { "name": "work", "activity": act("w") },
        { "name": "gate", "wait_signal": "go" } ] }));
    let sig = |name: &str| WorkflowEventKind::SignalReceived {
        name: name.into(),
        payload: Value::Null,
    };
    let j = JobId::generate();

    impossible(H::started().push(T0, sig("other")).fold(&wf), "not awaited");
    impossible(
        H::started()
            .push(T0, sig("go"))
            .push(T0, sig("go"))
            .fold(&wf),
        "one-shot latch",
    );
    impossible(
        H::started()
            .push(T0, WorkflowEventKind::WorkflowCompleted)
            .fold(&wf),
        "steps left",
    );
    impossible(
        H::started()
            .push(T0, sched("work", j))
            .push(T0, done("work", j))
            .push(T0, WorkflowEventKind::WorkflowCompleted)
            .fold(&wf),
        "steps left",
    );
    impossible(
        H::started()
            .push(T0, sched("work", j))
            .push(T0, WorkflowEventKind::WorkflowFailed { reason: "r".into() })
            .fold(&wf),
        "still outstanding",
    );
    impossible(
        H::started()
            .push(T0, sched("work", j))
            .push(
                T0,
                WorkflowEventKind::WorkflowCancelled { reason: "r".into() },
            )
            .fold(&wf),
        "still outstanding",
    );
    let ok = H::started()
        .push(T0, sched("work", j))
        .push(T0, done("work", j))
        .push(T0, sig("go"))
        .push(T0, WorkflowEventKind::WorkflowCompleted);
    assert_eq!(ok.fold(&wf).unwrap().status, RunStatus::Succeeded);
}

#[test]
fn a_rejected_event_leaves_the_state_unchanged() {
    let wf = sequential();
    let j = JobId::generate();
    let mut state = H::started().push(T0, sched("first", j)).fold(&wf).unwrap();
    let before = state.clone();
    for bad in [
        WorkflowEvent {
            seq: 3,
            at_ms: T0,
            kind: done("first", JobId::generate()),
        },
        WorkflowEvent {
            seq: 3,
            at_ms: T0,
            kind: sched("second", JobId::generate()),
        },
        WorkflowEvent {
            seq: 4,
            at_ms: T0,
            kind: done("first", j),
        },
        WorkflowEvent {
            seq: 3,
            at_ms: T0 - 1,
            kind: done("first", j),
        },
    ] {
        assert!(state.apply(&wf, &bad).is_err(), "{bad:?}");
        assert_eq!(state, before, "state changed by rejected {bad:?}");
    }
}

// ---- event encoding ------------------------------------------------------

fn every_kind() -> Vec<WorkflowEventKind> {
    let j = JobId::generate();
    vec![
        WorkflowEventKind::WorkflowStarted {
            input: json!({ "topic": "x" }),
        },
        WorkflowEventKind::ActivityScheduled {
            activity: "p.a".into(),
            job_id: j,
        },
        WorkflowEventKind::ActivityCompleted {
            activity: "p.a".into(),
            job_id: j,
        },
        WorkflowEventKind::ActivityFailed {
            activity: "p.a".into(),
            job_id: j,
            error: "boom".into(),
        },
        WorkflowEventKind::ActivityCancelled {
            activity: "p.a".into(),
            job_id: j,
        },
        WorkflowEventKind::TimerScheduled {
            step: "t".into(),
            fire_at_ms: T0,
        },
        WorkflowEventKind::TimerFired { step: "t".into() },
        WorkflowEventKind::SignalReceived {
            name: "go".into(),
            payload: json!([1, 2]),
        },
        WorkflowEventKind::WorkflowCompleted,
        WorkflowEventKind::WorkflowFailed { reason: "r".into() },
        WorkflowEventKind::WorkflowCancelled { reason: "c".into() },
    ]
}

#[test]
fn every_event_round_trips_through_its_columns() {
    for kind in every_kind() {
        let (step, job_id, data) = kind.columns();
        assert!(
            data.is_object(),
            "data column must be an object (migration CHECK)"
        );
        let back = WorkflowEventKind::from_columns(
            kind.type_name(),
            step.map(str::to_owned),
            job_id,
            &data,
        )
        .unwrap();
        assert_eq!(back, kind);
        // The CHECKs in 0005: job_id iff activity_*, step iff activity_* or timer_*.
        let t = kind.type_name();
        assert_eq!(job_id.is_some(), t.starts_with("activity_"), "{t}");
        assert_eq!(
            step.is_some(),
            t.starts_with("activity_") || t.starts_with("timer_"),
            "{t}"
        );
    }
}

#[test]
fn every_event_round_trips_through_json() {
    for (i, kind) in every_kind().into_iter().enumerate() {
        let event = WorkflowEvent {
            seq: i as u64 + 1,
            at_ms: T0,
            kind,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], event.kind.type_name());
        assert_eq!(
            serde_json::from_value::<WorkflowEvent>(json).unwrap(),
            event
        );
    }
}

#[test]
fn malformed_columns_are_typed_errors() {
    let j = Some(JobId::generate());
    assert_eq!(
        WorkflowEventKind::from_columns("activity_started", Some("a".into()), j, &json!({})),
        Err(ColumnsError::UnknownType("activity_started".into()))
    );
    assert_eq!(
        WorkflowEventKind::from_columns("activity_scheduled", None, j, &json!({})),
        Err(ColumnsError::Missing("step"))
    );
    assert_eq!(
        WorkflowEventKind::from_columns("activity_completed", Some("a".into()), None, &json!({})),
        Err(ColumnsError::Missing("job_id"))
    );
    assert_eq!(
        WorkflowEventKind::from_columns("activity_failed", Some("a".into()), j, &json!({})),
        Err(ColumnsError::Missing("error"))
    );
    assert_eq!(
        WorkflowEventKind::from_columns(
            "timer_scheduled",
            Some("t".into()),
            None,
            &json!({ "fire_at_ms": "soon" })
        ),
        Err(ColumnsError::Missing("fire_at_ms"))
    );
    assert_eq!(
        WorkflowEventKind::from_columns("workflow_started", None, None, &json!({})),
        Err(ColumnsError::Missing("input"))
    );
    assert_eq!(
        WorkflowEventKind::from_columns("signal_received", None, None, &json!({ "name": 3 })),
        Err(ColumnsError::Missing("name"))
    );
}
