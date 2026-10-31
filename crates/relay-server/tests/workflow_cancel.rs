//! Phase H: cancelling workflow runs, and its races with activity outcomes.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use common::{T0, open_store, raw_pool, register};
use relay_core::clock::ManualClock;
use relay_core::job::{AttemptOutcome, Job, JobState};
use relay_core::retry::RetryPolicy;
use relay_core::workflow::{
    StartWorkflowRequest, WorkflowEventKind, WorkflowId, WorkflowLimits, WorkflowState,
};
use relay_server::{CancelWorkflowOutcome, ReportOutcome, Store, StoreError, StoreOptions};
use serde_json::{Value, json};

const LEASE: Duration = Duration::from_secs(30);

fn options() -> StoreOptions {
    StoreOptions {
        retry: RetryPolicy::IMMEDIATE,
        ..StoreOptions::default()
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    clock: ManualClock,
    store: Store,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let clock = ManualClock::new(T0);
        let store = open_store(&dir.path().join("r.db"), &clock, options()).await;
        for w in ["w1", "w2", "w3"] {
            register(&store, w, &["mock"]).await;
        }
        Fixture { dir, clock, store }
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("r.db")
    }

    async fn start(&self, definition: Value) -> WorkflowId {
        let request: StartWorkflowRequest =
            serde_json::from_value(json!({ "definition": definition })).unwrap();
        let new = request.validate(&WorkflowLimits::default()).unwrap();
        let id = self.store.start_workflow(new).await.unwrap().run().id;
        self.store.decide_due(10).await.unwrap();
        id
    }

    async fn claim(&self, worker: &str, claim_id: &str) -> Job {
        self.store
            .claim(worker, claim_id, LEASE)
            .await
            .unwrap()
            .expect("a job")
    }

    async fn types(&self, id: WorkflowId) -> Vec<&'static str> {
        self.store
            .workflow_history(id)
            .await
            .unwrap()
            .iter()
            .map(|e| e.kind.type_name())
            .collect()
    }

    async fn run(&self, id: WorkflowId) -> relay_server::WorkflowRun {
        self.store.workflow_run(id).await.unwrap().unwrap()
    }

    async fn state(&self, job: &Job) -> JobState {
        self.store.get(job.id).await.unwrap().unwrap().state
    }
}

fn one_activity() -> Value {
    json!({ "name": "one", "steps": [
        { "name": "work", "activity": { "kind": "mock", "payload": { "prompt": "w" }, "max_attempts": 2 } },
        { "name": "after", "activity": { "kind": "mock", "payload": { "prompt": "a" } } } ] })
}

fn fan_out() -> Value {
    json!({ "name": "fan", "steps": [{ "name": "p", "parallel": [
        { "name": "a", "kind": "mock", "payload": { "prompt": "a" } },
        { "name": "b", "kind": "mock", "payload": { "prompt": "b" } },
        { "name": "c", "kind": "mock", "payload": { "prompt": "c" } } ] }] })
}

#[tokio::test]
async fn cancelling_mid_fan_out_cancels_every_outstanding_branch_atomically() {
    let f = Fixture::new().await;
    let id = f.start(fan_out()).await;
    let a = f.claim("w1", "a").await;
    let b = f.claim("w2", "b").await;
    f.store.complete(b.id, 1, "w2", &json!("B")).await.unwrap(); // b done, a running, c queued

    let out = f.store.cancel_workflow(id, "user abort").await.unwrap();
    assert!(
        matches!(&out, CancelWorkflowOutcome::Cancelled(run) if run.state == WorkflowState::Cancelled)
    );
    let run = f.run(id).await;
    assert_eq!(
        (run.state, run.wake_at, run.finished_at),
        (WorkflowState::Cancelled, None, Some(T0))
    );
    assert_eq!(
        f.types(id).await,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_scheduled",
            "activity_scheduled",
            "activity_completed",
            "activity_cancelled",
            "activity_cancelled",
            "workflow_cancelled"
        ]
    );
    match &f
        .store
        .workflow_history(id)
        .await
        .unwrap()
        .last()
        .unwrap()
        .kind
    {
        WorkflowEventKind::WorkflowCancelled { reason } => assert_eq!(reason, "user abort"),
        other => panic!("{other:?}"),
    }

    // The running branch's worker is fenced out, its attempt closed as cancelled.
    assert_eq!(f.state(&a).await, JobState::Cancelled);
    assert!(matches!(
        f.store.complete(a.id, 1, "w1", &json!("late")).await,
        Err(StoreError::StaleAttempt { .. })
    ));
    assert_eq!(
        f.store.attempts(a.id).await.unwrap()[0].outcome,
        Some(AttemptOutcome::Cancelled)
    );
    // The queued branch can never be claimed; the completed one is untouched.
    assert!(f.store.claim("w3", "c", LEASE).await.unwrap().is_none());
    assert_eq!(f.state(&b).await, JobState::Succeeded);
    // Nothing is left to decide.
    assert_eq!(f.store.decide_due(10).await.unwrap().decided, 0);
}

#[tokio::test]
async fn cancelling_a_sleeping_or_waiting_run_needs_no_jobs_and_stops_it() {
    let f = Fixture::new().await;
    let sleeping = f
        .start(json!({ "name": "s", "steps": [
        { "name": "wait", "sleep_ms": 60_000 },
        { "name": "after", "activity": { "kind": "mock", "payload": { "prompt": "a" } } } ] }))
        .await;
    assert_eq!(f.run(sleeping).await.wake_at, Some(T0 + 60_000));
    f.store
        .cancel_workflow(sleeping, "not needed")
        .await
        .unwrap();
    assert_eq!(
        f.run(sleeping).await.wake_at,
        None,
        "the timer no longer wakes it"
    );
    f.clock.advance_ms(120_000);
    assert_eq!(f.store.decide_due(10).await.unwrap().decided, 0);
    assert_eq!(
        f.types(sleeping).await.last(),
        Some(&"workflow_cancelled"),
        "the timer never fired"
    );

    let gated = f
        .start(json!({ "name": "g", "steps": [{ "name": "gate", "wait_signal": "go" }] }))
        .await;
    f.store.cancel_workflow(gated, "abandoned").await.unwrap();
    assert!(matches!(
        f.store.signal_workflow(gated, "go", Value::Null).await,
        Err(StoreError::WorkflowFinished {
            state: WorkflowState::Cancelled,
            ..
        })
    ));
}

#[tokio::test]
async fn cancel_is_idempotent_and_finished_runs_are_not_cancellable() {
    let f = Fixture::new().await;
    let id = f.start(one_activity()).await;
    f.store.cancel_workflow(id, "first").await.unwrap();
    let again = f.store.cancel_workflow(id, "second").await.unwrap();
    assert!(matches!(again, CancelWorkflowOutcome::AlreadyCancelled(_)));
    assert_eq!(
        f.types(id)
            .await
            .iter()
            .filter(|t| **t == "workflow_cancelled")
            .count(),
        1
    );

    let failed = f.start(one_activity()).await;
    let job = f.claim("w1", "c1").await;
    f.store.fail(job.id, 1, "w1", "bad", false).await.unwrap();
    f.store.decide_due(10).await.unwrap();
    assert!(matches!(
        f.store.cancel_workflow(failed, "late").await,
        Err(StoreError::WorkflowFinished {
            state: WorkflowState::Failed,
            ..
        })
    ));
    let missing = WorkflowId::generate();
    assert!(
        matches!(f.store.cancel_workflow(missing, "x").await, Err(StoreError::WorkflowNotFound(m)) if m == missing)
    );
}

/// Design §6 #11: workflow cancel racing an activity completion. Both take
/// the write lock, so exactly one wins, and the job, its attempt row and
/// the workflow history must all agree with the winner. Both orders must
/// actually happen.
#[tokio::test]
async fn workflow_cancel_racing_activity_completion_has_exactly_one_winner() {
    let mut cancels_won = 0;
    for round in 0..30 {
        let f = Fixture::new().await;
        let id = f.start(one_activity()).await;
        let job = f.claim("w1", "c1").await;

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let (a, b) = (f.store.clone(), f.store.clone());
        let (ba, bb) = (barrier.clone(), barrier.clone());
        let complete_task = async move {
            ba.wait().await;
            a.complete(job.id, 1, "w1", &json!("done")).await
        };
        let cancel_task = async move {
            bb.wait().await;
            b.cancel_workflow(id, "race").await
        };
        let (complete, cancel) = if round % 2 == 0 {
            let c = tokio::spawn(complete_task);
            let k = tokio::spawn(cancel_task);
            (c.await.unwrap(), k.await.unwrap())
        } else {
            let k = tokio::spawn(cancel_task);
            let c = tokio::spawn(complete_task);
            (c.await.unwrap(), k.await.unwrap())
        };
        // The run is cancelled either way: cancel never fails on a running run.
        assert!(
            matches!(cancel, Ok(CancelWorkflowOutcome::Cancelled(_))),
            "{cancel:?}"
        );
        assert_eq!(f.run(id).await.state, WorkflowState::Cancelled);

        let types = f.types(id).await;
        let final_job = f.store.get(job.id).await.unwrap().unwrap();
        let outcome = f.store.attempts(job.id).await.unwrap()[0].outcome;
        match complete {
            // Completion first: its result stands and is in the history;
            // the cancel found nothing outstanding.
            Ok(ReportOutcome::Applied(_)) => {
                assert_eq!(final_job.state, JobState::Succeeded);
                assert_eq!(outcome, Some(AttemptOutcome::Succeeded));
                assert_eq!(
                    types,
                    [
                        "workflow_started",
                        "activity_scheduled",
                        "activity_completed",
                        "workflow_cancelled"
                    ]
                );
            }
            // Cancel first: the job was cancelled and the report fenced out.
            Err(StoreError::StaleAttempt { .. }) => {
                cancels_won += 1;
                assert_eq!(final_job.state, JobState::Cancelled);
                assert_eq!(final_job.result, None);
                assert_eq!(outcome, Some(AttemptOutcome::Cancelled));
                assert_eq!(
                    types,
                    [
                        "workflow_started",
                        "activity_scheduled",
                        "activity_cancelled",
                        "workflow_cancelled"
                    ]
                );
            }
            other => panic!("invalid race outcome: {other:?}"),
        }
        // Whatever won, nothing more happens to the run.
        assert_eq!(f.store.decide_due(10).await.unwrap().decided, 0);
        assert!(
            f.store.claim("w2", "next", LEASE).await.unwrap().is_none(),
            "\"after\" was never scheduled"
        );
    }
    println!("workflow cancel won {cancels_won}/30 races");
    assert!(
        (1..30).contains(&cancels_won),
        "the race only went one way ({cancels_won}/30); both outcomes must be exercised"
    );
}

#[tokio::test]
async fn cancel_wins_over_a_failure_the_decider_has_not_seen_yet() {
    // Retries exhausted (activity_failed recorded, run due), then a cancel
    // lands before the decider runs: the run ends cancelled, not failed, and
    // the history shows both facts in order.
    let f = Fixture::new().await;
    let id = f.start(one_activity()).await;
    let job = f.claim("w1", "c1").await;
    f.store.fail(job.id, 1, "w1", "flaky", true).await.unwrap();
    f.claim("w1", "c2").await;
    f.store
        .fail(job.id, 2, "w1", "flaky again", true)
        .await
        .unwrap();
    assert!(
        f.run(id).await.wake_at.is_some(),
        "due: the decider would fail the run"
    );

    f.store.cancel_workflow(id, "operator").await.unwrap();
    assert_eq!(
        f.types(id).await,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_failed",
            "workflow_cancelled"
        ]
    );
    assert_eq!(
        f.store.decide_due(10).await.unwrap().decided,
        0,
        "the decider never sees a running run"
    );
    assert_eq!(f.run(id).await.state, WorkflowState::Cancelled);
}

#[tokio::test]
async fn a_cancelled_run_stays_cancelled_across_a_restart() {
    let mut f = Fixture::new().await;
    let id = f.start(fan_out()).await;
    let a = f.claim("w1", "a").await;
    f.store.cancel_workflow(id, "stop").await.unwrap();
    f.store.close().await;
    f.store = open_store(&f.db(), &f.clock, options()).await;

    assert_eq!(f.run(id).await.state, WorkflowState::Cancelled);
    assert_eq!(f.store.decide_due(10).await.unwrap().decided, 0);
    // The reaper can't resurrect a cancelled activity either.
    f.clock.advance_ms(LEASE.as_millis() as i64 + 1);
    assert!(f.store.reap_expired(10).await.unwrap().is_empty());
    assert_eq!(f.state(&a).await, JobState::Cancelled);
}

#[tokio::test]
async fn the_cancel_rolls_back_entirely_if_any_part_fails() {
    let f = Fixture::new().await;
    let id = f.start(fan_out()).await;
    let a = f.claim("w1", "a").await;
    let raw = raw_pool(&f.db()).await;
    sqlx::query(
        "CREATE TRIGGER sabotage BEFORE INSERT ON workflow_events WHEN NEW.type = 'workflow_cancelled'
         BEGIN SELECT RAISE(ABORT, 'injected failure before workflow_cancelled'); END",
    )
    .execute(&raw)
    .await
    .unwrap();

    assert!(f.store.cancel_workflow(id, "x").await.is_err());
    assert_eq!(
        f.state(&a).await,
        JobState::Running,
        "job cancels rolled back with the run's"
    );
    assert_eq!(f.run(id).await.state, WorkflowState::Running);
    assert!(!f.types(id).await.contains(&"activity_cancelled"));
    // The worker is still the owner and can finish normally.
    f.store.complete(a.id, 1, "w1", &json!("A")).await.unwrap();

    sqlx::query("DROP TRIGGER sabotage")
        .execute(&raw)
        .await
        .unwrap();
    f.store.cancel_workflow(id, "x").await.unwrap();
    assert_eq!(f.run(id).await.state, WorkflowState::Cancelled);
}
