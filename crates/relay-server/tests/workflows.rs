//! Phase C: sequential workflows through the real store, on a manual clock.
//! Activities are ordinary jobs claimed and completed through the existing
//! store API; the decider is driven with `decide_due` (no sleeps).
#![allow(clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{T0, open_store, raw_pool, register};
use relay_core::clock::ManualClock;
use relay_core::job::{Job, JobState};
use relay_core::workflow::{
    NewWorkflow, StartWorkflowRequest, WorkflowEventKind, WorkflowId, WorkflowLimits, WorkflowState,
};
use relay_server::{DecidePass, ReportOutcome, StartOutcome, Store, StoreError, StoreOptions};
use serde_json::{Value, json};

const LEASE: Duration = Duration::from_secs(30);

struct Fixture {
    dir: tempfile::TempDir,
    clock: ManualClock,
    store: Store,
}

impl Fixture {
    async fn new(options: StoreOptions) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let clock = ManualClock::new(T0);
        let store = open_store(&dir.path().join("r.db"), &clock, options).await;
        register(&store, "w1", &["mock"]).await;
        Fixture { dir, clock, store }
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("r.db")
    }

    /// Simulate a coordinator restart: a fresh `Store` on the same file.
    async fn restart(&mut self) {
        self.store.close().await;
        self.store = open_store(&self.db(), &self.clock, StoreOptions::default()).await;
    }

    async fn start(
        &self,
        definition: Value,
        input: Value,
        key: Option<&str>,
    ) -> Result<StartOutcome, StoreError> {
        let mut request: StartWorkflowRequest =
            serde_json::from_value(json!({ "definition": definition, "input": input })).unwrap();
        request.idempotency_key = key.map(str::to_owned);
        let new: NewWorkflow = request.validate(&WorkflowLimits::default()).unwrap();
        self.store.start_workflow(new).await
    }

    /// Claim the next job as `w1` and complete it with `result`.
    async fn run_next(&self, claim_id: &str, result: Value) -> Job {
        let job = self
            .store
            .claim("w1", claim_id, LEASE)
            .await
            .unwrap()
            .expect("a job to claim");
        let done = self
            .store
            .complete(job.id, job.attempt, "w1", &result)
            .await
            .unwrap();
        assert!(matches!(done, ReportOutcome::Applied(_)));
        job
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

    /// Attempts recorded for a job: the "was it executed again?" check.
    async fn attempts(&self, job: &Job) -> usize {
        self.store.attempts(job.id).await.unwrap().len()
    }
}

fn two_steps() -> Value {
    json!({ "name": "two", "steps": [
        { "name": "draft", "activity": { "kind": "mock", "payload": { "prompt": "draft" }, "inputs": ["input"] } },
        { "name": "polish", "activity": { "kind": "mock", "payload": { "prompt": "polish" }, "inputs": ["draft"] } },
    ] })
}

fn decided(n: usize) -> DecidePass {
    DecidePass {
        decided: n,
        deferred: 0,
        broken: 0,
    }
}

#[tokio::test]
async fn a_sequential_workflow_runs_end_to_end_through_ordinary_jobs() {
    let f = Fixture::new(StoreOptions::default()).await;
    let run = f
        .start(two_steps(), json!({ "topic": "rust" }), None)
        .await
        .unwrap();
    let id = run.run().id;
    assert!(matches!(run, StartOutcome::Created(_)));
    assert_eq!(f.run(id).await.wake_at, Some(T0), "due at once");
    assert_eq!(f.types(id).await, ["workflow_started"]);

    // First decision: one ordinary queued job, linked by activity_scheduled.
    assert_eq!(f.store.decide_due(10).await.unwrap(), decided(1));
    assert_eq!(f.run(id).await.wake_at, None, "waiting on the job");
    assert_eq!(
        f.store.decide_due(10).await.unwrap(),
        decided(0),
        "not due: nothing to do"
    );

    f.clock.advance_ms(5);
    let draft = f.run_next("c1", json!({ "text": "first draft" })).await;
    assert_eq!(draft.kind, "mock");
    assert_eq!(
        draft.payload,
        json!({ "prompt": "draft", "inputs": { "input": { "topic": "rust" } } })
    );
    // The completion appended the event and made the run due, atomically.
    assert_eq!(
        f.types(id).await,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_completed"
        ]
    );
    assert_eq!(f.run(id).await.wake_at, Some(T0 + 5));

    assert_eq!(f.store.decide_due(10).await.unwrap(), decided(1));
    f.clock.advance_ms(5);
    let polish = f.run_next("c2", json!("final text")).await;
    assert_eq!(
        polish.payload,
        json!({ "prompt": "polish", "inputs": { "draft": { "text": "first draft" } } }),
        "the previous activity's result was passed in"
    );

    assert_eq!(f.store.decide_due(10).await.unwrap(), decided(1));
    let run = f.run(id).await;
    assert_eq!(
        (run.state, run.wake_at, run.finished_at),
        (WorkflowState::Succeeded, None, Some(T0 + 10))
    );
    let history = f.store.workflow_history(id).await.unwrap();
    let types: Vec<_> = history.iter().map(|e| e.kind.type_name()).collect();
    assert_eq!(
        types,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_completed",
            "activity_scheduled",
            "activity_completed",
            "workflow_completed"
        ]
    );
    // Ordering guarantees (§3.2): contiguous seq, non-decreasing time.
    assert!(
        history
            .iter()
            .enumerate()
            .all(|(i, e)| e.seq == i as u64 + 1)
    );
    assert!(history.windows(2).all(|w| w[0].at_ms <= w[1].at_ms));
    // Each activity ran exactly once.
    assert_eq!(
        (f.attempts(&draft).await, f.attempts(&polish).await),
        (1, 1)
    );
    assert!(
        f.store.claim("w1", "c3", LEASE).await.unwrap().is_none(),
        "no stray jobs"
    );
}

#[tokio::test]
async fn a_restart_between_completion_and_decision_resumes_without_rerunning() {
    let mut f = Fixture::new(StoreOptions::default()).await;
    let id = f
        .start(two_steps(), Value::Null, None)
        .await
        .unwrap()
        .run()
        .id;
    f.store.decide_due(10).await.unwrap();
    let draft = f.run_next("c1", json!("d")).await;

    // Crash after the completion committed, before any decision.
    f.restart().await;
    let (_, state) = f.store.workflow_state(id).await.unwrap().unwrap();
    assert_eq!(state.activities.len(), 1, "history survived the restart");

    // The first pass after the restart schedules the next step exactly once.
    assert_eq!(f.store.decide_due(10).await.unwrap(), decided(1));
    assert_eq!(f.store.decide_due(10).await.unwrap(), decided(0));
    let polish = f.run_next("c2", json!("p")).await;
    assert_ne!(polish.id, draft.id);
    assert_eq!(
        f.attempts(&draft).await,
        1,
        "the completed activity was not executed again"
    );
    f.store.decide_due(10).await.unwrap();
    assert_eq!(f.run(id).await.state, WorkflowState::Succeeded);
}

#[tokio::test]
async fn a_restart_with_the_activity_in_flight_keeps_the_lease_semantics() {
    let mut f = Fixture::new(StoreOptions::default()).await;
    let id = f
        .start(two_steps(), Value::Null, None)
        .await
        .unwrap()
        .run()
        .id;
    f.store.decide_due(10).await.unwrap();
    let job = f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();

    f.restart().await;
    assert_eq!(
        f.store.decide_due(10).await.unwrap(),
        decided(0),
        "nothing to decide while it runs"
    );
    // The worker that held the lease reports after the restart: accepted.
    f.store
        .complete(job.id, job.attempt, "w1", &json!("d"))
        .await
        .unwrap();
    assert_eq!(f.store.decide_due(10).await.unwrap(), decided(1));
    let (_, state) = f.store.workflow_state(id).await.unwrap().unwrap();
    assert_eq!(state.activities.len(), 2);
}

#[tokio::test]
async fn duplicate_and_stale_completions_append_nothing() {
    let f = Fixture::new(StoreOptions::default()).await;
    register(&f.store, "w2", &["mock"]).await;
    let id = f
        .start(two_steps(), Value::Null, None)
        .await
        .unwrap()
        .run()
        .id;
    f.store.decide_due(10).await.unwrap();
    let job = f.run_next("c1", json!("d")).await;

    // The same worker replays its report (lost response): AlreadyApplied.
    let again = f
        .store
        .complete(job.id, job.attempt, "w1", &json!("d"))
        .await
        .unwrap();
    assert!(matches!(again, ReportOutcome::AlreadyApplied(_)));
    // Someone else reports for that attempt: fenced.
    assert!(matches!(
        f.store
            .complete(job.id, job.attempt, "w2", &json!("x"))
            .await,
        Err(StoreError::StaleAttempt { .. })
    ));
    assert_eq!(
        f.types(id).await,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_completed"
        ]
    );
}

#[tokio::test]
async fn starting_is_idempotent_per_key() {
    let f = Fixture::new(StoreOptions::default()).await;
    let a = f.start(two_steps(), json!(1), Some("k1")).await.unwrap();
    let b = f.start(two_steps(), json!(1), Some("k1")).await.unwrap();
    assert!(matches!(a, StartOutcome::Created(_)));
    assert!(matches!(b, StartOutcome::Existing(_)));
    assert_eq!(a.run().id, b.run().id);
    assert_eq!(
        f.types(a.run().id).await,
        ["workflow_started"],
        "replay appends nothing"
    );

    let conflict = f.start(two_steps(), json!(2), Some("k1")).await;
    assert!(matches!(
        conflict,
        Err(StoreError::WorkflowIdempotencyConflict { existing, .. }) if existing == a.run().id
    ));
    // No key: always a new run.
    let c = f.start(two_steps(), json!(1), None).await.unwrap();
    let d = f.start(two_steps(), json!(1), None).await.unwrap();
    assert_ne!(c.run().id, d.run().id);
}

#[tokio::test]
async fn a_full_queue_defers_scheduling_instead_of_failing_the_run() {
    let f = Fixture::new(StoreOptions {
        max_active_jobs: 1,
        ..StoreOptions::default()
    })
    .await;
    let blocker = common::submit_job(&f.store, common::prompt("blocker"), None).await;
    let id = f
        .start(two_steps(), Value::Null, None)
        .await
        .unwrap()
        .run()
        .id;

    let pass = f.store.decide_due(10).await.unwrap();
    assert_eq!(
        pass,
        DecidePass {
            decided: 0,
            deferred: 1,
            broken: 0
        }
    );
    let run = f.run(id).await;
    assert_eq!(
        (run.state, run.wake_at),
        (WorkflowState::Running, Some(T0 + 1_000))
    );
    assert_eq!(
        f.types(id).await,
        ["workflow_started"],
        "the rolled-back decision left no events"
    );

    // Still blocked when due again; then capacity frees up.
    f.clock.advance_ms(1_000);
    assert_eq!(f.store.decide_due(10).await.unwrap().deferred, 1);
    let claimed = f.store.claim("w1", "c0", LEASE).await.unwrap().unwrap();
    assert_eq!(claimed.id, blocker.id);
    f.store
        .complete(blocker.id, 1, "w1", &json!("ok"))
        .await
        .unwrap();
    f.clock.advance_ms(1_000);
    assert_eq!(f.store.decide_due(10).await.unwrap(), decided(1));
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled"]
    );
}

#[tokio::test]
async fn concurrent_decision_passes_schedule_each_activity_once() {
    let f = Fixture::new(StoreOptions::default()).await;
    let ids: Vec<_> = futures_join_all(&f, 5).await;
    let (a, b, c) = tokio::join!(
        f.store.decide_due(100),
        f.store.decide_due(100),
        f.store.decide_due(100)
    );
    let total = a.unwrap().decided + b.unwrap().decided + c.unwrap().decided;
    assert_eq!(total, 5, "each run decided exactly once across the passes");
    for id in ids {
        assert_eq!(
            f.types(id).await,
            ["workflow_started", "activity_scheduled"]
        );
    }
}

async fn futures_join_all(f: &Fixture, n: usize) -> Vec<WorkflowId> {
    let mut ids = Vec::new();
    for i in 0..n {
        ids.push(f.start(two_steps(), json!(i), None).await.unwrap().run().id);
    }
    ids
}

#[tokio::test]
async fn two_runs_stay_isolated() {
    let f = Fixture::new(StoreOptions::default()).await;
    let x = f
        .start(two_steps(), json!("x"), None)
        .await
        .unwrap()
        .run()
        .id;
    let y = f
        .start(two_steps(), json!("y"), None)
        .await
        .unwrap()
        .run()
        .id;
    f.store.decide_due(10).await.unwrap();
    // Complete whichever job is claimed first; only its run advances.
    let job = f.run_next("c1", json!("r")).await;
    let x_done = f.types(x).await.contains(&"activity_completed");
    let y_done = f.types(y).await.contains(&"activity_completed");
    assert!(x_done ^ y_done, "exactly one run saw the completion");
    let owner = if x_done { x } else { y };
    let history = f.store.workflow_history(owner).await.unwrap();
    assert!(history.iter().any(|e| matches!(&e.kind,
        WorkflowEventKind::ActivityCompleted { job_id, .. } if *job_id == job.id)));
}

#[tokio::test]
async fn a_corrupt_history_is_quarantined_not_advanced() {
    let f = Fixture::new(StoreOptions::default()).await;
    let id = f
        .start(two_steps(), Value::Null, None)
        .await
        .unwrap()
        .run()
        .id;
    // Sabotage behind the store's back: a timer event for a step that isn't one.
    let raw = raw_pool(&f.db()).await;
    sqlx::query(
        "INSERT INTO workflow_events (run_id, seq, type, step, data, at)
         VALUES (?, 2, 'timer_fired', 'draft', '{}', ?)",
    )
    .bind(id.to_string())
    .bind(T0)
    .execute(&raw)
    .await
    .unwrap();

    assert_eq!(
        f.store.decide_due(10).await.unwrap(),
        DecidePass {
            decided: 0,
            deferred: 0,
            broken: 1
        }
    );
    let run = f.run(id).await;
    assert_eq!(
        (run.state, run.wake_at),
        (WorkflowState::Running, Some(T0 + 60_000)),
        "retried later"
    );
    assert_eq!(
        f.store.workflow_history(id).await.unwrap().len(),
        2,
        "nothing appended"
    );
    assert!(
        f.store.claim("w1", "c1", LEASE).await.unwrap().is_none(),
        "no job created"
    );
}

#[tokio::test]
async fn the_startup_clock_floor_includes_workflow_writes() {
    let mut f = Fixture::new(StoreOptions::default()).await;
    let id = f
        .start(two_steps(), Value::Null, None)
        .await
        .unwrap()
        .run()
        .id;
    f.clock.advance_ms(10_000);
    f.store.decide_due(10).await.unwrap(); // event at T0 + 10 s; jobs/workers older
    f.restart().await;
    let latest = f.store.latest_timestamp().await.unwrap().unwrap();
    assert!(
        latest >= T0 + 10_000,
        "floor {latest} misses the workflow event"
    );
    let history = f.store.workflow_history(id).await.unwrap();
    assert_eq!(history.last().unwrap().at_ms, T0 + 10_000);
}

/// Design §5, the central invariant: a terminal activity job transition and
/// its workflow event commit atomically. Inject a failure between the job
/// update and the event insert; neither half may survive.
#[tokio::test]
async fn an_activity_completion_and_its_workflow_event_commit_atomically() {
    let f = Fixture::new(StoreOptions::default()).await;
    let id = f
        .start(two_steps(), Value::Null, None)
        .await
        .unwrap()
        .run()
        .id;
    f.store.decide_due(10).await.unwrap();
    let job = f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();

    let raw = raw_pool(&f.db()).await;
    sqlx::query(
        "CREATE TRIGGER sabotage BEFORE INSERT ON workflow_events
         WHEN NEW.type = 'activity_completed'
         BEGIN SELECT RAISE(ABORT, 'injected failure between job update and workflow event'); END",
    )
    .execute(&raw)
    .await
    .unwrap();

    let failed = f
        .store
        .complete(job.id, job.attempt, "w1", &json!("result"))
        .await;
    assert!(
        failed.is_err(),
        "the injected failure must surface: {failed:?}"
    );

    // Neither half applied.
    let now = f.store.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        (now.state, now.attempt, now.result.clone()),
        (JobState::Running, 1, None)
    );
    assert_eq!(
        now.worker_id.as_deref(),
        Some("w1"),
        "still owned by the worker"
    );
    let attempts = f.store.attempts(job.id).await.unwrap();
    assert_eq!(
        (
            attempts.len(),
            attempts[0].finished_at_ms,
            attempts[0].outcome
        ),
        (1, None, None)
    );
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled"]
    );
    assert_eq!(
        f.run(id).await.wake_at,
        None,
        "the run was not made due either"
    );

    // The worker retries its report once the fault is gone: both halves together.
    sqlx::query("DROP TRIGGER sabotage")
        .execute(&raw)
        .await
        .unwrap();
    let done = f
        .store
        .complete(job.id, job.attempt, "w1", &json!("result"))
        .await
        .unwrap();
    assert!(matches!(done, ReportOutcome::Applied(_)));
    assert_eq!(
        f.store.get(job.id).await.unwrap().unwrap().state,
        JobState::Succeeded
    );
    assert_eq!(
        f.types(id).await,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_completed"
        ]
    );
    assert_eq!(f.run(id).await.wake_at, Some(T0));
}
