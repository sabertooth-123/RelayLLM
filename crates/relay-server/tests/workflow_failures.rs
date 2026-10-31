//! Phase D: workflow failure paths. Every terminal job transition (fail,
//! reaper, cancel) reaches the history in the same transaction, retries stay
//! inside the activity's job, and fail-fast cancels siblings atomically.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{T0, open_store, raw_pool, register};
use relay_core::clock::ManualClock;
use relay_core::job::{AttemptOutcome, Job, JobState};
use relay_core::retry::RetryPolicy;
use relay_core::workflow::{
    StartWorkflowRequest, WorkflowEventKind, WorkflowId, WorkflowLimits, WorkflowState,
};
use relay_server::{ReportOutcome, Store, StoreError, StoreOptions};
use serde_json::{Value, json};

const LEASE: Duration = Duration::from_secs(30);
const LEASE_MS: i64 = 30_000;

fn options() -> StoreOptions {
    // Retries become claimable at once, so tests needn't wait out backoff.
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
        register(&store, "w1", &["mock"]).await;
        register(&store, "w2", &["mock"]).await;
        Fixture { dir, clock, store }
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("r.db")
    }

    async fn restart(&mut self) {
        self.store.close().await;
        self.store = open_store(&self.db(), &self.clock, options()).await;
    }

    /// Start and make the first decision.
    async fn start(&self, definition: Value) -> WorkflowId {
        let request: StartWorkflowRequest =
            serde_json::from_value(json!({ "definition": definition })).unwrap();
        let new = request.validate(&WorkflowLimits::default()).unwrap();
        let id = self.store.start_workflow(new).await.unwrap().run().id;
        assert_eq!(self.store.decide_due(10).await.unwrap().decided, 1);
        id
    }

    async fn claim(&self, worker: &str, claim_id: &str) -> Job {
        self.store
            .claim(worker, claim_id, LEASE)
            .await
            .unwrap()
            .expect("a job to claim")
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

    async fn failed_reason(&self, id: WorkflowId) -> String {
        let history = self.store.workflow_history(id).await.unwrap();
        match &history.last().unwrap().kind {
            WorkflowEventKind::WorkflowFailed { reason } => reason.clone(),
            other => panic!("expected workflow_failed last, got {other:?}"),
        }
    }

    async fn job(&self, job: &Job) -> Job {
        self.store.get(job.id).await.unwrap().unwrap()
    }
}

fn single(max_attempts: u32) -> Value {
    json!({ "name": "one", "steps": [
        { "name": "work", "activity": { "kind": "mock", "payload": { "prompt": "w" }, "max_attempts": max_attempts } },
        { "name": "after", "activity": { "kind": "mock", "payload": { "prompt": "a" } } },
    ] })
}

fn fan_out() -> Value {
    json!({ "name": "fan", "steps": [{ "name": "p", "parallel": [
        { "name": "a", "kind": "mock", "payload": { "prompt": "a" } },
        { "name": "b", "kind": "mock", "payload": { "prompt": "b" } },
        { "name": "c", "kind": "mock", "payload": { "prompt": "c" } },
    ] }] })
}

#[tokio::test]
async fn a_retryable_failure_retries_the_same_job_without_touching_the_history() {
    let f = Fixture::new().await;
    let id = f.start(single(3)).await;
    let first = f.claim("w1", "c1").await;
    f.store
        .fail(first.id, 1, "w1", "upstream 503", true)
        .await
        .unwrap();
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled"],
        "a retry is not an outcome"
    );
    assert_eq!(
        f.run(id).await.wake_at,
        None,
        "nothing for the workflow to decide"
    );

    let second = f.claim("w2", "c2").await;
    assert_eq!(
        (second.id, second.attempt),
        (first.id, 2),
        "same job, next attempt; no new activity job"
    );
    f.store
        .complete(second.id, 2, "w2", &json!("ok"))
        .await
        .unwrap();
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.types(id).await,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_completed",
            "activity_scheduled"
        ]
    );
}

#[tokio::test]
async fn exhausting_retries_fails_the_activity_and_then_the_run() {
    let f = Fixture::new().await;
    let id = f.start(single(2)).await;
    let job = f.claim("w1", "c1").await;
    f.store
        .fail(job.id, 1, "w1", "timeout", true)
        .await
        .unwrap();
    f.claim("w1", "c2").await;
    let last = f
        .store
        .fail(job.id, 2, "w1", "timeout again", true)
        .await
        .unwrap();
    assert_eq!(last.job().state, JobState::Failed);

    // Same transaction as the job's failure: the event and the wake-up.
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled", "activity_failed"]
    );
    assert_eq!(f.run(id).await.wake_at, Some(T0));

    f.store.decide_due(10).await.unwrap();
    let run = f.run(id).await;
    assert_eq!(
        (run.state, run.wake_at, run.finished_at),
        (WorkflowState::Failed, None, Some(T0))
    );
    assert_eq!(
        f.failed_reason(id).await,
        "activity \"work\" failed: timeout again"
    );
    assert!(
        f.store.claim("w1", "c3", LEASE).await.unwrap().is_none(),
        "the next step was never scheduled"
    );
}

#[tokio::test]
async fn a_permanent_failure_fails_the_run_on_the_first_attempt() {
    let f = Fixture::new().await;
    let id = f.start(single(5)).await;
    let job = f.claim("w1", "c1").await;
    f.store
        .fail(job.id, 1, "w1", "invalid payload", false)
        .await
        .unwrap();
    f.store.decide_due(10).await.unwrap();
    assert_eq!(f.run(id).await.state, WorkflowState::Failed);
    assert_eq!(f.store.attempts(job.id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn an_expired_last_lease_fails_the_activity_through_the_reaper() {
    let f = Fixture::new().await;
    let id = f.start(single(1)).await;
    f.claim("w1", "c1").await;
    f.clock.advance_ms(LEASE_MS + 1);
    let reaped = f.store.reap_expired(10).await.unwrap();
    assert_eq!((reaped.len(), reaped[0].new_state), (1, JobState::Failed));

    let history = f.store.workflow_history(id).await.unwrap();
    match &history.last().unwrap().kind {
        WorkflowEventKind::ActivityFailed {
            activity, error, ..
        } => {
            assert_eq!(activity, "work");
            assert!(error.contains("lease expired"), "{error}");
        }
        other => panic!("expected activity_failed, got {other:?}"),
    }
    f.store.decide_due(10).await.unwrap();
    assert_eq!(f.run(id).await.state, WorkflowState::Failed);
}

#[tokio::test]
async fn an_expired_lease_with_attempts_left_recovers_and_fences_the_old_worker() {
    let f = Fixture::new().await;
    let id = f.start(single(3)).await;
    let first = f.claim("w1", "c1").await;
    f.clock.advance_ms(LEASE_MS + 1);
    assert_eq!(
        f.store.reap_expired(10).await.unwrap()[0].new_state,
        JobState::RetryWaiting
    );
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled"]
    );

    let second = f.claim("w2", "c2").await;
    assert_eq!((second.id, second.attempt), (first.id, 2));
    f.store
        .complete(second.id, 2, "w2", &json!("from w2"))
        .await
        .unwrap();

    // The zombie from attempt 1 reports late: fenced, and the history keeps
    // exactly one completion, by attempt 2's job result.
    assert!(matches!(
        f.store.complete(first.id, 1, "w1", &json!("stale")).await,
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
    assert_eq!(f.job(&first).await.result, Some(json!("from w2")));
    let outcomes: Vec<_> = f
        .store
        .attempts(first.id)
        .await
        .unwrap()
        .into_iter()
        .map(|a| a.outcome)
        .collect();
    assert_eq!(
        outcomes,
        [
            Some(AttemptOutcome::LeaseExpired),
            Some(AttemptOutcome::Succeeded)
        ]
    );
}

#[tokio::test]
async fn cancelling_an_activity_job_directly_fails_its_run() {
    let f = Fixture::new().await;
    let id = f.start(single(3)).await;
    let job = f.claim("w1", "c1").await;
    f.store.cancel(job.id).await.unwrap();
    assert_eq!(
        f.types(id).await,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_cancelled"
        ]
    );
    f.store.decide_due(10).await.unwrap();
    assert_eq!(f.run(id).await.state, WorkflowState::Failed);
    assert_eq!(f.failed_reason(id).await, "activity \"work\" was cancelled");
    // Cancelling again is idempotent and appends nothing.
    f.store.cancel(job.id).await.unwrap();
    assert_eq!(f.types(id).await.len(), 4);
}

#[tokio::test]
async fn a_failed_branch_cancels_its_siblings_in_the_same_decision() {
    let f = Fixture::new().await;
    let id = f.start(fan_out()).await;
    let a = f.claim("w1", "c1").await;
    let b = f.claim("w2", "c2").await;
    f.store
        .fail(b.id, 1, "w2", "bad input", false)
        .await
        .unwrap();

    f.store.decide_due(10).await.unwrap();
    let types = f.types(id).await;
    assert_eq!(
        types,
        [
            "workflow_started",
            "activity_scheduled",
            "activity_scheduled",
            "activity_scheduled",
            "activity_failed",
            "activity_cancelled",
            "activity_cancelled",
            "workflow_failed"
        ]
    );
    assert_eq!(
        f.failed_reason(id).await,
        "activity \"p.b\" failed: bad input"
    );

    // Running sibling: cancelled and fenced, its attempt closed as cancelled.
    let a_now = f.job(&a).await;
    assert_eq!(a_now.state, JobState::Cancelled);
    assert!(a_now.last_error.unwrap().contains("another activity"));
    assert!(matches!(
        f.store.complete(a.id, 1, "w1", &json!("late")).await,
        Err(StoreError::StaleAttempt { .. })
    ));
    assert_eq!(
        f.store.attempts(a.id).await.unwrap()[0].outcome,
        Some(AttemptOutcome::Cancelled)
    );
    // Queued sibling: cancelled before anyone claimed it.
    assert!(f.store.claim("w1", "c3", LEASE).await.unwrap().is_none());
    assert_eq!(
        f.types(id).await.len(),
        types.len(),
        "the cancels' hook added nothing extra"
    );
}

#[tokio::test]
async fn a_restart_during_a_retry_neither_loses_nor_repeats_work() {
    let mut f = Fixture::new().await;
    let id = f.start(single(3)).await;
    let job = f.claim("w1", "c1").await;
    f.store.fail(job.id, 1, "w1", "blip", true).await.unwrap();

    f.restart().await;
    assert_eq!(
        f.store.decide_due(10).await.unwrap().decided,
        0,
        "nothing new to decide"
    );
    let again = f.claim("w2", "c2").await;
    assert_eq!((again.id, again.attempt), (job.id, 2));
    f.store
        .complete(job.id, 2, "w2", &json!("ok"))
        .await
        .unwrap();
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.store.attempts(job.id).await.unwrap().len(),
        2,
        "two attempts total, not three"
    );
    assert_eq!(
        f.types(id).await.last(),
        Some(&"activity_scheduled"),
        "the next step was scheduled"
    );
}

// ---- atomicity: fault injection between job update and workflow event ----

async fn sabotage(f: &Fixture, event_type: &str) -> sqlx::SqlitePool {
    let raw = raw_pool(&f.db()).await;
    sqlx::query(&format!(
        "CREATE TRIGGER sabotage BEFORE INSERT ON workflow_events
         WHEN NEW.type = '{event_type}'
         BEGIN SELECT RAISE(ABORT, 'injected failure before {event_type}'); END"
    ))
    .execute(&raw)
    .await
    .unwrap();
    raw
}

async fn repair(raw: &sqlx::SqlitePool) {
    sqlx::query("DROP TRIGGER sabotage")
        .execute(raw)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_terminal_fail_and_its_workflow_event_commit_atomically() {
    let f = Fixture::new().await;
    let id = f.start(single(1)).await;
    let job = f.claim("w1", "c1").await;
    let raw = sabotage(&f, "activity_failed").await;

    assert!(f.store.fail(job.id, 1, "w1", "boom", false).await.is_err());
    let now = f.job(&job).await;
    assert_eq!(
        (now.state, now.attempt, now.last_error.clone()),
        (JobState::Running, 1, None)
    );
    assert_eq!(
        f.store.attempts(job.id).await.unwrap()[0].outcome,
        None,
        "attempt still open"
    );
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled"]
    );

    repair(&raw).await;
    let retried = f.store.fail(job.id, 1, "w1", "boom", false).await.unwrap();
    assert!(matches!(retried, ReportOutcome::Applied(_)));
    assert_eq!(f.types(id).await.last(), Some(&"activity_failed"));
}

#[tokio::test]
async fn a_reaped_last_attempt_and_its_workflow_event_commit_atomically() {
    let f = Fixture::new().await;
    let id = f.start(single(1)).await;
    let job = f.claim("w1", "c1").await;
    f.clock.advance_ms(LEASE_MS + 1);
    let raw = sabotage(&f, "activity_failed").await;

    assert!(
        f.store.reap_expired(10).await.is_err(),
        "the whole reap batch rolls back"
    );
    let now = f.job(&job).await;
    assert_eq!(
        (now.state, now.worker_id.as_deref()),
        (JobState::Running, Some("w1"))
    );
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled"]
    );

    repair(&raw).await;
    assert_eq!(
        f.store.reap_expired(10).await.unwrap().len(),
        1,
        "the next pass recovers it"
    );
    assert_eq!(f.job(&job).await.state, JobState::Failed);
    assert_eq!(f.types(id).await.last(), Some(&"activity_failed"));
}

#[tokio::test]
async fn a_direct_cancel_and_its_workflow_event_commit_atomically() {
    let f = Fixture::new().await;
    let id = f.start(single(1)).await;
    let job = f.store.workflow_history(id).await.unwrap()[1]
        .kind
        .columns()
        .1
        .unwrap();
    let raw = sabotage(&f, "activity_cancelled").await;

    assert!(f.store.cancel(job).await.is_err());
    assert_eq!(
        f.store.get(job).await.unwrap().unwrap().state,
        JobState::Queued
    );
    repair(&raw).await;
    f.store.cancel(job).await.unwrap();
    assert_eq!(f.types(id).await.last(), Some(&"activity_cancelled"));
}

#[tokio::test]
async fn fail_fast_cancellation_and_the_run_failure_commit_atomically() {
    let f = Fixture::new().await;
    let id = f.start(fan_out()).await;
    let a = f.claim("w1", "c1").await;
    let b = f.claim("w2", "c2").await;
    f.store.fail(b.id, 1, "w2", "bad", false).await.unwrap();
    let raw = sabotage(&f, "workflow_failed").await;

    // The decision cancels a and c, then fails the run: one transaction.
    assert!(f.store.decide_due(10).await.is_err());
    assert_eq!(
        f.job(&a).await.state,
        JobState::Running,
        "sibling cancel rolled back with the failure"
    );
    assert_eq!(f.run(id).await.state, WorkflowState::Running);
    assert!(!f.types(id).await.contains(&"activity_cancelled"));

    repair(&raw).await;
    f.store.decide_due(10).await.unwrap();
    assert_eq!(f.job(&a).await.state, JobState::Cancelled);
    assert_eq!(f.run(id).await.state, WorkflowState::Failed);
}
