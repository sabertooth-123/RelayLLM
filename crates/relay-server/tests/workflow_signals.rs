//! Phase G: `wait_signal` as a one-shot durable latch.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{T0, open_store, raw_pool, register};
use relay_core::clock::ManualClock;
use relay_core::workflow::{
    StartWorkflowRequest, WorkflowEventKind, WorkflowId, WorkflowLimits, WorkflowState,
};
use relay_server::{DeciderConfig, SignalOutcome, Store, StoreError, StoreOptions, spawn_decider};
use serde_json::{Value, json};

const LEASE: Duration = Duration::from_secs(30);

struct Fixture {
    dir: tempfile::TempDir,
    clock: ManualClock,
    store: Store,
}

impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let clock = ManualClock::new(T0);
        let store = open_store(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
        register(&store, "w1", &["mock"]).await;
        Fixture { dir, clock, store }
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("r.db")
    }

    async fn restart(&mut self) {
        self.store.close().await;
        self.store = open_store(&self.db(), &self.clock, StoreOptions::default()).await;
    }

    async fn start(&self) -> WorkflowId {
        let request: StartWorkflowRequest =
            serde_json::from_value(json!({ "definition": approval() })).unwrap();
        let new = request.validate(&WorkflowLimits::default()).unwrap();
        let id = self.store.start_workflow(new).await.unwrap().run().id;
        self.store.decide_due(10).await.unwrap();
        id
    }

    async fn run_next(&self, claim_id: &str) {
        let job = self
            .store
            .claim("w1", claim_id, LEASE)
            .await
            .unwrap()
            .expect("a job");
        self.store
            .complete(job.id, job.attempt, "w1", &json!("ok"))
            .await
            .unwrap();
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

    async fn signals(&self, id: WorkflowId) -> Vec<(String, Value)> {
        self.store
            .workflow_history(id)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|e| match e.kind {
                WorkflowEventKind::SignalReceived { name, payload } => Some((name, payload)),
                _ => None,
            })
            .collect()
    }

    async fn wake_at(&self, id: WorkflowId) -> Option<i64> {
        self.store.workflow_run(id).await.unwrap().unwrap().wake_at
    }
}

/// report → wait for "approved" → publish
fn approval() -> Value {
    json!({ "name": "approval", "steps": [
        { "name": "report", "activity": { "kind": "mock", "payload": { "prompt": "draft" } } },
        { "name": "gate", "wait_signal": "approved" },
        { "name": "publish", "activity": { "kind": "mock", "payload": { "prompt": "ship" } } },
    ] })
}

#[tokio::test]
async fn a_run_waits_at_the_gate_until_the_signal_arrives() {
    let f = Fixture::new().await;
    let id = f.start().await;
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.wake_at(id).await,
        None,
        "waiting on a signal: nothing to wake for"
    );
    f.clock.advance_ms(86_400_000);
    assert_eq!(
        f.store.decide_due(10).await.unwrap().decided,
        0,
        "time alone never opens the gate"
    );

    let out = f
        .store
        .signal_workflow(id, "approved", json!({ "by": "pranav" }))
        .await
        .unwrap();
    assert_eq!(out, SignalOutcome::Recorded);
    assert_eq!(
        f.wake_at(id).await,
        Some(T0 + 86_400_000),
        "the signal made the run due"
    );
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.types(id).await.last(),
        Some(&"activity_scheduled"),
        "publish scheduled"
    );
    f.run_next("c2").await;
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.store.workflow_run(id).await.unwrap().unwrap().state,
        WorkflowState::Succeeded
    );
}

#[tokio::test]
async fn an_early_signal_is_kept_until_the_run_reaches_the_gate() {
    let f = Fixture::new().await;
    let id = f.start().await;
    // "report" is still running when the approval comes in.
    f.store
        .signal_workflow(id, "approved", json!(true))
        .await
        .unwrap();
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.types(id).await,
        ["workflow_started", "activity_scheduled", "signal_received"]
    );
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.types(id).await.last(),
        Some(&"activity_scheduled"),
        "passed the gate without waiting"
    );
}

#[tokio::test]
async fn an_acknowledged_signal_survives_a_restart() {
    let mut f = Fixture::new().await;
    let id = f.start().await;
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();
    // Acknowledged, then the coordinator dies before any decision.
    assert_eq!(
        f.store
            .signal_workflow(id, "approved", json!(1))
            .await
            .unwrap(),
        SignalOutcome::Recorded
    );
    f.restart().await;
    assert_eq!(f.signals(id).await, [("approved".to_owned(), json!(1))]);
    assert_eq!(
        f.store.decide_due(10).await.unwrap().decided,
        1,
        "still due after the restart"
    );
    assert_eq!(f.types(id).await.last(), Some(&"activity_scheduled"));
}

#[tokio::test]
async fn a_repeated_signal_is_idempotent_and_a_different_one_conflicts() {
    let f = Fixture::new().await;
    let id = f.start().await;
    let first = json!({ "by": "pranav", "note": "lgtm" });
    assert_eq!(
        f.store
            .signal_workflow(id, "approved", first)
            .await
            .unwrap(),
        SignalOutcome::Recorded
    );
    // A retry after a lost response, keys in another order: same payload.
    let retry = json!({ "note": "lgtm", "by": "pranav" });
    assert_eq!(
        f.store
            .signal_workflow(id, "approved", retry)
            .await
            .unwrap(),
        SignalOutcome::AlreadyReceived
    );
    assert!(matches!(
        f.store.signal_workflow(id, "approved", json!({ "by": "someone else" })).await,
        Err(StoreError::SignalConflict { name, .. }) if name == "approved"
    ));
    assert_eq!(
        f.signals(id).await.len(),
        1,
        "the latch holds the first payload only"
    );
}

#[tokio::test]
async fn invalid_signals_are_rejected_without_side_effects() {
    let f = Fixture::new().await;
    let id = f.start().await;
    let before = f.types(id).await;

    assert!(matches!(
        f.store.signal_workflow(id, "rejected", Value::Null).await,
        Err(StoreError::UnknownSignal { name, .. }) if name == "rejected"
    ));
    let missing = WorkflowId::generate();
    assert!(matches!(
        f.store.signal_workflow(missing, "approved", Value::Null).await,
        Err(StoreError::WorkflowNotFound(w)) if w == missing
    ));
    assert!(matches!(
        f.store
            .signal_workflow(id, "approved", json!("x".repeat(70_000)))
            .await,
        Err(StoreError::SignalTooLarge { max: 65_536, .. })
    ));
    assert_eq!(f.types(id).await, before);
}

#[tokio::test]
async fn a_finished_run_rejects_new_signals_but_accepts_retries_of_old_ones() {
    let f = Fixture::new().await;
    let id = f.start().await;
    f.store
        .signal_workflow(id, "approved", json!(1))
        .await
        .unwrap();
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();
    f.run_next("c2").await;
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.store.workflow_run(id).await.unwrap().unwrap().state,
        WorkflowState::Succeeded
    );

    // The original sender retries after a lost response: still fine.
    assert_eq!(
        f.store
            .signal_workflow(id, "approved", json!(1))
            .await
            .unwrap(),
        SignalOutcome::AlreadyReceived
    );

    // A run that ended without the latch set rejects it.
    let other = f.start().await;
    let job = f.store.claim("w1", "c3", LEASE).await.unwrap().unwrap();
    f.store
        .fail(job.id, job.attempt, "w1", "bad", false)
        .await
        .unwrap();
    f.store.decide_due(10).await.unwrap();
    assert!(matches!(
        f.store.signal_workflow(other, "approved", json!(1)).await,
        Err(StoreError::WorkflowFinished {
            state: WorkflowState::Failed,
            ..
        })
    ));
}

#[tokio::test]
async fn concurrent_deliveries_set_the_latch_exactly_once() {
    let f = Fixture::new().await;
    let id = f.start().await;
    let (a, b, c) = (json!(1), json!(1), json!(2));
    let (ra, rb, rc) = tokio::join!(
        f.store.signal_workflow(id, "approved", a),
        f.store.signal_workflow(id, "approved", b),
        f.store.signal_workflow(id, "approved", c),
    );
    let outcomes = [ra, rb, rc];
    let recorded = outcomes
        .iter()
        .filter(|r| matches!(r, Ok(SignalOutcome::Recorded)))
        .count();
    assert_eq!(recorded, 1, "{outcomes:?}");
    assert_eq!(f.signals(id).await.len(), 1);
    // Whichever payload won, every other delivery is consistent with it.
    let winner = f.signals(id).await[0].1.clone();
    for (r, sent) in outcomes.iter().zip([json!(1), json!(1), json!(2)]) {
        match r {
            Ok(_) => assert!(sent == winner || matches!(r, Ok(SignalOutcome::Recorded))),
            Err(StoreError::SignalConflict { .. }) => assert_ne!(sent, winner),
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test]
async fn the_signal_and_the_wake_up_commit_atomically() {
    let f = Fixture::new().await;
    let id = f.start().await;
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();
    let raw = raw_pool(&f.db()).await;
    sqlx::query(
        "CREATE TRIGGER sabotage BEFORE UPDATE OF wake_at ON workflow_runs
         BEGIN SELECT RAISE(ABORT, 'injected failure after the signal insert'); END",
    )
    .execute(&raw)
    .await
    .unwrap();

    assert!(
        f.store
            .signal_workflow(id, "approved", json!(1))
            .await
            .is_err()
    );
    assert!(
        f.signals(id).await.is_empty(),
        "the event rolled back with the failed wake-up"
    );

    sqlx::query("DROP TRIGGER sabotage")
        .execute(&raw)
        .await
        .unwrap();
    assert_eq!(
        f.store
            .signal_workflow(id, "approved", json!(1))
            .await
            .unwrap(),
        SignalOutcome::Recorded
    );
    assert_eq!(f.wake_at(id).await, Some(T0));
}

#[tokio::test]
async fn a_signal_nudges_the_decider() {
    let f = Fixture::new().await;
    let id = f.start().await;
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();
    let decider = spawn_decider(
        f.store.clone(),
        DeciderConfig {
            max_sleep: Duration::from_secs(60),
            batch: 10,
        },
    );
    tokio::time::sleep(Duration::from_millis(50)).await; // first pass: nothing due

    f.store
        .signal_workflow(id, "approved", json!(1))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while f.types(id).await.last() != Some(&"activity_scheduled") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the signal did not wake the decider"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    decider.abort();
}
