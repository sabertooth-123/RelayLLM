//! Phase F: durable timers and the decider task.
//!
//! Timer semantics are tested on the store with a manual clock (no sleeps).
//! The task tests use real tokio time only to wait for the background task,
//! with a long `max_sleep`, so anything that happens quickly can only have
//! been caused by the first pass or a nudge.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{T0, open_store, register};
use relay_core::clock::ManualClock;
use relay_core::workflow::{
    StartWorkflowRequest, WorkflowEventKind, WorkflowId, WorkflowLimits, WorkflowState,
};
use relay_server::{DeciderConfig, Store, StoreOptions, spawn_decider};
use serde_json::{Value, json};

const LEASE: Duration = Duration::from_secs(30);
const SLEEP_MS: i64 = 30_000;

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

    async fn start(&self, definition: Value) -> WorkflowId {
        let request: StartWorkflowRequest =
            serde_json::from_value(json!({ "definition": definition })).unwrap();
        let new = request.validate(&WorkflowLimits::default()).unwrap();
        self.store.start_workflow(new).await.unwrap().run().id
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

    async fn fire_at(&self, id: WorkflowId) -> i64 {
        self.store
            .workflow_history(id)
            .await
            .unwrap()
            .iter()
            .find_map(|e| match e.kind {
                WorkflowEventKind::TimerScheduled { fire_at_ms, .. } => Some(fire_at_ms),
                _ => None,
            })
            .expect("a scheduled timer")
    }
}

/// work → sleep 30 s → after
fn with_timer() -> Value {
    json!({ "name": "t", "steps": [
        { "name": "work", "activity": { "kind": "mock", "payload": { "prompt": "w" } } },
        { "name": "cooldown", "sleep_ms": SLEEP_MS },
        { "name": "after", "activity": { "kind": "mock", "payload": { "prompt": "a" } } },
    ] })
}

#[tokio::test]
async fn a_timer_waits_for_its_deadline_then_the_next_step_runs() {
    let f = Fixture::new().await;
    let id = f.start(with_timer()).await;
    f.store.decide_due(10).await.unwrap();
    f.clock.advance_ms(5);
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();

    let deadline = T0 + 5 + SLEEP_MS;
    assert_eq!(
        f.fire_at(id).await,
        deadline,
        "deadline = decision time + sleep_ms"
    );
    let run = f.store.workflow_run(id).await.unwrap().unwrap();
    assert_eq!(
        run.wake_at,
        Some(deadline),
        "the run sleeps as one row with a wake time"
    );
    assert_eq!(f.store.next_wake_at().await.unwrap(), Some(deadline));

    f.clock.set_ms(deadline - 1);
    assert_eq!(
        f.store.decide_due(10).await.unwrap().decided,
        0,
        "not due 1 ms early"
    );
    f.clock.set_ms(deadline);
    assert_eq!(f.store.decide_due(10).await.unwrap().decided, 1);
    let history = f.store.workflow_history(id).await.unwrap();
    let n = history.len();
    assert_eq!(
        [
            history[n - 2].kind.type_name(),
            history[n - 1].kind.type_name()
        ],
        ["timer_fired", "activity_scheduled"],
        "fired and the next step scheduled in one decision"
    );
    assert_eq!(history[n - 2].at_ms, deadline);
}

#[tokio::test]
async fn a_timer_survives_a_restart_with_its_original_deadline() {
    let mut f = Fixture::new().await;
    let id = f.start(with_timer()).await;
    f.store.decide_due(10).await.unwrap();
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();
    let deadline = f.fire_at(id).await;

    f.clock.advance_ms(10_000);
    f.restart().await;
    // Deciding again after the restart must not re-arm from the new "now".
    assert_eq!(f.store.decide_due(10).await.unwrap().decided, 0);
    assert_eq!(f.fire_at(id).await, deadline);
    assert_eq!(
        f.store.workflow_run(id).await.unwrap().unwrap().wake_at,
        Some(deadline)
    );

    f.clock.set_ms(deadline);
    f.store.decide_due(10).await.unwrap();
    let types = f.types(id).await;
    assert_eq!(types.iter().filter(|t| **t == "timer_scheduled").count(), 1);
    assert_eq!(types.iter().filter(|t| **t == "timer_fired").count(), 1);
}

#[tokio::test]
async fn an_overdue_timer_fires_once_on_the_first_pass_after_downtime() {
    let mut f = Fixture::new().await;
    let id = f.start(with_timer()).await;
    f.store.decide_due(10).await.unwrap();
    f.run_next("c1").await;
    f.store.decide_due(10).await.unwrap();

    // Coordinator down for an hour, far past the deadline.
    f.restart().await;
    f.clock.advance_ms(3_600_000);
    assert_eq!(f.store.decide_due(10).await.unwrap().decided, 1);
    let fired: Vec<_> = f
        .store
        .workflow_history(id)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.kind.type_name() == "timer_fired")
        .collect();
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].at_ms, T0 + 3_600_000, "fired late, not backdated");
    assert_eq!(
        f.store.decide_due(10).await.unwrap().decided,
        0,
        "fires once"
    );
}

#[tokio::test]
async fn a_long_timer_does_not_drag_the_startup_clock_floor_forward() {
    let mut f = Fixture::new().await;
    let id = f.start(json!({ "name": "long", "steps": [{ "name": "wait", "sleep_ms": 6 * 24 * 3_600_000_u64 }] })).await;
    f.store.decide_due(10).await.unwrap();
    let deadline = f.fire_at(id).await;
    f.restart().await;
    let floor = f.store.latest_timestamp().await.unwrap().unwrap();
    assert!(
        floor < deadline,
        "floor {floor} must ignore the 6-day deadline {deadline}"
    );
    assert_eq!(floor, T0);
}

#[tokio::test]
async fn back_to_back_timers_each_keep_their_own_deadline() {
    let f = Fixture::new().await;
    let id = f
        .start(json!({ "name": "two", "steps": [
        { "name": "first", "sleep_ms": 1_000 }, { "name": "second", "sleep_ms": 2_000 } ] }))
        .await;
    f.store.decide_due(10).await.unwrap();
    f.clock.set_ms(T0 + 1_000);
    f.store.decide_due(10).await.unwrap();
    let history = f.store.workflow_history(id).await.unwrap();
    let deadlines: Vec<_> = history
        .iter()
        .filter_map(|e| match &e.kind {
            WorkflowEventKind::TimerScheduled { step, fire_at_ms } => {
                Some((step.as_str(), *fire_at_ms))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        deadlines,
        [("first", T0 + 1_000), ("second", T0 + 3_000)],
        "second armed when first fired"
    );
    f.clock.set_ms(T0 + 3_000);
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.store.workflow_run(id).await.unwrap().unwrap().state,
        WorkflowState::Succeeded
    );
}

// ---- the decider task ------------------------------------------------------

/// Long enough that only the first pass or a nudge can explain fast progress.
const SLOW_TICK: Duration = Duration::from_secs(60);

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn the_first_pass_decides_runs_made_due_before_the_decider_started() {
    let f = Fixture::new().await;
    let id = f.start(with_timer()).await; // due now; nobody has decided it
    assert_eq!(f.types(id).await, ["workflow_started"]);

    let decider = spawn_decider(
        f.store.clone(),
        DeciderConfig {
            max_sleep: SLOW_TICK,
            batch: 10,
        },
    );
    eventually("the first pass schedules the first step", async || {
        f.types(id).await.contains(&"activity_scheduled")
    })
    .await;
    decider.abort();
}

#[tokio::test]
async fn commits_nudge_the_decider_instead_of_waiting_for_its_tick() {
    let f = Fixture::new().await;
    let decider = spawn_decider(
        f.store.clone(),
        DeciderConfig {
            max_sleep: SLOW_TICK,
            batch: 10,
        },
    );
    tokio::time::sleep(Duration::from_millis(50)).await; // let the first (empty) pass finish

    let id = f.start(with_timer()).await; // nudge from start_workflow
    eventually("start nudges the decider", async || {
        f.types(id).await.len() == 2
    })
    .await;

    f.run_next("c1").await; // nudge from the completion hook
    eventually("a completion nudges the decider", async || {
        f.types(id).await.contains(&"timer_scheduled")
    })
    .await;
    decider.abort();
}

#[tokio::test]
async fn the_decider_fires_a_timer_once_its_deadline_arrives() {
    let f = Fixture::new().await;
    let id = f
        .start(json!({ "name": "t", "steps": [
        { "name": "wait", "sleep_ms": SLEEP_MS },
        { "name": "after", "activity": { "kind": "mock", "payload": { "prompt": "a" } } } ] }))
        .await;
    // A short cap: the store's clock is manual, so the task re-checks often.
    let decider = spawn_decider(
        f.store.clone(),
        DeciderConfig {
            max_sleep: Duration::from_millis(20),
            batch: 10,
        },
    );
    eventually("timer armed", async || {
        f.types(id).await.contains(&"timer_scheduled")
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !f.types(id).await.contains(&"timer_fired"),
        "must not fire before the deadline"
    );

    f.clock.advance_ms(SLEEP_MS);
    eventually("timer fired and next step scheduled", async || {
        let types = f.types(id).await;
        types.contains(&"timer_fired") && types.last() == Some(&"activity_scheduled")
    })
    .await;
    decider.abort();
}
