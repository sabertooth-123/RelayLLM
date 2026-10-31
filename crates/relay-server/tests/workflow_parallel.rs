//! Phase E: parallel fan-out / fan-in, result passing between steps, and an
//! end-to-end run on real worker loops with the real mock handler.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{RunningWorker, T0, TestServer, fast_worker_config, open_store, register};
use relay_client::RelayClient;
use relay_core::clock::ManualClock;
use relay_core::job::{Job, JobState};
use relay_core::retry::RetryPolicy;
use relay_core::workflow::{
    StartWorkflowRequest, WorkflowEventKind, WorkflowId, WorkflowLimits, WorkflowState,
};
use relay_server::{DecidePass, Store, StoreOptions};
use relay_worker::{MockLlmHandler, Worker};
use serde_json::{Value, json};

const LEASE: Duration = Duration::from_secs(30);

fn options() -> StoreOptions {
    StoreOptions {
        retry: RetryPolicy::IMMEDIATE,
        ..StoreOptions::default()
    }
}

/// plan → research(a, b, c) → aggregate(inputs: a, b, c)
fn research() -> Value {
    json!({ "name": "research", "steps": [
        { "name": "plan", "activity": { "kind": "mock", "payload": { "prompt": "plan the work" }, "inputs": ["input"] } },
        { "name": "research", "parallel": [
            { "name": "a", "kind": "mock", "payload": { "prompt": "topic a" }, "inputs": ["plan"] },
            { "name": "b", "kind": "mock", "payload": { "prompt": "topic b" }, "inputs": ["plan"] },
            { "name": "c", "kind": "mock", "payload": { "prompt": "topic c" }, "inputs": ["plan"] } ] },
        { "name": "aggregate", "activity": { "kind": "mock", "payload": { "prompt": "merge findings" },
            "inputs": ["research.a", "research.b", "research.c"] } },
    ] })
}

fn request(definition: Value, input: Value) -> relay_core::workflow::NewWorkflow {
    let r: StartWorkflowRequest =
        serde_json::from_value(json!({ "definition": definition, "input": input })).unwrap();
    r.validate(&WorkflowLimits::default()).unwrap()
}

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
        for w in ["w1", "w2", "w3"] {
            register(&store, w, &["mock"]).await;
        }
        Fixture { dir, clock, store }
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("r.db")
    }

    async fn start(&self, definition: Value) -> WorkflowId {
        let id = self
            .store
            .start_workflow(request(definition, json!({ "topic": "rust" })))
            .await
            .unwrap()
            .run()
            .id;
        self.store.decide_due(10).await.unwrap();
        id
    }

    async fn claim(&self, worker: &str, claim_id: &str) -> Job {
        self.store
            .claim(worker, claim_id, LEASE)
            .await
            .unwrap()
            .expect("a job to claim")
    }

    async fn complete(&self, job: &Job, worker: &str, result: Value) {
        self.store
            .complete(job.id, job.attempt, worker, &result)
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

    /// Run "plan" to completion, then claim the three branches (claim order
    /// is job-creation order: a, b, c).
    async fn through_fan_out(&self, id: WorkflowId) -> [Job; 3] {
        let plan = self.claim("w1", "p").await;
        self.complete(&plan, "w1", json!({ "completion": "the plan" }))
            .await;
        self.store.decide_due(10).await.unwrap();
        let types = self.types(id).await;
        assert_eq!(
            types[types.len() - 3..],
            ["activity_scheduled"; 3],
            "all branches in one decision"
        );
        [
            self.claim("w1", "a").await,
            self.claim("w2", "b").await,
            self.claim("w3", "c").await,
        ]
    }

    async fn job_for(&self, id: WorkflowId, activity: &str) -> Job {
        let history = self.store.workflow_history(id).await.unwrap();
        let job_id = history
            .iter()
            .find_map(|e| match &e.kind {
                WorkflowEventKind::ActivityScheduled {
                    activity: a,
                    job_id,
                } if a == activity => Some(*job_id),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{activity} not scheduled"));
        self.store.get(job_id).await.unwrap().unwrap()
    }
}

#[tokio::test]
async fn branches_fan_out_together_and_fan_in_only_when_all_are_done() {
    let f = Fixture::new(options()).await;
    let id = f.start(research()).await;
    let [a, b, c] = f.through_fan_out(id).await;
    assert_eq!(
        a.payload["inputs"],
        json!({ "plan": { "completion": "the plan" } }),
        "each branch sees plan's result"
    );

    // Completed out of order; each completion makes the run due, but nothing
    // new is scheduled until the last branch is in.
    f.complete(&c, "w3", json!("C")).await;
    f.store.decide_due(10).await.unwrap();
    f.complete(&a, "w1", json!("A")).await;
    f.store.decide_due(10).await.unwrap();
    let before = f.types(id).await.len();
    assert!(!f.types(id).await.contains(&"workflow_completed"));
    assert_eq!(
        f.store.claim("w1", "x", LEASE).await.unwrap(),
        None,
        "aggregate not scheduled yet"
    );

    f.complete(&b, "w2", json!("B")).await;
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.types(id).await.len(),
        before + 2,
        "b's completion + aggregate's schedule"
    );
    let aggregate = f.claim("w1", "agg").await;
    assert_eq!(
        aggregate.payload["inputs"],
        json!({ "research.a": "A", "research.b": "B", "research.c": "C" })
    );
    f.complete(&aggregate, "w1", json!("merged")).await;
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.store.workflow_run(id).await.unwrap().unwrap().state,
        WorkflowState::Succeeded
    );
}

#[tokio::test]
async fn concurrent_branch_completions_each_get_their_own_event() {
    let f = Fixture::new(options()).await;
    let id = f.start(research()).await;
    let [a, b, c] = f.through_fan_out(id).await;
    let (va, vb, vc) = (json!("A"), json!("B"), json!("C"));
    let (ra, rb, rc) = tokio::join!(
        f.store.complete(a.id, 1, "w1", &va),
        f.store.complete(b.id, 1, "w2", &vb),
        f.store.complete(c.id, 1, "w3", &vc),
    );
    for r in [ra, rb, rc] {
        assert!(matches!(
            r.unwrap(),
            relay_server::ReportOutcome::Applied(_)
        ));
    }

    let history = f.store.workflow_history(id).await.unwrap();
    assert!(
        history
            .iter()
            .enumerate()
            .all(|(i, e)| e.seq == i as u64 + 1),
        "contiguous seq under concurrency"
    );
    let completed = history
        .iter()
        .filter(|e| e.kind.type_name() == "activity_completed")
        .count();
    assert_eq!(completed, 4, "plan + three branches");
    // Concurrent decision passes still schedule the aggregate exactly once.
    let (x, y) = tokio::join!(f.store.decide_due(10), f.store.decide_due(10));
    assert_eq!(x.unwrap().decided + y.unwrap().decided, 1);
    assert_eq!(
        f.types(id)
            .await
            .iter()
            .filter(|t| **t == "activity_scheduled")
            .count(),
        5
    );
}

#[tokio::test]
async fn a_restart_mid_fan_out_keeps_each_branch_where_it_was() {
    let mut f = Fixture::new(options()).await;
    let id = f.start(research()).await;
    let plan = f.claim("w1", "p").await;
    f.complete(&plan, "w1", json!("plan")).await;
    f.store.decide_due(10).await.unwrap();
    let a = f.claim("w1", "a").await;
    let b = f.claim("w2", "b").await; // c stays queued
    f.complete(&a, "w1", json!("A")).await;

    // Crash: a done, b running, c queued.
    f.store.close().await;
    f.store = open_store(&f.db(), &f.clock, options()).await;
    assert_eq!(
        f.store.decide_due(10).await.unwrap(),
        DecidePass {
            decided: 1,
            deferred: 0,
            broken: 0
        }
    );
    let (_, state) = f.store.workflow_state(id).await.unwrap().unwrap();
    let status = |k: &str| format!("{:?}", state.activities[k].status);
    assert_eq!(
        (
            status("research.a"),
            status("research.b"),
            status("research.c")
        ),
        ("Completed".into(), "Scheduled".into(), "Scheduled".into())
    );

    // b's original worker still owns it; c is claimed for the first time.
    f.complete(&b, "w2", json!("B")).await;
    let c = f.claim("w3", "c").await;
    assert_eq!(c.attempt, 1);
    f.complete(&c, "w3", json!("C")).await;
    f.store.decide_due(10).await.unwrap();
    let aggregate = f.claim("w1", "agg").await;
    assert_eq!(
        aggregate.payload["inputs"]["research.a"], "A",
        "a's result survived the restart"
    );
    for job in [&plan, &a, &b, &c] {
        assert_eq!(
            f.store.attempts(job.id).await.unwrap().len(),
            1,
            "nothing ran twice"
        );
    }
}

#[tokio::test]
async fn a_retrying_branch_holds_the_fan_in_until_it_succeeds() {
    let f = Fixture::new(options()).await;
    let id = f.start(research()).await;
    let [a, b, c] = f.through_fan_out(id).await;
    f.complete(&a, "w1", json!("A")).await;
    f.complete(&c, "w3", json!("C")).await;
    f.store
        .fail(b.id, 1, "w2", "rate limited", true)
        .await
        .unwrap();
    f.store.decide_due(10).await.unwrap();
    assert_eq!(
        f.store.claim("w1", "x", LEASE).await.unwrap().map(|j| j.id),
        Some(b.id),
        "only b's retry is claimable"
    );
    f.complete(&f.job_for(id, "research.b").await, "w1", json!("B2"))
        .await;
    f.store.decide_due(10).await.unwrap();
    let aggregate = f.claim("w2", "agg").await;
    assert_eq!(aggregate.payload["inputs"]["research.b"], "B2");
    assert_eq!(f.store.attempts(b.id).await.unwrap().len(), 2);
}

#[tokio::test]
async fn fan_out_wider_than_free_capacity_schedules_what_fits_and_finishes() {
    // 3 branches, room for 2: the first decision schedules a and b, then c
    // as soon as a slot frees. All-or-nothing scheduling would never start.
    let f = Fixture::new(StoreOptions {
        max_active_jobs: 2,
        ..options()
    })
    .await;
    let id = f.start(research()).await;
    let plan = f.claim("w1", "p").await;
    f.complete(&plan, "w1", json!("plan")).await;

    assert_eq!(
        f.store.decide_due(10).await.unwrap(),
        DecidePass {
            decided: 0,
            deferred: 1,
            broken: 0
        }
    );
    let run = f.store.workflow_run(id).await.unwrap().unwrap();
    assert_eq!(run.wake_at, Some(T0 + 1_000), "retry the rest in 1 s");
    assert_eq!(
        f.types(id)
            .await
            .iter()
            .filter(|t| **t == "activity_scheduled")
            .count(),
        3,
        "plan + a + b"
    );

    let a = f.claim("w1", "a").await;
    f.complete(&a, "w1", json!("A")).await;
    f.clock.advance_ms(1_000);
    assert_eq!(f.store.decide_due(10).await.unwrap().deferred, 0);
    assert_eq!(f.job_for(id, "research.c").await.state, JobState::Queued);
}

#[tokio::test]
async fn aggregated_inputs_over_the_payload_limit_fail_the_run_cleanly() {
    let f = Fixture::new(options()).await;
    let id = f.start(research()).await;
    let [a, b, c] = f.through_fan_out(id).await;
    let big = json!("x".repeat(30_000)); // 3 × 30 KB > 64 KB payload limit
    for (job, w) in [(&a, "w1"), (&b, "w2"), (&c, "w3")] {
        f.complete(job, w, big.clone()).await;
    }
    f.store.decide_due(10).await.unwrap();
    let run = f.store.workflow_run(id).await.unwrap().unwrap();
    assert_eq!(run.state, WorkflowState::Failed);
    let history = f.store.workflow_history(id).await.unwrap();
    match &history.last().unwrap().kind {
        WorkflowEventKind::WorkflowFailed { reason } => {
            assert!(
                reason.contains("inputs too large for activity \"aggregate\""),
                "{reason}"
            )
        }
        other => panic!("{other:?}"),
    }
    assert!(
        f.store.claim("w1", "x", LEASE).await.unwrap().is_none(),
        "no aggregate job"
    );
}

// ---- end to end on real worker loops --------------------------------------

#[tokio::test]
async fn the_research_workflow_runs_on_real_workers_with_the_mock_handler() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, options()).await;
    let workers: Vec<_> = (0..3)
        .map(|i| {
            RunningWorker::spawn(
                Worker::new(
                    RelayClient::new(server.url()).unwrap(),
                    fast_worker_config(&format!("w{i}"), 2),
                )
                .with_handler("mock", MockLlmHandler::new(Duration::from_millis(30))),
            )
        })
        .collect();

    let id = server
        .store
        .start_workflow(request(research(), json!({ "topic": "rust" })))
        .await
        .unwrap()
        .run()
        .id;
    // Drive the decider the way the coordinator's task will (Phase F).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        server.store.decide_due(10).await.unwrap();
        let run = server.store.workflow_run(id).await.unwrap().unwrap();
        if run.state != WorkflowState::Running {
            assert_eq!(run.state, WorkflowState::Succeeded);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "workflow did not finish in 20 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The real handler consumed the inputs; results flowed through the chain.
    let history = server.store.workflow_history(id).await.unwrap();
    let job_of = |name: &str| {
        history
            .iter()
            .find_map(|e| match &e.kind {
                WorkflowEventKind::ActivityCompleted { activity, job_id } if activity == name => {
                    Some(*job_id)
                }
                _ => None,
            })
            .unwrap()
    };
    let aggregate = server
        .store
        .get(job_of("aggregate"))
        .await
        .unwrap()
        .unwrap();
    let result = aggregate.result.unwrap();
    assert_eq!(
        result["inputs_seen"],
        json!(["research.a", "research.b", "research.c"])
    );
    assert_eq!(result["completion"], "[mock-llm] findings merge");
    let branch = server
        .store
        .get(job_of("research.b"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(branch.result.unwrap()["inputs_seen"], json!(["plan"]));
    // Exactly one job per activity, each executed once.
    let scheduled = history
        .iter()
        .filter(|e| e.kind.type_name() == "activity_scheduled")
        .count();
    assert_eq!(scheduled, 5);
    for name in [
        "plan",
        "research.a",
        "research.b",
        "research.c",
        "aggregate",
    ] {
        assert_eq!(
            server.store.attempts(job_of(name)).await.unwrap().len(),
            1,
            "{name}"
        );
    }

    for w in workers {
        w.stop().await;
    }
    server.stop().await;
}
