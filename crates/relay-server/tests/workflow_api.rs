//! Phase I: the `/v1/workflows` HTTP contract (docs/api.md), and a full run
//! through `RelayClient` with the decider task and real worker loops.
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::{RunningWorker, T0, TestServer, fast_worker_config};
use relay_client::RelayClient;
use relay_core::clock::ManualClock;
use relay_core::workflow::{ActivityViewStatus, StartWorkflowRequest, StepStatus, WorkflowState};
use relay_server::{DeciderConfig, StoreOptions, spawn_decider};
use relay_worker::{MockLlmHandler, Worker};
use serde_json::{Value, json};

fn gated() -> Value {
    json!({ "name": "gated", "steps": [
        { "name": "draft", "activity": { "kind": "mock", "payload": { "prompt": "draft it" }, "inputs": ["input"] } },
        { "name": "review", "parallel": [
            { "name": "style", "kind": "mock", "payload": { "prompt": "style" }, "inputs": ["draft"] },
            { "name": "facts", "kind": "mock", "payload": { "prompt": "facts" }, "inputs": ["draft"] } ] },
        { "name": "pause", "sleep_ms": 1 },
        { "name": "approval", "wait_signal": "approved" },
        { "name": "publish", "activity": { "kind": "mock", "payload": { "prompt": "publish" },
            "inputs": ["review.style", "review.facts"] } },
    ] })
}

fn start_request(definition: Value, key: Option<&str>) -> StartWorkflowRequest {
    let mut r: StartWorkflowRequest =
        serde_json::from_value(json!({ "definition": definition, "input": { "topic": "rust" } }))
            .unwrap();
    r.idempotency_key = key.map(str::to_owned);
    r
}

async fn server() -> (tempfile::TempDir, ManualClock, TestServer) {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    (dir, clock, server)
}

/// Raw POST, for status codes and bodies the typed client hides.
async fn post(url: &str, body: Option<Value>) -> (u16, Value) {
    let client = reqwest::Client::new();
    let mut req = client.post(url);
    if let Some(b) = body {
        req = req.json(&b);
    }
    let resp = req.send().await.unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

async fn get(url: &str) -> (u16, Value) {
    let resp = reqwest::get(url).await.unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

fn code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or("")
}

#[tokio::test]
async fn start_is_201_then_200_on_replay_and_409_on_a_different_request() {
    let (_d, _c, server) = server().await;
    let url = format!("{}/v1/workflows", server.url());
    let body = serde_json::to_value(start_request(gated(), Some("k1"))).unwrap();

    let (status, first) = post(&url, Some(body.clone())).await;
    assert_eq!(status, 201, "{first}");
    assert_eq!(first["created"], true);
    assert_eq!(first["workflow"]["state"], "running");
    assert_eq!(first["workflow"]["steps"][0]["status"], "active");
    assert_eq!(first["workflow"]["steps"][1]["kind"], "parallel");
    assert_eq!(first["workflow"]["steps"][3]["signal"], "approved");

    let (status, replay) = post(&url, Some(body)).await;
    assert_eq!((status, &replay["created"]), (200, &json!(false)));
    assert_eq!(replay["workflow"]["id"], first["workflow"]["id"]);

    let mut other = start_request(gated(), Some("k1"));
    other.input = json!({ "topic": "go" });
    let (status, conflict) = post(&url, Some(serde_json::to_value(other).unwrap())).await;
    assert_eq!((status, code(&conflict)), (409, "idempotency_conflict"));
}

#[tokio::test]
async fn invalid_requests_get_typed_errors() {
    let (_d, _c, server) = server().await;
    let base = server.url();
    let url = format!("{base}/v1/workflows");

    let (status, body) = post(
        &url,
        Some(json!({ "definition": { "name": "w", "steps": [] } })),
    )
    .await;
    assert_eq!((status, code(&body)), (422, "invalid_request"));
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("at least one step")
    );

    let bad_input = json!({ "definition": { "name": "w", "steps": [
        { "name": "a", "activity": { "kind": "mock", "payload": {}, "inputs": ["later"] } } ] } });
    let (status, body) = post(&url, Some(bad_input)).await;
    assert_eq!((status, code(&body)), (422, "invalid_request"));

    let (status, body) = post(
        &url,
        Some(json!({ "definition": { "name": "w", "steps": [] }, "extra": 1 })),
    )
    .await;
    assert_eq!(
        (status, code(&body)),
        (422, "bad_body"),
        "unknown fields are rejected"
    );

    let (status, body) = get(&format!("{base}/v1/workflows/not-a-uuid")).await;
    assert_eq!((status, code(&body)), (400, "invalid_workflow_id"));
    let missing = relay_core::workflow::WorkflowId::generate();
    for path in [
        format!("/v1/workflows/{missing}"),
        format!("/v1/workflows/{missing}/history"),
    ] {
        let (status, body) = get(&format!("{base}{path}")).await;
        assert_eq!((status, code(&body)), (404, "workflow_not_found"), "{path}");
    }
    let (status, body) = post(&format!("{base}/v1/workflows/{missing}/cancel"), None).await;
    assert_eq!((status, code(&body)), (404, "workflow_not_found"));
}

#[tokio::test]
async fn signals_over_http_follow_the_latch_rules() {
    let (_d, _c, server) = server().await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = client
        .start_workflow(&start_request(gated(), None))
        .await
        .unwrap()
        .workflow
        .id;
    let base = format!("{}/v1/workflows/{id}/signals", server.url());

    let (status, body) = post(&format!("{base}/rejected"), None).await;
    assert_eq!((status, code(&body)), (422, "unknown_signal"));

    // Empty body: a null payload.
    let (status, body) = post(&format!("{base}/approved"), None).await;
    assert_eq!((status, body), (200, json!({ "recorded": true })));
    let (status, body) = post(
        &format!("{base}/approved"),
        Some(json!({ "payload": null })),
    )
    .await;
    assert_eq!(
        (status, body),
        (200, json!({ "recorded": false })),
        "retry of the same delivery"
    );
    let (status, body) = post(
        &format!("{base}/approved"),
        Some(json!({ "payload": "different" })),
    )
    .await;
    assert_eq!((status, code(&body)), (409, "signal_conflict"));

    let big = json!({ "payload": "x".repeat(70_000) });
    let (status, body) = post(&format!("{base}/approved"), Some(big)).await;
    assert_eq!((status, code(&body)), (413, "signal_too_large"));
    let (status, body) = post(
        &format!("{base}/approved"),
        Some(json!({ "payload": 1, "oops": 2 })),
    )
    .await;
    assert_eq!((status, code(&body)), (422, "bad_body"));

    let view = client.workflow(id).await.unwrap();
    assert_eq!(
        view.steps[3].signal_received,
        Some(true),
        "visible in the step view"
    );
}

#[tokio::test]
async fn cancel_over_http_is_idempotent_and_ends_signalling() {
    let (_d, _c, server) = server().await;
    let client = RelayClient::new(server.url()).unwrap();
    let id = client
        .start_workflow(&start_request(gated(), None))
        .await
        .unwrap()
        .workflow
        .id;
    let url = format!("{}/v1/workflows/{id}/cancel", server.url());

    let (status, view) = post(&url, Some(json!({ "reason": "no longer needed" }))).await;
    assert_eq!(status, 200);
    assert_eq!(
        (&view["state"], &view["reason"]),
        (&json!("cancelled"), &json!("no longer needed"))
    );
    let (status, again) = post(&url, None).await;
    assert_eq!(
        (status, &again["reason"]),
        (200, &json!("no longer needed")),
        "idempotent; first reason kept"
    );

    let err = client
        .signal_workflow(id, "approved", Value::Null)
        .await
        .unwrap_err();
    assert_eq!(err.api_code(), Some("workflow_finished"));
    let history = client.workflow_history(id).await.unwrap();
    assert_eq!(
        history.events.last().unwrap().kind.type_name(),
        "workflow_cancelled"
    );
}

#[tokio::test]
async fn a_full_run_through_the_client_with_the_decider_and_real_workers() {
    let (_d, clock, server) = server().await;
    let decider = spawn_decider(server.store.clone(), DeciderConfig::default());
    let workers: Vec<_> = (0..2)
        .map(|i| {
            RunningWorker::spawn(
                Worker::new(
                    RelayClient::new(server.url()).unwrap(),
                    fast_worker_config(&format!("w{i}"), 2),
                )
                .with_handler("mock", MockLlmHandler::new(Duration::from_millis(20))),
            )
        })
        .collect();
    let client = RelayClient::new(server.url()).unwrap();
    let started = client
        .start_workflow(&start_request(gated(), None))
        .await
        .unwrap();
    assert!(started.created, "generated key: reported as created");
    let id = started.workflow.id;

    // Runs until the gate (the 1 ms timer needs the manual clock to move).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        clock.advance_ms(1);
        let view = client.workflow(id).await.unwrap();
        if view.steps[3].status == StepStatus::Active {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never reached the gate: {view:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let at_gate = client.workflow(id).await.unwrap();
    assert_eq!(
        at_gate.steps[..3]
            .iter()
            .map(|s| s.status)
            .collect::<Vec<_>>(),
        [StepStatus::Done; 3]
    );
    assert!(
        at_gate.steps[1]
            .activities
            .iter()
            .all(|a| a.status == ActivityViewStatus::Succeeded && a.attempt == Some(1))
    );
    assert!(
        at_gate.steps[2].fire_at_ms.is_some(),
        "the timer's persisted deadline is shown"
    );

    assert!(
        client
            .signal_workflow(id, "approved", json!({ "by": "test" }))
            .await
            .unwrap()
            .recorded
    );
    let done = client
        .wait_for_workflow(id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(done.state, WorkflowState::Succeeded, "{done:?}");
    assert!(done.steps.iter().all(|s| s.status == StepStatus::Done));
    let publish = done.steps[4].activities[0].job_id.unwrap();
    let result = client.result(publish).await.unwrap().result.unwrap();
    assert_eq!(
        result["inputs_seen"],
        json!(["review.facts", "review.style"])
    );

    let history = client.workflow_history(id).await.unwrap();
    assert_eq!(
        history.events.first().unwrap().kind.type_name(),
        "workflow_started"
    );
    assert_eq!(
        history.events.last().unwrap().kind.type_name(),
        "workflow_completed"
    );
    assert!(
        history
            .events
            .iter()
            .enumerate()
            .all(|(i, e)| e.seq == i as u64 + 1)
    );

    for w in workers {
        w.stop().await;
    }
    decider.abort();
    server.stop().await;
}

/// The crash window for starts: the run commits, then the response is lost.
/// The client's retry (same generated key) must get the committed run back,
/// not start a second one.
#[tokio::test]
async fn a_lost_start_response_is_retried_without_starting_a_second_run() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = TestServer::start_custom(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        relay_server::Limits::default(),
        listener,
        |router| common::lose_first_response(router, "/v1/workflows"),
    )
    .await;
    let client =
        RelayClient::new(server.url())
            .unwrap()
            .with_retry_policy(relay_client::RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(10),
            });

    let started = client
        .start_workflow(&start_request(gated(), None))
        .await
        .unwrap();
    assert!(started.created);
    let raw = common::raw_pool(&dir.path().join("r.db")).await;
    let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs")
        .fetch_one(&raw)
        .await
        .unwrap();
    assert_eq!(
        runs, 1,
        "the retry found the committed run instead of starting another"
    );

    // Without the generated key, the same fault would have left two runs:
    // the plain HTTP retry below is not idempotent.
    let unkeyed = serde_json::to_value(start_request(gated(), None)).unwrap();
    let url = format!("{}/v1/workflows", server.url());
    let (a, _) = post(&url, Some(unkeyed.clone())).await;
    let (b, _) = post(&url, Some(unkeyed)).await;
    assert_eq!((a, b), (201, 201));
    let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs")
        .fetch_one(&raw)
        .await
        .unwrap();
    assert_eq!(
        runs, 3,
        "an unkeyed retry is a new run; the key is what makes retries safe"
    );
    server.stop().await;
}

async fn scrape(base: &str) -> std::collections::HashMap<String, f64> {
    reqwest::get(format!("{base}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|l| {
            let (name, value) = l.rsplit_once(' ').unwrap();
            (name.to_owned(), value.parse().unwrap())
        })
        .collect()
}

#[tokio::test]
async fn metrics_count_runs_activity_attempts_and_due_runs_from_the_database() {
    let (_d, clock, server) = server().await;
    let client = RelayClient::new(server.url()).unwrap();
    let store = &server.store;
    common::register(store, "w1", &["mock"]).await;
    let lease = Duration::from_secs(30);

    client
        .start_workflow(&start_request(gated(), None))
        .await
        .unwrap();
    let cancelled = client
        .start_workflow(&start_request(gated(), None))
        .await
        .unwrap()
        .workflow
        .id;
    let m = scrape(&server.url()).await;
    assert_eq!(
        m["relay_workflow_due_runs"], 2.0,
        "started, not yet decided"
    );

    store.decide_due(10).await.unwrap();
    // Cancelled while its first activity is still queued: no attempt ran.
    client.cancel_workflow(cancelled, None).await.unwrap();
    // The other run's first activity: one retryable failure, then success.
    let job = store.claim("w1", "c1", lease).await.unwrap().unwrap();
    store.fail(job.id, 1, "w1", "blip", true).await.unwrap();
    let again = loop {
        if let Some(j) = store.claim("w1", "c2", lease).await.unwrap() {
            break j;
        }
        clock.advance_ms(60_001); // default retry backoff
    };
    store
        .complete(again.id, again.attempt, "w1", &json!("ok"))
        .await
        .unwrap();
    // A plain job's attempts must not count as workflow activity attempts.
    let plain = common::submit_job(store, common::prompt("plain"), None).await;
    store.claim("w1", "c3", lease).await.unwrap().unwrap();
    store
        .complete(plain.id, 1, "w1", &json!("p"))
        .await
        .unwrap();

    let m = scrape(&server.url()).await;
    assert_eq!(m["relay_workflow_runs{state=\"running\"}"], 1.0);
    assert_eq!(m["relay_workflow_runs{state=\"cancelled\"}"], 1.0);
    assert_eq!(m["relay_workflow_runs{state=\"succeeded\"}"], 0.0);
    assert_eq!(
        m["relay_workflow_activity_attempts_total{outcome=\"retryable_error\"}"],
        1.0
    );
    assert_eq!(
        m["relay_workflow_activity_attempts_total{outcome=\"succeeded\"}"], 1.0,
        "the plain job is excluded"
    );
    assert_eq!(
        m["relay_workflow_activity_attempts_total{outcome=\"cancelled\"}"], 0.0,
        "the cancelled run's job was still queued: no attempt to count"
    );
    assert_eq!(
        m["relay_workflow_due_runs"], 1.0,
        "the completion made the running run due"
    );
}
