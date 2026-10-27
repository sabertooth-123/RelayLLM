//! The worker-facing HTTP contract (docs/api.md), independent of the Rust
//! worker implementation: plain HTTP requests and status codes.
#![allow(clippy::unwrap_used)]

mod common;

use common::{T0, TestServer};
use relay_core::api::{Assignment, AttemptView, ErrorBody, JobStatusView, WorkerView};
use relay_core::clock::ManualClock;
use relay_core::job::{AttemptOutcome, JobState};
use relay_server::StoreOptions;
use reqwest::{Client, StatusCode};
use serde_json::json;

async fn error_code(response: reqwest::Response) -> String {
    response.json::<ErrorBody>().await.unwrap().error.code
}

#[tokio::test]
async fn full_worker_protocol_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let http = Client::new();
    let base = server.url();

    // Claiming before registering is refused.
    let response = http
        .post(format!("{base}/v1/workers/w1/claims"))
        .json(&json!({"claim_id": "c1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(error_code(response).await, "worker_not_registered");

    // Register twice: idempotent upsert.
    for _ in 0..2 {
        let response = http
            .put(format!("{base}/v1/workers/w1"))
            .json(&json!({"name": "box-a", "concurrency": 2, "kinds": ["mock"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let workers: Vec<WorkerView> = http
        .get(format!("{base}/v1/workers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(workers.len(), 1);

    // Empty queue: 204.
    let response = http
        .post(format!("{base}/v1/workers/w1/claims"))
        .json(&json!({"claim_id": "c1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let submitted: serde_json::Value = http
        .post(format!("{base}/v1/jobs"))
        .json(&json!({"kind": "mock", "payload": {"prompt": "hi"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let job_id = submitted["job"]["id"].as_str().unwrap().to_owned();

    // Claim, then replay the same claim id: same assignment.
    let mut assignments = Vec::new();
    for _ in 0..2 {
        let response = http
            .post(format!("{base}/v1/workers/w1/claims"))
            .json(&json!({"claim_id": "c2"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assignments.push(response.json::<Assignment>().await.unwrap());
    }
    assert_eq!(assignments[0], assignments[1]);
    let a = &assignments[0];
    assert_eq!(a.job_id.to_string(), job_id);
    assert_eq!(a.attempt, 1);
    assert_eq!(a.payload, json!({"prompt": "hi"}));
    assert_eq!(a.lease_expires_at_ms, T0 + 30_000);

    // A wrong attempt number is a stale attempt.
    let response = http
        .post(format!("{base}/v1/jobs/{job_id}/attempts/2/complete"))
        .json(&json!({"worker_id": "w1", "result": {"x": 1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(error_code(response).await, "stale_attempt");

    // Heartbeat: 200 with the lease and the deadline it can't pass. The
    // default timeout is 60 s and the grace 30 s.
    assert_eq!(a.heartbeat_interval_ms, 10_000);
    let beat: serde_json::Value = http
        .post(format!("{base}/v1/jobs/{job_id}/attempts/1/heartbeat"))
        .json(&json!({"worker_id": "w1"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(beat["lease_expires_at_ms"], json!(T0 + 30_000));
    assert_eq!(beat["attempt_deadline_ms"], json!(T0 + 90_000));
    let response = http
        .post(format!("{base}/v1/jobs/{job_id}/attempts/1/heartbeat"))
        .json(&json!({"worker_id": "w2"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(error_code(response).await, "stale_attempt");

    // Complete, then retry the identical report: both 200.
    for _ in 0..2 {
        let response = http
            .post(format!("{base}/v1/jobs/{job_id}/attempts/1/complete"))
            .json(&json!({"worker_id": "w1", "result": {"x": 1}}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let status: JobStatusView = response.json().await.unwrap();
        assert_eq!(status.state, JobState::Succeeded);
    }

    let attempts: Vec<AttemptView> = http
        .get(format!("{base}/v1/jobs/{job_id}/attempts"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, Some(AttemptOutcome::Succeeded));

    let result: serde_json::Value = http
        .get(format!("{base}/v1/jobs/{job_id}/result"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(result["result"], json!({"x": 1}));
    server.stop().await;
}

#[tokio::test]
async fn invalid_worker_requests_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let http = Client::new();
    let base = server.url();
    let job_id = relay_core::job::JobId::generate();

    let cases = vec![
        // Path-unsafe worker id (decoded to "bad/id").
        (
            http.put(format!("{base}/v1/workers/bad%2Fid"))
                .json(&json!({"name": "n", "concurrency": 1, "kinds": ["mock"]})),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            http.put(format!("{base}/v1/workers/w1"))
                .json(&json!({"name": "n", "concurrency": 0, "kinds": ["mock"]})),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            http.put(format!("{base}/v1/workers/w1"))
                .json(&json!({"name": "n", "concurrency": 1, "kinds": []})),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            http.post(format!("{base}/v1/workers/w1/claims"))
                .json(&json!({"claim_id": "has space"})),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            http.post(format!("{base}/v1/jobs/{job_id}/attempts/0/complete"))
                .json(&json!({"worker_id": "w1", "result": {}})),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            http.post(format!("{base}/v1/jobs/{job_id}/attempts/1/complete"))
                .json(&json!({"worker_id": "w1", "result": {}})),
            StatusCode::NOT_FOUND,
        ),
    ];
    for (request, expected) in cases {
        let response = request.send().await.unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(status, expected, "{body}");
    }
    server.stop().await;
}
