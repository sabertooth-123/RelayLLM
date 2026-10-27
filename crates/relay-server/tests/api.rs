//! HTTP-level tests: the documented API contract, end to end through Axum,
//! including a coordinator restart.
#![allow(clippy::unwrap_used)]

mod common;

use common::{T0, TestServer, mock_request, prompt};
use relay_client::{ClientError, RelayClient};
use relay_core::api::{ErrorBody, JobStatusView, SubmitJobResponse};
use relay_core::clock::ManualClock;
use relay_core::job::{JobId, JobState};
use relay_server::{Limits, StoreOptions};
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn job_survives_coordinator_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("relay.db");
    let clock = ManualClock::new(T0);

    let server = TestServer::start(&db, &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();
    let submitted = client
        .submit(&mock_request(prompt("survive me"), Some("restart-key")))
        .await
        .unwrap();
    assert!(submitted.created);
    server.stop().await;

    // New process-equivalent: new store, new listener (new port), same file.
    let server = TestServer::start(&db, &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();

    let status = client.status(submitted.job.id).await.unwrap();
    assert_eq!(status, submitted.job);

    let result = client.result(submitted.job.id).await.unwrap();
    assert_eq!(result.state, JobState::Queued);
    assert_eq!(result.result, None);

    let replay = client
        .submit(&mock_request(prompt("survive me"), Some("restart-key")))
        .await
        .unwrap();
    assert!(!replay.created);
    assert_eq!(replay.job.id, submitted.job.id);
    server.stop().await;
}

#[tokio::test]
async fn submit_returns_201_then_200_on_replay() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let http = reqwest::Client::new();
    let url = format!("{}/v1/jobs", server.url());
    let body = json!({"kind": "mock", "payload": {"prompt": "hi"}, "idempotency_key": "abc"});

    let first = http.post(&url).json(&body).send().await.unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);
    let first: SubmitJobResponse = first.json().await.unwrap();
    assert!(first.created);
    assert_eq!(first.job.state, JobState::Queued);
    assert_eq!(first.job.attempt, 0);
    assert_eq!(first.job.max_attempts, 3);
    assert_eq!(first.job.created_at_ms, T0);

    let second = http.post(&url).json(&body).send().await.unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second: SubmitJobResponse = second.json().await.unwrap();
    assert!(!second.created);
    assert_eq!(second.job.id, first.job.id);
    server.stop().await;
}

#[tokio::test]
async fn reused_key_with_different_payload_is_409() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();

    let original = client
        .submit(&mock_request(prompt("one"), Some("dup")))
        .await
        .unwrap();
    let err = client
        .submit(&mock_request(prompt("two"), Some("dup")))
        .await
        .unwrap_err();
    match &err {
        ClientError::Api {
            status,
            code,
            message,
        } => {
            assert_eq!(*status, 409);
            assert_eq!(code, "idempotency_conflict");
            assert!(message.contains(&original.job.id.to_string()), "{message}");
        }
        other => panic!("expected API error, got {other:?}"),
    }
    server.stop().await;
}

#[tokio::test]
async fn full_queue_is_503_with_retry_after() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let options = StoreOptions {
        max_active_jobs: 1,
        ..StoreOptions::default()
    };
    let server = TestServer::start(&dir.path().join("r.db"), &clock, options).await;
    let http = reqwest::Client::new();
    let url = format!("{}/v1/jobs", server.url());

    let ok = http
        .post(&url)
        .json(&json!({"kind": "mock", "payload": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::CREATED);

    let full = http
        .post(&url)
        .json(&json!({"kind": "mock", "payload": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(full.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(full.headers()["retry-after"], "1");
    let body: ErrorBody = full.json().await.unwrap();
    assert_eq!(body.error.code, "queue_full");
    server.stop().await;
}

#[tokio::test]
async fn invalid_requests_get_typed_errors() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let limits = Limits {
        max_request_bytes: 1024,
        ..Limits::default()
    };
    let server = TestServer::start_with_limits(
        &dir.path().join("r.db"),
        &clock,
        StoreOptions::default(),
        limits,
    )
    .await;
    let http = reqwest::Client::new();
    let jobs = format!("{}/v1/jobs", server.url());

    let cases: Vec<(reqwest::RequestBuilder, StatusCode, &str)> = vec![
        (
            http.post(&jobs)
                .json(&json!({"kind": "Bad Kind", "payload": {}})),
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
        ),
        (
            http.post(&jobs)
                .json(&json!({"kind": "mock", "payload": [1, 2]})),
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
        ),
        (
            // Typos in field names are rejected, not silently ignored.
            http.post(&jobs)
                .json(&json!({"kind": "mock", "payload": {}, "idempotencyKey": "x"})),
            StatusCode::UNPROCESSABLE_ENTITY,
            "bad_body",
        ),
        (
            http.post(&jobs)
                .header("content-type", "application/json")
                .body("{not json"),
            StatusCode::BAD_REQUEST,
            "bad_body",
        ),
        (
            http.post(&jobs)
                .json(&json!({"kind": "mock", "payload": {"prompt": "x".repeat(2048)}})),
            StatusCode::PAYLOAD_TOO_LARGE,
            "bad_body",
        ),
        (
            http.get(format!("{}/v1/jobs/not-a-uuid", server.url())),
            StatusCode::BAD_REQUEST,
            "invalid_job_id",
        ),
        (
            http.get(format!("{}/v1/jobs/{}", server.url(), JobId::generate())),
            StatusCode::NOT_FOUND,
            "job_not_found",
        ),
    ];

    for (request, expected_status, expected_code) in cases {
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), expected_status);
        let body: ErrorBody = response.json().await.unwrap();
        assert_eq!(body.error.code, expected_code, "{}", body.error.message);
    }
    server.stop().await;
}

#[tokio::test]
async fn result_is_202_until_terminal_and_status_hides_payload() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();
    let job = client
        .submit(&mock_request(prompt("secret prompt"), None))
        .await
        .unwrap()
        .job;

    let response = reqwest::get(format!("{}/v1/jobs/{}/result", server.url(), job.id))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let status_body = reqwest::get(format!("{}/v1/jobs/{}", server.url(), job.id))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!status_body.contains("secret prompt"));
    let parsed: JobStatusView = serde_json::from_str(&status_body).unwrap();
    assert_eq!(parsed.id, job.id);

    let health = client.health().await.unwrap();
    assert_eq!(health.status, "ok");
    server.stop().await;
}
