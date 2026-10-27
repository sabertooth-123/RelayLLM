//! Failure-injection tests: what happens when the process dies or the network
//! drops *between* two steps, not on the happy path.
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::Method;
use axum::middleware::{self, Next};
use axum::response::Response;
use common::{T0, TestServer, mock_request, prompt};
use relay_client::RelayClient;
use relay_core::clock::ManualClock;
use relay_server::{Limits, StoreOptions};
use tokio::net::TcpListener;

/// Lets the submit commit, then kills the connection before the response is
/// written: the coordinator crashing (or the network dropping) right after
/// `COMMIT`. From the client's side this is indistinguishable from "the
/// request never arrived".
async fn lose_first_submit_response(
    State(armed): State<Arc<AtomicBool>>,
    request: Request,
    next: Next,
) -> Response {
    let is_submit = request.method() == Method::POST;
    let response = next.run(request).await;
    if is_submit && armed.swap(false, Ordering::SeqCst) {
        // Panicking aborts this connection's task, so the client sees a
        // dropped connection with no response. The server keeps running.
        panic!("injected fault: connection lost after commit, before response");
    }
    response
}

async fn start_lossy_server(db: &std::path::Path, clock: &ManualClock) -> TestServer {
    let armed = Arc::new(AtomicBool::new(true));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    TestServer::start_custom(
        db,
        clock,
        StoreOptions::default(),
        Limits::default(),
        listener,
        move |router: Router| {
            router.layer(middleware::from_fn_with_state(
                armed,
                lose_first_submit_response,
            ))
        },
    )
    .await
}

async fn total_jobs(server: &TestServer) -> u64 {
    server
        .store
        .count_by_state()
        .await
        .unwrap()
        .iter()
        .map(|(_, n)| n)
        .sum()
}

/// Before the fix, `submit` without a key surfaced a transport error even
/// though the job *was* committed. The caller's only recourse, resubmitting,
/// created a duplicate. The client must make the retry safe on its own.
#[tokio::test]
async fn lost_submit_response_without_key_is_retried_safely() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = start_lossy_server(&dir.path().join("r.db"), &clock).await;
    let client = RelayClient::new(server.url()).unwrap();

    let response = client
        .submit(&mock_request(prompt("charge my card once"), None))
        .await
        .expect("an ambiguous failure must be retried, not reported");

    assert!(
        response.created,
        "this call created the job, even though the retry saw it as existing"
    );
    assert_eq!(
        total_jobs(&server).await,
        1,
        "retry must not duplicate the job"
    );
    server.stop().await;
}

/// A client waiting on a result must ride out a coordinator restart. The old
/// CLI wait loop aborted on the first refused connection.
#[tokio::test]
async fn waiting_for_a_result_survives_a_coordinator_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("r.db");
    let clock = ManualClock::new(T0);

    let first = TestServer::start(&db, &clock, StoreOptions::default()).await;
    let job = RelayClient::new(first.url())
        .unwrap()
        .submit(&mock_request(prompt("wait for me"), None))
        .await
        .unwrap()
        .job;
    first.stop().await;

    // Reserve a port with no server behind it yet: the coordinator is "down".
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = RelayClient::new(format!("http://{addr}")).unwrap();
    let waiter = tokio::spawn(async move {
        client
            .wait_for_result(job.id, std::time::Duration::from_millis(1_500))
            .await
    });
    drop(listener);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // The coordinator comes back on the same address with the same database.
    let listener = TcpListener::bind(addr).await.unwrap();
    let second = TestServer::start_custom(
        &db,
        &clock,
        StoreOptions::default(),
        Limits::default(),
        listener,
        |router| router,
    )
    .await;

    // No worker exists yet, so the job stays queued until the deadline. What
    // matters is that the wait reached the restarted coordinator instead of
    // failing on the refused connections.
    let view = waiter
        .await
        .unwrap()
        .expect("wait must survive the restart");
    assert_eq!(view.id, job.id);
    assert_eq!(view.state, relay_core::job::JobState::Queued);
    second.stop().await;
}

#[tokio::test]
async fn lost_submit_response_with_key_returns_the_committed_job() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = start_lossy_server(&dir.path().join("r.db"), &clock).await;
    let client = RelayClient::new(server.url()).unwrap();

    let response = client
        .submit(&mock_request(prompt("hello"), Some("user-key")))
        .await
        .expect("an ambiguous failure must be retried, not reported");

    assert_eq!(response.job.idempotency_key.as_deref(), Some("user-key"));
    assert_eq!(total_jobs(&server).await, 1);
    server.stop().await;
}
