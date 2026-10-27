//! Cancellation, including its races with completion and claiming
//! (requirement 12).
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::{T0, TestServer, mock_request, open_store, prompt, register, submit_job};
use relay_client::RelayClient;
use relay_core::clock::ManualClock;
use relay_core::job::{AttemptOutcome, JobState};
use relay_core::retry::RetryPolicy;
use relay_server::{CANCELLED_MESSAGE, CancelOutcome, Store, StoreError, StoreOptions};
use serde_json::json;

const LEASE: Duration = Duration::from_secs(30);

async fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let options = StoreOptions {
        retry: RetryPolicy::IMMEDIATE,
        ..StoreOptions::default()
    };
    let store = open_store(&dir.path().join("r.db"), &ManualClock::new(T0), options).await;
    register(&store, "w1", &["mock"]).await;
    (dir, store)
}

#[tokio::test]
async fn queued_and_retry_waiting_jobs_cancel_cleanly() {
    let (_dir, store) = store().await;
    let queued = submit_job(&store, prompt("q"), None).await;
    let cancelled = store.cancel(queued.id).await.unwrap();
    let job = cancelled.job();
    assert!(matches!(cancelled, CancelOutcome::Cancelled(_)));
    assert_eq!((job.state, job.attempt), (JobState::Cancelled, 0));
    assert_eq!(job.finished_at, Some(T0));
    assert_eq!(job.last_error.as_deref(), Some(CANCELLED_MESSAGE));
    assert!(
        store.claim("w1", "c1", LEASE).await.unwrap().is_none(),
        "cancelled jobs are never claimed"
    );

    let retrying = submit_job(&store, prompt("r"), None).await;
    store.claim("w1", "c2", LEASE).await.unwrap().unwrap();
    store
        .fail(retrying.id, 1, "w1", "flaky", true)
        .await
        .unwrap();
    let cancelled = store.cancel(retrying.id).await.unwrap();
    assert_eq!(cancelled.job().state, JobState::Cancelled);
    // The failed attempt keeps its own outcome; cancel doesn't rewrite history.
    let history = store.attempts(retrying.id).await.unwrap();
    assert_eq!(history[0].outcome, Some(AttemptOutcome::RetryableError));
}

#[tokio::test]
async fn cancelling_a_running_job_fences_out_its_worker() {
    let (_dir, store) = store().await;
    let job = submit_job(&store, prompt("x"), None).await;
    store.claim("w1", "c1", LEASE).await.unwrap().unwrap();

    store.cancel(job.id).await.unwrap();
    let history = store.attempts(job.id).await.unwrap();
    assert_eq!(history[0].outcome, Some(AttemptOutcome::Cancelled));

    let report = store
        .complete(job.id, 1, "w1", &json!({"too": "late"}))
        .await;
    assert!(
        matches!(report, Err(StoreError::StaleAttempt { .. })),
        "{report:?}"
    );
    let beat = store.heartbeat(job.id, 1, "w1", LEASE).await;
    assert!(matches!(
        beat,
        Err(StoreError::StaleAttempt {
            state: JobState::Cancelled,
            ..
        })
    ));
    assert_eq!(store.get(job.id).await.unwrap().unwrap().result, None);
}

#[tokio::test]
async fn cancel_is_idempotent_and_terminal_jobs_are_not_cancellable() {
    let (_dir, store) = store().await;
    let job = submit_job(&store, prompt("x"), None).await;
    store.cancel(job.id).await.unwrap();
    assert!(matches!(
        store.cancel(job.id).await.unwrap(),
        CancelOutcome::AlreadyCancelled(_)
    ));

    let done = submit_job(&store, prompt("y"), None).await;
    store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    store.complete(done.id, 1, "w1", &json!({})).await.unwrap();
    let err = store.cancel(done.id).await.unwrap_err();
    assert!(matches!(
        err,
        StoreError::NotCancellable {
            state: JobState::Succeeded,
            ..
        }
    ));
    assert_eq!(
        store.get(done.id).await.unwrap().unwrap().state,
        JobState::Succeeded
    );

    let missing = relay_core::job::JobId::generate();
    assert!(matches!(
        store.cancel(missing).await.unwrap_err(),
        StoreError::JobNotFound(_)
    ));
}

/// Requirement 12: cancel and complete race for the same running attempt.
/// Exactly one wins, and job state and attempt history agree with it.
#[tokio::test]
async fn cancel_and_complete_race_has_exactly_one_winner() {
    let mut cancels_won = 0;
    for round in 0..30 {
        let (_dir, store) = store().await;
        let job = submit_job(&store, prompt("contested"), None).await;
        store.claim("w1", "c1", LEASE).await.unwrap().unwrap();

        // Release both at once, and alternate which is spawned first, so
        // both orderings actually occur (asserted below).
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let (a, b) = (store.clone(), store.clone());
        let (ba, bb) = (barrier.clone(), barrier.clone());
        let complete_task = async move {
            ba.wait().await;
            a.complete(job.id, 1, "w1", &json!({"ok": 1})).await
        };
        let cancel_task = async move {
            bb.wait().await;
            b.cancel(job.id).await
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

        let final_job = store.get(job.id).await.unwrap().unwrap();
        let outcome = store.attempts(job.id).await.unwrap()[0].outcome;
        match (&complete, &cancel) {
            (
                Ok(_),
                Err(StoreError::NotCancellable {
                    state: JobState::Succeeded,
                    ..
                }),
            ) => {
                assert_eq!(final_job.state, JobState::Succeeded);
                assert_eq!(final_job.result, Some(json!({"ok": 1})));
                assert_eq!(outcome, Some(AttemptOutcome::Succeeded));
            }
            (Err(StoreError::StaleAttempt { .. }), Ok(CancelOutcome::Cancelled(_))) => {
                cancels_won += 1;
                assert_eq!(final_job.state, JobState::Cancelled);
                assert_eq!(final_job.result, None);
                assert_eq!(outcome, Some(AttemptOutcome::Cancelled));
            }
            other => panic!("invalid race outcome: {other:?}"),
        }
    }
    println!("cancel won {cancels_won}/30 races");
    assert!(
        (1..30).contains(&cancels_won),
        "the race only ever went one way ({cancels_won}/30 cancels won); both outcomes must be exercised"
    );
}

/// Cancel racing a claim of the same queued job: either the claim gets
/// nothing, or it gets the job and the cancel fences it out. Never a
/// cancelled job with a live owner.
#[tokio::test]
async fn cancel_and_claim_race_never_leaves_an_owned_cancelled_job() {
    for _ in 0..30 {
        let (_dir, store) = store().await;
        let job = submit_job(&store, prompt("x"), None).await;
        let (a, b) = (store.clone(), store.clone());
        let claim = tokio::spawn(async move { a.claim("w1", "c1", LEASE).await });
        let cancel = tokio::spawn(async move { b.cancel(job.id).await });
        let (claimed, cancelled) = (
            claim.await.unwrap().unwrap(),
            cancel.await.unwrap().unwrap(),
        );

        let final_job = store.get(job.id).await.unwrap().unwrap();
        assert_eq!(final_job.state, JobState::Cancelled);
        assert_eq!(
            (final_job.worker_id, final_job.lease_expires_at),
            (None, None)
        );
        assert!(matches!(cancelled, CancelOutcome::Cancelled(_)));
        let history = store.attempts(job.id).await.unwrap();
        match claimed {
            None => assert!(history.is_empty()),
            Some(_) => assert_eq!(history[0].outcome, Some(AttemptOutcome::Cancelled)),
        }
    }
}

#[tokio::test]
async fn cancel_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let server = TestServer::start(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    let client = RelayClient::new(server.url()).unwrap();
    let job = client
        .submit(&mock_request(prompt("x"), None))
        .await
        .unwrap()
        .job;

    for _ in 0..2 {
        let view = client.cancel(job.id).await.unwrap();
        assert_eq!(view.state, JobState::Cancelled);
    }
    let result = client.result(job.id).await.unwrap();
    assert_eq!(result.error.as_deref(), Some(CANCELLED_MESSAGE));

    // Replaying the idempotency key of a cancelled job returns that job;
    // run the work again with a new key.
    let keyed = client
        .submit(&mock_request(prompt("k"), Some("key-1")))
        .await
        .unwrap()
        .job;
    client.cancel(keyed.id).await.unwrap();
    let replay = client
        .submit(&mock_request(prompt("k"), Some("key-1")))
        .await
        .unwrap();
    assert_eq!(
        (replay.created, replay.job.state),
        (false, JobState::Cancelled)
    );

    server
        .store
        .register_worker(
            "w1",
            &relay_core::api::RegisterWorkerRequest {
                name: "w".into(),
                concurrency: 1,
                kinds: vec!["mock".into()],
            },
        )
        .await
        .unwrap();
    let finished = client
        .submit(&mock_request(prompt("f"), None))
        .await
        .unwrap()
        .job;
    server
        .store
        .claim("w1", "c1", LEASE)
        .await
        .unwrap()
        .unwrap();
    server
        .store
        .complete(finished.id, 1, "w1", &json!({}))
        .await
        .unwrap();
    let err = client.cancel(finished.id).await.unwrap_err();
    assert_eq!(err.api_code(), Some("not_cancellable"));
    server.stop().await;
}
