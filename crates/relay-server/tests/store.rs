//! Store-level tests: persistence, idempotency and capacity against a real
//! SQLite file.
#![allow(clippy::unwrap_used)]

mod common;

use common::{T0, mock_request, open_store, prompt};
use relay_core::clock::ManualClock;
use relay_core::job::JobState;
use relay_core::submission::SubmissionLimits;
use relay_server::{StoreError, StoreOptions, SubmitOutcome};

fn limits() -> SubmissionLimits {
    SubmissionLimits::default()
}

#[tokio::test]
async fn submitted_jobs_survive_store_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("relay.db");
    let clock = ManualClock::new(T0);

    let store = open_store(&db, &clock, StoreOptions::default()).await;
    let a = store
        .submit(
            mock_request(prompt("first"), None)
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    let b = store
        .submit(
            mock_request(prompt("second"), Some("key-b"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    let (SubmitOutcome::Created(a), SubmitOutcome::Created(b)) = (a, b) else {
        panic!("both submits should create jobs");
    };
    store.close().await;
    drop(store);

    // A fresh store on the same file sees exactly what was committed, and the
    // migration is not re-applied.
    clock.advance_ms(60_000);
    let reopened = open_store(&db, &clock, StoreOptions::default()).await;
    assert_eq!(reopened.get(a.id).await.unwrap(), Some(a.clone()));
    assert_eq!(reopened.get(b.id).await.unwrap(), Some(b.clone()));
    assert_eq!(a.state, JobState::Queued);
    assert_eq!(a.created_at, T0);

    // Idempotency survives the restart too.
    let replay = reopened
        .submit(
            mock_request(prompt("second"), Some("key-b"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay, SubmitOutcome::Existing(b));
}

#[tokio::test]
async fn every_pooled_connection_is_wal_and_synchronous_full() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let store = open_store(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;
    // Run concurrently so the check spreads over several pooled connections,
    // not just the first one.
    let checks: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.durability().await.unwrap() })
        })
        .collect();
    for check in checks {
        assert_eq!(check.await.unwrap(), ("wal".to_owned(), 2));
    }
}

#[tokio::test]
async fn same_key_same_request_returns_existing_job() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let store = open_store(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;

    let first = store
        .submit(
            mock_request(prompt("hi"), Some("k"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    // Same logical request with JSON keys in a different order.
    let payload = serde_json::json!({"b": 2, "prompt": "hi", "a": 1});
    let reordered = serde_json::json!({"a": 1, "prompt": "hi", "b": 2});
    let x = store
        .submit(
            mock_request(payload, Some("k2"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    let y = store
        .submit(
            mock_request(reordered, Some("k2"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    let second = store
        .submit(
            mock_request(prompt("hi"), Some("k"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(matches!(first, SubmitOutcome::Created(_)));
    assert_eq!(second, SubmitOutcome::Existing(first.job().clone()));
    assert!(matches!(y, SubmitOutcome::Existing(ref j) if j.id == x.job().id));
    let total: u64 = store
        .count_by_state()
        .await
        .unwrap()
        .iter()
        .map(|(_, n)| n)
        .sum();
    assert_eq!(total, 2);
}

#[tokio::test]
async fn same_key_different_request_is_a_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let store = open_store(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;

    let original = store
        .submit(
            mock_request(prompt("hi"), Some("k"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    let err = store
        .submit(
            mock_request(prompt("different"), Some("k"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap_err();

    match err {
        StoreError::IdempotencyConflict { key, existing } => {
            assert_eq!(key, "k");
            assert_eq!(existing, original.job().id);
        }
        other => panic!("expected IdempotencyConflict, got {other:?}"),
    }
    // The original job is untouched.
    assert_eq!(
        store.get(original.job().id).await.unwrap().unwrap().payload,
        prompt("hi")
    );
}

#[tokio::test]
async fn concurrent_submits_with_one_key_create_exactly_one_job() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let store = open_store(&dir.path().join("r.db"), &clock, StoreOptions::default()).await;

    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .submit(
                        mock_request(prompt("race"), Some("hot-key"))
                            .validate(&limits())
                            .unwrap(),
                    )
                    .await
                    .unwrap()
            })
        })
        .collect();

    let mut created = 0;
    let mut ids = std::collections::HashSet::new();
    for task in tasks {
        let outcome = task.await.unwrap();
        if matches!(outcome, SubmitOutcome::Created(_)) {
            created += 1;
        }
        ids.insert(outcome.job().id);
    }
    assert_eq!(created, 1, "exactly one submit may create the job");
    assert_eq!(ids.len(), 1, "every caller must see the same job id");
}

#[tokio::test]
async fn capacity_is_enforced_but_replays_still_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let options = StoreOptions {
        max_active_jobs: 2,
        ..StoreOptions::default()
    };
    let store = open_store(&dir.path().join("r.db"), &clock, options).await;

    let first = store
        .submit(
            mock_request(prompt("1"), Some("k1"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    store
        .submit(mock_request(prompt("2"), None).validate(&limits()).unwrap())
        .await
        .unwrap();

    let err = store
        .submit(mock_request(prompt("3"), None).validate(&limits()).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::QueueFull { capacity: 2 }));

    let err = store
        .submit(
            mock_request(prompt("3"), Some("new-key"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::QueueFull { capacity: 2 }));
    assert!(
        store
            .get_by_idempotency_key("new-key")
            .await
            .unwrap()
            .is_none()
    );

    // A retry of an already-accepted request is not new work, so a full
    // queue must not reject it.
    let replay = store
        .submit(
            mock_request(prompt("1"), Some("k1"))
                .validate(&limits())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay, SubmitOutcome::Existing(first.job().clone()));
}

#[tokio::test]
async fn concurrent_submits_never_exceed_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let options = StoreOptions {
        max_active_jobs: 5,
        ..StoreOptions::default()
    };
    let store = open_store(&dir.path().join("r.db"), &clock, options).await;

    let tasks: Vec<_> = (0..20)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .submit(
                        mock_request(prompt(&i.to_string()), None)
                            .validate(&limits())
                            .unwrap(),
                    )
                    .await
            })
        })
        .collect();

    let mut accepted = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => accepted += 1,
            Err(StoreError::QueueFull { .. }) => {}
            Err(other) => panic!("unexpected error {other:?}"),
        }
    }
    assert_eq!(accepted, 5);
}
