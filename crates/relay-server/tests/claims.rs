//! Claim / complete / fail protocol at the store level: fencing, idempotent
//! retries, races, and atomicity of the two-row writes.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use common::{T0, open_store, prompt, raw_pool, register, submit_job};
use relay_core::clock::ManualClock;
use relay_core::job::{AttemptOutcome, JobState};
use relay_server::{ReportOutcome, Store, StoreError, StoreOptions};
use serde_json::json;

const LEASE: Duration = Duration::from_secs(30);

struct Fixture {
    _dir: tempfile::TempDir,
    db: PathBuf,
    clock: ManualClock,
    store: Store,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("r.db");
    let clock = ManualClock::new(T0);
    let store = open_store(&db, &clock, StoreOptions::default()).await;
    register(&store, "w1", &["mock"]).await;
    register(&store, "w2", &["mock"]).await;
    Fixture {
        _dir: dir,
        db,
        clock,
        store,
    }
}

impl Fixture {
    /// Move past the longest possible retry backoff (default policy caps at
    /// 60 s before jitter).
    fn wait_out_backoff(&self) {
        self.clock.advance_ms(60_001);
    }
}

fn assert_stale(result: Result<ReportOutcome, StoreError>) {
    assert!(
        matches!(result, Err(StoreError::StaleAttempt { .. })),
        "expected StaleAttempt, got {result:?}"
    );
}

#[tokio::test]
async fn claim_takes_the_oldest_job_and_records_the_attempt() {
    let f = fixture().await;
    let first = submit_job(&f.store, prompt("first"), None).await;
    f.clock.advance_ms(1);
    let second = submit_job(&f.store, prompt("second"), None).await;

    let job = f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    assert_eq!(job.id, first.id);
    assert_eq!(job.state, JobState::Running);
    assert_eq!(job.attempt, 1);
    assert_eq!(job.worker_id.as_deref(), Some("w1"));
    assert_eq!(job.lease_expires_at, Some(T0 + 1 + 30_000));

    let attempts = f.store.attempts(first.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].worker_id, "w1");
    assert_eq!(attempts[0].outcome, None);

    let next = f.store.claim("w1", "c2", LEASE).await.unwrap().unwrap();
    assert_eq!(next.id, second.id);
    assert!(f.store.claim("w1", "c3", LEASE).await.unwrap().is_none());
}

#[tokio::test]
async fn claim_only_returns_kinds_the_worker_registered_for() {
    let f = fixture().await;
    register(&f.store, "summarizer", &["summarize"]).await;
    let job = submit_job(&f.store, prompt("x"), None).await; // kind "mock"

    assert!(
        f.store
            .claim("summarizer", "c1", LEASE)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        f.store.get(job.id).await.unwrap().unwrap().state,
        JobState::Queued
    );
    assert!(f.store.claim("w1", "c1", LEASE).await.unwrap().is_some());
}

#[tokio::test]
async fn unregistered_worker_cannot_claim() {
    let f = fixture().await;
    submit_job(&f.store, prompt("x"), None).await;
    let err = f.store.claim("ghost", "c1", LEASE).await.unwrap_err();
    assert!(matches!(err, StoreError::WorkerNotRegistered(ref id) if id == "ghost"));
}

/// §5.1 #1: the claim committed but the response was lost. Retrying with the
/// same claim id must return the same assignment, not burn a second job.
#[tokio::test]
async fn replaying_a_claim_id_returns_the_same_assignment() {
    let f = fixture().await;
    let a = submit_job(&f.store, prompt("a"), None).await;
    let b = submit_job(&f.store, prompt("b"), None).await;

    let first = f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    let replay = f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    assert_eq!(replay.id, first.id);
    assert_eq!(replay.attempt, 1, "a replay must not create attempt 2");
    assert_eq!(f.store.attempts(a.id).await.unwrap().len(), 1);
    assert_eq!(
        f.store.get(b.id).await.unwrap().unwrap().state,
        JobState::Queued
    );

    // Same claim id from a *different* worker is a different claim.
    let other = f.store.claim("w2", "c1", LEASE).await.unwrap().unwrap();
    assert_eq!(other.id, b.id);
}

#[tokio::test]
async fn claim_replay_survives_a_coordinator_restart() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), None).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    f.store.close().await;

    let reopened = open_store(&f.db, &f.clock, StoreOptions::default()).await;
    let replay = reopened.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    assert_eq!((replay.id, replay.attempt), (job.id, 1));
}

#[tokio::test]
async fn replay_after_the_assignment_ended_claims_nothing() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), None).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    f.store
        .fail(job.id, 1, "w1", "transient", true)
        .await
        .unwrap();
    // Past the backoff, so a `None` below can only come from the claim id.
    f.wait_out_backoff();

    // The job is claimable again, but not under the old claim id.
    assert!(f.store.claim("w1", "c1", LEASE).await.unwrap().is_none());
    assert_eq!(
        f.store.get(job.id).await.unwrap().unwrap().state,
        JobState::RetryWaiting
    );
    let retry = f.store.claim("w1", "c2", LEASE).await.unwrap().unwrap();
    assert_eq!(retry.attempt, 2);
}

/// Requirement 5: two workers cannot both validly claim the same attempt.
#[tokio::test]
async fn concurrent_claims_assign_a_job_exactly_once() {
    let f = fixture().await;
    for i in 0..10 {
        register(&f.store, &format!("racer{i}"), &["mock"]).await;
    }
    let job = submit_job(&f.store, prompt("contended"), None).await;

    let tasks: Vec<_> = (0..10)
        .map(|i| {
            let store = f.store.clone();
            tokio::spawn(async move { store.claim(&format!("racer{i}"), "c", LEASE).await })
        })
        .collect();
    let mut winners = 0;
    for task in tasks {
        if task.await.unwrap().unwrap().is_some() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
    let job = f.store.get(job.id).await.unwrap().unwrap();
    assert_eq!(job.attempt, 1);
    assert_eq!(f.store.attempts(job.id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_claims_spread_jobs_without_duplicates() {
    let f = fixture().await;
    let mut ids = HashSet::new();
    for i in 0..30 {
        ids.insert(submit_job(&f.store, prompt(&i.to_string()), None).await.id);
    }
    for w in 0..6 {
        register(&f.store, &format!("w{w}"), &["mock"]).await;
    }

    let tasks: Vec<_> = (0..60)
        .map(|n| {
            let store = f.store.clone();
            let worker = format!("w{}", n % 6);
            tokio::spawn(async move { store.claim(&worker, &format!("c{n}"), LEASE).await })
        })
        .collect();
    let mut claimed = Vec::new();
    for task in tasks {
        if let Some(job) = task.await.unwrap().unwrap() {
            assert_eq!(job.attempt, 1);
            claimed.push(job.id);
        }
    }
    assert_eq!(
        claimed.len(),
        30,
        "every job claimed once, extra claims empty"
    );
    assert_eq!(claimed.iter().copied().collect::<HashSet<_>>(), ids);
}

#[tokio::test]
async fn completion_is_fenced_by_attempt_and_worker() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), None).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    let result = json!({"completion": "ok"});

    assert_stale(f.store.complete(job.id, 2, "w1", &result).await);
    assert_stale(f.store.complete(job.id, 1, "w2", &result).await);

    let done = f.store.complete(job.id, 1, "w1", &result).await.unwrap();
    let ReportOutcome::Applied(done) = done else {
        panic!("first completion must apply");
    };
    assert_eq!(done.state, JobState::Succeeded);
    assert_eq!(done.result, Some(result.clone()));
    assert_eq!(done.finished_at, Some(T0));
    assert_eq!(done.worker_id, None);

    // §5.1 #2: a retry after a lost response is a 200, not a 409.
    let again = f.store.complete(job.id, 1, "w1", &result).await.unwrap();
    assert!(matches!(again, ReportOutcome::AlreadyApplied(_)));
    // But a *different* outcome for the finished attempt is rejected.
    assert_stale(f.store.fail(job.id, 1, "w1", "late", true).await);

    let attempts = f.store.attempts(job.id).await.unwrap();
    assert_eq!(attempts[0].outcome, Some(AttemptOutcome::Succeeded));
}

/// Requirement 7: a stale worker cannot commit after a newer attempt owns
/// the job. (Phase 3 adds the lease-expiry route to this situation; here the
/// newer attempt exists because the first one was reported as failed.)
#[tokio::test]
async fn a_stale_attempt_cannot_overwrite_a_newer_one() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), None).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    f.store
        .fail(job.id, 1, "w1", "flaky upstream", true)
        .await
        .unwrap();
    f.wait_out_backoff();
    let second = f.store.claim("w2", "c1", LEASE).await.unwrap().unwrap();
    assert_eq!(second.attempt, 2);

    // Worker 1 comes back with a result for attempt 1.
    assert_stale(
        f.store
            .complete(job.id, 1, "w1", &json!({"stale": true}))
            .await,
    );
    let current = f.store.get(job.id).await.unwrap().unwrap();
    assert_eq!((current.state, current.attempt), (JobState::Running, 2));
    assert_eq!(current.result, None);

    // Worker 1 retrying its *already-recorded* failure is still a 200.
    let retried = f
        .store
        .fail(job.id, 1, "w1", "flaky upstream", true)
        .await
        .unwrap();
    assert!(matches!(retried, ReportOutcome::AlreadyApplied(_)));

    let done = f
        .store
        .complete(job.id, 2, "w2", &json!({"fresh": true}))
        .await
        .unwrap();
    assert_eq!(done.job().result, Some(json!({"fresh": true})));
}

#[tokio::test]
async fn retryable_failures_retry_until_attempts_are_exhausted() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), Some(2)).await;

    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    let after_first = f.store.fail(job.id, 1, "w1", "e1", true).await.unwrap();
    assert_eq!(after_first.job().state, JobState::RetryWaiting);
    assert_eq!(after_first.job().finished_at, None);

    f.wait_out_backoff();
    f.store.claim("w1", "c2", LEASE).await.unwrap().unwrap();
    let after_second = f.store.fail(job.id, 2, "w1", "e2", true).await.unwrap();
    let job = after_second.job();
    assert_eq!(job.state, JobState::Failed, "retryable but out of attempts");
    assert_eq!(job.last_error.as_deref(), Some("e2"));
    assert_eq!(job.finished_at, Some(T0 + 60_001));
    f.wait_out_backoff();
    assert!(f.store.claim("w1", "c3", LEASE).await.unwrap().is_none());

    let outcomes: Vec<_> = f
        .store
        .attempts(job.id)
        .await
        .unwrap()
        .into_iter()
        .map(|a| (a.attempt, a.outcome, a.error))
        .collect();
    assert_eq!(
        outcomes,
        vec![
            (1, Some(AttemptOutcome::RetryableError), Some("e1".into())),
            (2, Some(AttemptOutcome::RetryableError), Some("e2".into())),
        ]
    );
}

#[tokio::test]
async fn permanent_failure_is_terminal_immediately() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), Some(5)).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    let failed = f
        .store
        .fail(job.id, 1, "w1", "bad payload", false)
        .await
        .unwrap();
    assert_eq!(failed.job().state, JobState::Failed);
    assert_eq!(failed.job().attempt, 1);
}

/// Requirement 12 (partial; cancel lands in Phase 4): two reports for the
/// same attempt race. Exactly one wins; history agrees with the job.
#[tokio::test]
async fn complete_and_fail_race_has_exactly_one_winner() {
    for _ in 0..20 {
        let f = fixture().await;
        let job = submit_job(&f.store, prompt("a"), None).await;
        f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();

        let (s1, s2) = (f.store.clone(), f.store.clone());
        let complete =
            tokio::spawn(async move { s1.complete(job.id, 1, "w1", &json!({"ok": 1})).await });
        let fail = tokio::spawn(async move { s2.fail(job.id, 1, "w1", "boom", false).await });
        let (complete, fail) = (complete.await.unwrap(), fail.await.unwrap());

        let winners = [complete.is_ok(), fail.is_ok()]
            .iter()
            .filter(|ok| **ok)
            .count();
        assert_eq!(winners, 1, "complete={complete:?} fail={fail:?}");

        let final_job = f.store.get(job.id).await.unwrap().unwrap();
        let attempt = &f.store.attempts(job.id).await.unwrap()[0];
        if complete.is_ok() {
            assert_eq!(final_job.state, JobState::Succeeded);
            assert_eq!(attempt.outcome, Some(AttemptOutcome::Succeeded));
        } else {
            assert_eq!(final_job.state, JobState::Failed);
            assert_eq!(attempt.outcome, Some(AttemptOutcome::PermanentError));
        }
    }
}

/// §5.1 #4: the claim's two writes (job row, attempt row) are one
/// transaction. A trigger makes the *second* write fail, standing in for a
/// crash between them; the first must roll back.
#[tokio::test]
async fn a_claim_is_atomic_with_its_attempt_row() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), None).await;
    let raw = raw_pool(&f.db).await;
    sqlx::query(
        "CREATE TRIGGER sabotage BEFORE INSERT ON job_attempts
         BEGIN SELECT RAISE(ABORT, 'injected failure between claim writes'); END",
    )
    .execute(&raw)
    .await
    .unwrap();

    let err = f.store.claim("w1", "c1", LEASE).await.unwrap_err();
    assert!(matches!(err, StoreError::Database(_)), "{err:?}");
    let untouched = f.store.get(job.id).await.unwrap().unwrap();
    assert_eq!(untouched.state, JobState::Queued);
    assert_eq!(untouched.attempt, 0, "the job update must have rolled back");
    assert_eq!(untouched.worker_id, None);

    sqlx::query("DROP TRIGGER sabotage")
        .execute(&raw)
        .await
        .unwrap();
    let claimed = f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    assert_eq!(
        claimed.attempt, 1,
        "no attempt was burned by the failed claim"
    );
}

#[tokio::test]
async fn a_completion_is_atomic_with_its_attempt_row() {
    let f = fixture().await;
    let job = submit_job(&f.store, prompt("a"), None).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    let raw = raw_pool(&f.db).await;
    sqlx::query(
        "CREATE TRIGGER sabotage BEFORE UPDATE ON job_attempts
         BEGIN SELECT RAISE(ABORT, 'injected failure between completion writes'); END",
    )
    .execute(&raw)
    .await
    .unwrap();

    assert!(f.store.complete(job.id, 1, "w1", &json!({})).await.is_err());
    let still_running = f.store.get(job.id).await.unwrap().unwrap();
    assert_eq!(still_running.state, JobState::Running);
    assert_eq!(still_running.result, None);

    sqlx::query("DROP TRIGGER sabotage")
        .execute(&raw)
        .await
        .unwrap();
    let done = f.store.complete(job.id, 1, "w1", &json!({})).await.unwrap();
    assert!(matches!(done, ReportOutcome::Applied(_)));
}

#[tokio::test]
async fn registration_is_an_idempotent_upsert() {
    let f = fixture().await;
    let before = f.store.list_workers().await.unwrap();
    f.clock.advance_ms(500);
    register(&f.store, "w1", &["mock", "summarize"]).await;
    let after = f.store.list_workers().await.unwrap();
    assert_eq!(
        after.len(),
        before.len(),
        "re-registering must not add a row"
    );
    let w1 = after.iter().find(|w| w.id == "w1").unwrap();
    assert_eq!(w1.registered_at_ms, T0);
    assert_eq!(w1.last_seen_at_ms, T0 + 500);
    assert_eq!(w1.kinds, vec!["mock", "summarize"]);
}
