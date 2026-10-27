//! Leases, heartbeats, the reaper, the per-attempt deadline and the startup
//! grace period, driven deterministically with a manual clock.
#![allow(clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{T0, mock_request, open_store, prompt, register};
use relay_core::clock::ManualClock;
use relay_core::job::{AttemptOutcome, Job, JobState};
use relay_core::retry::RetryPolicy;
use relay_core::submission::SubmissionLimits;
use relay_server::{ReportOutcome, Store, StoreError, StoreOptions};
use serde_json::json;

const LEASE: Duration = Duration::from_secs(30);
const LEASE_MS: i64 = 30_000;
const GRACE_MS: i64 = 10_000;

/// Deterministic-enough backoff: every first retry waits 500–1000 ms.
fn options() -> StoreOptions {
    StoreOptions {
        retry: RetryPolicy {
            base: Duration::from_millis(1_000),
            max: Duration::from_millis(1_000),
        },
        deadline_grace: Duration::from_millis(GRACE_MS as u64),
        ..StoreOptions::default()
    }
}

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
    let store = open_store(&db, &clock, options()).await;
    register(&store, "w1", &["mock"]).await;
    register(&store, "w2", &["mock"]).await;
    Fixture {
        _dir: dir,
        db,
        clock,
        store,
    }
}

async fn submit(store: &Store, max_attempts: u32, timeout_secs: u64) -> Job {
    let mut request = mock_request(prompt("x"), None);
    request.max_attempts = Some(max_attempts);
    request.timeout_secs = Some(timeout_secs);
    store
        .submit(request.validate(&SubmissionLimits::default()).unwrap())
        .await
        .unwrap()
        .job()
        .clone()
}

/// Submit a job (timeout 60 s, 3 attempts) and have `w1` claim it at T0.
async fn claimed(f: &Fixture) -> Job {
    let job = submit(&f.store, 3, 60).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    f.store.get(job.id).await.unwrap().unwrap()
}

#[tokio::test]
async fn claim_sets_a_deadline_and_caps_the_lease_at_it() {
    let f = fixture().await;
    let job = claimed(&f).await;
    assert_eq!(job.attempt_deadline, Some(T0 + 60_000 + GRACE_MS));
    assert_eq!(job.lease_expires_at, Some(T0 + LEASE_MS));

    // A job whose timeout + grace is shorter than the lease gets a lease
    // that ends at its deadline.
    submit(&f.store, 3, 5).await;
    let short = f.store.claim("w2", "c1", LEASE).await.unwrap().unwrap();
    assert_eq!(short.attempt_deadline, Some(T0 + 5_000 + GRACE_MS));
    assert_eq!(short.lease_expires_at, short.attempt_deadline);
}

/// §5.1 #3: a hung handler whose heartbeat task keeps running can't hold the
/// job forever.
#[tokio::test]
async fn heartbeats_extend_the_lease_but_never_past_the_deadline() {
    let f = fixture().await;
    let job = claimed(&f).await;
    let deadline = T0 + 60_000 + GRACE_MS; // T0 + 70 s

    f.clock.advance_ms(20_000);
    let renewed = f.store.heartbeat(job.id, 1, "w1", LEASE).await.unwrap();
    assert_eq!(renewed.lease_expires_at, Some(T0 + 20_000 + LEASE_MS));

    f.clock.advance_ms(30_000); // T0 + 50 s: now + lease would be T0 + 80 s
    let capped = f.store.heartbeat(job.id, 1, "w1", LEASE).await.unwrap();
    assert_eq!(capped.lease_expires_at, Some(deadline));

    f.clock.set_ms(deadline + 1);
    // Heartbeating past the deadline is accepted but can't extend anything.
    let late = f.store.heartbeat(job.id, 1, "w1", LEASE).await.unwrap();
    assert_eq!(late.lease_expires_at, Some(deadline));

    let reaped = f.store.reap_expired(100).await.unwrap();
    assert_eq!(reaped.len(), 1);
    assert!(
        reaped[0].reason.contains("deadline exceeded"),
        "{}",
        reaped[0].reason
    );
}

#[tokio::test]
async fn heartbeats_are_fenced_like_reports() {
    let f = fixture().await;
    let job = claimed(&f).await;

    for (attempt, worker) in [(2, "w1"), (1, "w2")] {
        let err = f
            .store
            .heartbeat(job.id, attempt, worker, LEASE)
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::StaleAttempt { .. }), "{err:?}");
    }
    let missing = relay_core::job::JobId::generate();
    assert!(matches!(
        f.store
            .heartbeat(missing, 1, "w1", LEASE)
            .await
            .unwrap_err(),
        StoreError::JobNotFound(_)
    ));
}

#[tokio::test]
async fn expiry_is_strictly_after_the_lease() {
    let f = fixture().await;
    claimed(&f).await;
    f.clock.set_ms(T0 + LEASE_MS);
    assert!(
        f.store.reap_expired(100).await.unwrap().is_empty(),
        "lease still valid at its end instant"
    );
    f.clock.advance_ms(1);
    assert_eq!(f.store.reap_expired(100).await.unwrap().len(), 1);
}

/// Requirement 6 at the store level: an abandoned job becomes recoverable,
/// waits out its backoff, and is claimed as a new attempt.
#[tokio::test]
async fn an_expired_lease_requeues_the_job_with_backoff() {
    let f = fixture().await;
    let job = claimed(&f).await;
    f.clock.set_ms(T0 + LEASE_MS + 1);
    let now = T0 + LEASE_MS + 1;

    let reaped = f.store.reap_expired(100).await.unwrap();
    assert_eq!(reaped.len(), 1);
    assert_eq!(reaped[0].new_state, JobState::RetryWaiting);
    assert_eq!(reaped[0].worker_id, "w1");

    let waiting = f.store.get(job.id).await.unwrap().unwrap();
    assert_eq!(waiting.state, JobState::RetryWaiting);
    assert_eq!(
        (
            waiting.worker_id,
            waiting.lease_expires_at,
            waiting.attempt_deadline
        ),
        (None, None, None)
    );
    assert!(
        (now + 500..=now + 1_000).contains(&waiting.available_at),
        "backoff out of range: {}",
        waiting.available_at - now
    );
    assert!(
        f.store.claim("w2", "c1", LEASE).await.unwrap().is_none(),
        "claimable before backoff ended"
    );

    f.clock.set_ms(waiting.available_at);
    let retry = f.store.claim("w2", "c2", LEASE).await.unwrap().unwrap();
    assert_eq!(retry.attempt, 2);

    let history = f.store.attempts(job.id).await.unwrap();
    assert_eq!(history[0].outcome, Some(AttemptOutcome::LeaseExpired));
    assert_eq!(
        history[0].error.as_deref(),
        Some("lease expired: worker w1 stopped heartbeating")
    );
    assert_eq!(
        (history[1].worker_id.as_str(), history[1].outcome),
        ("w2", None)
    );
}

/// Requirement 7 via lease expiry: the original worker comes back after its
/// job was recovered and re-assigned. None of its writes may land.
#[tokio::test]
async fn a_worker_that_returns_after_expiry_is_fenced_out() {
    let f = fixture().await;
    let job = claimed(&f).await;
    f.clock.set_ms(T0 + LEASE_MS + 1);
    f.store.reap_expired(100).await.unwrap();

    // Even before anyone re-claims it, the old attempt can't complete it.
    let early = f
        .store
        .complete(job.id, 1, "w1", &json!({"late": true}))
        .await;
    assert!(
        matches!(early, Err(StoreError::StaleAttempt { .. })),
        "{early:?}"
    );

    f.clock.advance_ms(1_000);
    f.store.claim("w2", "c1", LEASE).await.unwrap().unwrap();

    let stale_complete = f
        .store
        .complete(job.id, 1, "w1", &json!({"late": true}))
        .await;
    assert!(matches!(
        stale_complete,
        Err(StoreError::StaleAttempt {
            current_attempt: 2,
            ..
        })
    ));
    let stale_beat = f.store.heartbeat(job.id, 1, "w1", LEASE).await;
    assert!(matches!(stale_beat, Err(StoreError::StaleAttempt { .. })));

    let done = f
        .store
        .complete(job.id, 2, "w2", &json!({"fresh": true}))
        .await
        .unwrap();
    assert!(matches!(done, ReportOutcome::Applied(_)));
    assert_eq!(done.job().result, Some(json!({"fresh": true})));
}

#[tokio::test]
async fn expiry_on_the_last_attempt_fails_the_job() {
    let f = fixture().await;
    let job = submit(&f.store, 1, 60).await;
    f.store.claim("w1", "c1", LEASE).await.unwrap().unwrap();
    f.clock.set_ms(T0 + LEASE_MS + 1);

    let reaped = f.store.reap_expired(100).await.unwrap();
    assert_eq!(reaped[0].new_state, JobState::Failed);
    let failed = f.store.get(job.id).await.unwrap().unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.finished_at, Some(T0 + LEASE_MS + 1));
    assert!(failed.last_error.unwrap().contains("lease expired"));
}

/// Recovery must be safe if several reap passes run at once.
#[tokio::test]
async fn concurrent_reapers_expire_each_job_exactly_once() {
    let f = fixture().await;
    let mut ids = Vec::new();
    for n in 0..20 {
        ids.push(submit(&f.store, 3, 60).await.id);
        f.store
            .claim("w1", &format!("c{n}"), LEASE)
            .await
            .unwrap()
            .unwrap();
    }
    f.clock.set_ms(T0 + LEASE_MS + 1);

    let reapers: Vec<_> = (0..4)
        .map(|_| {
            let store = f.store.clone();
            tokio::spawn(async move { store.reap_expired(500).await.unwrap().len() })
        })
        .collect();
    let mut total = 0;
    for reaper in reapers {
        total += reaper.await.unwrap();
    }
    assert_eq!(total, 20, "each job reaped by exactly one pass");
    for id in ids {
        let history = f.store.attempts(id).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].outcome, Some(AttemptOutcome::LeaseExpired));
    }
}

#[tokio::test]
async fn reaping_is_batched() {
    let f = fixture().await;
    for n in 0..5 {
        submit(&f.store, 3, 60).await;
        f.store
            .claim("w1", &format!("c{n}"), LEASE)
            .await
            .unwrap()
            .unwrap();
    }
    f.clock.set_ms(T0 + LEASE_MS + 1);
    assert_eq!(f.store.reap_expired(2).await.unwrap().len(), 2);
    assert_eq!(f.store.reap_expired(2).await.unwrap().len(), 2);
    assert_eq!(f.store.reap_expired(2).await.unwrap().len(), 1);
    assert!(f.store.reap_expired(2).await.unwrap().is_empty());
}

/// The lease is a recovery *trigger*, not the ownership check: a heartbeat
/// that lands after expiry but before the reaper acts keeps the job.
#[tokio::test]
async fn a_late_heartbeat_that_beats_the_reaper_keeps_the_job() {
    let f = fixture().await;
    let job = claimed(&f).await;
    f.clock.set_ms(T0 + LEASE_MS + 5_000);
    f.store.heartbeat(job.id, 1, "w1", LEASE).await.unwrap();
    assert!(f.store.reap_expired(100).await.unwrap().is_empty());
    let done = f.store.complete(job.id, 1, "w1", &json!({})).await.unwrap();
    assert_eq!(done.job().attempt, 1);
}

/// §5.1 #5 (control): without the grace period, a coordinator outage longer
/// than the lease makes a healthy worker's job look abandoned.
#[tokio::test]
async fn without_grace_an_outage_reaps_healthy_jobs() {
    let f = fixture().await;
    let job = claimed(&f).await;
    f.store.close().await;

    f.clock.advance_ms(120_000); // coordinator down for 2 minutes
    let restarted = open_store(&f.db, &f.clock, options()).await;
    let reaped = restarted.reap_expired(100).await.unwrap();
    assert_eq!(reaped.len(), 1, "control: the job is lost to the outage");
    let late = restarted.complete(job.id, 1, "w1", &json!({})).await;
    assert!(matches!(late, Err(StoreError::StaleAttempt { .. })));
}

/// §5.1 #5: with the grace period, the worker that kept running through the
/// outage can still deliver, even past the attempt deadline (T0 + 70 s).
#[tokio::test]
async fn startup_grace_lets_workers_reconnect_after_an_outage() {
    let f = fixture().await;
    let job = claimed(&f).await;
    let untouched = submit(&f.store, 3, 60).await; // queued, not running
    f.store.close().await;

    f.clock.advance_ms(120_000);
    let restarted = open_store(&f.db, &f.clock, options()).await;
    let now = T0 + 120_000;
    assert_eq!(restarted.grant_lease_grace(LEASE).await.unwrap(), 1);
    assert!(restarted.reap_expired(100).await.unwrap().is_empty());

    // The heartbeat must not *shorten* the grace lease to the (past)
    // deadline: MAX(current, MIN(now + lease, deadline)).
    let beat = restarted.heartbeat(job.id, 1, "w1", LEASE).await.unwrap();
    assert_eq!(beat.lease_expires_at, Some(now + LEASE_MS));

    let done = restarted
        .complete(job.id, 1, "w1", &json!({"ok": 1}))
        .await
        .unwrap();
    assert_eq!(
        (done.job().state, done.job().attempt),
        (JobState::Succeeded, 1)
    );
    let queued = restarted.get(untouched.id).await.unwrap().unwrap();
    assert_eq!(
        (queued.state, queued.lease_expires_at),
        (JobState::Queued, None)
    );

    // Grace is one lease, not forever: a worker that claims a job after the
    // restart and then disappears is still recovered. (FIFO: this claims the
    // job that was queued before the outage.)
    let abandoned = restarted.claim("w2", "c9", LEASE).await.unwrap().unwrap();
    assert_eq!(abandoned.id, untouched.id);
    f.clock.advance_ms(LEASE_MS + 1);
    let reaped = restarted.reap_expired(100).await.unwrap();
    assert_eq!(
        reaped.iter().map(|r| r.job_id).collect::<Vec<_>>(),
        vec![untouched.id]
    );
}

#[tokio::test]
async fn latest_timestamp_reports_the_newest_write() {
    let f = fixture().await;
    claimed(&f).await;
    f.clock.advance_ms(7_000);
    register(&f.store, "w1", &["mock"]).await; // last_seen_at = T0 + 7 s
    assert_eq!(f.store.latest_timestamp().await.unwrap(), Some(T0 + 7_000));
}
