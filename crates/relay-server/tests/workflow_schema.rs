//! Migration 0005: the database itself rejects impossible workflow rows and
//! histories, independent of the decider (docs/WORKFLOWS_DESIGN.md §3.2, §7).
#![allow(clippy::unwrap_used)]

mod common;

use common::{T0, open_store, prompt, raw_pool, submit_job};
use relay_core::clock::ManualClock;
use relay_server::StoreOptions;
use sqlx::SqlitePool;

struct Fixture {
    _dir: tempfile::TempDir,
    pool: SqlitePool,
    /// Two real job ids, for the foreign key on `workflow_events.job_id`.
    jobs: [String; 2],
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("r.db");
    let store = open_store(&db, &ManualClock::new(T0), StoreOptions::default()).await;
    let a = submit_job(&store, prompt("a"), None).await.id.to_string();
    let b = submit_job(&store, prompt("b"), None).await.id.to_string();
    store.close().await;
    let pool = raw_pool(&db).await;
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&pool)
        .await
        .unwrap();
    insert_run(&pool, "run-1", "running", None, None)
        .await
        .unwrap();
    insert_run(&pool, "run-2", "running", None, None)
        .await
        .unwrap();
    Fixture {
        _dir: dir,
        pool,
        jobs: [a, b],
    }
}

async fn insert_run(
    pool: &SqlitePool,
    id: &str,
    state: &str,
    wake_at: Option<i64>,
    finished_at: Option<i64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO workflow_runs (id, name, definition, input, request_fingerprint, state,
                                    wake_at, created_at, updated_at, finished_at)
         VALUES (?, 'wf', '{\"steps\":[]}', '{}', 'fp', ?, ?, 0, 0, ?)",
    )
    .bind(id)
    .bind(state)
    .bind(wake_at)
    .bind(finished_at)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn event(
    pool: &SqlitePool,
    run: &str,
    seq: i64,
    kind: &str,
    step: Option<&str>,
    job: Option<&str>,
    data: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO workflow_events (run_id, seq, type, step, job_id, data, at)
         VALUES (?, ?, ?, ?, ?, ?, 0)",
    )
    .bind(run)
    .bind(seq)
    .bind(kind)
    .bind(step)
    .bind(job)
    .bind(data)
    .execute(pool)
    .await
    .map(|_| ())
}

fn rejected(result: Result<(), sqlx::Error>, why: &str) {
    let err = result.expect_err(why);
    let msg = err.to_string();
    assert!(
        msg.contains("constraint") || msg.contains("FOREIGN KEY"),
        "{why}: unexpected error {msg}"
    );
}

#[tokio::test]
async fn run_rows_obey_their_invariants() {
    let f = fixture().await;
    let p = &f.pool;
    insert_run(p, "ok-done", "succeeded", None, Some(5))
        .await
        .unwrap();
    insert_run(p, "ok-due", "running", Some(5), None)
        .await
        .unwrap();

    rejected(
        insert_run(p, "x1", "paused", None, None).await,
        "unknown state",
    );
    rejected(
        insert_run(p, "x2", "running", None, Some(5)).await,
        "running but finished",
    );
    rejected(
        insert_run(p, "x3", "failed", None, None).await,
        "terminal without finished_at",
    );
    rejected(
        insert_run(p, "x4", "cancelled", Some(5), Some(5)).await,
        "terminal run is due",
    );
    rejected(
        sqlx::query(
            "INSERT INTO workflow_runs (id, name, definition, input, request_fingerprint, state,
                                        created_at, updated_at)
             VALUES ('x5', 'wf', 'not json', '{}', 'fp', 'running', 0, 0)",
        )
        .execute(p)
        .await
        .map(|_| ()),
        "definition must be JSON",
    );
    rejected(
        sqlx::query(
            "INSERT INTO workflow_runs (id, name, definition, input, request_fingerprint,
                                        idempotency_key, state, created_at, updated_at)
             VALUES ('x6', 'wf', '{}', '{}', 'fp', 'k', 'running', 0, 0),
                    ('x7', 'wf', '{}', '{}', 'fp', 'k', 'running', 0, 0)",
        )
        .execute(p)
        .await
        .map(|_| ()),
        "idempotency key is unique",
    );
}

#[tokio::test]
async fn event_columns_match_their_type() {
    let f = fixture().await;
    let (p, job) = (&f.pool, f.jobs[0].as_str());
    event(p, "run-1", 1, "workflow_started", None, None, "{}")
        .await
        .unwrap();

    rejected(
        event(p, "run-1", 2, "activity_scheduled", Some("a"), None, "{}").await,
        "activity without job",
    );
    rejected(
        event(p, "run-1", 2, "activity_scheduled", None, Some(job), "{}").await,
        "activity without step",
    );
    rejected(
        event(p, "run-1", 2, "timer_scheduled", None, None, "{}").await,
        "timer without step",
    );
    rejected(
        event(p, "run-1", 2, "timer_fired", Some("t"), Some(job), "{}").await,
        "timer with job",
    );
    rejected(
        event(p, "run-1", 2, "workflow_completed", Some("a"), None, "{}").await,
        "run event with step",
    );
    rejected(
        event(p, "run-1", 2, "signal_received", None, Some(job), "{}").await,
        "signal with job",
    );
    rejected(
        event(
            p,
            "run-1",
            2,
            "activity_started",
            Some("a"),
            Some(job),
            "{}",
        )
        .await,
        "unknown type",
    );
    rejected(
        event(p, "run-1", 2, "workflow_completed", None, None, "[1]").await,
        "data must be an object",
    );
    rejected(
        event(p, "run-1", 2, "workflow_completed", None, None, "nope").await,
        "data must be JSON",
    );
    rejected(
        event(p, "run-1", 0, "workflow_completed", None, None, "{}").await,
        "seq starts at 1",
    );
    rejected(
        event(p, "run-1", 1, "workflow_completed", None, None, "{}").await,
        "duplicate seq",
    );
    rejected(
        event(
            p,
            "run-1",
            2,
            "activity_scheduled",
            Some("a"),
            Some("no-such-job"),
            "{}",
        )
        .await,
        "job must exist",
    );
    rejected(
        event(p, "no-such-run", 1, "workflow_started", None, None, "{}").await,
        "run must exist",
    );
}

#[tokio::test]
async fn history_is_append_only() {
    let f = fixture().await;
    let p = &f.pool;
    event(p, "run-1", 1, "workflow_started", None, None, "{}")
        .await
        .unwrap();
    rejected(
        sqlx::query("UPDATE workflow_events SET at = 9 WHERE run_id = 'run-1'")
            .execute(p)
            .await
            .map(|_| ()),
        "update",
    );
    rejected(
        sqlx::query("DELETE FROM workflow_events WHERE run_id = 'run-1'")
            .execute(p)
            .await
            .map(|_| ()),
        "delete",
    );
}

#[tokio::test]
async fn uniqueness_rules_reject_double_scheduling_and_double_outcomes() {
    let f = fixture().await;
    let p = &f.pool;
    let [a, b] = [f.jobs[0].as_str(), f.jobs[1].as_str()];
    event(p, "run-1", 1, "workflow_started", None, None, "{}")
        .await
        .unwrap();
    event(
        p,
        "run-1",
        2,
        "activity_scheduled",
        Some("plan"),
        Some(a),
        "{}",
    )
    .await
    .unwrap();

    rejected(
        event(
            p,
            "run-1",
            3,
            "activity_scheduled",
            Some("plan"),
            Some(b),
            "{}",
        )
        .await,
        "step scheduled twice",
    );
    rejected(
        event(
            p,
            "run-2",
            1,
            "activity_scheduled",
            Some("other"),
            Some(a),
            "{}",
        )
        .await,
        "one job linked to two activities",
    );

    event(
        p,
        "run-1",
        3,
        "activity_completed",
        Some("plan"),
        Some(a),
        "{}",
    )
    .await
    .unwrap();
    rejected(
        event(
            p,
            "run-1",
            4,
            "activity_failed",
            Some("plan"),
            Some(a),
            "{}",
        )
        .await,
        "second outcome",
    );
    rejected(
        event(
            p,
            "run-1",
            4,
            "activity_completed",
            Some("plan"),
            Some(a),
            "{}",
        )
        .await,
        "duplicate completion",
    );

    event(
        p,
        "run-1",
        4,
        "timer_scheduled",
        Some("wait"),
        None,
        "{\"fire_at_ms\":5}",
    )
    .await
    .unwrap();
    rejected(
        event(p, "run-1", 5, "timer_scheduled", Some("wait"), None, "{}").await,
        "timer re-armed",
    );
    event(p, "run-1", 5, "timer_fired", Some("wait"), None, "{}")
        .await
        .unwrap();
    rejected(
        event(p, "run-1", 6, "timer_fired", Some("wait"), None, "{}").await,
        "timer fired twice",
    );

    event(
        p,
        "run-1",
        6,
        "signal_received",
        None,
        None,
        "{\"name\":\"approved\"}",
    )
    .await
    .unwrap();
    event(
        p,
        "run-1",
        7,
        "signal_received",
        None,
        None,
        "{\"name\":\"other\"}",
    )
    .await
    .unwrap();
    rejected(
        event(
            p,
            "run-1",
            8,
            "signal_received",
            None,
            None,
            "{\"name\":\"approved\",\"payload\":1}",
        )
        .await,
        "signal latch",
    );
    // The latch is per run.
    event(
        p,
        "run-2",
        1,
        "signal_received",
        None,
        None,
        "{\"name\":\"approved\"}",
    )
    .await
    .unwrap();

    event(p, "run-1", 8, "workflow_completed", None, None, "{}")
        .await
        .unwrap();
    rejected(
        event(p, "run-1", 9, "workflow_failed", None, None, "{}").await,
        "second run outcome",
    );
}
