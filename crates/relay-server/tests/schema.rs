//! The database itself must reject rows that violate job invariants, so a bug
//! in any future store operation (claim, complete, reaper...) fails loudly at
//! the write instead of persisting an impossible state that a restart would
//! then "recover".
#![allow(clippy::unwrap_used)]

mod common;

use common::{T0, open_store};
use relay_core::clock::ManualClock;
use relay_server::StoreOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};

struct Row {
    why: &'static str,
    state: &'static str,
    attempt: i64,
    max_attempts: i64,
    worker: Option<&'static str>,
    lease: Option<i64>,
    deadline: Option<i64>,
    result: Option<&'static str>,
    finished: Option<i64>,
    payload: &'static str,
}

const VALID: Row = Row {
    why: "",
    state: "queued",
    attempt: 0,
    max_attempts: 3,
    worker: None,
    lease: None,
    deadline: None,
    result: None,
    finished: None,
    payload: "{}",
};

async fn insert(pool: &SqlitePool, id: usize, row: &Row) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO jobs (id, kind, payload, request_fingerprint, state, attempt, max_attempts,
                           timeout_ms, available_at, worker_id, lease_expires_at, attempt_deadline, result,
                           created_at, updated_at, finished_at)
         VALUES (?, 'mock', ?, 'fp', ?, ?, ?, 1000, 0, ?, ?, ?, ?, 0, 0, ?)",
    )
    .bind(format!("row-{id}"))
    .bind(row.payload)
    .bind(row.state)
    .bind(row.attempt)
    .bind(row.max_attempts)
    .bind(row.worker)
    .bind(row.lease)
    .bind(row.deadline)
    .bind(row.result)
    .bind(row.finished)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn migrated_pool() -> (tempfile::TempDir, SqlitePool) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("r.db");
    let store = open_store(&db, &ManualClock::new(T0), StoreOptions::default()).await;
    store.close().await;
    let pool = SqlitePool::connect_with(SqliteConnectOptions::new().filename(&db))
        .await
        .unwrap();
    (dir, pool)
}

#[tokio::test]
async fn every_legal_state_shape_is_accepted() {
    let (_dir, pool) = migrated_pool().await;
    let legal = [
        Row {
            why: "queued",
            ..VALID
        },
        Row {
            why: "running",
            state: "running",
            attempt: 1,
            worker: Some("w1"),
            lease: Some(T0),
            deadline: Some(T0 + 60_000),
            ..VALID
        },
        Row {
            why: "retry_waiting",
            state: "retry_waiting",
            attempt: 1,
            ..VALID
        },
        Row {
            why: "succeeded",
            state: "succeeded",
            attempt: 2,
            result: Some("{\"text\":\"ok\"}"),
            finished: Some(T0),
            ..VALID
        },
        Row {
            why: "failed",
            state: "failed",
            attempt: 3,
            finished: Some(T0),
            ..VALID
        },
        Row {
            why: "cancelled before any claim",
            state: "cancelled",
            attempt: 0,
            finished: Some(T0),
            ..VALID
        },
    ];
    for (i, row) in legal.iter().enumerate() {
        insert(&pool, i, row)
            .await
            .unwrap_or_else(|e| panic!("{} should be accepted: {e}", row.why));
    }
}

#[tokio::test]
async fn impossible_states_are_rejected_by_the_database() {
    let (_dir, pool) = migrated_pool().await;
    let illegal = [
        Row {
            why: "running without an owner",
            state: "running",
            attempt: 1,
            lease: Some(T0),
            deadline: Some(T0 + 60_000),
            ..VALID
        },
        Row {
            why: "queued but owned",
            worker: Some("w1"),
            lease: Some(T0),
            ..VALID
        },
        Row {
            why: "succeeded without a result",
            state: "succeeded",
            attempt: 1,
            finished: Some(T0),
            ..VALID
        },
        Row {
            why: "result on a failed job",
            state: "failed",
            attempt: 1,
            result: Some("{}"),
            finished: Some(T0),
            ..VALID
        },
        Row {
            why: "result on a running job",
            state: "running",
            attempt: 1,
            worker: Some("w"),
            lease: Some(T0),
            deadline: Some(T0 + 60_000),
            result: Some("{}"),
            ..VALID
        },
        Row {
            why: "terminal without finished_at",
            state: "failed",
            attempt: 1,
            ..VALID
        },
        Row {
            why: "active with finished_at",
            finished: Some(T0),
            ..VALID
        },
        Row {
            why: "more attempts than allowed",
            state: "running",
            attempt: 4,
            worker: Some("w"),
            lease: Some(T0),
            deadline: Some(T0 + 60_000),
            ..VALID
        },
        Row {
            why: "queued but already attempted",
            attempt: 1,
            ..VALID
        },
        Row {
            why: "retry_waiting before any attempt",
            state: "retry_waiting",
            attempt: 0,
            ..VALID
        },
        Row {
            why: "running before any attempt",
            state: "running",
            attempt: 0,
            worker: Some("w"),
            lease: Some(T0),
            deadline: Some(T0 + 60_000),
            ..VALID
        },
        Row {
            why: "payload not JSON",
            payload: "{oops",
            ..VALID
        },
        Row {
            why: "payload not an object",
            payload: "[1,2]",
            ..VALID
        },
        Row {
            why: "result not JSON",
            state: "succeeded",
            attempt: 1,
            result: Some("nope"),
            finished: Some(T0),
            ..VALID
        },
        Row {
            why: "running without an attempt deadline",
            state: "running",
            attempt: 1,
            worker: Some("w"),
            lease: Some(T0),
            ..VALID
        },
        Row {
            why: "attempt deadline on a queued job",
            deadline: Some(T0),
            ..VALID
        },
    ];
    let mut accepted = Vec::new();
    for (i, row) in illegal.iter().enumerate() {
        match insert(&pool, i, row).await {
            Ok(()) => accepted.push(row.why),
            Err(e) => {
                let msg = e
                    .as_database_error()
                    .map(|d| d.message().to_owned())
                    .unwrap_or_default();
                assert!(msg.contains("CHECK"), "{}: wrong error {e}", row.why);
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "database accepted impossible rows: {accepted:#?}"
    );
}
