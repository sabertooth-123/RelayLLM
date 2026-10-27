//! Shared harness for coordinator integration tests.
#![allow(dead_code, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::middleware::{self, Next};
use relay_core::api::{RegisterWorkerRequest, SubmitJobRequest};
use relay_core::clock::ManualClock;
use relay_core::job::{Job, JobId};
use relay_core::submission::SubmissionLimits;
use relay_server::{AppState, DEFAULT_LEASE, Limits, Store, StoreOptions};
use relay_worker::{Worker, WorkerConfig, WorkerStats};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub const T0: i64 = 1_700_000_000_000;

pub async fn open_store(path: &Path, clock: &ManualClock, options: StoreOptions) -> Store {
    Store::open(path, Arc::new(clock.clone()), options)
        .await
        .unwrap()
}

pub fn mock_request(payload: Value, key: Option<&str>) -> SubmitJobRequest {
    SubmitJobRequest {
        kind: "mock".into(),
        payload,
        idempotency_key: key.map(str::to_owned),
        max_attempts: None,
        timeout_secs: None,
    }
}

pub fn prompt(text: &str) -> Value {
    json!({ "prompt": text })
}

/// Submit a validated job directly through the store.
pub async fn submit_job(store: &Store, payload: Value, max_attempts: Option<u32>) -> Job {
    let mut request = mock_request(payload, None);
    request.max_attempts = max_attempts;
    store
        .submit(request.validate(&SubmissionLimits::default()).unwrap())
        .await
        .unwrap()
        .job()
        .clone()
}

pub async fn register(store: &Store, worker_id: &str, kinds: &[&str]) {
    store
        .register_worker(
            worker_id,
            &RegisterWorkerRequest {
                name: "test".into(),
                concurrency: 4,
                kinds: kinds.iter().map(|k| (*k).to_owned()).collect(),
            },
        )
        .await
        .unwrap();
}

/// A second, independent connection to the same database file, for
/// inspecting or sabotaging state behind the store's back.
pub async fn raw_pool(db: &Path) -> sqlx::SqlitePool {
    sqlx::SqlitePool::connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(db))
        .await
        .unwrap()
}

/// Poll until every job is terminal, or panic after `timeout`.
pub async fn wait_until_terminal(store: &Store, ids: &[JobId], timeout: Duration) -> Vec<Job> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut jobs = Vec::with_capacity(ids.len());
        for id in ids {
            jobs.push(store.get(*id).await.unwrap().unwrap());
        }
        if jobs.iter().all(|j| j.state.is_terminal()) {
            return jobs;
        }
        if tokio::time::Instant::now() >= deadline {
            let pending: Vec<_> = jobs
                .iter()
                .filter(|j| !j.state.is_terminal())
                .map(|j| (j.id, j.state, j.attempt))
                .collect();
            panic!("jobs not terminal after {timeout:?}: {pending:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wraps a router so the first request whose path ends with `suffix`
/// *executes and commits*, but its connection is killed before the response
/// is written: the classic "committed, response lost" window.
pub fn lose_first_response(router: Router, suffix: &'static str) -> Router {
    let armed = Arc::new(AtomicBool::new(true));
    router.layer(middleware::from_fn(move |request: Request, next: Next| {
        let armed = Arc::clone(&armed);
        async move {
            let matches = request.uri().path().ends_with(suffix);
            let response = next.run(request).await;
            if matches && response.status().is_success() && armed.swap(false, Ordering::SeqCst) {
                panic!("injected fault: response to {suffix} lost after commit");
            }
            response
        }
    }))
}

/// Worker timings shrunk so tests run in milliseconds, not seconds.
pub fn fast_worker_config(name: &str, concurrency: u32) -> WorkerConfig {
    WorkerConfig {
        idle_poll_min: Duration::from_millis(5),
        idle_poll_max: Duration::from_millis(40),
        error_backoff_min: Duration::from_millis(20),
        error_backoff_max: Duration::from_millis(100),
        ..WorkerConfig::new(name, concurrency)
    }
}

/// A worker running in a background task.
pub struct RunningWorker {
    pub id: String,
    pub stats: Arc<WorkerStats>,
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<Result<(), relay_worker::WorkerError>>,
}

impl RunningWorker {
    pub fn spawn(worker: Worker) -> Self {
        let id = worker.id().to_owned();
        let stats = worker.stats();
        let (shutdown, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(worker.run(async {
            let _ = rx.await;
        }));
        Self {
            id,
            stats,
            shutdown,
            handle,
        }
    }

    /// Request shutdown and wait (bounded) for in-flight jobs to drain.
    pub async fn stop(self) {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(10), self.handle)
            .await
            .expect("worker did not stop within 10s")
            .unwrap()
            .unwrap();
    }
}

/// An in-process coordinator on an ephemeral port, backed by a real SQLite file.
pub struct TestServer {
    pub addr: SocketAddr,
    pub store: Store,
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    pub async fn start(db: &Path, clock: &ManualClock, options: StoreOptions) -> Self {
        Self::start_with_limits(db, clock, options, Limits::default()).await
    }

    pub async fn start_with_limits(
        db: &Path,
        clock: &ManualClock,
        options: StoreOptions,
        limits: Limits,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self::start_custom(db, clock, options, limits, listener, |router| router).await
    }

    /// Start on a caller-chosen listener, with the production router wrapped
    /// by `wrap` (used to inject faults such as a lost response).
    pub async fn start_custom(
        db: &Path,
        clock: &ManualClock,
        options: StoreOptions,
        limits: Limits,
        listener: TcpListener,
        wrap: impl FnOnce(Router) -> Router,
    ) -> Self {
        Self::start_inner(db, clock, options, limits, listener, DEFAULT_LEASE, wrap).await
    }

    /// A coordinator with a custom lease, so workers heartbeat every
    /// `lease / 3` of real time. Leases still *expire* only on the manual
    /// clock, so tests decide exactly when.
    pub async fn start_with_lease(
        db: &Path,
        clock: &ManualClock,
        options: StoreOptions,
        lease: Duration,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self::start_inner(
            db,
            clock,
            options,
            Limits::default(),
            listener,
            lease,
            |r| r,
        )
        .await
    }

    async fn start_inner(
        db: &Path,
        clock: &ManualClock,
        options: StoreOptions,
        limits: Limits,
        listener: TcpListener,
        lease: Duration,
        wrap: impl FnOnce(Router) -> Router,
    ) -> Self {
        let store = open_store(db, clock, options).await;
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = oneshot::channel::<()>();
        let state = AppState::new(store.clone(), limits).with_lease(lease);
        let app = wrap(relay_server::router(state));
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await
        });
        Self {
            addr,
            store,
            shutdown,
            handle,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Graceful stop: drain requests, then close every DB connection, the
    /// same sequence as `relay coordinator` on Ctrl-C.
    pub async fn stop(self) {
        let _ = self.shutdown.send(());
        self.handle.await.unwrap().unwrap();
        self.store.close().await;
    }
}
