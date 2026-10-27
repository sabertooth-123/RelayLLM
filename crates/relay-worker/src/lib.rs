//! The RelayLLM worker: register, claim, run a handler, report the outcome.
//!
//! Crash-safety rules this loop follows (ARCHITECTURE §5.1):
//!
//! * **One `claim_id` per claim, reused only after an ambiguous failure.** If
//!   the coordinator committed our claim but the response was lost, the retry
//!   returns the same job instead of stranding it.
//! * **Bounded everything.** A semaphore permit is taken *before* claiming
//!   and held until the result is reported, so at most `concurrency` jobs are
//!   in flight. When the coordinator is unreachable, the loop backs off
//!   instead of spawning tasks.
//! * **No independent retry policy.** Handler failures are reported as
//!   retryable or permanent; the coordinator decides whether the job runs
//!   again. The worker only retries *reports*, which are idempotent per
//!   attempt.
//! * **The coordinator owns time.** The worker heartbeats on the interval the
//!   coordinator chose, never compares lease deadlines against its own
//!   clock, and gives a job up only when the coordinator says so (`409`).

pub mod handler;
pub mod mock;
pub mod openai;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use relay_client::{ClientError, RelayClient};
use relay_core::api::{
    Assignment, CompleteRequest, FailRequest, HeartbeatRequest, RegisterWorkerRequest,
};
use relay_core::job::JobId;
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

pub use handler::{BoxFuture, HandlerError, JobContext, JobHandler};
pub use mock::MockLlmHandler;
pub use openai::{CHAT_KIND, OpenAiConfig, OpenAiHandler};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Unique per process. Generated from `name` by [`WorkerConfig::new`].
    pub worker_id: String,
    pub name: String,
    pub concurrency: u32,
    /// Polling delay when the queue is empty: starts at `min`, doubles to `max`.
    pub idle_poll_min: Duration,
    pub idle_poll_max: Duration,
    /// Delay after a coordinator error: starts at `min`, doubles to `max`.
    pub error_backoff_min: Duration,
    pub error_backoff_max: Duration,
    /// Tries to deliver one result before giving up. Lease expiry then
    /// recovers the job.
    pub max_report_attempts: u32,
}

impl WorkerConfig {
    pub fn new(name: impl Into<String>, concurrency: u32) -> Self {
        let name = name.into();
        Self {
            worker_id: format!("{name}-{}", JobId::generate()),
            name,
            concurrency,
            idle_poll_min: Duration::from_millis(50),
            idle_poll_max: Duration::from_secs(1),
            error_backoff_min: Duration::from_millis(200),
            error_backoff_max: Duration::from_secs(5),
            max_report_attempts: 8,
        }
    }
}

/// Counters for tests and (Phase 4) metrics.
#[derive(Debug, Default)]
pub struct WorkerStats {
    pub registrations: AtomicU64,
    pub claims: AtomicU64,
    pub empty_polls: AtomicU64,
    pub coordinator_errors: AtomicU64,
    pub jobs_succeeded: AtomicU64,
    pub jobs_failed: AtomicU64,
    /// Reports rejected with `409 stale_attempt`: the result was discarded.
    pub reports_stale: AtomicU64,
    /// Reports that could not be delivered at all.
    pub reports_abandoned: AtomicU64,
    pub heartbeats: AtomicU64,
    /// Heartbeats answered `409`: the job was reaped or superseded and the
    /// handler was aborted.
    pub leases_lost: AtomicU64,
    pub in_flight: AtomicU64,
    pub max_in_flight: AtomicU64,
}

impl WorkerStats {
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::SeqCst)
    }

    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("invalid worker configuration: {0}")]
    Config(String),
    #[error("coordinator rejected the worker registration: {0}")]
    Registration(#[source] ClientError),
}

pub struct Worker {
    client: RelayClient,
    config: WorkerConfig,
    handlers: HashMap<String, Arc<dyn JobHandler>>,
    stats: Arc<WorkerStats>,
}

impl Worker {
    pub fn new(client: RelayClient, config: WorkerConfig) -> Self {
        Self {
            client,
            config,
            handlers: HashMap::new(),
            stats: Arc::new(WorkerStats::default()),
        }
    }

    pub fn with_handler(mut self, kind: impl Into<String>, handler: impl JobHandler) -> Self {
        self.handlers.insert(kind.into(), Arc::new(handler));
        self
    }

    pub fn stats(&self) -> Arc<WorkerStats> {
        Arc::clone(&self.stats)
    }

    pub fn id(&self) -> &str {
        &self.config.worker_id
    }

    /// Run until `shutdown` resolves, then stop claiming and wait for
    /// in-flight jobs to finish and report.
    pub async fn run<F: Future<Output = ()>>(self, shutdown: F) -> Result<(), WorkerError> {
        let mut kinds: Vec<String> = self.handlers.keys().cloned().collect();
        kinds.sort();
        let registration = RegisterWorkerRequest {
            name: self.config.name.clone(),
            concurrency: self.config.concurrency,
            kinds,
        };
        relay_core::worker::validate_worker_id(&self.config.worker_id)
            .map_err(|e| WorkerError::Config(e.to_string()))?;
        registration
            .validate(64)
            .map_err(|e| WorkerError::Config(e.to_string()))?;
        let slots = usize::try_from(self.config.concurrency)
            .map_err(|e| WorkerError::Config(e.to_string()))?;

        let inner = Arc::new(Inner {
            client: self.client,
            config: self.config,
            handlers: self.handlers,
            stats: self.stats,
        });
        let mut shutdown = std::pin::pin!(shutdown);
        if !inner.register(&registration, &mut shutdown).await? {
            return Ok(());
        }

        let semaphore = Arc::new(Semaphore::new(slots));
        let mut tasks = JoinSet::new();
        let mut claim_id = new_claim_id();
        let mut idle = Backoff::new(inner.config.idle_poll_min, inner.config.idle_poll_max);
        let mut errors = Backoff::new(
            inner.config.error_backoff_min,
            inner.config.error_backoff_max,
        );

        loop {
            while let Some(done) = tasks.try_join_next() {
                log_task_exit(done);
            }

            // Wait for a free slot *before* claiming: never hold a job we
            // have no capacity to run.
            let permit = tokio::select! {
                biased;
                () = &mut shutdown => break,
                permit = Arc::clone(&semaphore).acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break, // semaphore closed: not expected
                },
            };

            let claimed = tokio::select! {
                biased;
                () = &mut shutdown => break,
                claimed = inner.client.claim(&inner.config.worker_id, &claim_id) => claimed,
            };

            let pause = match claimed {
                Ok(Some(assignment)) => {
                    WorkerStats::bump(&inner.stats.claims);
                    claim_id = new_claim_id();
                    idle.reset();
                    errors.reset();
                    tasks.spawn(execute(Arc::clone(&inner), assignment, permit));
                    None
                }
                Ok(None) => {
                    WorkerStats::bump(&inner.stats.empty_polls);
                    claim_id = new_claim_id();
                    errors.reset();
                    Some(idle.next_delay())
                }
                Err(err) if err.api_code() == Some("worker_not_registered") => {
                    // The coordinator lost our registration (e.g. a fresh
                    // database). Register again; the claim id stays valid.
                    warn!(worker_id = %inner.config.worker_id, "coordinator does not know this worker; re-registering");
                    if !inner.register(&registration, &mut shutdown).await? {
                        break;
                    }
                    None
                }
                Err(err) => {
                    WorkerStats::bump(&inner.stats.coordinator_errors);
                    if err.is_ambiguous() {
                        // Keep `claim_id`: if the claim committed, the retry
                        // must return that job rather than claim another.
                        warn!(error = %err, "claim failed; retrying with the same claim id");
                    } else {
                        claim_id = new_claim_id();
                        warn!(error = %err, "claim rejected");
                    }
                    Some(errors.next_delay())
                }
            };

            if let Some(delay) = pause {
                tokio::select! {
                    biased;
                    () = &mut shutdown => break,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }

        info!(
            in_flight = tasks.len(),
            "shutdown requested; draining in-flight jobs"
        );
        while let Some(done) = tasks.join_next().await {
            log_task_exit(done);
        }
        info!("worker stopped");
        Ok(())
    }
}

struct Inner {
    client: RelayClient,
    config: WorkerConfig,
    handlers: HashMap<String, Arc<dyn JobHandler>>,
    stats: Arc<WorkerStats>,
}

impl Inner {
    /// Register, retrying ambiguous failures with backoff. `Ok(false)` means
    /// shutdown was requested first. Definite rejections (e.g. `422`) are
    /// configuration errors and are returned instead of retried forever.
    async fn register<F: Future<Output = ()>>(
        &self,
        request: &RegisterWorkerRequest,
        shutdown: &mut Pin<&mut F>,
    ) -> Result<bool, WorkerError> {
        let mut backoff =
            Backoff::new(self.config.error_backoff_min, self.config.error_backoff_max);
        loop {
            let attempt = tokio::select! {
                biased;
                () = &mut *shutdown => return Ok(false),
                attempt = self.client.register_worker(&self.config.worker_id, request) => attempt,
            };
            WorkerStats::bump(&self.stats.registrations);
            match attempt {
                Ok(view) => {
                    info!(worker_id = %view.id, kinds = ?view.kinds, concurrency = view.concurrency, "registered with coordinator");
                    return Ok(true);
                }
                Err(err) if err.is_ambiguous() => {
                    WorkerStats::bump(&self.stats.coordinator_errors);
                    warn!(error = %err, "coordinator unavailable; retrying registration");
                }
                Err(err) => return Err(WorkerError::Registration(err)),
            }
            tokio::select! {
                biased;
                () = &mut *shutdown => return Ok(false),
                () = tokio::time::sleep(backoff.next_delay()) => {}
            }
        }
    }

    /// Run the handler under the job's timeout, heartbeating while it runs.
    ///
    /// The handler runs in its own task, so a panic becomes an error instead
    /// of killing the worker, and a timed-out or superseded handler is
    /// aborted instead of running on without a slot.
    async fn run_handler(&self, assignment: &Assignment) -> Execution {
        let Some(handler) = self.handlers.get(&assignment.kind).cloned() else {
            return Execution::Finished(Err(HandlerError::permanent(format!(
                "this worker has no handler for kind {:?}",
                assignment.kind
            ))));
        };
        let ctx = JobContext {
            job_id: assignment.job_id,
            attempt: assignment.attempt,
            kind: assignment.kind.clone(),
            payload: assignment.payload.clone(),
        };
        let mut task = tokio::spawn(async move { handler.run(ctx).await });

        let timeout = tokio::time::sleep(Duration::from_millis(assignment.timeout_ms));
        tokio::pin!(timeout);
        let every = Duration::from_millis(assignment.heartbeat_interval_ms.max(1));
        let mut heartbeats = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        heartbeats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                joined = &mut task => {
                    return Execution::Finished(joined.unwrap_or_else(|join_error| {
                        Err(HandlerError::retryable(format!("handler panicked: {join_error}")))
                    }));
                }
                () = &mut timeout => {
                    task.abort();
                    return Execution::Finished(Err(HandlerError::retryable(format!(
                        "handler timed out after {} ms",
                        assignment.timeout_ms
                    ))));
                }
                _ = heartbeats.tick() => {
                    if self.heartbeat(assignment, every).await == Heartbeat::Lost {
                        task.abort();
                        return Execution::LostOwnership;
                    }
                }
            }
        }
    }

    /// One heartbeat, bounded by the heartbeat interval so a slow coordinator
    /// can't delay timeout enforcement. Only a definite `409` counts as lost
    /// ownership. If the coordinator is unreachable, keep working: the
    /// coordinator, not this worker's guess, decides when the lease is gone.
    async fn heartbeat(&self, assignment: &Assignment, bound: Duration) -> Heartbeat {
        let request = HeartbeatRequest {
            worker_id: self.config.worker_id.clone(),
        };
        let sent = self
            .client
            .heartbeat(assignment.job_id, assignment.attempt, &request);
        match tokio::time::timeout(bound, sent).await {
            Ok(Ok(_)) => {
                WorkerStats::bump(&self.stats.heartbeats);
                Heartbeat::Renewed
            }
            Ok(Err(err)) if err.api_code() == Some("stale_attempt") => {
                WorkerStats::bump(&self.stats.leases_lost);
                warn!(
                    job_id = %assignment.job_id, attempt = assignment.attempt,
                    "lost ownership of the job (lease expired or superseded); aborting handler"
                );
                Heartbeat::Lost
            }
            Ok(Err(err)) => {
                warn!(job_id = %assignment.job_id, error = %err, "heartbeat failed; continuing");
                Heartbeat::Unconfirmed
            }
            Err(_) => {
                warn!(job_id = %assignment.job_id, "heartbeat timed out; continuing");
                Heartbeat::Unconfirmed
            }
        }
    }

    /// Deliver the outcome. Retries only ambiguous failures, which is safe
    /// because reports are idempotent per attempt.
    async fn report(&self, assignment: &Assignment, outcome: &Result<Value, HandlerError>) {
        let mut backoff =
            Backoff::new(self.config.error_backoff_min, self.config.error_backoff_max);
        let worker_id = &self.config.worker_id;
        let mut outcome = outcome.clone();
        for try_number in 1..=self.config.max_report_attempts {
            let delivered = match &outcome {
                Ok(result) => {
                    self.client
                        .complete(
                            assignment.job_id,
                            assignment.attempt,
                            &CompleteRequest {
                                worker_id: worker_id.clone(),
                                result: result.clone(),
                            },
                        )
                        .await
                }
                Err(failure) => {
                    self.client
                        .fail(
                            assignment.job_id,
                            assignment.attempt,
                            &FailRequest {
                                worker_id: worker_id.clone(),
                                error: failure.message.clone(),
                                retryable: failure.retryable,
                            },
                        )
                        .await
                }
            };
            match delivered {
                Ok(job) => {
                    let counter = if outcome.is_ok() {
                        &self.stats.jobs_succeeded
                    } else {
                        &self.stats.jobs_failed
                    };
                    WorkerStats::bump(counter);
                    info!(job_id = %job.id, attempt = assignment.attempt, state = %job.state, "reported");
                    return;
                }
                Err(err) if err.api_code() == Some("stale_attempt") => {
                    WorkerStats::bump(&self.stats.reports_stale);
                    warn!(job_id = %assignment.job_id, attempt = assignment.attempt, "result discarded: a newer attempt owns this job");
                    return;
                }
                // The coordinator refused the *result itself* (e.g. larger
                // than its limit). Re-running would produce the same result,
                // so report a permanent failure instead of abandoning the job
                // to lease expiry and a pointless retry.
                Err(err) if outcome.is_ok() && result_rejected(&err) => {
                    warn!(job_id = %assignment.job_id, error = %err, "coordinator rejected the result; failing the job");
                    outcome = Err(HandlerError::permanent(format!(
                        "coordinator rejected the result: {err}"
                    )));
                }
                Err(err) if err.is_ambiguous() && try_number < self.config.max_report_attempts => {
                    warn!(job_id = %assignment.job_id, error = %err, "report failed; retrying");
                    tokio::time::sleep(backoff.next_delay()).await;
                }
                Err(err) => {
                    WorkerStats::bump(&self.stats.reports_abandoned);
                    error!(
                        job_id = %assignment.job_id, attempt = assignment.attempt, error = %err,
                        "giving up on reporting; the job stays running until its lease is recovered"
                    );
                    return;
                }
            }
        }
    }
}

enum Execution {
    Finished(Result<Value, HandlerError>),
    /// A heartbeat was rejected: another attempt owns the job now. There is
    /// nothing to report; the result would be rejected as stale anyway.
    LostOwnership,
}

#[derive(Debug, PartialEq, Eq)]
enum Heartbeat {
    Renewed,
    Lost,
    Unconfirmed,
}

/// One claimed job: run it, report it, then release the slot.
async fn execute(inner: Arc<Inner>, assignment: Assignment, _permit: OwnedSemaphorePermit) {
    let running = inner.stats.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    inner
        .stats
        .max_in_flight
        .fetch_max(running, Ordering::SeqCst);
    let execution = inner.run_handler(&assignment).await;
    inner.stats.in_flight.fetch_sub(1, Ordering::SeqCst);
    if let Execution::Finished(outcome) = execution {
        inner.report(&assignment, &outcome).await;
    }
}

/// The coordinator refused a completion because of its content: over the
/// result limit (`422`) or over the request body limit (`413`).
fn result_rejected(err: &ClientError) -> bool {
    matches!(err, ClientError::Api { status: 413, .. }) || err.api_code() == Some("invalid_request")
}

fn log_task_exit(done: Result<(), tokio::task::JoinError>) {
    if let Err(err) = done {
        error!(error = %err, "job task ended abnormally");
    }
}

fn new_claim_id() -> String {
    format!("c-{}", JobId::generate())
}

/// Exponential backoff with "equal jitter": each delay is uniformly random in
/// `[d/2, d]`. Without jitter, workers that lost the coordinator at the same
/// moment would all retry in lockstep when it comes back.
struct Backoff {
    min: Duration,
    max: Duration,
    current: Duration,
}

impl Backoff {
    fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max,
            current: min,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let base = self.current;
        self.current = (self.current * 2).min(self.max);
        let half = base / 2;
        let spread = u64::try_from((base - half).as_nanos()).unwrap_or(u64::MAX);
        half + Duration::from_nanos(fastrand::u64(0..=spread))
    }

    fn reset(&mut self) {
        self.current = self.min;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_with_jitter_and_caps() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_millis(400));
        let expected_bases = [100, 200, 400, 400];
        for base in expected_bases {
            let delay = backoff.next_delay();
            let base = Duration::from_millis(base);
            assert!(delay >= base / 2 && delay <= base, "{delay:?} vs {base:?}");
        }
        backoff.reset();
        assert!(backoff.next_delay() <= Duration::from_millis(100));
    }
}
