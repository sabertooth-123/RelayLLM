//! The pluggable job-handler interface.

use std::future::Future;
use std::pin::Pin;

use relay_core::job::JobId;
use serde_json::Value;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Everything a handler gets for one attempt.
#[derive(Debug, Clone)]
pub struct JobContext {
    pub job_id: JobId,
    /// 1 on the first try. Handlers with external side effects can combine
    /// `job_id` with this as a downstream idempotency key, or use `job_id`
    /// alone to dedupe across retries.
    pub attempt: u32,
    pub kind: String,
    pub payload: Value,
}

/// A failed attempt. `retryable` decides whether the coordinator may run the
/// job again; the coordinator alone decides *when* and *whether attempts
/// remain*. Handlers never retry internally.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct HandlerError {
    pub retryable: bool,
    pub message: String,
}

impl HandlerError {
    /// Transient: timeouts, rate limits, upstream 5xx.
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            message: message.into(),
        }
    }

    /// Retrying can't help: invalid payload, unsupported model, upstream 4xx.
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            message: message.into(),
        }
    }
}

/// Executes one kind of job. Implementations return a boxed future so
/// handlers of different types can live in one registry:
///
/// ```ignore
/// fn run(&self, ctx: JobContext) -> BoxFuture<'_, Result<Value, HandlerError>> {
///     Box::pin(async move { Ok(serde_json::json!({"echo": ctx.payload})) })
/// }
/// ```
///
/// The worker enforces the job's timeout and aborts the future when it
/// expires, so handlers should `.await` regularly, not block a thread.
pub trait JobHandler: Send + Sync + 'static {
    fn run(&self, ctx: JobContext) -> BoxFuture<'_, Result<Value, HandlerError>>;
}
