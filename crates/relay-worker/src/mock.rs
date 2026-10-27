//! A deterministic stand-in for an LLM, so the whole system can be run and
//! tested without a model or an API key.
//!
//! Payload (`kind: "mock"`):
//!
//! ```json
//! { "prompt": "text",            // required
//!   "delay_ms": 250,             // optional; overrides the handler default (max 60000)
//!   "fail_attempts": 2,          // optional; attempts 1..=2 fail, attempt 3 succeeds
//!   "fail_kind": "retryable" }   // optional; "retryable" (default) or "permanent"
//! ```
//!
//! The output depends only on the prompt, never on which worker or attempt
//! produced it, so a job run twice yields identical results.

use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::handler::{BoxFuture, HandlerError, JobContext, JobHandler};

pub const MAX_MOCK_DELAY_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub struct MockLlmHandler {
    default_delay: Duration,
}

impl MockLlmHandler {
    pub fn new(default_delay: Duration) -> Self {
        Self { default_delay }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MockPayload {
    prompt: String,
    #[serde(default)]
    delay_ms: Option<u64>,
    #[serde(default)]
    fail_attempts: u32,
    #[serde(default)]
    fail_kind: FailKind,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FailKind {
    #[default]
    Retryable,
    Permanent,
}

impl JobHandler for MockLlmHandler {
    fn run(&self, ctx: JobContext) -> BoxFuture<'_, Result<Value, HandlerError>> {
        Box::pin(async move {
            let payload: MockPayload = serde_json::from_value(ctx.payload)
                .map_err(|e| HandlerError::permanent(format!("invalid mock payload: {e}")))?;
            let delay = match payload.delay_ms {
                Some(ms) => Duration::from_millis(ms.min(MAX_MOCK_DELAY_MS)),
                None => self.default_delay,
            };
            tokio::time::sleep(delay).await;

            if ctx.attempt <= payload.fail_attempts {
                let message = format!(
                    "scripted failure on attempt {} of {}",
                    ctx.attempt, payload.fail_attempts
                );
                return Err(match payload.fail_kind {
                    FailKind::Retryable => HandlerError::retryable(message),
                    FailKind::Permanent => HandlerError::permanent(message),
                });
            }
            Ok(complete(&payload.prompt))
        })
    }
}

/// Deterministic "completion": a function of the prompt only.
fn complete(prompt: &str) -> Value {
    let words: Vec<&str> = prompt.split_whitespace().collect();
    let reversed: Vec<&str> = words.iter().rev().copied().collect();
    let completion = format!("[mock-llm] {}", reversed.join(" "));
    json!({
        "model": "mock-llm-1",
        "completion": completion,
        "usage": {
            "prompt_tokens": words.len(),
            "completion_tokens": words.len() + 1,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_core::job::JobId;

    fn ctx(attempt: u32, payload: Value) -> JobContext {
        JobContext {
            job_id: JobId::generate(),
            attempt,
            kind: "mock".into(),
            payload,
        }
    }

    #[tokio::test]
    async fn output_depends_only_on_the_prompt() {
        let handler = MockLlmHandler::new(Duration::ZERO);
        let a = handler
            .run(ctx(1, json!({"prompt": "hello big world"})))
            .await
            .unwrap();
        let b = handler
            .run(ctx(3, json!({"prompt": "hello big world"})))
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a["completion"], "[mock-llm] world big hello");
        assert_eq!(a["usage"]["prompt_tokens"], 3);
    }

    #[tokio::test]
    async fn scripted_failures_stop_after_fail_attempts() {
        let handler = MockLlmHandler::new(Duration::ZERO);
        let payload = json!({"prompt": "x", "fail_attempts": 2});
        assert!(
            handler
                .run(ctx(1, payload.clone()))
                .await
                .unwrap_err()
                .retryable
        );
        assert!(
            handler
                .run(ctx(2, payload.clone()))
                .await
                .unwrap_err()
                .retryable
        );
        assert!(handler.run(ctx(3, payload)).await.is_ok());

        let permanent = json!({"prompt": "x", "fail_attempts": 1, "fail_kind": "permanent"});
        assert!(!handler.run(ctx(1, permanent)).await.unwrap_err().retryable);
    }

    #[tokio::test]
    async fn invalid_payload_is_a_permanent_error() {
        let handler = MockLlmHandler::new(Duration::ZERO);
        for payload in [
            json!({}),
            json!({"prompt": 5}),
            json!({"prompt": "x", "typo": 1}),
        ] {
            let err = handler.run(ctx(1, payload)).await.unwrap_err();
            assert!(!err.retryable, "{err}");
        }
    }
}
