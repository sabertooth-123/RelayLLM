//! Handler for OpenAI-compatible `/chat/completions` endpoints: Ollama,
//! llama.cpp server, vLLM, LM Studio, and so on.
//!
//! Payload (`kind: "chat"`):
//!
//! ```json
//! { "prompt": "text" }                                  // or:
//! { "messages": [{"role": "user", "content": "text"}],
//!   "system": "optional system prompt",
//!   "model": "optional per-job override",
//!   "max_tokens": 256, "temperature": 0.2,
//!   "inputs": { "plan": {...} } }   // optional; filled by a workflow
//! ```
//!
//! `inputs` (earlier workflow results) are appended to the last user message
//! as a "Context from earlier steps" block, one `### address` section each.
//!
//! Failure classification decides retries. Retrying can fix a timeout, a
//! rate limit or an overloaded server, but never a bad request or a missing
//! model:
//!
//! | upstream | error |
//! |---|---|
//! | connect/read failure, timeout | retryable |
//! | 408, 409, 425, 429, 5xx | retryable |
//! | other 4xx (bad model, bad auth, bad request) | permanent |
//! | 200 that isn't a chat completion | permanent |
//! | invalid job payload | permanent |
//!
//! Every request carries `Idempotency-Key: <job id>`. A backend that honours
//! it can deduplicate the re-run that at-least-once delivery may cause.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::handler::{BoxFuture, HandlerError, JobContext, JobHandler};

pub const CHAT_KIND: &str = "chat";
const MAX_ERROR_BODY_CHARS: usize = 300;

#[derive(Clone)]
pub struct OpenAiConfig {
    /// e.g. `http://localhost:11434/v1`. `/chat/completions` is appended.
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
}

/// Never print the key, even in debug logs.
impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiConfig")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl OpenAiConfig {
    /// Every problem, not just the first.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.base_url.starts_with("https://") {
            errors.push(
                "the OpenAI-compatible endpoint must use http://: this build has no TLS \
                 support (it targets local inference servers); put a local proxy in front \
                 of remote https APIs"
                    .to_owned(),
            );
        } else if !self.base_url.starts_with("http://") || self.base_url.len() <= "http://".len() {
            errors.push(format!(
                "OpenAI base URL {:?} must look like http://host:port/v1",
                self.base_url
            ));
        }
        if self.model.trim().is_empty() {
            errors.push("an OpenAI model name is required".to_owned());
        }
        errors
    }
}

pub struct OpenAiHandler {
    http: reqwest::Client,
    endpoint: String,
    config: OpenAiConfig,
}

impl OpenAiHandler {
    pub fn new(config: OpenAiConfig) -> Result<Self, String> {
        let errors = config.validate();
        if !errors.is_empty() {
            return Err(errors.join("; "));
        }
        // No overall timeout: the worker enforces the job's own timeout and
        // aborts this future. Connecting, though, should fail fast.
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| format!("building HTTP client: {e}"))?;
        let endpoint = format!("{}/chat/completions", config.base_url.trim_end_matches('/'));
        Ok(Self {
            http,
            endpoint,
            config,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatPayload {
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    messages: Option<Vec<Message>>,
    #[serde(default)]
    system: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    temperature: Option<f32>,
    /// Filled by a workflow: earlier activity results by address.
    #[serde(default)]
    inputs: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    #[serde(default)]
    model: Option<String>,
    choices: Vec<Choice>,
    #[serde(default)]
    usage: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: Message,
    #[serde(default)]
    finish_reason: Option<String>,
}

impl ChatPayload {
    /// Build the message list from `prompt` or `messages` (+ `system`),
    /// consuming those fields.
    fn take_messages(&mut self) -> Result<Vec<Message>, HandlerError> {
        let mut messages = match (self.prompt.take(), self.messages.take()) {
            (Some(prompt), None) => vec![Message {
                role: "user".into(),
                content: prompt,
            }],
            (None, Some(messages)) if !messages.is_empty() => messages,
            (None, Some(_)) => return Err(HandlerError::permanent("messages must not be empty")),
            _ => {
                return Err(HandlerError::permanent(
                    "chat payload needs exactly one of \"prompt\" or \"messages\"",
                ));
            }
        };
        if let Some(system) = self.system.take() {
            messages.insert(
                0,
                Message {
                    role: "system".into(),
                    content: system,
                },
            );
        }
        if let Some(inputs) = self.inputs.take().filter(|i| !i.is_empty()) {
            let target = messages
                .iter_mut()
                .rev()
                .find(|m| m.role == "user")
                .ok_or_else(|| {
                    HandlerError::permanent("inputs need a user message to attach to")
                })?;
            target.content.push_str(&context_block(&inputs));
        }
        Ok(messages)
    }
}

/// Earlier workflow results as a plain-text block after the prompt. A result
/// with a `completion` string (what `chat` and `mock` return) contributes
/// that text; anything else contributes its compact JSON. Keys are sorted
/// (serde_json's `Map` is ordered), so the prompt is deterministic.
fn context_block(inputs: &Map<String, Value>) -> String {
    let mut block = String::from("\n\nContext from earlier steps:");
    for (address, value) in inputs {
        let text = value
            .get("completion")
            .and_then(Value::as_str)
            .map_or_else(|| value.to_string(), str::to_owned);
        block.push_str(&format!("\n\n### {address}\n{text}"));
    }
    block
}

fn retryable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 408 | 409 | 425 | 429)
}

fn truncate(text: &str) -> String {
    let mut short: String = text.chars().take(MAX_ERROR_BODY_CHARS).collect();
    if short.len() < text.len() {
        short.push('…');
    }
    short
}

impl JobHandler for OpenAiHandler {
    fn run(&self, ctx: JobContext) -> BoxFuture<'_, Result<Value, HandlerError>> {
        Box::pin(async move {
            let mut payload: ChatPayload = serde_json::from_value(ctx.payload)
                .map_err(|e| HandlerError::permanent(format!("invalid chat payload: {e}")))?;
            let messages = payload.take_messages()?;
            let model = payload.model.unwrap_or_else(|| self.config.model.clone());

            let mut body = json!({ "model": model, "messages": messages, "stream": false });
            if let Some(max_tokens) = payload.max_tokens {
                body["max_tokens"] = json!(max_tokens);
            }
            if let Some(temperature) = payload.temperature {
                body["temperature"] = json!(temperature);
            }

            let mut request = self
                .http
                .post(&self.endpoint)
                .header("Idempotency-Key", ctx.job_id.to_string())
                .json(&body);
            if let Some(key) = &self.config.api_key {
                request = request.bearer_auth(key);
            }

            let response = request.send().await.map_err(|e| {
                // reqwest's message names the URL, never headers, so the key
                // can't leak through it.
                HandlerError::retryable(format!("inference endpoint unreachable: {e}"))
            })?;
            let status = response.status();
            let text = response
                .text()
                .await
                .map_err(|e| HandlerError::retryable(format!("reading inference response: {e}")))?;

            if !status.is_success() {
                let message = format!("inference endpoint returned {status}: {}", truncate(&text));
                return Err(if retryable_status(status) {
                    HandlerError::retryable(message)
                } else {
                    HandlerError::permanent(message)
                });
            }

            let parsed: ChatResponse = serde_json::from_str(&text).map_err(|e| {
                HandlerError::permanent(format!(
                    "endpoint answered 200 but not with a chat completion ({e}): {}",
                    truncate(&text)
                ))
            })?;
            let choice = parsed
                .choices
                .into_iter()
                .next()
                .ok_or_else(|| HandlerError::permanent("chat completion had no choices"))?;
            Ok(json!({
                "model": parsed.model.unwrap_or(model),
                "completion": choice.message.content,
                "finish_reason": choice.finish_reason,
                "usage": parsed.usage,
            }))
        })
    }
}
