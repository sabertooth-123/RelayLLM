//! Typed HTTP client for the coordinator API, shared by the CLI and workers.

use std::time::{Duration, Instant};

use relay_core::api::{
    Assignment, AttemptView, ClaimRequest, CompleteRequest, ErrorBody, FailRequest, HealthResponse,
    HeartbeatRequest, HeartbeatResponse, JobResultView, JobStatusView, RegisterWorkerRequest,
    SubmitJobRequest, SubmitJobResponse, WorkerView,
};
use relay_core::job::JobId;
use reqwest::{RequestBuilder, Response};
use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("could not reach the coordinator at {url}: {source}")]
    Transport {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    /// The coordinator answered with a non-2xx status and an error body.
    #[error("coordinator returned {status} {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("unexpected response from the coordinator: {0}")]
    Decode(#[source] reqwest::Error),
}

impl ClientError {
    /// The stable error code for API errors, e.g. `queue_full`.
    pub fn api_code(&self) -> Option<&str> {
        match self {
            ClientError::Api { code, .. } => Some(code),
            _ => None,
        }
    }

    /// True when we can't know whether the coordinator applied the request:
    /// the connection dropped, timed out, the response was garbled, or the
    /// server failed mid-request. Such requests may only be retried if
    /// retrying is idempotent.
    ///
    /// `503 queue_full` is deliberately *not* ambiguous: it's a definite
    /// "not accepted", and hammering a full queue defeats backpressure.
    pub fn is_ambiguous(&self) -> bool {
        match self {
            ClientError::Transport { .. } | ClientError::Decode(_) => true,
            ClientError::Api { status, .. } => matches!(status, 500 | 502 | 504),
        }
    }
}

/// Retry schedule for requests whose outcome is ambiguous.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total tries, including the first.
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RelayClient {
    base_url: String,
    http: reqwest::Client,
    retry: RetryPolicy,
}

impl RelayClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self, ClientError> {
        Self::with_timeout(base_url, Duration::from_secs(30))
    }

    /// `timeout` bounds each whole request, so a hung coordinator can't stall
    /// the caller forever.
    pub fn with_timeout(
        base_url: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, ClientError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        let http = reqwest::Client::builder()
            .timeout(timeout)
            // A blackholed address would otherwise stall each request for
            // the whole `timeout` before the caller can back off.
            .connect_timeout(timeout.min(Duration::from_secs(5)))
            .build()
            .map_err(|source| ClientError::Transport {
                url: base_url.clone(),
                source,
            })?;
        Ok(Self {
            base_url,
            http,
            retry: RetryPolicy::default(),
        })
    }

    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        self.send(self.http.get(self.url("/healthz"))).await
    }

    /// Submit a job. Safe against lost responses: retries of an ambiguous
    /// failure never create a duplicate.
    ///
    /// The crash window this closes: the coordinator commits the job, then
    /// dies (or the connection drops) before the `201` reaches us. Without an
    /// idempotency key a retry would create a second job. So if the caller
    /// gave no key, we generate one for this call. All retries share it, and
    /// the fingerprint ignores the key, so the retry matches the committed job.
    ///
    /// With a generated key, `created` is reported as `true` even when a
    /// retry returned the existing job, because only this call could have
    /// created it. With a caller-supplied key, `created` is passed through
    /// unchanged: we can't tell our earlier attempt from an earlier call.
    pub async fn submit(
        &self,
        request: &SubmitJobRequest,
    ) -> Result<SubmitJobResponse, ClientError> {
        let mut request = request.clone();
        let generated_key = request.idempotency_key.is_none();
        if generated_key {
            request.idempotency_key = Some(format!("auto-{}", JobId::generate()));
        }

        let mut backoff = self.retry.initial_backoff;
        let mut attempt = 1;
        loop {
            match self
                .send::<SubmitJobResponse>(self.http.post(self.url("/v1/jobs")).json(&request))
                .await
            {
                Ok(mut response) => {
                    response.created |= generated_key;
                    return Ok(response);
                }
                Err(err) if err.is_ambiguous() && attempt < self.retry.max_attempts => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.retry.max_backoff);
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub async fn status(&self, id: JobId) -> Result<JobStatusView, ClientError> {
        self.send(self.http.get(self.url(&format!("/v1/jobs/{id}"))))
            .await
    }

    /// Returns the result view whether or not the job has finished; check
    /// `state.is_terminal()`.
    pub async fn result(&self, id: JobId) -> Result<JobResultView, ClientError> {
        self.send(self.http.get(self.url(&format!("/v1/jobs/{id}/result"))))
            .await
    }

    /// Poll until the job is terminal or `timeout` elapses, then return the
    /// last view. Reads are idempotent, so ambiguous errors (for example, the
    /// coordinator restarting) are retried until the deadline instead of
    /// aborting the wait. Definite errors such as `404` return immediately.
    pub async fn wait_for_result(
        &self,
        id: JobId,
        timeout: Duration,
    ) -> Result<JobResultView, ClientError> {
        let deadline = Instant::now() + timeout;
        let mut delay = Duration::from_millis(200);
        loop {
            let outcome = self.result(id).await;
            match &outcome {
                Ok(view) if view.state.is_terminal() => return outcome,
                Ok(_) => {}
                Err(err) if err.is_ambiguous() => {}
                Err(_) => return outcome,
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return outcome;
            }
            tokio::time::sleep(delay.min(remaining)).await;
            delay = (delay * 2).min(Duration::from_secs(2));
        }
    }

    /// Cancel a job. Idempotent: cancelling a cancelled job returns it again.
    /// `409 not_cancellable` means it already succeeded or failed.
    pub async fn cancel(&self, id: JobId) -> Result<JobStatusView, ClientError> {
        self.send(self.http.post(self.url(&format!("/v1/jobs/{id}/cancel"))))
            .await
    }

    pub async fn attempts(&self, id: JobId) -> Result<Vec<AttemptView>, ClientError> {
        self.send(self.http.get(self.url(&format!("/v1/jobs/{id}/attempts"))))
            .await
    }

    // ---- Worker API. These are single-shot: the worker loop owns retry
    // policy, because only it knows which ids to reuse on a retry. ----

    /// Idempotent upsert; safe to retry blindly.
    pub async fn register_worker(
        &self,
        worker_id: &str,
        request: &RegisterWorkerRequest,
    ) -> Result<WorkerView, ClientError> {
        self.send(
            self.http
                .put(self.url(&format!("/v1/workers/{worker_id}")))
                .json(request),
        )
        .await
    }

    pub async fn list_workers(&self) -> Result<Vec<WorkerView>, ClientError> {
        self.send(self.http.get(self.url("/v1/workers"))).await
    }

    /// `Ok(None)` means the queue has nothing eligible for this worker. After
    /// an *ambiguous* error, retry with the same `claim_id`: if the first
    /// request did claim a job, the retry returns that job.
    pub async fn claim(
        &self,
        worker_id: &str,
        claim_id: &str,
    ) -> Result<Option<Assignment>, ClientError> {
        let request = self
            .http
            .post(self.url(&format!("/v1/workers/{worker_id}/claims")))
            .json(&ClaimRequest {
                claim_id: claim_id.to_owned(),
            });
        let response = request
            .send()
            .await
            .map_err(|source| ClientError::Transport {
                url: self.base_url.clone(),
                source,
            })?;
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        decode(response).await.map(Some)
    }

    /// Fenced by `attempt`. Safe to retry: a duplicate of a report that
    /// already landed returns `200`; a superseded attempt gets
    /// `409 stale_attempt`.
    pub async fn complete(
        &self,
        job_id: JobId,
        attempt: u32,
        request: &CompleteRequest,
    ) -> Result<JobStatusView, ClientError> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/jobs/{job_id}/attempts/{attempt}/complete")))
                .json(request),
        )
        .await
    }

    /// Extend the lease of `attempt`. `409 stale_attempt` means the attempt
    /// lost the job (its lease was reaped, or a newer attempt took over):
    /// stop working on it.
    pub async fn heartbeat(
        &self,
        job_id: JobId,
        attempt: u32,
        request: &HeartbeatRequest,
    ) -> Result<HeartbeatResponse, ClientError> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/jobs/{job_id}/attempts/{attempt}/heartbeat")))
                .json(request),
        )
        .await
    }

    /// See [`Self::complete`] for retry semantics.
    pub async fn fail(
        &self,
        job_id: JobId,
        attempt: u32,
        request: &FailRequest,
    ) -> Result<JobStatusView, ClientError> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/jobs/{job_id}/attempts/{attempt}/fail")))
                .json(request),
        )
        .await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn send<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T, ClientError> {
        let response = request
            .send()
            .await
            .map_err(|source| ClientError::Transport {
                url: self.base_url.clone(),
                source,
            })?;
        decode(response).await
    }
}

async fn decode<T: DeserializeOwned>(response: Response) -> Result<T, ClientError> {
    let status = response.status();
    if status.is_success() {
        return response.json::<T>().await.map_err(ClientError::Decode);
    }
    let text = response.text().await.map_err(ClientError::Decode)?;
    Err(match serde_json::from_str::<ErrorBody>(&text) {
        Ok(body) => ClientError::Api {
            status: status.as_u16(),
            code: body.error.code,
            message: body.error.message,
        },
        Err(_) => ClientError::Api {
            status: status.as_u16(),
            code: "unknown".into(),
            message: text,
        },
    })
}
