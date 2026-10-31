//! The OpenAI-compatible handler against a scripted fake endpoint.
#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use relay_core::job::JobId;
use relay_worker::{HandlerError, JobContext, JobHandler, OpenAiConfig, OpenAiHandler};
use serde_json::{Value, json};
use tokio::net::TcpListener;

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<(HeaderMap, Value)>>>);

/// A fake `/v1/chat/completions` that answers every request with `status` and
/// `body`, and records what it received.
async fn fake_endpoint(status: StatusCode, body: Value) -> (String, Captured) {
    let captured = Captured::default();
    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(
                move |State(seen): State<Captured>,
                      headers: HeaderMap,
                      Json(request): Json<Value>| {
                    let body = body.clone();
                    async move {
                        seen.0.lock().unwrap().push((headers, request));
                        (status, Json(body))
                    }
                },
            ),
        )
        .with_state(captured.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (url, captured)
}

fn handler(base_url: &str, api_key: Option<&str>) -> OpenAiHandler {
    OpenAiHandler::new(OpenAiConfig {
        base_url: base_url.to_owned(),
        model: "llama3.2".into(),
        api_key: api_key.map(str::to_owned),
    })
    .unwrap()
}

fn ctx(payload: Value) -> JobContext {
    JobContext {
        job_id: JobId::generate(),
        attempt: 1,
        kind: "chat".into(),
        payload,
    }
}

fn completion(content: &str) -> Value {
    json!({
        "model": "llama3.2:3b",
        "choices": [{"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 5, "completion_tokens": 7}
    })
}

#[tokio::test]
async fn sends_a_chat_completion_and_maps_the_answer() {
    let (url, seen) = fake_endpoint(StatusCode::OK, completion("Paris.")).await;
    let job = ctx(json!({"prompt": "Capital of France?", "max_tokens": 32}));
    let job_id = job.job_id;

    let result = handler(&url, Some("sk-test-123")).run(job).await.unwrap();
    assert_eq!(
        result,
        json!({
            "model": "llama3.2:3b",
            "completion": "Paris.",
            "finish_reason": "stop",
            "usage": {"prompt_tokens": 5, "completion_tokens": 7}
        })
    );

    let (headers, request) = seen.0.lock().unwrap()[0].clone();
    assert_eq!(
        request,
        json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "Capital of France?"}],
            "stream": false,
            "max_tokens": 32
        })
    );
    assert_eq!(headers["authorization"], "Bearer sk-test-123");
    assert_eq!(headers["idempotency-key"], job_id.to_string().as_str());
}

#[tokio::test]
async fn messages_system_prompt_and_model_override() {
    let (url, seen) = fake_endpoint(StatusCode::OK, completion("ok")).await;
    let payload = json!({
        "system": "Be terse.",
        "messages": [{"role": "user", "content": "hi"}],
        "model": "qwen2.5",
        "temperature": 0.5
    });
    handler(&url, None).run(ctx(payload)).await.unwrap();

    let (headers, request) = seen.0.lock().unwrap()[0].clone();
    assert_eq!(request["model"], "qwen2.5");
    assert_eq!(request["temperature"], 0.5);
    assert_eq!(
        request["messages"][0],
        json!({"role": "system", "content": "Be terse."})
    );
    assert_eq!(
        request["messages"][1],
        json!({"role": "user", "content": "hi"})
    );
    assert!(
        headers.get("authorization").is_none(),
        "no key configured, no header"
    );
}

/// Workflow inputs become a context block on the last user message: the
/// `completion` text for model results, compact JSON for anything else.
#[tokio::test]
async fn workflow_inputs_are_appended_as_context() {
    let (url, seen) = fake_endpoint(StatusCode::OK, completion("merged")).await;
    let payload = json!({
        "system": "Be terse.",
        "messages": [
            {"role": "user", "content": "first question"},
            {"role": "assistant", "content": "first answer"},
            {"role": "user", "content": "Merge the findings."}
        ],
        "inputs": {
            "research.b": {"model": "m", "completion": "B found y."},
            "research.a": {"model": "m", "completion": "A found x."},
            "input": {"topic": "rust"}
        }
    });
    handler(&url, None).run(ctx(payload)).await.unwrap();

    let (_, request) = seen.0.lock().unwrap()[0].clone();
    assert_eq!(
        request["messages"][1]["content"], "first question",
        "earlier turns untouched"
    );
    assert_eq!(
        request["messages"][3]["content"],
        "Merge the findings.\n\nContext from earlier steps:\
         \n\n### input\n{\"topic\":\"rust\"}\
         \n\n### research.a\nA found x.\
         \n\n### research.b\nB found y."
    );
    assert!(
        request.get("inputs").is_none(),
        "inputs are not forwarded as a field"
    );

    // Inputs with only a system message have nowhere to go: permanent.
    let bad = json!({"messages": [{"role": "assistant", "content": "x"}], "inputs": {"a": 1}});
    assert!(
        !handler(&url, None)
            .run(ctx(bad))
            .await
            .unwrap_err()
            .retryable
    );
}

/// Retryable vs. permanent is what keeps the coordinator from either giving
/// up on a rate limit or burning attempts on a request that can't succeed.
#[tokio::test]
async fn upstream_errors_are_classified_for_the_retry_policy() {
    let cases = [
        (StatusCode::TOO_MANY_REQUESTS, true),
        (StatusCode::SERVICE_UNAVAILABLE, true),
        (StatusCode::INTERNAL_SERVER_ERROR, true),
        (StatusCode::REQUEST_TIMEOUT, true),
        (StatusCode::BAD_REQUEST, false),
        (StatusCode::UNAUTHORIZED, false),
        (StatusCode::NOT_FOUND, false),
    ];
    for (status, retryable) in cases {
        let (url, _) = fake_endpoint(status, json!({"error": {"message": "nope"}})).await;
        let err = handler(&url, None)
            .run(ctx(json!({"prompt": "x"})))
            .await
            .unwrap_err();
        assert_eq!(err.retryable, retryable, "{status}: {err}");
        assert!(err.message.contains(status.as_str()), "{err}");
    }
}

#[tokio::test]
async fn an_unreachable_endpoint_is_retryable() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    drop(listener);
    let err = handler(&url, None)
        .run(ctx(json!({"prompt": "x"})))
        .await
        .unwrap_err();
    assert!(err.retryable, "{err}");
    assert!(err.message.contains("unreachable"), "{err}");
}

#[tokio::test]
async fn a_200_that_is_not_a_chat_completion_is_permanent() {
    let (url, _) = fake_endpoint(StatusCode::OK, json!({"hello": "world"})).await;
    let err = handler(&url, None)
        .run(ctx(json!({"prompt": "x"})))
        .await
        .unwrap_err();
    assert!(!err.retryable, "{err}");
}

#[tokio::test]
async fn invalid_payloads_are_permanent_and_never_sent() {
    let (url, seen) = fake_endpoint(StatusCode::OK, completion("x")).await;
    let h = handler(&url, None);
    for payload in [
        json!({}),
        json!({"prompt": "a", "messages": [{"role": "user", "content": "b"}]}),
        json!({"messages": []}),
        json!({"prompt": "a", "unknown": 1}),
    ] {
        let err: HandlerError = h.run(ctx(payload.clone())).await.unwrap_err();
        assert!(!err.retryable, "{payload}: {err}");
    }
    assert!(seen.0.lock().unwrap().is_empty());
}

#[test]
fn config_is_validated_and_never_leaks_the_key() {
    let config = OpenAiConfig {
        base_url: "https://api.example.com/v1".into(),
        model: " ".into(),
        api_key: Some("sk-super-secret".into()),
    };
    let errors = config.validate();
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(errors[0].contains("no TLS"));
    assert!(!format!("{config:?}").contains("sk-super-secret"));
    assert!(OpenAiHandler::new(config).is_err());
}
