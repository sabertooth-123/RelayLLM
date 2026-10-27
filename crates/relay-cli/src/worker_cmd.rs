use std::process::ExitCode;
use std::time::Duration;

use clap::Args;
use relay_client::RelayClient;
use relay_core::api::RegisterWorkerRequest;
use relay_worker::mock::MAX_MOCK_DELAY_MS;
use relay_worker::{CHAT_KIND, MockLlmHandler, OpenAiConfig, OpenAiHandler, Worker, WorkerConfig};

use crate::runtime::{LogFormat, Validation, init_tracing, shutdown_signal};

#[derive(Debug, Args)]
pub struct WorkerArgs {
    /// Display name, from [A-Za-z0-9_.-]. The worker id is `<name>-<uuid>`,
    /// unique per process.
    #[arg(long, env = "RELAY_WORKER_NAME", default_value = "worker")]
    name: String,

    /// Maximum jobs this process runs at once.
    #[arg(long, env = "RELAY_WORKER_CONCURRENCY", default_value_t = 4)]
    concurrency: u32,

    /// Default simulated inference time for `mock` jobs (a job's
    /// `delay_ms` overrides it).
    #[arg(long, env = "RELAY_MOCK_DELAY_MS", default_value_t = 200)]
    mock_delay_ms: u64,

    /// Base URL of an OpenAI-compatible server (http only), e.g.
    /// http://localhost:11434/v1 for Ollama. Enables `chat` jobs.
    #[arg(long, env = "RELAY_OPENAI_BASE_URL")]
    openai_base_url: Option<String>,

    /// Model for `chat` jobs (a job's `model` field overrides it).
    #[arg(long, env = "RELAY_OPENAI_MODEL")]
    openai_model: Option<String>,

    /// *Name* of the environment variable holding the API key, if the
    /// server needs one. The key itself is never a flag, so it never shows
    /// up in process listings.
    #[arg(long, env = "RELAY_OPENAI_API_KEY_ENV")]
    openai_api_key_env: Option<String>,

    #[arg(long, env = "RELAY_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
}

impl WorkerArgs {
    /// Every problem at once. Returns the OpenAI config if `chat` is enabled.
    fn validate(&self) -> (Validation, Option<OpenAiConfig>) {
        let mut v = Validation::default();
        // Reuse the coordinator's own registration rules, so a worker fails
        // at startup rather than on its first registration attempt.
        let probe = RegisterWorkerRequest {
            name: self.name.clone(),
            concurrency: self.concurrency,
            kinds: vec!["mock".into()],
        };
        if let Err(e) = probe.validate(64) {
            v.error(format!("--name/--concurrency: {e}"));
        }
        if self.mock_delay_ms > MAX_MOCK_DELAY_MS {
            v.error(format!(
                "--mock-delay-ms must be at most {MAX_MOCK_DELAY_MS}"
            ));
        }

        let openai = match (&self.openai_base_url, &self.openai_model) {
            (None, None) => {
                if self.openai_api_key_env.is_some() {
                    v.warn("--openai-api-key-env is ignored without --openai-base-url");
                }
                None
            }
            (Some(_), None) | (None, Some(_)) => {
                v.error("--openai-base-url and --openai-model must be given together");
                None
            }
            (Some(base_url), Some(model)) => {
                let api_key = match &self.openai_api_key_env {
                    Some(var) => match std::env::var(var) {
                        Ok(key) if !key.is_empty() => Some(key),
                        _ => {
                            v.error(format!(
                                "--openai-api-key-env names {var:?}, but that variable is unset or empty"
                            ));
                            None
                        }
                    },
                    None => None,
                };
                let config = OpenAiConfig {
                    base_url: base_url.clone(),
                    model: model.clone(),
                    api_key,
                };
                for e in config.validate() {
                    v.error(format!("--openai-base-url/--openai-model: {e}"));
                }
                Some(config)
            }
        };
        (v, openai)
    }
}

pub async fn run(server: &str, args: WorkerArgs) -> anyhow::Result<ExitCode> {
    init_tracing(args.log_format);
    let (validation, openai) = args.validate();
    validation.finish("worker")?;

    let client = RelayClient::new(server)?;
    let mut worker = Worker::new(client, WorkerConfig::new(args.name, args.concurrency))
        .with_handler(
            "mock",
            MockLlmHandler::new(Duration::from_millis(args.mock_delay_ms)),
        );
    if let Some(config) = openai {
        // `Debug` redacts the key.
        tracing::info!(?config, "chat jobs enabled");
        let handler = OpenAiHandler::new(config).map_err(anyhow::Error::msg)?;
        worker = worker.with_handler(CHAT_KIND, handler);
    }
    tracing::info!(worker_id = %worker.id(), server, "worker starting");
    worker.run(shutdown_signal()).await?;
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct Wrapper {
        #[command(flatten)]
        args: WorkerArgs,
    }

    fn validate(argv: &[&str]) -> Validation {
        let mut full = vec!["relay"];
        full.extend_from_slice(argv);
        Wrapper::parse_from(full).args.validate().0
    }

    #[test]
    fn defaults_are_valid() {
        let v = validate(&[]);
        assert!(v.errors.is_empty() && v.warnings.is_empty(), "{v:?}");
    }

    #[test]
    fn every_error_is_reported_at_once() {
        let v = validate(&[
            "--name",
            "bad name",
            "--mock-delay-ms",
            "999999",
            "--openai-base-url",
            "https://api.example.com/v1",
        ]);
        assert_eq!(v.errors.len(), 3, "{:#?}", v.errors);
        let joined = v.errors.join("\n");
        assert!(joined.contains("--name"));
        assert!(joined.contains("--mock-delay-ms"));
        assert!(joined.contains("together"));
    }

    #[test]
    fn chat_config_is_checked() {
        let v = validate(&[
            "--openai-base-url",
            "https://api.example.com/v1",
            "--openai-model",
            "",
            "--openai-api-key-env",
            "RELAY_TEST_SURELY_UNSET_VAR",
        ]);
        let joined = v.errors.join("\n");
        assert!(joined.contains("RELAY_TEST_SURELY_UNSET_VAR"), "{joined}");
        assert!(joined.contains("no TLS"), "{joined}");
        assert!(joined.contains("model name is required"), "{joined}");
    }
}
