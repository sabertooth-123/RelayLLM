//! Client subcommands: submit, status, result.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use relay_client::RelayClient;
use relay_core::api::{JobResultView, JobStatusView, SubmitJobRequest};
use relay_core::job::{JobId, JobState};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Args)]
pub struct SubmitArgs {
    /// Handler that should run the job.
    #[arg(long, default_value = "mock")]
    kind: String,

    #[command(flatten)]
    payload: PayloadSource,

    /// Resubmitting with the same key returns the original job instead of
    /// creating a duplicate. If omitted, a key is generated for this command
    /// so its own internal retries are safe; a *separate* later command
    /// without a key creates a new job.
    #[arg(long)]
    idempotency_key: Option<String>,

    #[arg(long)]
    max_attempts: Option<u32>,

    #[arg(long)]
    timeout_secs: Option<u64>,
}

/// Exactly one payload source. `--prompt` exists because quoting JSON on
/// the Windows PowerShell 5.1 command line is unreliable.
#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct PayloadSource {
    /// Shorthand for `--payload '{"prompt": "<text>"}'`.
    #[arg(long)]
    prompt: Option<String>,

    /// Payload as an inline JSON object.
    #[arg(long)]
    payload: Option<String>,

    /// Read the JSON payload from a file.
    #[arg(long)]
    payload_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    id: JobId,
}

#[derive(Debug, Args)]
pub struct ResultArgs {
    id: JobId,

    /// Poll until the job reaches a terminal state.
    #[arg(long)]
    wait: bool,

    /// Give up waiting after this many seconds.
    #[arg(long, default_value_t = 300, requires = "wait")]
    wait_timeout_secs: u64,
}

pub async fn submit(server: &str, json_out: bool, args: SubmitArgs) -> anyhow::Result<ExitCode> {
    let payload = args.payload.into_value()?;
    let client = RelayClient::new(server)?;
    let response = client
        .submit(&SubmitJobRequest {
            kind: args.kind,
            payload,
            idempotency_key: args.idempotency_key,
            max_attempts: args.max_attempts,
            timeout_secs: args.timeout_secs,
        })
        .await?;

    if json_out {
        print_json(&response)?;
    } else if response.created {
        println!("submitted job {} ({})", response.job.id, response.job.state);
    } else {
        println!(
            "idempotency key matched existing job {} ({}); nothing new was created",
            response.job.id, response.job.state
        );
    }
    Ok(ExitCode::SUCCESS)
}

pub async fn status(server: &str, json_out: bool, args: StatusArgs) -> anyhow::Result<ExitCode> {
    let client = RelayClient::new(server)?;
    let job = client.status(args.id).await?;
    if json_out {
        print_json(&job)?;
    } else {
        print_status(&job);
    }
    Ok(ExitCode::SUCCESS)
}

pub async fn cancel(server: &str, json_out: bool, args: StatusArgs) -> anyhow::Result<ExitCode> {
    let client = RelayClient::new(server)?;
    match client.cancel(args.id).await {
        Ok(job) if json_out => print_json(&job)?,
        Ok(job) => println!("job {} is {}", job.id, job.state),
        Err(err) if err.api_code() == Some("not_cancellable") => {
            // Not an error in the tool sense: the job simply finished first.
            eprintln!("{err}");
            return Ok(ExitCode::from(2));
        }
        Err(err) => return Err(err.into()),
    }
    Ok(ExitCode::SUCCESS)
}

pub async fn health(server: &str) -> anyhow::Result<ExitCode> {
    // Short timeout: a health probe that hangs is itself a failure.
    let client = RelayClient::with_timeout(server, Duration::from_secs(3))?;
    let health = client.health().await?;
    println!("{}", health.status);
    Ok(ExitCode::SUCCESS)
}

pub async fn workers(server: &str, json_out: bool) -> anyhow::Result<ExitCode> {
    let client = RelayClient::new(server)?;
    let workers = client.list_workers().await?;
    if json_out {
        print_json(&workers)?;
        return Ok(ExitCode::SUCCESS);
    }
    if workers.is_empty() {
        println!("no workers registered");
        return Ok(ExitCode::SUCCESS);
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    println!(
        "{:<52} {:>7} {:>11} {:>10}  KINDS",
        "WORKER", "RUNNING", "CONCURRENCY", "LAST SEEN"
    );
    for w in workers {
        let seen = format!(
            "{:.1}s ago",
            (now - w.last_seen_at_ms).max(0) as f64 / 1000.0
        );
        println!(
            "{:<52} {:>7} {:>11} {:>10}  {}",
            w.id,
            w.running_jobs,
            w.concurrency,
            seen,
            w.kinds.join(",")
        );
    }
    Ok(ExitCode::SUCCESS)
}

pub async fn attempts(server: &str, json_out: bool, args: StatusArgs) -> anyhow::Result<ExitCode> {
    let client = RelayClient::new(server)?;
    let attempts = client.attempts(args.id).await?;
    if json_out {
        print_json(&attempts)?;
        return Ok(ExitCode::SUCCESS);
    }
    if attempts.is_empty() {
        println!("job {} has not been claimed yet", args.id);
    }
    for a in attempts {
        let outcome = a
            .outcome
            .map_or_else(|| "in progress".to_owned(), |o| o.to_string());
        let took = a
            .finished_at_ms
            .map(|f| format!(" in {} ms", f - a.started_at_ms))
            .unwrap_or_default();
        println!("#{} {} -> {outcome}{took}", a.attempt, a.worker_id);
        if let Some(err) = a.error {
            println!("    {err}");
        }
    }
    Ok(ExitCode::SUCCESS)
}

pub async fn result(server: &str, json_out: bool, args: ResultArgs) -> anyhow::Result<ExitCode> {
    let client = RelayClient::new(server)?;
    let view = if args.wait {
        client
            .wait_for_result(args.id, Duration::from_secs(args.wait_timeout_secs))
            .await?
    } else {
        client.result(args.id).await?
    };

    if json_out {
        print_json(&view)?;
    } else {
        print_result(&view)?;
    }
    // Scripts can branch on the exit code: 0 succeeded, 1 failed/cancelled or
    // error, 2 still in progress.
    Ok(match view.state {
        JobState::Succeeded => ExitCode::SUCCESS,
        state if state.is_terminal() => ExitCode::FAILURE,
        _ => ExitCode::from(2),
    })
}

impl PayloadSource {
    fn into_value(self) -> anyhow::Result<Value> {
        match (self.prompt, self.payload, self.payload_file) {
            (Some(prompt), None, None) => Ok(json!({ "prompt": prompt })),
            (None, Some(raw), None) => {
                serde_json::from_str(&raw).context("--payload is not valid JSON")
            }
            (None, None, Some(path)) => {
                let raw = std::fs::read_to_string(&path)
                    .with_context(|| format!("reading {}", path.display()))?;
                serde_json::from_str(&raw)
                    .with_context(|| format!("{} is not valid JSON", path.display()))
            }
            _ => anyhow::bail!("give exactly one of --prompt, --payload or --payload-file"),
        }
    }
}

fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn print_status(job: &JobStatusView) {
    println!("job       {}", job.id);
    println!("kind      {}", job.kind);
    println!("state     {}", job.state);
    println!("attempt   {}/{}", job.attempt, job.max_attempts);
    if let Some(key) = &job.idempotency_key {
        println!("idem key  {key}");
    }
    if let Some(worker) = &job.worker_id {
        println!("worker    {worker}");
    }
    if let Some(err) = &job.last_error {
        println!("error     {err}");
    }
    println!("created   {} ms", job.created_at_ms);
    println!("updated   {} ms", job.updated_at_ms);
    if let Some(finished) = job.finished_at_ms {
        println!("finished  {finished} ms");
    }
}

fn print_result(view: &JobResultView) -> anyhow::Result<()> {
    match view.state {
        JobState::Succeeded => match &view.result {
            Some(result) => println!("{}", serde_json::to_string_pretty(result)?),
            None => println!("job {} succeeded with no output", view.id),
        },
        JobState::Failed | JobState::Cancelled => println!(
            "job {} {}: {}",
            view.id,
            view.state,
            view.error.as_deref().unwrap_or("no error recorded")
        ),
        state => println!("job {} is still {state}", view.id),
    }
    Ok(())
}
