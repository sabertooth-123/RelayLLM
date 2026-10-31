//! `relay workflow ...`: start, inspect, signal and cancel workflow runs.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Subcommand};
use relay_client::RelayClient;
use relay_core::workflow::{
    ActivityViewStatus, StartWorkflowRequest, StepKind, WorkflowDefinition, WorkflowEventKind,
    WorkflowHistoryView, WorkflowId, WorkflowState, WorkflowView,
};
use serde_json::Value;

#[derive(Debug, Subcommand)]
pub enum WorkflowCommand {
    /// Start a run from a definition file (docs/api.md has the format).
    Start(StartArgs),
    /// Show a run's state and every step's progress.
    Status(IdArgs),
    /// Show a run's durable event history, in order.
    History(IdArgs),
    /// Deliver a signal (one-shot latch) to a run's wait_signal step.
    Signal(SignalArgs),
    /// Cancel a run and its outstanding activities.
    Cancel(CancelArgs),
}

#[derive(Debug, Args)]
pub struct StartArgs {
    /// JSON workflow definition: {"name": ..., "steps": [...]}.
    #[arg(long)]
    file: PathBuf,
    /// Run input as inline JSON (the `input` activity address).
    #[arg(long, conflicts_with = "input_file")]
    input: Option<String>,
    #[arg(long)]
    input_file: Option<PathBuf>,
    /// Starting again with the same key returns the original run.
    #[arg(long)]
    idempotency_key: Option<String>,
    /// Wait for the run to finish; exit 1 unless it succeeded.
    #[arg(long)]
    wait: bool,
    #[arg(long, default_value_t = 600, requires = "wait")]
    wait_timeout_secs: u64,
}

#[derive(Debug, Args)]
pub struct IdArgs {
    id: WorkflowId,
}

#[derive(Debug, Args)]
pub struct SignalArgs {
    id: WorkflowId,
    name: String,
    /// Inline JSON payload (default: null).
    #[arg(long)]
    payload: Option<String>,
}

#[derive(Debug, Args)]
pub struct CancelArgs {
    id: WorkflowId,
    #[arg(long)]
    reason: Option<String>,
}

pub async fn run(
    server: &str,
    json_out: bool,
    command: WorkflowCommand,
) -> anyhow::Result<ExitCode> {
    let client = RelayClient::new(server)?;
    match command {
        WorkflowCommand::Start(args) => start(&client, json_out, args).await,
        WorkflowCommand::Status(args) => {
            let view = client.workflow(args.id).await?;
            print_view(json_out, &view)?;
            Ok(ExitCode::SUCCESS)
        }
        WorkflowCommand::History(args) => {
            let history = client.workflow_history(args.id).await?;
            if json_out {
                println!("{}", serde_json::to_string_pretty(&history)?);
            } else {
                print_history(&history);
            }
            Ok(ExitCode::SUCCESS)
        }
        WorkflowCommand::Signal(args) => {
            let payload = parse_json(args.payload.as_deref(), "--payload")?;
            let response = client.signal_workflow(args.id, &args.name, payload).await?;
            if json_out {
                println!("{}", serde_json::to_string(&response)?);
            } else if response.recorded {
                println!("signal {:?} recorded", args.name);
            } else {
                println!("signal {:?} was already recorded (no change)", args.name);
            }
            Ok(ExitCode::SUCCESS)
        }
        WorkflowCommand::Cancel(args) => {
            let view = client.cancel_workflow(args.id, args.reason).await?;
            print_view(json_out, &view)?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

async fn start(client: &RelayClient, json_out: bool, args: StartArgs) -> anyhow::Result<ExitCode> {
    let raw = std::fs::read_to_string(&args.file)
        .with_context(|| format!("reading {}", args.file.display()))?;
    let definition: WorkflowDefinition = serde_json::from_str(&raw)
        .with_context(|| format!("{} is not a valid workflow definition", args.file.display()))?;
    let input = match (&args.input, &args.input_file) {
        (Some(inline), _) => parse_json(Some(inline), "--input")?,
        (None, Some(path)) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?;
            serde_json::from_str(&text)
                .with_context(|| format!("{} is not JSON", path.display()))?
        }
        (None, None) => Value::Null,
    };
    let response = client
        .start_workflow(&StartWorkflowRequest {
            definition,
            input,
            idempotency_key: args.idempotency_key,
        })
        .await?;
    let id = response.workflow.id;
    if !args.wait {
        if json_out {
            println!("{}", serde_json::to_string_pretty(&response)?);
        } else {
            let verb = if response.created {
                "started"
            } else {
                "already exists:"
            };
            println!("workflow {verb} {id}");
        }
        return Ok(ExitCode::SUCCESS);
    }
    if !json_out {
        eprintln!("workflow {id} started; waiting...");
    }
    let view = client
        .wait_for_workflow(id, Duration::from_secs(args.wait_timeout_secs))
        .await?;
    print_view(json_out, &view)?;
    Ok(match view.state {
        WorkflowState::Succeeded => ExitCode::SUCCESS,
        WorkflowState::Running => {
            eprintln!("still running after {} s", args.wait_timeout_secs);
            ExitCode::FAILURE
        }
        WorkflowState::Failed | WorkflowState::Cancelled => ExitCode::FAILURE,
    })
}

fn parse_json(text: Option<&str>, flag: &str) -> anyhow::Result<Value> {
    match text {
        None => Ok(Value::Null),
        Some(t) => serde_json::from_str(t).with_context(|| format!("{flag} is not valid JSON")),
    }
}

fn print_view(json_out: bool, view: &WorkflowView) -> anyhow::Result<()> {
    if json_out {
        println!("{}", serde_json::to_string_pretty(view)?);
        return Ok(());
    }
    println!("workflow {} ({})  {}", view.id, view.name, view.state);
    if let Some(reason) = &view.reason {
        println!("  reason: {reason}");
    }
    for step in &view.steps {
        let status = format!("{:?}", step.status).to_lowercase();
        match step.kind {
            StepKind::Activity | StepKind::Parallel => {
                for (i, a) in step.activities.iter().enumerate() {
                    let name = if i == 0 { step.name.as_str() } else { "" };
                    let shown_status = if i == 0 { status.as_str() } else { "" };
                    let attempts = match (a.attempt, a.max_attempts) {
                        (Some(n), Some(max)) => format!("attempt {n}/{max}"),
                        _ => String::new(),
                    };
                    println!(
                        "  {name:<14} {shown_status:<9} {:<22} {:<10} {attempts}",
                        a.address,
                        activity_label(a.status)
                    );
                }
            }
            StepKind::Sleep => {
                let when = step.fire_at_ms.map_or_else(
                    || "timer not armed".to_owned(),
                    |at| format!("fires at {at} ms"),
                );
                println!("  {:<14} {status:<9} {when}", step.name);
            }
            StepKind::WaitSignal => {
                let received = if step.signal_received == Some(true) {
                    "received"
                } else {
                    "not received"
                };
                println!(
                    "  {:<14} {status:<9} signal {:?} ({received})",
                    step.name,
                    step.signal.as_deref().unwrap_or("")
                );
            }
        }
    }
    Ok(())
}

fn activity_label(status: ActivityViewStatus) -> &'static str {
    match status {
        ActivityViewStatus::Pending => "pending",
        ActivityViewStatus::Queued => "queued",
        ActivityViewStatus::Running => "running",
        ActivityViewStatus::Retrying => "retrying",
        ActivityViewStatus::Succeeded => "succeeded",
        ActivityViewStatus::Failed => "failed",
        ActivityViewStatus::Cancelled => "cancelled",
    }
}

fn print_history(history: &WorkflowHistoryView) {
    println!("history of workflow {}", history.id);
    for e in &history.events {
        let detail = match &e.kind {
            WorkflowEventKind::WorkflowStarted { .. } => String::new(),
            WorkflowEventKind::ActivityScheduled { activity, job_id }
            | WorkflowEventKind::ActivityCompleted { activity, job_id }
            | WorkflowEventKind::ActivityCancelled { activity, job_id } => {
                format!("{activity} (job {job_id})")
            }
            WorkflowEventKind::ActivityFailed {
                activity,
                job_id,
                error,
            } => {
                format!("{activity} (job {job_id}): {error}")
            }
            WorkflowEventKind::TimerScheduled { step, fire_at_ms } => {
                format!("{step}, fires at {fire_at_ms} ms")
            }
            WorkflowEventKind::TimerFired { step } => step.clone(),
            WorkflowEventKind::SignalReceived { name, .. } => name.clone(),
            WorkflowEventKind::WorkflowCompleted => String::new(),
            WorkflowEventKind::WorkflowFailed { reason }
            | WorkflowEventKind::WorkflowCancelled { reason } => reason.clone(),
        };
        println!(
            "  #{:<3} {:>15}  {:<20} {detail}",
            e.seq,
            e.at_ms,
            e.kind.type_name()
        );
    }
}
