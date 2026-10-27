//! `relay`: one binary for running the coordinator and talking to it.

mod bench;
mod client_cmds;
mod cluster;
mod coordinator;
mod demo;
mod runtime;
mod worker_cmd;

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "relay",
    version,
    about = "Fault-tolerant job queue for LLM workloads"
)]
struct Cli {
    /// Coordinator base URL used by client commands.
    #[arg(
        long,
        global = true,
        env = "RELAY_URL",
        default_value = "http://127.0.0.1:7070"
    )]
    server: String,

    /// Print machine-readable JSON instead of human-readable text.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the coordinator (HTTP API + SQLite store).
    Coordinator(coordinator::CoordinatorArgs),
    /// Run a worker process that claims and executes jobs.
    Worker(worker_cmd::WorkerArgs),
    /// Cancel a queued, running or retry-waiting job.
    Cancel(client_cmds::StatusArgs),
    /// List registered workers.
    Workers,
    /// Exit 0 if the coordinator is up and can reach its database, else 1.
    Health,
    /// Run the end-to-end failure-recovery demo on a local cluster.
    Demo(demo::DemoArgs),
    /// Benchmark throughput, latency and recovery on local clusters.
    Bench(bench::BenchArgs),
    /// Show every attempt of a job: which worker ran it and how it ended.
    Attempts(client_cmds::StatusArgs),
    /// Submit a job.
    Submit(client_cmds::SubmitArgs),
    /// Show a job's status.
    Status(client_cmds::StatusArgs),
    /// Show a job's result, optionally waiting for it to finish.
    Result(client_cmds::ResultArgs),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let outcome = match cli.command {
        Command::Coordinator(args) => coordinator::run(args).await,
        Command::Worker(args) => worker_cmd::run(&cli.server, args).await,
        Command::Cancel(args) => client_cmds::cancel(&cli.server, cli.json, args).await,
        Command::Workers => client_cmds::workers(&cli.server, cli.json).await,
        Command::Health => client_cmds::health(&cli.server).await,
        Command::Demo(args) => demo::run(args).await,
        Command::Bench(args) => bench::run(args).await,
        Command::Attempts(args) => client_cmds::attempts(&cli.server, cli.json, args).await,
        Command::Submit(args) => client_cmds::submit(&cli.server, cli.json, args).await,
        Command::Status(args) => client_cmds::status(&cli.server, cli.json, args).await,
        Command::Result(args) => client_cmds::result(&cli.server, cli.json, args).await,
    };
    match outcome {
        Ok(code) => code,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}
