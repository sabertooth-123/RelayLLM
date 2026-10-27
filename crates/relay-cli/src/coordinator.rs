use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use relay_core::clock::{MonotonicClock, SystemClock};
use relay_core::retry::RetryPolicy;
use relay_server::{AppState, Durability, Limits, ReaperConfig, Store, StoreOptions, spawn_reaper};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SyncLevel {
    Full,
    Normal,
}

impl From<SyncLevel> for Durability {
    fn from(level: SyncLevel) -> Self {
        match level {
            SyncLevel::Full => Durability::Full,
            SyncLevel::Normal => Durability::Normal,
        }
    }
}
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::runtime::{LogFormat, Validation, init_tracing, shutdown_signal};

#[derive(Debug, Args)]
pub struct CoordinatorArgs {
    /// SQLite database file (created if missing). Keep it on a local disk.
    #[arg(long, env = "RELAY_DB", default_value = "relay.db")]
    db: PathBuf,

    /// Address to listen on. There is no authentication, so keep this on
    /// loopback or a private network.
    #[arg(long, env = "RELAY_BIND", default_value = "127.0.0.1:7070")]
    bind: SocketAddr,

    /// Maximum queued + running + retry-waiting jobs before submits get 503.
    #[arg(long, env = "RELAY_MAX_ACTIVE_JOBS", default_value_t = 10_000)]
    max_active_jobs: u64,

    /// Seconds a claimed job belongs to its worker without a heartbeat.
    /// Workers heartbeat every third of this. Shorter: faster recovery from
    /// dead workers. Longer: fewer duplicate runs during network blips.
    #[arg(long, env = "RELAY_LEASE_SECS", default_value_t = 30)]
    lease_secs: u64,

    /// `full`: every acknowledged write survives power loss (fsync per
    /// commit). `normal`: it survives a coordinator crash, but the last
    /// writes can be lost on power loss/OS crash; several times faster.
    /// See bench/README.md.
    #[arg(long, env = "RELAY_SQLITE_SYNCHRONOUS", value_enum, default_value_t = SyncLevel::Full)]
    sqlite_synchronous: SyncLevel,

    /// Retry backoff for failed or abandoned attempts: base * 2^(n-1),
    /// capped at --retry-max-ms, with jitter into [d/2, d].
    #[arg(long, env = "RELAY_RETRY_BASE_MS", default_value_t = 1_000)]
    retry_base_ms: u64,

    #[arg(long, env = "RELAY_RETRY_MAX_MS", default_value_t = 60_000)]
    retry_max_ms: u64,

    /// Added to a job's timeout to form its absolute attempt deadline, which
    /// heartbeats can't extend.
    #[arg(long, env = "RELAY_DEADLINE_GRACE_SECS", default_value_t = 30)]
    deadline_grace_secs: u64,

    /// Any request (including reading its body) taking longer gets
    /// `504 request_timeout`. Keep it well under the lease.
    #[arg(long, env = "RELAY_REQUEST_TIMEOUT_SECS", default_value_t = 10)]
    request_timeout_secs: u64,

    /// On shutdown, wait at most this long for in-flight requests.
    #[arg(long, env = "RELAY_SHUTDOWN_DRAIN_SECS", default_value_t = 10)]
    shutdown_drain_secs: u64,

    #[arg(long, env = "RELAY_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,

    /// Development only: enable `POST /v1/dev/workers/{id}/expire-leases`,
    /// which simulates a worker failure for the dashboard. Unauthenticated,
    /// so it is refused unless --bind is loopback.
    #[arg(long)]
    dev_controls: bool,
}

impl CoordinatorArgs {
    /// Check every setting and report *all* problems at once.
    fn validate(&self) -> Validation {
        let mut v = Validation::default();
        if self.max_active_jobs == 0 {
            v.error("--max-active-jobs must be at least 1");
        }
        if !(1..=3_600).contains(&self.lease_secs) {
            v.error("--lease-secs must be between 1 and 3600");
        }
        if self.retry_base_ms > self.retry_max_ms {
            v.error(format!(
                "--retry-base-ms ({}) must not exceed --retry-max-ms ({})",
                self.retry_base_ms, self.retry_max_ms
            ));
        }
        if self.retry_max_ms > 3_600_000 {
            v.error("--retry-max-ms must be at most 3600000 (1 hour)");
        }
        if !(1..=3_600).contains(&self.deadline_grace_secs) {
            v.error("--deadline-grace-secs must be between 1 and 3600");
        }
        if !(1..=300).contains(&self.request_timeout_secs) {
            v.error("--request-timeout-secs must be between 1 and 300");
        }
        if self.shutdown_drain_secs > 300 {
            v.error("--shutdown-drain-secs must be at most 300");
        }
        if self.request_timeout_secs >= self.lease_secs {
            v.warn(format!(
                "--request-timeout-secs ({}) >= --lease-secs ({}): a single slow heartbeat \
                 request could outlive the lease it is renewing",
                self.request_timeout_secs, self.lease_secs
            ));
        }
        if self.dev_controls && !self.bind.ip().is_loopback() {
            v.error(format!(
                "--dev-controls can disrupt running jobs and has no authentication; \
                 it requires a loopback --bind, not {}",
                self.bind
            ));
        }
        if !self.bind.ip().is_loopback() {
            v.warn(format!(
                "--bind {} is not loopback and RelayLLM has no authentication; \
                 only do this on a trusted private network (e.g. inside Docker)",
                self.bind
            ));
        }
        v
    }
}

pub async fn run(args: CoordinatorArgs) -> anyhow::Result<ExitCode> {
    init_tracing(args.log_format);
    args.validate().finish("coordinator")?;
    let lease = Duration::from_secs(args.lease_secs);

    let options = StoreOptions {
        max_active_jobs: args.max_active_jobs,
        retry: RetryPolicy {
            base: Duration::from_millis(args.retry_base_ms),
            max: Duration::from_millis(args.retry_max_ms),
        },
        deadline_grace: Duration::from_secs(args.deadline_grace_secs),
        durability: args.sqlite_synchronous.into(),
        ..StoreOptions::default()
    };
    // A relative path resolves against the current directory, so starting the
    // coordinator from another folder would silently create a fresh, empty
    // database: "all my jobs vanished after restart". Resolve it once and log
    // the absolute path and whether the file already existed.
    let db = std::path::absolute(&args.db)
        .with_context(|| format!("resolving database path {}", args.db.display()))?;
    let existed = db.try_exists().unwrap_or(false);
    let clock = Arc::new(MonotonicClock::new(SystemClock));
    let store = Store::open(&db, clock.clone(), options)
        .await
        .with_context(|| format!("opening database {}", db.display()))?;
    // Never stamp anything earlier than what a previous run already wrote,
    // even if the wall clock moved backwards while we were down.
    if let Some(latest) = store.latest_timestamp().await? {
        clock.raise_floor(latest);
    }

    // Refuse to run with weaker durability than configured.
    let expected: Durability = args.sqlite_synchronous.into();
    let (journal, synchronous) = store.durability().await?;
    if journal != "wal" || synchronous != expected.pragma_value() {
        anyhow::bail!(
            "database is journal_mode={journal}, synchronous={synchronous}; \
             expected wal/{} ({:?}). Is the file on a network drive?",
            expected.pragma_value(),
            expected
        );
    }
    if expected == Durability::Normal {
        warn!(
            "--sqlite-synchronous normal: acknowledged writes survive a coordinator crash \
             but the most recent ones can be lost on power loss or an OS crash"
        );
    }
    let counts = store
        .count_by_state()
        .await?
        .iter()
        .map(|(state, n)| format!("{state}={n}"))
        .collect::<Vec<_>>()
        .join(" ");
    info!(
        db = %db.display(),
        existing_file = existed,
        jobs = %if counts.is_empty() { "none".to_owned() } else { counts },
        "database opened"
    );

    // Startup recovery (§5.1 #5). Running jobs get one full lease from *now*
    // before the reaper may touch them: their workers couldn't heartbeat
    // while we were down, which says nothing about whether they are alive.
    let in_flight = store.grant_lease_grace(lease).await?;
    if in_flight > 0 {
        info!(
            running_jobs = in_flight,
            grace_secs = args.lease_secs,
            "recovered running jobs from the previous run; waiting one lease for their workers"
        );
    }
    let reaper = spawn_reaper(store.clone(), ReaperConfig::for_lease(lease));

    let listener = TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("binding {}", args.bind))?;
    info!(
        addr = %listener.local_addr()?,
        max_active_jobs = args.max_active_jobs,
        lease_secs = args.lease_secs,
        "coordinator listening"
    );

    let limits = Limits {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        ..Limits::default()
    };
    if args.dev_controls {
        warn!("--dev-controls: development failure controls are enabled");
    }
    let state = AppState::new(store.clone(), limits)
        .with_lease(lease)
        .with_dev_controls(args.dev_controls);
    let drain = Duration::from_secs(args.shutdown_drain_secs);
    let served = relay_server::serve(listener, state, shutdown_signal(), drain).await;
    reaper.abort();
    served?;
    // `close` waits for every pooled connection to come back, including one
    // stuck in SQLite's busy wait behind a request the drain deadline already
    // gave up on. Bound it too. Exiting with a statement in flight is safe:
    // SQLite writes are atomic.
    if tokio::time::timeout(Duration::from_secs(2), store.close())
        .await
        .is_err()
    {
        warn!("database connections still busy after shutdown; exiting anyway");
    }
    info!("coordinator stopped");
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct Wrapper {
        #[command(flatten)]
        args: CoordinatorArgs,
    }

    fn validate(argv: &[&str]) -> Validation {
        let mut full = vec!["relay"];
        full.extend_from_slice(argv);
        Wrapper::parse_from(full).args.validate()
    }

    #[test]
    fn defaults_are_valid_without_warnings() {
        let v = validate(&[]);
        assert!(v.errors.is_empty(), "{:?}", v.errors);
        assert!(v.warnings.is_empty(), "{:?}", v.warnings);
    }

    #[test]
    fn every_error_is_reported_at_once() {
        let v = validate(&[
            "--max-active-jobs",
            "0",
            "--lease-secs",
            "0",
            "--retry-base-ms",
            "5000",
            "--retry-max-ms",
            "100",
            "--request-timeout-secs",
            "0",
        ]);
        assert_eq!(v.errors.len(), 4, "{:#?}", v.errors);
        let joined = v.errors.join("\n");
        for flag in [
            "--max-active-jobs",
            "--lease-secs",
            "--retry-base-ms (5000)",
            "--request-timeout-secs",
        ] {
            assert!(joined.contains(flag), "missing {flag} in:\n{joined}");
        }
    }

    #[test]
    fn risky_but_legal_settings_warn() {
        let v = validate(&[
            "--bind",
            "0.0.0.0:7070",
            "--lease-secs",
            "10",
            "--request-timeout-secs",
            "30",
        ]);
        assert!(v.errors.is_empty());
        assert_eq!(v.warnings.len(), 2, "{:#?}", v.warnings);
    }

    #[test]
    fn dev_controls_require_a_loopback_bind() {
        assert!(validate(&["--dev-controls"]).errors.is_empty());
        let v = validate(&["--dev-controls", "--bind", "0.0.0.0:7070"]);
        assert_eq!(v.errors.len(), 1, "{:#?}", v.errors);
        assert!(v.errors[0].contains("--dev-controls"));
    }
}
