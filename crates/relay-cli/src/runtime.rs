//! Process plumbing shared by long-running commands (coordinator, worker).

use clap::ValueEnum;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

pub fn init_tracing(format: LogFormat) {
    let filter = EnvFilter::try_from_env("RELAY_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    // Colour only on a terminal: in `docker logs` or a log file the escape
    // codes are noise.
    let ansi = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(ansi);
    match format {
        LogFormat::Text => builder.init(),
        LogFormat::Json => builder.json().init(),
    }
}

/// Configuration problems, collected so they can all be reported at once:
/// a bad config is fixed in one round trip, not five. Warnings are legal
/// but probably mistakes.
#[derive(Debug, Default)]
pub(crate) struct Validation {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl Validation {
    pub fn error(&mut self, message: impl Into<String>) {
        self.errors.push(message.into());
    }

    pub fn warn(&mut self, message: impl Into<String>) {
        self.warnings.push(message.into());
    }

    /// Log the warnings; fail with every error listed.
    pub fn finish(self, what: &str) -> anyhow::Result<()> {
        for warning in &self.warnings {
            tracing::warn!("{warning}");
        }
        if self.errors.is_empty() {
            return Ok(());
        }
        let list = self
            .errors
            .iter()
            .map(|e| format!("  - {e}"))
            .collect::<Vec<_>>()
            .join("\n");
        anyhow::bail!("invalid {what} configuration:\n{list}")
    }
}

/// Resolves on Ctrl-C, or on SIGTERM on Unix, which is what `docker stop` and
/// Kubernetes send. Without SIGTERM handling the container would be killed
/// after the stop timeout with no drain.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            // Without a signal handler we can't shut down gracefully; keep
            // running rather than exiting immediately.
            error!(error = %err, "could not install Ctrl-C handler");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(err) => {
                error!(error = %err, "could not install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => info!("shutdown requested (Ctrl-C)"),
        () = terminate => info!("shutdown requested (SIGTERM)"),
    }
}
