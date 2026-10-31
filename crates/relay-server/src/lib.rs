//! The RelayLLM coordinator: durable job store plus HTTP API.

pub mod api;
mod dashboard_api;
pub mod decider;
pub mod error;
pub mod events;
mod metrics;
mod observe;
pub mod reaper;
pub mod store;
mod worker_api;
mod workflow_api;

pub use api::{AppState, DEFAULT_LEASE, Limits, router, serve};
pub use decider::{DeciderConfig, spawn_decider};
pub use events::EventBus;
pub use reaper::{ReaperConfig, spawn_reaper};
pub use store::{
    CANCELLED_MESSAGE, CancelOutcome, CancelWorkflowOutcome, DecidePass, Durability,
    MetricsSnapshot, ReapedJob, ReportOutcome, SignalOutcome, StartOutcome, Store, StoreError,
    StoreOptions, SubmitOutcome, WorkflowRun,
};
