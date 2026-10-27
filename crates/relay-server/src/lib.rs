//! The RelayLLM coordinator: durable job store plus HTTP API.

pub mod api;
mod dashboard_api;
pub mod error;
pub mod events;
mod metrics;
mod observe;
pub mod reaper;
pub mod store;
mod worker_api;

pub use api::{AppState, DEFAULT_LEASE, Limits, router, serve};
pub use events::EventBus;
pub use reaper::{ReaperConfig, spawn_reaper};
pub use store::{
    CANCELLED_MESSAGE, CancelOutcome, Durability, MetricsSnapshot, ReapedJob, ReportOutcome, Store,
    StoreError, StoreOptions, SubmitOutcome,
};
