//! Domain model shared by the RelayLLM coordinator, workers and CLI.
//!
//! This crate performs no I/O. Everything here is deterministic and unit
//! testable: job identity, the state machine, submission validation, request
//! fingerprints and the clock abstraction.

pub mod api;
pub mod clock;
pub mod events;
pub mod job;
pub mod retry;
pub mod submission;
pub mod worker;
pub mod workflow;
