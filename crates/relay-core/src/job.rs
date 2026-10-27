//! Job identity, lifecycle states and the state-transition table.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::api::{JobResultView, JobStatusView};
use crate::clock::Timestamp;
use crate::events::JobSummary;

/// Unique job identifier. UUIDv7, so ids sort roughly by creation time and
/// don't reveal how many jobs exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(f)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid job id {0:?}: expected a UUID")]
pub struct ParseJobIdError(String);

impl FromStr for JobId {
    type Err = ParseJobIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ParseJobIdError(s.to_owned()))
    }
}

/// Lifecycle state of a job. See `ARCHITECTURE.md` §2 for the diagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Accepted and waiting for its first claim.
    Queued,
    /// Claimed by a worker that holds an unexpired lease.
    Running,
    /// A previous attempt failed or was abandoned; eligible again after `available_at`.
    RetryWaiting,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub const ALL: [JobState; 6] = [
        JobState::Queued,
        JobState::Running,
        JobState::RetryWaiting,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Cancelled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::RetryWaiting => "retry_waiting",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }

    /// Terminal states are absorbing: no transition leaves them.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Succeeded | JobState::Failed | JobState::Cancelled
        )
    }

    /// Active jobs count against queue capacity.
    pub fn is_active(self) -> bool {
        !self.is_terminal()
    }

    /// The single source of truth for legal transitions.
    pub fn can_transition_to(self, to: JobState) -> bool {
        use JobState::*;
        matches!(
            (self, to),
            (Queued, Running)
                | (Queued, Cancelled)
                | (RetryWaiting, Running)
                | (RetryWaiting, Cancelled)
                | (Running, Succeeded)
                | (Running, RetryWaiting)
                | (Running, Failed)
                | (Running, Cancelled)
        )
    }

    pub fn transition_to(self, to: JobState) -> Result<JobState, InvalidTransition> {
        if self.can_transition_to(to) {
            Ok(to)
        } else {
            Err(InvalidTransition { from: self, to })
        }
    }

    /// States from which `to` may be entered, in a stable order. Store
    /// operations use this to build their `WHERE state IN (...)` guards.
    pub fn predecessors(to: JobState) -> Vec<JobState> {
        Self::ALL
            .into_iter()
            .filter(|from| from.can_transition_to(to))
            .collect()
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("unknown job state {0:?}")]
pub struct ParseJobStateError(String);

impl FromStr for JobState {
    type Err = ParseJobStateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|state| state.as_str() == s)
            .ok_or_else(|| ParseJobStateError(s.to_owned()))
    }
}

/// How one attempt ended. Recorded in `job_attempts` in the same transaction
/// as the job's state change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    Succeeded,
    RetryableError,
    PermanentError,
    LeaseExpired,
    Cancelled,
}

impl AttemptOutcome {
    pub const ALL: [AttemptOutcome; 5] = [
        AttemptOutcome::Succeeded,
        AttemptOutcome::RetryableError,
        AttemptOutcome::PermanentError,
        AttemptOutcome::LeaseExpired,
        AttemptOutcome::Cancelled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            AttemptOutcome::Succeeded => "succeeded",
            AttemptOutcome::RetryableError => "retryable_error",
            AttemptOutcome::PermanentError => "permanent_error",
            AttemptOutcome::LeaseExpired => "lease_expired",
            AttemptOutcome::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for AttemptOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("unknown attempt outcome {0:?}")]
pub struct ParseAttemptOutcomeError(String);

impl FromStr for AttemptOutcome {
    type Err = ParseAttemptOutcomeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|outcome| outcome.as_str() == s)
            .ok_or_else(|| ParseAttemptOutcomeError(s.to_owned()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid job state transition {from} -> {to}")]
pub struct InvalidTransition {
    pub from: JobState,
    pub to: JobState,
}

/// A job as persisted by the coordinator.
#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    pub id: JobId,
    pub kind: String,
    pub payload: Value,
    /// SHA-256 of the canonical submission; see [`crate::submission`].
    pub request_fingerprint: String,
    pub idempotency_key: Option<String>,
    pub state: JobState,
    /// Number of times this job has been claimed. Doubles as the fencing token
    /// of the current attempt.
    pub attempt: u32,
    pub max_attempts: u32,
    pub timeout_ms: u64,
    /// Earliest time the job may be claimed (backoff for retries).
    pub available_at: Timestamp,
    pub worker_id: Option<String>,
    pub lease_expires_at: Option<Timestamp>,
    /// Absolute end of the current attempt: claim time + timeout + grace.
    /// Heartbeats can't extend the lease past it. Set iff `running`.
    pub attempt_deadline: Option<Timestamp>,
    pub result: Option<Value>,
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub finished_at: Option<Timestamp>,
}

impl Job {
    /// Public status view. Never includes the payload or result, so status
    /// polling doesn't leak prompt contents into logs or terminals.
    pub fn status_view(&self) -> JobStatusView {
        JobStatusView {
            id: self.id,
            kind: self.kind.clone(),
            state: self.state,
            attempt: self.attempt,
            max_attempts: self.max_attempts,
            idempotency_key: self.idempotency_key.clone(),
            worker_id: self.worker_id.clone(),
            last_error: self.last_error.clone(),
            created_at_ms: self.created_at,
            updated_at_ms: self.updated_at,
            finished_at_ms: self.finished_at,
        }
    }

    /// Summary for broadcast (dashboard events and snapshots). Carries no
    /// payload, no result and not the idempotency key itself, only whether
    /// one exists. Error text is cut to 300 characters.
    pub fn summary(&self) -> JobSummary {
        JobSummary {
            id: self.id,
            kind: self.kind.clone(),
            state: self.state,
            attempt: self.attempt,
            max_attempts: self.max_attempts,
            worker_id: self.worker_id.clone(),
            has_idempotency_key: self.idempotency_key.is_some(),
            last_error: self
                .last_error
                .as_ref()
                .map(|e| e.chars().take(300).collect()),
            created_at_ms: self.created_at,
            updated_at_ms: self.updated_at,
            finished_at_ms: self.finished_at,
            available_at_ms: self.available_at,
            lease_expires_at_ms: self.lease_expires_at,
        }
    }

    pub fn result_view(&self) -> JobResultView {
        JobResultView {
            id: self.id,
            state: self.state,
            result: self.result.clone(),
            error: match self.state {
                JobState::Failed | JobState::Cancelled => self.last_error.clone(),
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use JobState::*;

    #[test]
    fn terminal_states_are_absorbing() {
        for from in JobState::ALL.into_iter().filter(|s| s.is_terminal()) {
            for to in JobState::ALL {
                assert!(
                    !from.can_transition_to(to),
                    "{from} -> {to} must be illegal"
                );
            }
        }
    }

    #[test]
    fn happy_path_and_retry_path_are_legal() {
        assert_eq!(Queued.transition_to(Running), Ok(Running));
        assert_eq!(Running.transition_to(Succeeded), Ok(Succeeded));
        assert_eq!(Running.transition_to(RetryWaiting), Ok(RetryWaiting));
        assert_eq!(RetryWaiting.transition_to(Running), Ok(Running));
        assert_eq!(Running.transition_to(Failed), Ok(Failed));
    }

    #[test]
    fn shortcuts_that_skip_execution_are_illegal() {
        assert!(Queued.transition_to(Succeeded).is_err());
        assert!(Queued.transition_to(Failed).is_err());
        assert!(RetryWaiting.transition_to(Succeeded).is_err());
        assert!(Queued.transition_to(RetryWaiting).is_err());
        // Running -> Running is a lease renewal, not a state transition.
        assert!(Running.transition_to(Running).is_err());
    }

    #[test]
    fn cancellation_is_allowed_only_from_active_states() {
        assert_eq!(
            JobState::predecessors(Cancelled),
            vec![Queued, Running, RetryWaiting]
        );
        assert_eq!(
            Succeeded.transition_to(Cancelled),
            Err(InvalidTransition {
                from: Succeeded,
                to: Cancelled
            })
        );
    }

    #[test]
    fn claim_predecessors_are_queued_and_retry_waiting() {
        assert_eq!(JobState::predecessors(Running), vec![Queued, RetryWaiting]);
        assert_eq!(JobState::predecessors(Succeeded), vec![Running]);
        assert!(JobState::predecessors(Queued).is_empty());
    }

    #[test]
    fn state_strings_round_trip_through_str_and_serde() {
        for state in JobState::ALL {
            assert_eq!(state.as_str().parse::<JobState>().unwrap(), state);
            let json = serde_json::to_string(&state).unwrap();
            assert_eq!(json, format!("\"{}\"", state.as_str()));
        }
        assert!("RUNNING".parse::<JobState>().is_err());
    }

    #[test]
    fn job_id_round_trips_and_rejects_garbage() {
        let id = JobId::generate();
        assert_eq!(id.to_string().parse::<JobId>().unwrap(), id);
        assert!("not-a-uuid".parse::<JobId>().is_err());
    }

    #[test]
    fn job_ids_sort_by_creation_order() {
        let a = JobId::generate();
        let b = JobId::generate();
        assert!(a < b);
    }
}
