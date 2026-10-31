//! Starting a run: the request, its validation, and run identity/state.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::submission::canonical_sha256;
use crate::workflow::definition::{DefinitionError, Workflow, WorkflowDefinition, WorkflowLimits};

/// Workflow run id. UUIDv7, like [`crate::job::JobId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowId(Uuid);

impl WorkflowId {
    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.hyphenated().fmt(f)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid workflow id {0:?}: expected a UUID")]
pub struct ParseWorkflowIdError(String);

impl FromStr for WorkflowId {
    type Err = ParseWorkflowIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ParseWorkflowIdError(s.to_owned()))
    }
}

/// The `workflow_runs.state` column: a cache of the terminal event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl WorkflowState {
    pub const ALL: [WorkflowState; 4] = [
        WorkflowState::Running,
        WorkflowState::Succeeded,
        WorkflowState::Failed,
        WorkflowState::Cancelled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            WorkflowState::Running => "running",
            WorkflowState::Succeeded => "succeeded",
            WorkflowState::Failed => "failed",
            WorkflowState::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        self != WorkflowState::Running
    }
}

impl fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("unknown workflow state {0:?}")]
pub struct ParseWorkflowStateError(String);

impl FromStr for WorkflowState {
    type Err = ParseWorkflowStateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|state| state.as_str() == s)
            .ok_or_else(|| ParseWorkflowStateError(s.to_owned()))
    }
}

/// Body of `POST /v1/workflows`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartWorkflowRequest {
    pub definition: WorkflowDefinition,
    /// Available to activities as the `input` address.
    #[serde(default)]
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// A start request that passed validation.
#[derive(Debug, Clone, PartialEq)]
pub struct NewWorkflow {
    pub workflow: Workflow,
    /// Stored verbatim; re-validated into [`Workflow`] whenever it's loaded.
    pub definition: WorkflowDefinition,
    pub input: Value,
    pub idempotency_key: Option<String>,
    /// SHA-256 of the canonical `{definition, input}`, excluding the key.
    pub request_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Definition(#[from] DefinitionError),
    #[error("input is {actual} bytes; the limit is {max}")]
    InputTooLarge { actual: usize, max: usize },
    #[error("idempotency_key must be 1..={max} printable ASCII characters without spaces")]
    InvalidIdempotencyKey { max: usize },
}

impl StartWorkflowRequest {
    pub fn validate(self, limits: &WorkflowLimits) -> Result<NewWorkflow, StartError> {
        let workflow = Workflow::from_definition(&self.definition, limits)?;
        // The input is copied into activity payloads, so the payload limit
        // is the natural bound.
        let input_bytes = self.input.to_string().len();
        let max = limits.submission.max_payload_bytes;
        if input_bytes > max {
            return Err(StartError::InputTooLarge {
                actual: input_bytes,
                max,
            });
        }
        if let Some(key) = &self.idempotency_key {
            let max = limits.submission.max_idempotency_key_len;
            let ok =
                !key.is_empty() && key.len() <= max && key.bytes().all(|b| b.is_ascii_graphic());
            if !ok {
                return Err(StartError::InvalidIdempotencyKey { max });
            }
        }
        let request_fingerprint =
            canonical_sha256(&json!({ "definition": self.definition, "input": self.input }));
        Ok(NewWorkflow {
            workflow,
            definition: self.definition,
            input: self.input,
            idempotency_key: self.idempotency_key,
            request_fingerprint,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(input: Value) -> StartWorkflowRequest {
        serde_json::from_value(json!({
            "definition": { "name": "w", "steps": [{ "name": "s", "sleep_ms": 5 }] },
            "input": input,
        }))
        .unwrap()
    }

    #[test]
    fn fingerprint_covers_definition_and_input_but_not_key_or_key_order() {
        let limits = WorkflowLimits::default();
        let a = request(json!({ "x": 1, "y": 2 }))
            .validate(&limits)
            .unwrap();
        let b = request(json!({ "y": 2, "x": 1 }))
            .validate(&limits)
            .unwrap();
        assert_eq!(a.request_fingerprint, b.request_fingerprint);

        let c = request(json!({ "x": 1, "y": 3 }))
            .validate(&limits)
            .unwrap();
        assert_ne!(a.request_fingerprint, c.request_fingerprint);

        let mut keyed = request(json!({ "x": 1, "y": 2 }));
        keyed.idempotency_key = Some("k".into());
        assert_eq!(
            keyed.validate(&limits).unwrap().request_fingerprint,
            a.request_fingerprint
        );

        let mut other_def = request(json!({ "x": 1, "y": 2 }));
        other_def.definition.steps[0].sleep_ms = Some(6);
        assert_ne!(
            other_def.validate(&limits).unwrap().request_fingerprint,
            a.request_fingerprint
        );
    }

    #[test]
    fn start_requests_are_validated() {
        let limits = WorkflowLimits::default();
        assert_eq!(
            request(Value::Null).validate(&limits).unwrap().input,
            Value::Null
        );

        let big = request(json!("x".repeat(70_000)));
        assert!(matches!(
            big.validate(&limits),
            Err(StartError::InputTooLarge { .. })
        ));

        let mut bad_key = request(Value::Null);
        bad_key.idempotency_key = Some("has space".into());
        assert!(matches!(
            bad_key.validate(&limits),
            Err(StartError::InvalidIdempotencyKey { .. })
        ));

        let mut bad_def = request(Value::Null);
        bad_def.definition.steps.clear();
        assert_eq!(
            bad_def.validate(&limits),
            Err(StartError::Definition(DefinitionError::NoSteps))
        );

        assert!(
            serde_json::from_value::<StartWorkflowRequest>(json!({
            "definition": { "name": "w", "steps": [] }, "extra": 1 }))
            .is_err()
        );
    }

    #[test]
    fn ids_and_states_round_trip() {
        let id = WorkflowId::generate();
        assert_eq!(id.to_string().parse::<WorkflowId>().unwrap(), id);
        assert!("nope".parse::<WorkflowId>().is_err());
        for s in WorkflowState::ALL {
            assert_eq!(s.as_str().parse::<WorkflowState>().unwrap(), s);
            assert_eq!(serde_json::to_value(s).unwrap(), s.as_str());
        }
        assert!(!WorkflowState::Running.is_terminal());
        assert!(WorkflowState::Cancelled.is_terminal());
    }
}
