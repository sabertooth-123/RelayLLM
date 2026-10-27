//! Validation for the worker-facing API: registration, claims and reports.

use serde_json::Value;

use crate::api::{ClaimRequest, CompleteRequest, FailRequest, RegisterWorkerRequest};
use crate::submission::is_valid_kind;

pub const MAX_WORKER_ID_LEN: usize = 128;
pub const MAX_CLAIM_ID_LEN: usize = 128;
pub const MAX_WORKER_NAME_LEN: usize = 64;
pub const MAX_WORKER_CONCURRENCY: u32 = 1_024;
pub const MAX_WORKER_KINDS: usize = 32;
/// Longer failure messages are truncated, not rejected: a worker that
/// reports a huge stack trace must still be able to fail the job.
pub const MAX_ERROR_MESSAGE_LEN: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkerValidationError {
    #[error("worker id must be 1..={MAX_WORKER_ID_LEN} characters from [A-Za-z0-9_.-]")]
    InvalidWorkerId,
    #[error("worker name must be 1..={MAX_WORKER_NAME_LEN} characters from [A-Za-z0-9_.-]")]
    InvalidName,
    #[error("concurrency must be between 1 and {MAX_WORKER_CONCURRENCY}")]
    InvalidConcurrency,
    #[error("kinds must list 1..={MAX_WORKER_KINDS} distinct valid job kinds")]
    InvalidKinds,
    #[error("claim_id must be 1..={MAX_CLAIM_ID_LEN} characters from [A-Za-z0-9_.-]")]
    InvalidClaimId,
    #[error("attempt must be a positive integer")]
    InvalidAttempt,
    #[error("result is {actual} bytes; the limit is {max}")]
    ResultTooLarge { actual: usize, max: usize },
}

/// Ids that are safe in a URL path segment without escaping.
fn is_path_safe_id(id: &str, max_len: usize) -> bool {
    !id.is_empty()
        && id.len() <= max_len
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

pub fn validate_worker_id(id: &str) -> Result<(), WorkerValidationError> {
    if is_path_safe_id(id, MAX_WORKER_ID_LEN) {
        Ok(())
    } else {
        Err(WorkerValidationError::InvalidWorkerId)
    }
}

pub fn parse_attempt(raw: &str) -> Result<u32, WorkerValidationError> {
    match raw.parse::<u32>() {
        Ok(attempt) if attempt >= 1 => Ok(attempt),
        _ => Err(WorkerValidationError::InvalidAttempt),
    }
}

impl RegisterWorkerRequest {
    pub fn validate(&self, max_kind_len: usize) -> Result<(), WorkerValidationError> {
        if !is_path_safe_id(&self.name, MAX_WORKER_NAME_LEN) {
            return Err(WorkerValidationError::InvalidName);
        }
        if !(1..=MAX_WORKER_CONCURRENCY).contains(&self.concurrency) {
            return Err(WorkerValidationError::InvalidConcurrency);
        }
        let mut seen = std::collections::HashSet::new();
        let kinds_ok = !self.kinds.is_empty()
            && self.kinds.len() <= MAX_WORKER_KINDS
            && self
                .kinds
                .iter()
                .all(|k| is_valid_kind(k, max_kind_len) && seen.insert(k.as_str()));
        if !kinds_ok {
            return Err(WorkerValidationError::InvalidKinds);
        }
        Ok(())
    }
}

impl ClaimRequest {
    pub fn validate(&self) -> Result<(), WorkerValidationError> {
        if is_path_safe_id(&self.claim_id, MAX_CLAIM_ID_LEN) {
            Ok(())
        } else {
            Err(WorkerValidationError::InvalidClaimId)
        }
    }
}

impl CompleteRequest {
    pub fn validate(&self, max_result_bytes: usize) -> Result<(), WorkerValidationError> {
        validate_worker_id(&self.worker_id)?;
        let actual = json_len(&self.result);
        if actual > max_result_bytes {
            return Err(WorkerValidationError::ResultTooLarge {
                actual,
                max: max_result_bytes,
            });
        }
        Ok(())
    }
}

impl FailRequest {
    /// Validates the worker id and truncates the message to
    /// [`MAX_ERROR_MESSAGE_LEN`] bytes on a character boundary.
    pub fn validate(mut self) -> Result<Self, WorkerValidationError> {
        validate_worker_id(&self.worker_id)?;
        if self.error.len() > MAX_ERROR_MESSAGE_LEN {
            let mut cut = MAX_ERROR_MESSAGE_LEN;
            while !self.error.is_char_boundary(cut) {
                cut -= 1;
            }
            self.error.truncate(cut);
        }
        Ok(self)
    }
}

fn json_len(value: &Value) -> usize {
    value.to_string().len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn registration(kinds: &[&str]) -> RegisterWorkerRequest {
        RegisterWorkerRequest {
            name: "w1".into(),
            concurrency: 4,
            kinds: kinds.iter().map(|k| (*k).to_owned()).collect(),
        }
    }

    #[test]
    fn registration_rules() {
        assert_eq!(registration(&["mock"]).validate(64), Ok(()));
        assert_eq!(
            registration(&[]).validate(64),
            Err(WorkerValidationError::InvalidKinds)
        );
        assert_eq!(
            registration(&["mock", "mock"]).validate(64),
            Err(WorkerValidationError::InvalidKinds)
        );
        assert_eq!(
            registration(&["Bad"]).validate(64),
            Err(WorkerValidationError::InvalidKinds)
        );
        let mut bad = registration(&["mock"]);
        bad.concurrency = 0;
        assert_eq!(
            bad.validate(64),
            Err(WorkerValidationError::InvalidConcurrency)
        );
        bad.concurrency = 1;
        bad.name = "has space".into();
        assert_eq!(bad.validate(64), Err(WorkerValidationError::InvalidName));
    }

    #[test]
    fn ids_must_be_path_safe() {
        assert!(validate_worker_id("w1-0193.a_b").is_ok());
        let too_long = "x".repeat(129);
        for bad in ["", "a/b", "a b", "a%2Fb", too_long.as_str()] {
            assert!(validate_worker_id(bad).is_err(), "{bad:?}");
        }
        assert!(
            ClaimRequest {
                claim_id: "c-1".into()
            }
            .validate()
            .is_ok()
        );
        assert!(
            ClaimRequest {
                claim_id: "c?1".into()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn attempts_are_positive() {
        assert_eq!(parse_attempt("3"), Ok(3));
        assert!(parse_attempt("0").is_err());
        assert!(parse_attempt("-1").is_err());
        assert!(parse_attempt("x").is_err());
    }

    #[test]
    fn long_error_messages_are_truncated_on_a_char_boundary() {
        let request = FailRequest {
            worker_id: "w".into(),
            error: "é".repeat(MAX_ERROR_MESSAGE_LEN),
            retryable: true,
        }
        .validate()
        .unwrap();
        assert!(request.error.len() <= MAX_ERROR_MESSAGE_LEN);
        assert!(request.error.chars().all(|c| c == 'é'));
    }

    #[test]
    fn oversized_results_are_rejected() {
        let request = CompleteRequest {
            worker_id: "w".into(),
            result: json!({"text": "x".repeat(100)}),
        };
        assert!(matches!(
            request.validate(50),
            Err(WorkerValidationError::ResultTooLarge { max: 50, .. })
        ));
        assert!(request.validate(1_000).is_ok());
    }
}
