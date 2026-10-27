//! Validation of job submissions and idempotency fingerprints.

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::api::SubmitJobRequest;

/// Limits applied to every submission. Configurable on the coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionLimits {
    pub max_kind_len: usize,
    pub max_idempotency_key_len: usize,
    /// Size of the payload re-serialized as compact JSON.
    pub max_payload_bytes: usize,
    pub default_max_attempts: u32,
    pub max_max_attempts: u32,
    pub default_timeout_secs: u64,
    pub max_timeout_secs: u64,
}

impl Default for SubmissionLimits {
    fn default() -> Self {
        Self {
            max_kind_len: 64,
            max_idempotency_key_len: 128,
            max_payload_bytes: 64 * 1024,
            default_max_attempts: 3,
            max_max_attempts: 20,
            default_timeout_secs: 60,
            max_timeout_secs: 3_600,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    #[error("kind must be 1..={max} characters from [a-z0-9_.-]")]
    InvalidKind { max: usize },
    #[error("payload must be a JSON object")]
    PayloadNotObject,
    #[error("payload is {actual} bytes; the limit is {max}")]
    PayloadTooLarge { actual: usize, max: usize },
    #[error("idempotency_key must be 1..={max} printable ASCII characters without spaces")]
    InvalidIdempotencyKey { max: usize },
    #[error("max_attempts must be between 1 and {max}")]
    InvalidMaxAttempts { max: u32 },
    #[error("timeout_secs must be between 1 and {max}")]
    InvalidTimeout { max: u64 },
}

/// Job kinds: 1..=`max_len` characters from `[a-z0-9_.-]`.
pub fn is_valid_kind(kind: &str, max_len: usize) -> bool {
    !kind.is_empty()
        && kind.len() <= max_len
        && kind
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_.-".contains(&b))
}

/// A submission that passed validation, with defaults resolved.
#[derive(Debug, Clone, PartialEq)]
pub struct NewJob {
    pub kind: String,
    pub payload: Value,
    pub request_fingerprint: String,
    pub idempotency_key: Option<String>,
    pub max_attempts: u32,
    pub timeout_ms: u64,
}

impl SubmitJobRequest {
    pub fn validate(self, limits: &SubmissionLimits) -> Result<NewJob, ValidationError> {
        if !is_valid_kind(&self.kind, limits.max_kind_len) {
            return Err(ValidationError::InvalidKind {
                max: limits.max_kind_len,
            });
        }

        if !self.payload.is_object() {
            return Err(ValidationError::PayloadNotObject);
        }
        let payload_bytes = self.payload.to_string().len();
        if payload_bytes > limits.max_payload_bytes {
            return Err(ValidationError::PayloadTooLarge {
                actual: payload_bytes,
                max: limits.max_payload_bytes,
            });
        }

        if let Some(key) = &self.idempotency_key {
            let key_ok = !key.is_empty()
                && key.len() <= limits.max_idempotency_key_len
                && key.bytes().all(|b| b.is_ascii_graphic());
            if !key_ok {
                return Err(ValidationError::InvalidIdempotencyKey {
                    max: limits.max_idempotency_key_len,
                });
            }
        }

        let max_attempts = self.max_attempts.unwrap_or(limits.default_max_attempts);
        if !(1..=limits.max_max_attempts).contains(&max_attempts) {
            return Err(ValidationError::InvalidMaxAttempts {
                max: limits.max_max_attempts,
            });
        }

        let timeout_secs = self.timeout_secs.unwrap_or(limits.default_timeout_secs);
        if !(1..=limits.max_timeout_secs).contains(&timeout_secs) {
            return Err(ValidationError::InvalidTimeout {
                max: limits.max_timeout_secs,
            });
        }

        let request_fingerprint = fingerprint(&self);
        Ok(NewJob {
            kind: self.kind,
            payload: self.payload,
            request_fingerprint,
            idempotency_key: self.idempotency_key,
            max_attempts,
            timeout_ms: timeout_secs.saturating_mul(1_000),
        })
    }
}

/// SHA-256 (hex) of the request *as sent*, excluding the idempotency key.
///
/// Object keys are sorted recursively, so `{"a":1,"b":2}` and `{"b":2,"a":1}`
/// are the same request. Server-side defaults are deliberately not resolved
/// first: changing a default must not make a client's replay "conflict".
pub fn fingerprint(request: &SubmitJobRequest) -> String {
    let canonical = canonicalize(&json!({
        "kind": request.kind,
        "payload": request.payload,
        "max_attempts": request.max_attempts,
        "timeout_secs": request.timeout_secs,
    }));
    let digest = Sha256::digest(canonical.to_string().as_bytes());
    format!("{digest:x}")
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let mut sorted = Map::new();
            for (k, v) in entries {
                sorted.insert(k.clone(), canonicalize(v));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(payload: Value) -> SubmitJobRequest {
        SubmitJobRequest {
            kind: "mock".into(),
            payload,
            idempotency_key: None,
            max_attempts: None,
            timeout_secs: None,
        }
    }

    #[test]
    fn defaults_are_applied() {
        let job = request(json!({"prompt": "hi"}))
            .validate(&SubmissionLimits::default())
            .unwrap();
        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.timeout_ms, 60_000);
        assert_eq!(job.request_fingerprint.len(), 64);
    }

    #[test]
    fn rejects_bad_kind() {
        let limits = SubmissionLimits::default();
        let too_long = "x".repeat(65);
        for kind in ["", "Mock", "has space", "emoji🙂", too_long.as_str()] {
            let mut req = request(json!({}));
            req.kind = kind.to_owned();
            assert_eq!(
                req.validate(&limits),
                Err(ValidationError::InvalidKind { max: 64 }),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn rejects_non_object_and_oversized_payloads() {
        let limits = SubmissionLimits {
            max_payload_bytes: 20,
            ..SubmissionLimits::default()
        };
        assert_eq!(
            request(json!("just a string")).validate(&limits),
            Err(ValidationError::PayloadNotObject)
        );
        assert!(matches!(
            request(json!({"prompt": "this is far too long"})).validate(&limits),
            Err(ValidationError::PayloadTooLarge { max: 20, .. })
        ));
    }

    #[test]
    fn rejects_bad_idempotency_keys_and_ranges() {
        let limits = SubmissionLimits::default();
        let mut req = request(json!({}));
        req.idempotency_key = Some("has space".into());
        assert!(matches!(
            req.validate(&limits),
            Err(ValidationError::InvalidIdempotencyKey { .. })
        ));

        let mut req = request(json!({}));
        req.max_attempts = Some(0);
        assert!(matches!(
            req.validate(&limits),
            Err(ValidationError::InvalidMaxAttempts { .. })
        ));

        let mut req = request(json!({}));
        req.timeout_secs = Some(3_601);
        assert!(matches!(
            req.validate(&limits),
            Err(ValidationError::InvalidTimeout { .. })
        ));
    }

    #[test]
    fn fingerprint_ignores_key_order_but_not_values() {
        let a = request(json!({"prompt": "hi", "opts": {"x": 1, "y": [1, {"b": 2, "a": 1}]}}));
        let b = request(json!({"opts": {"y": [1, {"a": 1, "b": 2}], "x": 1}, "prompt": "hi"}));
        assert_eq!(fingerprint(&a), fingerprint(&b));

        let c = request(json!({"prompt": "hi!", "opts": {"x": 1, "y": [1, {"b": 2, "a": 1}]}}));
        assert_ne!(fingerprint(&a), fingerprint(&c));
    }

    #[test]
    fn fingerprint_covers_options_but_not_the_key_itself() {
        let base = request(json!({"prompt": "hi"}));

        let mut with_attempts = base.clone();
        with_attempts.max_attempts = Some(5);
        assert_ne!(fingerprint(&base), fingerprint(&with_attempts));

        let mut with_key = base.clone();
        with_key.idempotency_key = Some("k1".into());
        assert_eq!(fingerprint(&base), fingerprint(&with_key));
    }
}
