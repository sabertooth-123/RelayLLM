//! Workflow definitions: the JSON wire format and its validated form.
//!
//! The wire types reject unknown fields, so a typo such as `"inptus"` is an
//! error instead of a silently ignored key. [`Workflow::from_definition`]
//! turns a wire definition into a [`Workflow`] whose invariants the decider
//! relies on (docs/WORKFLOWS_DESIGN.md §2.1). Nothing here does I/O.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::api::SubmitJobRequest;
use crate::submission::{SubmissionLimits, ValidationError};

/// The reserved activity address for the run's own input.
pub const INPUT_ADDRESS: &str = "input";

/// The payload key the decider fills from `inputs`; reserved in definitions.
pub const INPUTS_PAYLOAD_KEY: &str = "inputs";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowLimits {
    /// Size of the definition re-serialized as compact JSON.
    pub max_definition_bytes: usize,
    pub max_steps: usize,
    pub max_branches: usize,
    pub max_sleep_ms: u64,
    pub max_name_len: usize,
    /// Each activity's job is validated against these.
    pub submission: SubmissionLimits,
}

impl Default for WorkflowLimits {
    fn default() -> Self {
        Self {
            max_definition_bytes: 64 * 1024,
            max_steps: 64,
            max_branches: 16,
            max_sleep_ms: 7 * 24 * 60 * 60 * 1_000,
            max_name_len: 64,
            submission: SubmissionLimits::default(),
        }
    }
}

// ---- wire format ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowDefinition {
    pub name: String,
    pub steps: Vec<StepDefinition>,
}

/// Exactly one of `activity`, `parallel`, `sleep_ms` and `wait_signal`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<ActivityDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel: Option<Vec<BranchDefinition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_signal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityDefinition {
    pub kind: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// One activity of a `parallel` step. The same fields as
/// [`ActivityDefinition`] plus a `name` (no `flatten`: it can't be combined
/// with `deny_unknown_fields`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchDefinition {
    pub name: String,
    pub kind: String,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

// ---- validated form --------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Workflow {
    pub name: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub name: String,
    pub body: StepBody,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StepBody {
    Activity(Activity),
    /// All branches are scheduled together; the step is done when all succeed.
    Parallel(Vec<Activity>),
    Sleep {
        ms: u64,
    },
    /// One-shot latch: done once a signal with this name was received.
    WaitSignal {
        signal: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Activity {
    /// `step` for an activity step, `step.branch` inside `parallel`.
    pub address: String,
    pub kind: String,
    pub payload: Map<String, Value>,
    /// Earlier activity addresses (or [`INPUT_ADDRESS`]) whose results are
    /// merged into the payload under [`INPUTS_PAYLOAD_KEY`].
    pub inputs: Vec<String>,
    pub max_attempts: Option<u32>,
    pub timeout_secs: Option<u64>,
}

impl Activity {
    /// The job request with `payload` as given; the decider adds `inputs`.
    pub fn job_request(&self, payload: Map<String, Value>) -> SubmitJobRequest {
        SubmitJobRequest {
            kind: self.kind.clone(),
            payload: Value::Object(payload),
            idempotency_key: None,
            max_attempts: self.max_attempts,
            timeout_secs: self.timeout_secs,
        }
    }
}

impl Step {
    /// The step's activities, in definition order (empty for timers/signals).
    pub fn activities(&self) -> &[Activity] {
        match &self.body {
            StepBody::Activity(a) => std::slice::from_ref(a),
            StepBody::Parallel(branches) => branches,
            StepBody::Sleep { .. } | StepBody::WaitSignal { .. } => &[],
        }
    }
}

impl Workflow {
    /// The step index and activity for an address.
    pub fn activity(&self, address: &str) -> Option<(usize, &Activity)> {
        self.steps.iter().enumerate().find_map(|(i, step)| {
            step.activities()
                .iter()
                .find(|a| a.address == address)
                .map(|a| (i, a))
        })
    }

    pub fn step_index(&self, name: &str) -> Option<usize> {
        self.steps.iter().position(|s| s.name == name)
    }

    /// Whether some `wait_signal` step waits for `name`.
    pub fn declares_signal(&self, name: &str) -> bool {
        self.steps
            .iter()
            .any(|s| matches!(&s.body, StepBody::WaitSignal { signal } if signal == name))
    }

    /// Validate a wire definition. All checks happen here, before anything
    /// is stored; the decider never re-validates.
    pub fn from_definition(
        definition: &WorkflowDefinition,
        limits: &WorkflowLimits,
    ) -> Result<Self, DefinitionError> {
        let bytes = serde_json::to_string(definition).map_or(usize::MAX, |s| s.len());
        if bytes > limits.max_definition_bytes {
            return Err(DefinitionError::TooLarge {
                actual: bytes,
                max: limits.max_definition_bytes,
            });
        }
        check_name(&definition.name, "workflow name", limits)?;
        if definition.steps.is_empty() {
            return Err(DefinitionError::NoSteps);
        }
        if definition.steps.len() > limits.max_steps {
            return Err(DefinitionError::TooManySteps {
                max: limits.max_steps,
            });
        }

        let mut step_names = BTreeSet::new();
        let mut signals = BTreeSet::new();
        // Addresses of activities in steps strictly before the current one.
        let mut earlier: BTreeSet<String> = BTreeSet::from([INPUT_ADDRESS.to_owned()]);
        let mut steps = Vec::with_capacity(definition.steps.len());

        for raw in &definition.steps {
            let at = format!("step {:?}", raw.name);
            check_name(&raw.name, &at, limits)?;
            if raw.name == INPUT_ADDRESS {
                return Err(DefinitionError::ReservedName {
                    name: raw.name.clone(),
                });
            }
            if !step_names.insert(raw.name.clone()) {
                return Err(DefinitionError::DuplicateStep {
                    name: raw.name.clone(),
                });
            }

            let set = usize::from(raw.activity.is_some())
                + usize::from(raw.parallel.is_some())
                + usize::from(raw.sleep_ms.is_some())
                + usize::from(raw.wait_signal.is_some());
            if set != 1 {
                return Err(DefinitionError::StepKind {
                    step: raw.name.clone(),
                });
            }

            let body = if let Some(a) = &raw.activity {
                let fields = ActivityFields::from(a);
                StepBody::Activity(fields.validate(raw.name.clone(), &earlier, limits)?)
            } else if let Some(branches) = &raw.parallel {
                if branches.is_empty() {
                    return Err(DefinitionError::EmptyParallel {
                        step: raw.name.clone(),
                    });
                }
                if branches.len() > limits.max_branches {
                    return Err(DefinitionError::TooManyBranches {
                        step: raw.name.clone(),
                        max: limits.max_branches,
                    });
                }
                let mut names = BTreeSet::new();
                let mut activities = Vec::with_capacity(branches.len());
                for b in branches {
                    check_name(&b.name, &format!("branch {:?} of {at}", b.name), limits)?;
                    if !names.insert(b.name.clone()) {
                        return Err(DefinitionError::DuplicateBranch {
                            step: raw.name.clone(),
                            name: b.name.clone(),
                        });
                    }
                    let address = format!("{}.{}", raw.name, b.name);
                    activities.push(ActivityFields::from(b).validate(address, &earlier, limits)?);
                }
                StepBody::Parallel(activities)
            } else if let Some(ms) = raw.sleep_ms {
                if !(1..=limits.max_sleep_ms).contains(&ms) {
                    return Err(DefinitionError::InvalidSleep {
                        step: raw.name.clone(),
                        max_ms: limits.max_sleep_ms,
                    });
                }
                StepBody::Sleep { ms }
            } else {
                let signal = raw.wait_signal.clone().unwrap_or_default();
                check_name(&signal, &format!("signal of {at}"), limits)?;
                if !signals.insert(signal.clone()) {
                    return Err(DefinitionError::DuplicateSignal { signal });
                }
                StepBody::WaitSignal { signal }
            };

            let step = Step {
                name: raw.name.clone(),
                body,
            };
            earlier.extend(step.activities().iter().map(|a| a.address.clone()));
            steps.push(step);
        }

        Ok(Workflow {
            name: definition.name.clone(),
            steps,
        })
    }
}

/// The fields shared by activity steps and parallel branches.
struct ActivityFields<'a> {
    kind: &'a str,
    payload: &'a Value,
    inputs: &'a [String],
    max_attempts: Option<u32>,
    timeout_secs: Option<u64>,
}

impl<'a> From<&'a ActivityDefinition> for ActivityFields<'a> {
    fn from(a: &'a ActivityDefinition) -> Self {
        Self {
            kind: &a.kind,
            payload: &a.payload,
            inputs: &a.inputs,
            max_attempts: a.max_attempts,
            timeout_secs: a.timeout_secs,
        }
    }
}

impl<'a> From<&'a BranchDefinition> for ActivityFields<'a> {
    fn from(b: &'a BranchDefinition) -> Self {
        Self {
            kind: &b.kind,
            payload: &b.payload,
            inputs: &b.inputs,
            max_attempts: b.max_attempts,
            timeout_secs: b.timeout_secs,
        }
    }
}

impl ActivityFields<'_> {
    fn validate(
        self,
        address: String,
        earlier: &BTreeSet<String>,
        limits: &WorkflowLimits,
    ) -> Result<Activity, DefinitionError> {
        // Reuse the job submission rules: an activity is a job.
        SubmitJobRequest {
            kind: self.kind.to_owned(),
            payload: self.payload.clone(),
            idempotency_key: None,
            max_attempts: self.max_attempts,
            timeout_secs: self.timeout_secs,
        }
        .validate(&limits.submission)
        .map_err(|source| DefinitionError::Activity {
            activity: address.clone(),
            source,
        })?;

        let Value::Object(payload) = self.payload else {
            unreachable!("validate() rejects non-object payloads");
        };
        if payload.contains_key(INPUTS_PAYLOAD_KEY) {
            return Err(DefinitionError::ReservedPayloadKey { activity: address });
        }

        let mut seen = BTreeSet::new();
        for input in self.inputs {
            if !seen.insert(input) {
                return Err(DefinitionError::DuplicateInput {
                    activity: address,
                    input: input.clone(),
                });
            }
            if !earlier.contains(input) {
                return Err(DefinitionError::UnknownInput {
                    activity: address,
                    input: input.clone(),
                });
            }
        }

        Ok(Activity {
            address,
            kind: self.kind.to_owned(),
            payload: payload.clone(),
            inputs: self.inputs.to_vec(),
            max_attempts: self.max_attempts,
            timeout_secs: self.timeout_secs,
        })
    }
}

/// Names: 1..=max characters from `[A-Za-z0-9_-]`. No `.`: it separates a
/// step from its branch in activity addresses.
fn check_name(name: &str, at: &str, limits: &WorkflowLimits) -> Result<(), DefinitionError> {
    let ok = !name.is_empty()
        && name.len() <= limits.max_name_len
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(DefinitionError::InvalidName {
            at: at.to_owned(),
            max: limits.max_name_len,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DefinitionError {
    #[error("definition is {actual} bytes; the limit is {max}")]
    TooLarge { actual: usize, max: usize },
    #[error("{at}: names must be 1..={max} characters from [A-Za-z0-9_-]")]
    InvalidName { at: String, max: usize },
    #[error("{name:?} is reserved (it addresses the run's input)")]
    ReservedName { name: String },
    #[error("a workflow needs at least one step")]
    NoSteps,
    #[error("at most {max} steps are allowed")]
    TooManySteps { max: usize },
    #[error("step name {name:?} is used twice")]
    DuplicateStep { name: String },
    #[error("step {step:?} must set exactly one of activity, parallel, sleep_ms, wait_signal")]
    StepKind { step: String },
    #[error("parallel step {step:?} has no branches")]
    EmptyParallel { step: String },
    #[error("parallel step {step:?} has more than {max} branches")]
    TooManyBranches { step: String, max: usize },
    #[error("branch name {name:?} is used twice in step {step:?}")]
    DuplicateBranch { step: String, name: String },
    #[error("activity {activity:?}: {source}")]
    Activity {
        activity: String,
        source: ValidationError,
    },
    #[error("activity {activity:?}: payload key \"inputs\" is reserved; use the inputs field")]
    ReservedPayloadKey { activity: String },
    #[error(
        "activity {activity:?}: input {input:?} is not an activity of an earlier step (or \"input\")"
    )]
    UnknownInput { activity: String, input: String },
    #[error("activity {activity:?}: input {input:?} is listed twice")]
    DuplicateInput { activity: String, input: String },
    #[error("step {step:?}: sleep_ms must be between 1 and {max_ms}")]
    InvalidSleep { step: String, max_ms: u64 },
    #[error("signal {signal:?} is awaited by two steps; wait_signal is a one-shot latch")]
    DuplicateSignal { signal: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(v: Value) -> Result<Workflow, DefinitionError> {
        let def: WorkflowDefinition = serde_json::from_value(v).expect("wire format");
        Workflow::from_definition(&def, &WorkflowLimits::default())
    }

    fn act(prompt: &str) -> Value {
        json!({ "kind": "mock", "payload": { "prompt": prompt } })
    }

    fn research() -> Value {
        json!({
            "name": "research",
            "steps": [
                { "name": "plan", "activity": act("plan") },
                { "name": "research", "parallel": [
                    { "name": "a", "kind": "mock", "payload": { "prompt": "a" }, "inputs": ["plan"] },
                    { "name": "b", "kind": "mock", "payload": { "prompt": "b" }, "max_attempts": 5 },
                ] },
                { "name": "aggregate", "activity": {
                    "kind": "mock", "payload": { "prompt": "merge" },
                    "inputs": ["research.a", "research.b", "input"] } },
                { "name": "cooldown", "sleep_ms": 30000 },
                { "name": "approval", "wait_signal": "approved" },
                { "name": "final", "activity": act("final") },
            ]
        })
    }

    #[test]
    fn a_full_definition_validates_into_addressed_activities() {
        let wf = parse(research()).unwrap();
        assert_eq!(wf.steps.len(), 6);
        let addrs: Vec<_> = wf
            .steps
            .iter()
            .flat_map(|s| s.activities())
            .map(|a| a.address.as_str())
            .collect();
        assert_eq!(
            addrs,
            ["plan", "research.a", "research.b", "aggregate", "final"]
        );
        assert_eq!(
            wf.activity("research.b").map(|(i, a)| (i, a.max_attempts)),
            Some((1, Some(5)))
        );
        assert_eq!(wf.step_index("cooldown"), Some(3));
        assert!(wf.declares_signal("approved"));
        assert!(!wf.declares_signal("nope"));
        assert!(matches!(wf.steps[3].body, StepBody::Sleep { ms: 30_000 }));
    }

    #[test]
    fn the_wire_format_rejects_unknown_fields() {
        for bad in [
            json!({ "name": "w", "steps": [], "extra": 1 }),
            json!({ "name": "w", "steps": [{ "name": "s", "activity": act("x"), "retries": 3 }] }),
            json!({ "name": "w", "steps": [{ "name": "s", "activity":
                { "kind": "mock", "payload": {}, "inptus": [] } }] }),
            json!({ "name": "w", "steps": [{ "name": "s", "parallel": [
                { "name": "b", "kind": "mock", "payload": {}, "oops": true }] }] }),
        ] {
            assert!(
                serde_json::from_value::<WorkflowDefinition>(bad.clone()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_wire_definition_round_trips() {
        let def: WorkflowDefinition = serde_json::from_value(research()).unwrap();
        let back: WorkflowDefinition =
            serde_json::from_str(&serde_json::to_string(&def).unwrap()).unwrap();
        assert_eq!(def, back);
    }

    fn one_step(step: Value) -> Result<Workflow, DefinitionError> {
        parse(json!({ "name": "w", "steps": [step] }))
    }

    #[test]
    fn each_step_sets_exactly_one_kind() {
        for step in [
            json!({ "name": "s" }),
            json!({ "name": "s", "activity": act("x"), "sleep_ms": 5 }),
            json!({ "name": "s", "wait_signal": "go", "parallel": [] }),
        ] {
            assert_eq!(
                one_step(step),
                Err(DefinitionError::StepKind { step: "s".into() })
            );
        }
    }

    #[test]
    fn names_are_checked_everywhere() {
        let long = "x".repeat(65);
        for bad in ["", "has space", "dot.ted", long.as_str()] {
            let r = parse(json!({ "name": bad, "steps": [{ "name": "s", "sleep_ms": 1 }] }));
            assert!(
                matches!(r, Err(DefinitionError::InvalidName { .. })),
                "workflow {bad:?}"
            );
            let r = one_step(json!({ "name": bad, "sleep_ms": 1 }));
            assert!(
                matches!(r, Err(DefinitionError::InvalidName { .. })),
                "step {bad:?}"
            );
            let r = one_step(json!({ "name": "s", "wait_signal": bad }));
            assert!(
                matches!(r, Err(DefinitionError::InvalidName { .. })),
                "signal {bad:?}"
            );
            let r = one_step(
                json!({ "name": "s", "parallel": [{ "name": bad, "kind": "mock", "payload": {} }] }),
            );
            assert!(
                matches!(r, Err(DefinitionError::InvalidName { .. })),
                "branch {bad:?}"
            );
        }
        assert_eq!(
            one_step(json!({ "name": "input", "sleep_ms": 1 })),
            Err(DefinitionError::ReservedName {
                name: "input".into()
            })
        );
    }

    #[test]
    fn duplicates_are_rejected() {
        let r = parse(json!({ "name": "w", "steps": [
            { "name": "s", "sleep_ms": 1 }, { "name": "s", "sleep_ms": 2 }] }));
        assert_eq!(r, Err(DefinitionError::DuplicateStep { name: "s".into() }));

        let r = one_step(json!({ "name": "p", "parallel": [
            { "name": "a", "kind": "mock", "payload": {} }, { "name": "a", "kind": "mock", "payload": {} }] }));
        assert_eq!(
            r,
            Err(DefinitionError::DuplicateBranch {
                step: "p".into(),
                name: "a".into()
            })
        );

        let r = parse(json!({ "name": "w", "steps": [
            { "name": "s1", "wait_signal": "go" }, { "name": "s2", "wait_signal": "go" }] }));
        assert_eq!(
            r,
            Err(DefinitionError::DuplicateSignal {
                signal: "go".into()
            })
        );
    }

    #[test]
    fn counts_and_sizes_are_bounded() {
        assert_eq!(
            parse(json!({ "name": "w", "steps": [] })),
            Err(DefinitionError::NoSteps)
        );

        let steps: Vec<_> = (0..65)
            .map(|i| json!({ "name": format!("s{i}"), "sleep_ms": 1 }))
            .collect();
        assert_eq!(
            parse(json!({ "name": "w", "steps": steps })),
            Err(DefinitionError::TooManySteps { max: 64 })
        );

        assert_eq!(
            one_step(json!({ "name": "p", "parallel": [] })),
            Err(DefinitionError::EmptyParallel { step: "p".into() })
        );
        let branches: Vec<_> = (0..17)
            .map(|i| json!({ "name": format!("b{i}"), "kind": "mock", "payload": {} }))
            .collect();
        assert_eq!(
            one_step(json!({ "name": "p", "parallel": branches })),
            Err(DefinitionError::TooManyBranches {
                step: "p".into(),
                max: 16
            })
        );

        for ms in [0, 7 * 24 * 60 * 60 * 1_000 + 1] {
            assert!(matches!(
                one_step(json!({ "name": "t", "sleep_ms": ms })),
                Err(DefinitionError::InvalidSleep { .. })
            ));
        }
        assert!(one_step(json!({ "name": "t", "sleep_ms": 7 * 24 * 60 * 60 * 1_000 })).is_ok());

        let limits = WorkflowLimits {
            max_definition_bytes: 100,
            ..WorkflowLimits::default()
        };
        let def: WorkflowDefinition = serde_json::from_value(research()).unwrap();
        assert!(matches!(
            Workflow::from_definition(&def, &limits),
            Err(DefinitionError::TooLarge { max: 100, .. })
        ));
    }

    #[test]
    fn activities_follow_job_submission_rules() {
        let cases = [
            (json!({ "kind": "BAD", "payload": {} }), "kind"),
            (json!({ "kind": "mock", "payload": [1] }), "payload object"),
            (
                json!({ "kind": "mock", "payload": {}, "max_attempts": 0 }),
                "attempts",
            ),
            (
                json!({ "kind": "mock", "payload": {}, "timeout_secs": 100_000 }),
                "timeout",
            ),
        ];
        for (activity, why) in cases {
            let r = one_step(json!({ "name": "s", "activity": activity }));
            assert!(
                matches!(r, Err(DefinitionError::Activity { .. })),
                "{why}: {r:?}"
            );
        }
        assert_eq!(
            one_step(
                json!({ "name": "s", "activity": { "kind": "mock", "payload": { "inputs": {} } } })
            ),
            Err(DefinitionError::ReservedPayloadKey {
                activity: "s".into()
            })
        );
    }

    #[test]
    fn inputs_may_only_reference_earlier_activities() {
        let mk = |inputs: Value| {
            parse(json!({ "name": "w", "steps": [
                { "name": "a", "activity": act("a") },
                { "name": "p", "parallel": [
                    { "name": "x", "kind": "mock", "payload": {}, "inputs": inputs.clone() },
                    { "name": "y", "kind": "mock", "payload": {} } ] },
                { "name": "t", "sleep_ms": 5 },
            ] }))
        };
        assert!(mk(json!(["a", "input"])).is_ok());
        for (input, why) in [
            ("p.y", "sibling in the same step"),
            ("p.x", "itself"),
            ("p", "a whole parallel step"),
            ("t", "a timer step"),
            ("later", "unknown"),
            ("a.x", "not a branch"),
        ] {
            assert_eq!(
                mk(json!([input])),
                Err(DefinitionError::UnknownInput {
                    activity: "p.x".into(),
                    input: input.into()
                }),
                "{why}"
            );
        }
        assert_eq!(
            mk(json!(["a", "a"])),
            Err(DefinitionError::DuplicateInput {
                activity: "p.x".into(),
                input: "a".into()
            })
        );
        // Forward references fail too: "final" can't be an input of "a".
        let r = parse(json!({ "name": "w", "steps": [
            { "name": "a", "activity": { "kind": "mock", "payload": {}, "inputs": ["final"] } },
            { "name": "final", "activity": act("f") } ] }));
        assert!(matches!(r, Err(DefinitionError::UnknownInput { .. })));
    }
}
