//! Experiment configuration (JSON). See docs/EXPERIMENTS.md.
//!
//! One file describes a base configuration, `profiles` (`fast`, `full`)
//! that patch it, and `variants` that patch it again. Patches are JSON Merge
//! Patch (RFC 7386): objects merge key by key; anything else, including
//! arrays, replaces. The fully resolved configuration of every variant is
//! written to `metadata.json`, so a run never depends on reading the
//! original file again.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Raw file: base fields plus `profiles` and `variants`, both patches.
/// (No `deny_unknown_fields` here: serde can't combine it with `flatten`.
/// Unknown base fields are still rejected, when each variant is parsed
/// into [`Experiment`].)
#[derive(Debug, Clone, Deserialize)]
pub struct ExperimentFile {
    #[serde(flatten)]
    pub base: Value,
    #[serde(default)]
    pub profiles: serde_json::Map<String, Value>,
    #[serde(default)]
    pub variants: Vec<VariantPatch>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct VariantPatch {
    pub label: String,
    /// Merge patch over the (profile-patched) base.
    #[serde(default)]
    pub set: Value,
}

/// A fully resolved configuration: what one variant runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Experiment {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// Master seed. Each repetition derives its own (see `plan::rep_seed`).
    pub seed: u64,
    pub repetitions: u32,
    /// Per repetition: workflows still running then are recorded as
    /// `timed_out` and the cluster is torn down.
    pub timeout_secs: u64,
    pub cluster: ClusterConfig,
    pub workload: Workload,
    #[serde(default)]
    pub failures: Failures,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub workers: u32,
    /// Slots per worker. 1 means a worker crash kills exactly one attempt.
    pub concurrency: u32,
    /// `relay coordinator --lease-secs` (whole seconds, 1..=3600).
    pub lease_secs: u64,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub max_active_jobs: u64,
    /// `full` (fsync per commit; the default) or `normal`.
    pub sqlite_synchronous: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    pub workflows: u32,
    /// Gap between workflow submissions; 0 = one burst.
    pub submit_interval_ms: u64,
    pub shape: Shape,
    pub duration: Duration,
    /// Per activity job.
    pub max_attempts: u32,
}

/// Every workflow is: `fanout` (a parallel step of `width` activities),
/// then an optional durable `sleep_ms` step, then an optional `join`
/// activity whose inputs are all branches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shape {
    pub width: u32,
    #[serde(default)]
    pub sleep_ms: u64,
    #[serde(default)]
    pub join: bool,
}

/// Activity duration (`delay_ms` of the mock handler), drawn per activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "distribution", rename_all = "snake_case", deny_unknown_fields)]
pub enum Duration {
    Fixed {
        ms: u64,
    },
    /// Integer milliseconds, uniform on `[min_ms, max_ms]`.
    Uniform {
        min_ms: u64,
        max_ms: u64,
    },
    /// `median_ms × exp(sigma × Z)`, Z standard normal, clamped to
    /// `[1, max_ms]`. The median is exact (before clamping); the mean is
    /// `median × exp(sigma²/2)`; `sigma = 0.8` gives p99 ≈ 6.4 × median.
    Lognormal {
        median_ms: u64,
        sigma: f64,
        max_ms: u64,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failures {
    #[serde(default)]
    pub worker_crash: Option<WorkerCrash>,
    #[serde(default)]
    pub coordinator_crash: Option<CoordinatorCrash>,
}

/// SIGKILL the worker running a planned activity's first attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCrash {
    /// Each activity is independently targeted with this probability.
    pub probability: f64,
    /// Kill at `attempt start + crash_point × planned duration`.
    pub crash_point: f64,
    /// Start a replacement worker process (same name, new id) at once, so
    /// capacity stays constant.
    #[serde(default = "yes")]
    pub restart_worker: bool,
}

fn yes() -> bool {
    true
}

/// SIGKILL the coordinator when each condition is first observed, in order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorCrash {
    pub points: Vec<CrashPoint>,
    pub downtime_ms: u64,
    /// Give up on a point (recorded as `not_observed`) after this long.
    pub observe_timeout_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrashPoint {
    /// Some activity attempt is running.
    ActivitiesRunning,
    /// Some fan-out has ≥ 1 succeeded and ≥ 1 unfinished branch.
    BranchesPartiallyDone,
    /// Some run sleeps on an armed timer that fires ≥ 1 s from now.
    TimerPending,
    /// Some run is due for a decision now (`wake_at ≤ now`, no timer).
    DecisionPending,
}

impl CrashPoint {
    pub fn as_str(self) -> &'static str {
        match self {
            CrashPoint::ActivitiesRunning => "activities_running",
            CrashPoint::BranchesPartiallyDone => "branches_partially_done",
            CrashPoint::TimerPending => "timer_pending",
            CrashPoint::DecisionPending => "decision_pending",
        }
    }
}

/// RFC 7386 JSON Merge Patch.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(t), Value::Object(p)) => {
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else {
                    merge_patch(t.entry(k.clone()).or_insert(Value::Null), v);
                }
            }
        }
        (t, p) => *t = p.clone(),
    }
}

/// Resolve `base ⊕ profile ⊕ variant` for every variant. With no variants,
/// one variant labelled `default` runs the base.
pub fn resolve(file: &ExperimentFile, profile: &str) -> anyhow::Result<Vec<(String, Experiment)>> {
    let mut base = file.base.clone();
    if let Some(patch) = file.profiles.get(profile) {
        // Profiles may also replace the variant list.
        merge_patch(&mut base, patch);
    } else if !file.profiles.is_empty() {
        anyhow::bail!(
            "unknown profile {profile:?}; this file defines: {}",
            file.profiles.keys().cloned().collect::<Vec<_>>().join(", ")
        );
    }
    let variants: Vec<VariantPatch> = match base.as_object_mut().and_then(|o| o.remove("variants"))
    {
        Some(v) => serde_json::from_value(v)?,
        None => file.variants.clone(),
    };
    let variants = if variants.is_empty() {
        vec![VariantPatch {
            label: "default".into(),
            set: Value::Null,
        }]
    } else {
        variants
    };

    let mut labels = std::collections::BTreeSet::new();
    let mut out = Vec::with_capacity(variants.len());
    for v in variants {
        anyhow::ensure!(
            !v.label.is_empty()
                && v.label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "variant label {:?} must be [A-Za-z0-9_-]+",
            v.label
        );
        anyhow::ensure!(
            labels.insert(v.label.clone()),
            "variant label {:?} is used twice",
            v.label
        );
        let mut config = base.clone();
        if !v.set.is_null() {
            // A missing `set` means "no changes", not RFC 7386's "replace with null".
            merge_patch(&mut config, &v.set);
        }
        let experiment: Experiment = serde_json::from_value(config)
            .map_err(|e| anyhow::anyhow!("variant {:?}: {e}", v.label))?;
        let problems = experiment.validate();
        anyhow::ensure!(
            problems.is_empty(),
            "variant {:?} is invalid:\n  - {}",
            v.label,
            problems.join("\n  - ")
        );
        out.push((v.label, experiment));
    }
    Ok(out)
}

impl Experiment {
    /// Every problem at once.
    pub fn validate(&self) -> Vec<String> {
        let mut p = Vec::new();
        let c = &self.cluster;
        let w = &self.workload;
        if self.repetitions == 0 {
            p.push("repetitions must be ≥ 1".into());
        }
        if self.timeout_secs == 0 {
            p.push("timeout_secs must be ≥ 1".into());
        }
        if c.workers == 0 || c.concurrency == 0 {
            p.push("cluster.workers and cluster.concurrency must be ≥ 1".into());
        }
        if !(1..=3_600).contains(&c.lease_secs) {
            p.push("cluster.lease_secs must be 1..=3600 (the coordinator's own limit)".into());
        }
        if c.retry_base_ms > c.retry_max_ms {
            p.push("cluster.retry_base_ms must not exceed retry_max_ms".into());
        }
        if !matches!(c.sqlite_synchronous.as_str(), "full" | "normal") {
            p.push("cluster.sqlite_synchronous must be full or normal".into());
        }
        if w.workflows == 0 {
            p.push("workload.workflows must be ≥ 1".into());
        }
        if !(1..=16).contains(&w.shape.width) {
            p.push("workload.shape.width must be 1..=16 (the workflow parallel limit)".into());
        }
        if w.shape.sleep_ms > 7 * 24 * 3_600_000 {
            p.push("workload.shape.sleep_ms is over 7 days".into());
        }
        if !(1..=20).contains(&w.max_attempts) {
            p.push("workload.max_attempts must be 1..=20".into());
        }
        // A capacity below one workflow's width is allowed on purpose:
        // partial fan-out still progresses, and that is worth measuring.
        if c.max_active_jobs == 0 {
            p.push("cluster.max_active_jobs must be ≥ 1".into());
        }
        const MOCK_MAX: u64 = 60_000; // relay-worker MAX_MOCK_DELAY_MS
        match &w.duration {
            Duration::Fixed { ms } if *ms > MOCK_MAX => {
                p.push(format!("duration.ms must be ≤ {MOCK_MAX} (mock limit)"))
            }
            Duration::Uniform { min_ms, max_ms } if min_ms > max_ms || *max_ms > MOCK_MAX => {
                p.push(format!("uniform needs min_ms ≤ max_ms ≤ {MOCK_MAX}"))
            }
            Duration::Lognormal {
                median_ms,
                sigma,
                max_ms,
            } => {
                if *median_ms == 0 || *max_ms > MOCK_MAX || median_ms > max_ms {
                    p.push(format!(
                        "lognormal needs 1 ≤ median_ms ≤ max_ms ≤ {MOCK_MAX}"
                    ));
                }
                if !(sigma.is_finite() && *sigma >= 0.0 && *sigma <= 3.0) {
                    p.push("lognormal sigma must be in [0, 3]".into());
                }
            }
            _ => {}
        }
        if let Some(wc) = &self.failures.worker_crash {
            if !(0.0..=1.0).contains(&wc.probability) {
                p.push("worker_crash.probability must be in [0, 1]".into());
            }
            if !(0.0..1.0).contains(&wc.crash_point) {
                p.push("worker_crash.crash_point must be in [0, 1)".into());
            }
            if wc.probability > 0.0 && w.max_attempts < 2 {
                p.push("worker_crash needs workload.max_attempts ≥ 2, or every crash fails its workflow".into());
            }
        }
        if let Some(cc) = &self.failures.coordinator_crash {
            if cc.points.is_empty() {
                p.push("coordinator_crash.points must not be empty".into());
            }
            if cc.observe_timeout_secs == 0 {
                p.push("coordinator_crash.observe_timeout_secs must be ≥ 1".into());
            }
        }
        p
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn base() -> Value {
        json!({
            "name": "t", "seed": 7, "repetitions": 2, "timeout_secs": 60,
            "cluster": { "workers": 2, "concurrency": 1, "lease_secs": 2, "retry_base_ms": 100,
                         "retry_max_ms": 1000, "max_active_jobs": 1000, "sqlite_synchronous": "normal" },
            "workload": { "workflows": 3, "submit_interval_ms": 0,
                          "shape": { "width": 2, "join": true },
                          "duration": { "distribution": "fixed", "ms": 100 }, "max_attempts": 3 }
        })
    }

    fn file(v: Value) -> ExperimentFile {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn merge_patch_follows_rfc_7386() {
        let mut t = json!({ "a": 1, "b": { "c": 2, "d": 3 }, "e": [1, 2] });
        merge_patch(
            &mut t,
            &json!({ "a": null, "b": { "c": 9 }, "e": [3], "f": true }),
        );
        assert_eq!(t, json!({ "b": { "c": 9, "d": 3 }, "e": [3], "f": true }));
    }

    #[test]
    fn profiles_and_variants_resolve_in_order() {
        let mut v = base();
        v["profiles"] = json!({ "fast": { "repetitions": 1 }, "full": { "repetitions": 5 } });
        v["variants"] = json!([
            { "label": "lease-1s", "set": { "cluster": { "lease_secs": 1 } } },
            { "label": "lease-5s", "set": { "cluster": { "lease_secs": 5 }, "repetitions": 9 } },
        ]);
        let resolved = resolve(&file(v.clone()), "fast").unwrap();
        let summary: Vec<_> = resolved
            .iter()
            .map(|(l, e)| (l.as_str(), e.cluster.lease_secs, e.repetitions))
            .collect();
        assert_eq!(
            summary,
            [("lease-1s", 1, 1), ("lease-5s", 5, 9)],
            "variant patches apply after the profile"
        );
        assert_eq!(
            resolve(&file(v.clone()), "full").unwrap()[0].1.repetitions,
            5
        );
        assert!(
            resolve(&file(v), "huge")
                .unwrap_err()
                .to_string()
                .contains("unknown profile")
        );
    }

    #[test]
    fn a_profile_may_replace_the_variant_list() {
        let mut v = base();
        v["variants"] = json!([{ "label": "a", "set": {} }, { "label": "b", "set": {} }]);
        v["profiles"] = json!({ "fast": { "variants": [{ "label": "only", "set": {} }] } });
        let labels: Vec<_> = resolve(&file(v), "fast")
            .unwrap()
            .into_iter()
            .map(|(l, _)| l)
            .collect();
        assert_eq!(labels, ["only"]);
    }

    #[test]
    fn invalid_configs_list_every_problem() {
        let mut v = base();
        v["repetitions"] = json!(0);
        v["cluster"]["lease_secs"] = json!(0);
        v["workload"]["shape"]["width"] = json!(33);
        v["workload"]["duration"] = json!({ "distribution": "lognormal", "median_ms": 2000, "sigma": 9.0, "max_ms": 90000 });
        v["failures"] = json!({ "worker_crash": { "probability": 1.5, "crash_point": 1.0 } });
        let err = resolve(&file(v), "fast").unwrap_err().to_string();
        for needle in [
            "repetitions",
            "lease_secs",
            "width",
            "lognormal needs",
            "sigma",
            "probability",
            "crash_point",
        ] {
            assert!(err.contains(needle), "{needle} missing from:\n{err}");
        }
    }

    #[test]
    fn unknown_fields_and_bad_labels_are_rejected() {
        let mut v = base();
        v["workload"]["shape"]["widht"] = json!(4);
        assert!(resolve(&file(v), "fast").is_err());
        let mut v = base();
        v["variants"] = json!([{ "label": "has space", "set": {} }]);
        assert!(resolve(&file(v), "fast").is_err());
        let mut v = base();
        v["variants"] = json!([{ "label": "a", "set": {} }, { "label": "a", "set": {} }]);
        assert!(
            resolve(&file(v), "fast")
                .unwrap_err()
                .to_string()
                .contains("twice")
        );
    }

    #[test]
    fn crashes_need_retries_to_be_survivable() {
        let mut v = base();
        v["workload"]["max_attempts"] = json!(1);
        v["failures"] = json!({ "worker_crash": { "probability": 0.1, "crash_point": 0.5 } });
        assert!(
            resolve(&file(v), "fast")
                .unwrap_err()
                .to_string()
                .contains("max_attempts")
        );
    }
}
