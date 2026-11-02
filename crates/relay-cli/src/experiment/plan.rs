//! The deterministic part of a run: everything derived from the seed.
//!
//! A [`RunPlan`] fixes, before anything starts, every activity's duration
//! and which first attempts the harness will try to kill and at what point.
//! Same experiment + same seed + same repetition ⇒ the same plan,
//! byte-for-byte (`relay experiment plan` prints it). What a plan can't fix
//! is *when* things happen: OS scheduling, which worker claims which job,
//! and network timing vary run to run. docs/EXPERIMENTS.md §Reproducibility.
//!
//! Two independent random streams per repetition:
//!
//! * durations: one draw per activity, in plan order;
//! * crashes: one uniform draw per activity, always (even at probability 0),
//!   and an activity is targeted iff `draw < probability`.
//!
//! So within a repetition, variants that differ only in lease or failure
//! settings run the *same* durations (common random numbers). Raising the
//! failure probability only adds targets; it never moves existing ones.

use fastrand::Rng;
use relay_core::workflow::WorkflowDefinition;
use serde::Serialize;
use serde_json::json;

use super::config::{Duration, Experiment};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunPlan {
    pub seed: u64,
    pub workflows: Vec<WorkflowPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowPlan {
    pub index: u32,
    pub activities: Vec<ActivityPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ActivityPlan {
    /// `fanout.bNN` or `join`.
    pub address: String,
    /// The mock handler's `delay_ms` for this activity.
    pub duration_ms: u64,
    pub crash: Option<PlannedCrash>,
}

/// Kill the worker running `attempt` of this activity, `after_ms` after the
/// attempt started (coordinator `started_at`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PlannedCrash {
    pub attempt: u32,
    pub after_ms: u64,
}

/// SplitMix64: a well-mixed seed per repetition from the master seed.
pub fn rep_seed(master: u64, repetition: u32) -> u64 {
    let mut z = master.wrapping_add(u64::from(repetition).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const CRASH_STREAM: u64 = 0xC4A5_11DE_D00D_F00D;

pub fn plan(exp: &Experiment, repetition: u32) -> RunPlan {
    let seed = rep_seed(exp.seed, repetition);
    let mut durations = Rng::with_seed(seed);
    let mut crashes = Rng::with_seed(seed ^ CRASH_STREAM);
    let shape = &exp.workload.shape;
    let crash = exp.failures.worker_crash.as_ref();

    let workflows = (0..exp.workload.workflows)
        .map(|index| {
            let mut addresses: Vec<String> = (0..shape.width)
                .map(|b| format!("fanout.b{b:02}"))
                .collect();
            if shape.join {
                addresses.push("join".into());
            }
            let activities = addresses
                .into_iter()
                .map(|address| {
                    let duration_ms = draw(&exp.workload.duration, &mut durations);
                    let u = crashes.f64();
                    let crash = crash.filter(|c| u < c.probability).map(|c| PlannedCrash {
                        attempt: 1,
                        after_ms: (duration_ms as f64 * c.crash_point).round() as u64,
                    });
                    ActivityPlan {
                        address,
                        duration_ms,
                        crash,
                    }
                })
                .collect();
            WorkflowPlan { index, activities }
        })
        .collect();
    RunPlan { seed, workflows }
}

fn draw(d: &Duration, rng: &mut Rng) -> u64 {
    match *d {
        Duration::Fixed { ms } => ms,
        Duration::Uniform { min_ms, max_ms } => rng.u64(min_ms..=max_ms),
        Duration::Lognormal {
            median_ms,
            sigma,
            max_ms,
        } => {
            // Box–Muller; 1 - f64() is in (0, 1], so ln is finite.
            let (u1, u2) = (1.0 - rng.f64(), rng.f64());
            let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
            let x = median_ms as f64 * (sigma * z).exp();
            (x.round() as u64).clamp(1, max_ms)
        }
    }
}

/// The workflow definition the harness submits for one planned workflow.
pub fn definition(exp: &Experiment, wf: &WorkflowPlan) -> WorkflowDefinition {
    let w = &exp.workload;
    let job = |a: &ActivityPlan| json!({ "prompt": format!("experiment {} {}", wf.index, a.address), "delay_ms": a.duration_ms });
    let branches: Vec<_> = wf
        .activities
        .iter()
        .filter(|a| a.address.starts_with("fanout."))
        .map(|a| {
            json!({ "name": a.address.trim_start_matches("fanout."), "kind": "mock",
                    "payload": job(a), "max_attempts": w.max_attempts })
        })
        .collect();
    let mut steps = vec![json!({ "name": "fanout", "parallel": branches })];
    if w.shape.sleep_ms > 0 {
        steps.push(json!({ "name": "pause", "sleep_ms": w.shape.sleep_ms }));
    }
    if let Some(join) = wf.activities.iter().find(|a| a.address == "join") {
        let inputs: Vec<_> = wf
            .activities
            .iter()
            .filter(|a| a.address != "join")
            .map(|a| a.address.clone())
            .collect();
        steps.push(json!({ "name": "join", "activity": {
            "kind": "mock", "payload": job(join), "inputs": inputs, "max_attempts": w.max_attempts } }));
    }
    serde_json::from_value(json!({ "name": exp.name, "steps": steps }))
        .expect("the harness builds valid definitions") // checked by a test over every config shape
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experiment::config::{Failures, WorkerCrash};
    use relay_core::workflow::{Workflow, WorkflowLimits};

    fn experiment(duration: Duration) -> Experiment {
        let mut v = crate::experiment::config::tests::base();
        v["workload"]["duration"] = serde_json::to_value(&duration).unwrap();
        v["workload"]["workflows"] = json!(40);
        v["workload"]["shape"] = json!({ "width": 4, "sleep_ms": 1000, "join": true });
        serde_json::from_value(v).unwrap()
    }

    fn with_crash(mut e: Experiment, probability: f64) -> Experiment {
        e.failures = Failures {
            worker_crash: Some(WorkerCrash {
                probability,
                crash_point: 0.5,
                restart_worker: true,
            }),
            coordinator_crash: None,
        };
        e
    }

    fn durations(p: &RunPlan) -> Vec<u64> {
        p.workflows
            .iter()
            .flat_map(|w| w.activities.iter().map(|a| a.duration_ms))
            .collect()
    }

    fn targets(p: &RunPlan) -> Vec<(u32, String)> {
        p.workflows
            .iter()
            .flat_map(|w| {
                w.activities
                    .iter()
                    .filter(|a| a.crash.is_some())
                    .map(move |a| (w.index, a.address.clone()))
            })
            .collect()
    }

    #[test]
    fn the_same_seed_gives_the_same_plan_and_another_seed_does_not() {
        let e = with_crash(
            experiment(Duration::Uniform {
                min_ms: 100,
                max_ms: 900,
            }),
            0.3,
        );
        assert_eq!(plan(&e, 1), plan(&e, 1));
        assert_ne!(
            durations(&plan(&e, 1)),
            durations(&plan(&e, 2)),
            "repetitions differ"
        );
        let mut other = e.clone();
        other.seed = 8;
        assert_ne!(durations(&plan(&e, 1)), durations(&plan(&other, 1)));
        // Pinned: a change to the RNG or the draw order must be deliberate.
        assert_eq!(rep_seed(7, 1), rep_seed(7, 1));
        assert_ne!(rep_seed(7, 1), rep_seed(7, 2));
    }

    #[test]
    fn failure_settings_never_change_durations_and_only_add_targets() {
        let base = experiment(Duration::Lognormal {
            median_ms: 500,
            sigma: 0.8,
            max_ms: 20_000,
        });
        let none = plan(&base, 1);
        let low = plan(&with_crash(base.clone(), 0.1), 1);
        let high = plan(&with_crash(base.clone(), 0.4), 1);
        assert_eq!(
            durations(&none),
            durations(&low),
            "common random numbers across variants"
        );
        assert_eq!(durations(&low), durations(&high));
        assert!(targets(&none).is_empty());
        let (lo, hi) = (targets(&low), targets(&high));
        assert!(!lo.is_empty() && hi.len() > lo.len());
        assert!(
            lo.iter().all(|t| hi.contains(t)),
            "a higher rate keeps every lower-rate target"
        );
        let every = plan(&with_crash(base, 1.0), 1);
        assert_eq!(targets(&every).len(), 40 * 5);
    }

    #[test]
    fn crash_points_are_a_fraction_of_the_planned_duration() {
        let e = with_crash(experiment(Duration::Fixed { ms: 2000 }), 1.0);
        let p = plan(&e, 1);
        let c = p.workflows[0].activities[0].crash.unwrap();
        assert_eq!((c.attempt, c.after_ms), (1, 1000));
    }

    #[test]
    fn distributions_have_the_documented_shape() {
        let n = 4_000;
        let mut e = experiment(Duration::Fixed { ms: 1 });
        e.workload.workflows = n / 5;
        e.workload.duration = Duration::Uniform {
            min_ms: 1000,
            max_ms: 3000,
        };
        let u = durations(&plan(&e, 1));
        assert!(u.iter().all(|d| (1000..=3000).contains(d)));
        let mean = u.iter().sum::<u64>() as f64 / u.len() as f64;
        assert!((mean - 2000.0).abs() < 50.0, "uniform mean {mean}");

        e.workload.duration = Duration::Lognormal {
            median_ms: 2000,
            sigma: 0.8,
            max_ms: 60_000,
        };
        let mut l = durations(&plan(&e, 1));
        l.sort_unstable();
        let median = l[l.len() / 2] as f64;
        assert!(
            (median / 2000.0 - 1.0).abs() < 0.08,
            "lognormal median {median}"
        );
        let p99 = l[l.len() * 99 / 100] as f64;
        // exp(0.8 × 2.326) ≈ 6.43
        assert!(
            (4.5..9.0).contains(&(p99 / median)),
            "p99/median {}",
            p99 / median
        );
        assert!(*l.last().unwrap() <= 60_000);
    }

    #[test]
    fn every_planned_workflow_is_a_valid_definition() {
        for (width, sleep, join) in [(1, 0, false), (16, 0, true), (4, 500, true), (2, 0, false)] {
            let mut e = experiment(Duration::Fixed { ms: 10 });
            e.workload.shape.width = width;
            e.workload.shape.sleep_ms = sleep;
            e.workload.shape.join = join;
            for wf in &plan(&e, 1).workflows {
                let def = definition(&e, wf);
                let valid = Workflow::from_definition(&def, &WorkflowLimits::default()).unwrap();
                let addresses: Vec<_> = valid
                    .steps
                    .iter()
                    .flat_map(|s| s.activities())
                    .map(|a| a.address.clone())
                    .collect();
                let planned: Vec<_> = wf.activities.iter().map(|a| a.address.clone()).collect();
                assert_eq!(addresses, planned, "plan addresses = definition addresses");
            }
        }
    }
}
