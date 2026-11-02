//! `relay experiment`: reproducible, seeded experiments on local clusters.
//!
//! ```text
//! relay experiment run experiments/lease_sweep.json --profile fast
//! relay experiment plan experiments/durations.json --variant lognormal
//! ```
//!
//! A config file declares a base experiment, profiles (`fast`, `full`) and
//! variants, each a JSON merge patch (config.rs). For every repetition and
//! variant the harness derives a seeded plan (plan.rs), runs it against
//! real processes (runner.rs), and turns what the public API reports into
//! raw rows, metrics and invariant checks (analysis.rs). Output, under
//! `<out>/<run-id>/`:
//!
//! * `metadata.json`: everything needed to rerun it;
//! * `runs.csv`, `workflows.csv`, `activities.csv`, `attempts.csv`,
//!   `failures.csv`, `samples.csv`, `invariants.csv`: raw data;
//! * `summary.json` / `summary.csv`: pooled aggregates (stats.rs);
//! * `<variant>/rep-<k>/`: that repetition's plan and process logs.
//!
//! See docs/EXPERIMENTS.md.

mod analysis;
mod config;
mod plan;
mod runner;
mod stats;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::Context;
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use analysis::{Key, Rows, Sample};
use config::{Experiment, ExperimentFile};
use stats::{Summary, summarize};

use crate::bench::{cpu_model, unix_ms};

#[derive(Debug, Subcommand)]
pub enum ExperimentCommand {
    /// Run every variant × repetition of an experiment config.
    Run(RunArgs),
    /// Print the seeded plan (durations, crash targets) without running it.
    Plan(PlanArgs),
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Experiment config (JSON), e.g. experiments/lease_sweep.json.
    config: PathBuf,
    /// Profile from the config's `profiles`.
    #[arg(long, default_value = "fast")]
    profile: String,
    /// Results root; each run writes `<out>/<run-id>/`.
    #[arg(long, default_value = "experiments/results")]
    out: PathBuf,
    /// Override the config's master seed.
    #[arg(long)]
    seed: Option<u64>,
    /// Override the number of repetitions.
    #[arg(long)]
    repetitions: Option<u32>,
    /// Run only these variants (comma-separated labels).
    #[arg(long, value_delimiter = ',')]
    variants: Vec<String>,
}

#[derive(Debug, Args)]
pub struct PlanArgs {
    config: PathBuf,
    #[arg(long, default_value = "fast")]
    profile: String,
    /// Variant label; defaults to the first.
    #[arg(long)]
    variant: Option<String>,
    #[arg(long, default_value_t = 1)]
    repetition: u32,
    #[arg(long)]
    seed: Option<u64>,
}

pub async fn run(command: ExperimentCommand) -> anyhow::Result<ExitCode> {
    match command {
        ExperimentCommand::Run(args) => run_experiment(args).await,
        ExperimentCommand::Plan(args) => print_plan(&args),
    }
}

fn load(
    path: &Path,
    profile: &str,
    seed: Option<u64>,
) -> anyhow::Result<(Value, Vec<(String, Experiment)>)> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let raw: Value =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    let file: ExperimentFile = serde_json::from_value(raw.clone())?;
    let mut variants = config::resolve(&file, profile)?;
    if let Some(seed) = seed {
        for (_, e) in &mut variants {
            e.seed = seed;
        }
    }
    Ok((raw, variants))
}

fn print_plan(args: &PlanArgs) -> anyhow::Result<ExitCode> {
    let (_, variants) = load(&args.config, &args.profile, args.seed)?;
    let (label, exp) = match &args.variant {
        Some(v) => variants
            .iter()
            .find(|(l, _)| l == v)
            .with_context(|| format!("no variant {v:?}"))?,
        None => &variants[0],
    };
    let plan = plan::plan(exp, args.repetition);
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({ "variant": label, "repetition": args.repetition, "plan": plan })
        )?
    );
    Ok(ExitCode::SUCCESS)
}

async fn run_experiment(args: RunArgs) -> anyhow::Result<ExitCode> {
    let (raw, mut variants) = load(&args.config, &args.profile, args.seed)?;
    if !args.variants.is_empty() {
        for v in &args.variants {
            anyhow::ensure!(variants.iter().any(|(l, _)| l == v), "no variant {v:?}");
        }
        variants.retain(|(l, _)| args.variants.contains(l));
    }
    if let Some(r) = args.repetitions {
        anyhow::ensure!(r > 0, "--repetitions must be positive");
        for (_, e) in &mut variants {
            e.repetitions = r;
        }
    }
    let name = variants[0].1.name.clone();
    let started = unix_ms();
    let run_id = format!("{name}-{}-{started}", args.profile);
    let out = args.out.join(&run_id);
    std::fs::create_dir_all(&out).with_context(|| format!("creating {}", out.display()))?;

    let mut metadata = metadata(&run_id, &args, &raw, &variants, started);
    write_json(&out.join("metadata.json"), &metadata)?;
    if cfg!(debug_assertions) {
        println!("WARNING: debug build; build with --release for representative numbers.");
    }
    println!(
        "experiment {name} ({} variants, profile {}) -> {}",
        variants.len(),
        args.profile,
        out.display()
    );

    // Repetition-major order: rep 1 of every variant, then rep 2, ... so
    // slow drift of the machine spreads across variants instead of
    // landing on whichever ran last.
    let max_reps = variants
        .iter()
        .map(|(_, e)| e.repetitions)
        .max()
        .unwrap_or(1);
    let mut all = Rows::default();
    for rep in 1..=max_reps {
        for (label, exp) in variants.iter().filter(|(_, e)| rep <= e.repetitions) {
            let plan = plan::plan(exp, rep);
            let dir = out.join(label).join(format!("rep-{rep}"));
            std::fs::create_dir_all(&dir)?;
            write_json(&dir.join("plan.json"), &plan)?;
            let t0 = std::time::Instant::now();
            let collected = runner::run(
                exp,
                &format!("{run_id}-{label}-{rep}"),
                &plan,
                &dir.join("cluster"),
            )
            .await
            .with_context(|| format!("variant {label} repetition {rep}"))?;
            let key = Key {
                variant: label.clone(),
                repetition: rep,
            };
            let rows = analysis::build(&key, &plan, &collected);
            let r = &rows.runs[0];
            println!(
                "  {label:<16} rep {rep}: {}/{} succeeded{} | p50 {} p99 {} ms | kills {}/{} worker, {}/{} coordinator | {} violations | {:.1} s",
                r.succeeded,
                r.workflows,
                if r.timed_out { " (TIMED OUT)" } else { "" },
                fmt_ms(r.workflow_latency_p50_ms),
                fmt_ms(r.workflow_latency_p99_ms),
                r.worker_kills_executed,
                r.worker_kills_requested,
                r.coordinator_kills_executed,
                r.coordinator_kills_requested,
                r.invariant_violations,
                t0.elapsed().as_secs_f64(),
            );
            append(&mut all, rows);
            // Rewritten after every repetition, so an interrupted run keeps
            // everything finished so far.
            write_raw(&out, &all)?;
        }
    }
    let summary = summary(&all);
    write_json(
        &out.join("summary.json"),
        &json!({ "aggregation": AGGREGATION, "rows": summary }),
    )?;
    write_csv(&out.join("summary.csv"), &summary)?;

    let violations: Vec<_> = all.invariants.iter().filter(|i| !i.ok).collect();
    let timed_out = all.runs.iter().filter(|r| r.timed_out).count();
    metadata["finished_at_unix_ms"] = json!(unix_ms());
    metadata["outcome"] =
        json!({ "invariant_violations": violations.len(), "timed_out_runs": timed_out });
    write_json(&out.join("metadata.json"), &metadata)?;

    for v in &violations {
        println!(
            "INVARIANT VIOLATION {} rep {} workflow {}: {} ({})",
            v.key.variant, v.key.repetition, v.workflow_index, v.invariant, v.detail
        );
    }
    if timed_out > 0 {
        println!(
            "{timed_out} run(s) hit the timeout with unfinished workflows: a liveness problem or too short a timeout_secs."
        );
    }
    println!("results: {}", out.display());
    Ok(if violations.is_empty() && timed_out == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn fmt_ms(v: Option<f64>) -> String {
    v.map_or_else(|| "-".into(), |v| format!("{v:.0}"))
}

fn append(all: &mut Rows, rows: Rows) {
    all.runs.extend(rows.runs);
    all.workflows.extend(rows.workflows);
    all.activities.extend(rows.activities);
    all.attempts.extend(rows.attempts);
    all.failures.extend(rows.failures);
    all.invariants.extend(rows.invariants);
    all.samples.extend(rows.samples);
}

// ---- metadata ---------------------------------------------------------------

fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn metadata(
    run_id: &str,
    args: &RunArgs,
    raw: &Value,
    variants: &[(String, Experiment)],
    started: i64,
) -> Value {
    let sha = git(&["rev-parse", "HEAD"]);
    let dirty = git(&["status", "--porcelain", "--untracked-files=no"]).map(|s| !s.is_empty());
    json!({
        "run_id": run_id,
        "experiment": variants[0].1.name,
        "description": variants[0].1.description,
        "config_file": args.config.display().to_string(),
        "config_file_contents": raw,
        "profile": args.profile,
        "seed_override": args.seed,
        "repetitions_override": args.repetitions,
        "variant_filter": args.variants,
        "started_at_unix_ms": started,
        "finished_at_unix_ms": null,
        "git": { "sha": sha, "dirty_tracked_files": dirty },
        "relay_version": env!("CARGO_PKG_VERSION"),
        "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "environment": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "logical_cpus": std::thread::available_parallelism().map_or(0, |n| n.get()),
            "cpu_model": cpu_model(),
            "handler": "mock (sleeps delay_ms; no model inference)",
        },
        "repetition_order": "repetition-major: rep 1 of every variant, then rep 2, ...",
        "variants": variants.iter().map(|(label, e)| json!({
            "label": label,
            "resolved_config": e,
            "repetition_seeds": (1..=e.repetitions).map(|r| plan::rep_seed(e.seed, r)).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "reproducibility": "Same config + seed => identical plans (durations, crash targets, crash \
            offsets): see <variant>/rep-<k>/plan.json. Not reproduced: OS scheduling, which worker \
            claims which job, poll/claim timing, and so every measured latency.",
        "outcome": null,
    })
}

// ---- output -----------------------------------------------------------------

fn write_json<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_string_pretty(value)?)
        .with_context(|| format!("writing {}", path.display()))
}

#[derive(Serialize)]
struct SampleRow<'a> {
    #[serde(flatten)]
    key: &'a Key,
    #[serde(flatten)]
    sample: &'a Sample,
}

fn write_raw(out: &Path, all: &Rows) -> anyhow::Result<()> {
    write_csv(&out.join("runs.csv"), &all.runs)?;
    write_csv(&out.join("workflows.csv"), &all.workflows)?;
    write_csv(&out.join("activities.csv"), &all.activities)?;
    write_csv(&out.join("attempts.csv"), &all.attempts)?;
    write_csv(&out.join("failures.csv"), &all.failures)?;
    write_csv(&out.join("invariants.csv"), &all.invariants)?;
    let samples: Vec<_> = all
        .samples
        .iter()
        .map(|(key, sample)| SampleRow { key, sample })
        .collect();
    write_csv(&out.join("samples.csv"), &samples)
}

/// A JSON object's fields in serialization (= struct declaration) order.
struct Ordered(Vec<(String, Value)>);

impl<'de> Deserialize<'de> for Ordered {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Ordered;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut m: A,
            ) -> Result<Ordered, A::Error> {
                let mut fields = Vec::new();
                while let Some(entry) = m.next_entry()? {
                    fields.push(entry);
                }
                Ok(Ordered(fields))
            }
        }
        d.deserialize_map(V)
    }
}

/// RFC 4180 cell: quoted when it holds a comma, quote or newline.
fn cell(v: &Value) -> String {
    let s = match v {
        Value::Null => return String::new(),
        Value::String(s) => s.clone(),
        Value::Number(_) | Value::Bool(_) => return v.to_string(),
        other => other.to_string(),
    };
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}

/// CSV with a header row; columns in struct order. An empty table is an
/// empty file (no rows, so no known columns).
fn to_csv<T: Serialize>(rows: &[T]) -> anyhow::Result<String> {
    let mut out = String::new();
    let mut header: Option<Vec<String>> = None;
    for row in rows {
        let Ordered(fields) = serde_json::from_str(&serde_json::to_string(row)?)?;
        let names: Vec<String> = fields.iter().map(|(k, _)| k.clone()).collect();
        match &header {
            None => {
                let _ = writeln!(out, "{}", names.join(","));
                header = Some(names);
            }
            Some(h) => anyhow::ensure!(*h == names, "CSV rows disagree on columns"),
        }
        let cells: Vec<String> = fields.iter().map(|(_, v)| cell(v)).collect();
        let _ = writeln!(out, "{}", cells.join(","));
    }
    Ok(out)
}

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> anyhow::Result<()> {
    std::fs::write(path, to_csv(rows)?).with_context(|| format!("writing {}", path.display()))
}

// ---- summary ------------------------------------------------------------------

const AGGREGATION: &str = "Per variant. scope=pooled: every observation from every repetition in \
    one sample, then nearest-rank percentiles (never an average of per-repetition percentiles). \
    scope=per_repetition: one value per repetition (n = repetitions); sd is the sample standard \
    deviation across repetitions. Units are in the metric name.";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SummaryRow {
    pub variant: String,
    pub metric: &'static str,
    pub scope: &'static str,
    #[serde(flatten)]
    pub stats: Summary,
}

fn summary(all: &Rows) -> Vec<SummaryRow> {
    let mut labels: Vec<&str> = Vec::new();
    for r in &all.runs {
        if !labels.contains(&r.key.variant.as_str()) {
            labels.push(&r.key.variant);
        }
    }
    let f = |v: Option<i64>| v.map(|x| x as f64);
    let mut out = Vec::new();
    for label in labels {
        let of = |k: &Key| k.variant == label;
        let wfs: Vec<_> = all.workflows.iter().filter(|w| of(&w.key)).collect();
        let acts: Vec<_> = all.activities.iter().filter(|a| of(&a.key)).collect();
        let fails: Vec<_> = all.failures.iter().filter(|x| of(&x.key)).collect();
        let runs: Vec<_> = all.runs.iter().filter(|r| of(&r.key)).collect();
        let mut pooled: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
        let mut put = |m: &'static str, v: Vec<f64>| {
            pooled.insert(m, v);
        };
        put(
            "workflow_latency_ms",
            wfs.iter()
                .filter(|w| w.state == "succeeded")
                .filter_map(|w| f(w.latency_ms))
                .collect(),
        );
        put(
            "fanout_ms",
            wfs.iter().filter_map(|w| f(w.fanout_ms)).collect(),
        );
        put(
            "timer_lateness_ms",
            wfs.iter().filter_map(|w| f(w.timer_lateness_ms)).collect(),
        );
        put(
            "activity_latency_ms",
            acts.iter().filter_map(|a| f(a.latency_ms)).collect(),
        );
        put(
            "scheduling_delay_ms",
            acts.iter()
                .filter_map(|a| f(a.scheduling_delay_ms))
                .collect(),
        );
        put(
            "planned_duration_ms",
            acts.iter().map(|a| a.planned_duration_ms as f64).collect(),
        );
        put(
            "attempts_per_activity",
            acts.iter()
                .filter(|a| a.job_id.is_some())
                .map(|a| a.attempts as f64)
                .collect(),
        );
        put(
            "detection_latency_ms",
            fails
                .iter()
                .filter_map(|x| f(x.detection_latency_ms))
                .collect(),
        );
        put(
            "reschedule_latency_ms",
            fails
                .iter()
                .filter_map(|x| f(x.reschedule_latency_ms))
                .collect(),
        );
        put(
            "recovery_latency_ms",
            fails
                .iter()
                .filter_map(|x| f(x.recovery_latency_ms))
                .collect(),
        );
        put(
            "lost_execution_ms",
            fails
                .iter()
                .filter_map(|x| f(x.lost_execution_ms))
                .collect(),
        );
        put(
            "coordinator_downtime_ms",
            fails.iter().filter_map(|x| f(x.downtime_ms)).collect(),
        );
        put(
            "first_progress_after_restart_ms",
            fails
                .iter()
                .filter_map(|x| f(x.first_progress_after_restart_ms))
                .collect(),
        );
        put(
            "queue_depth",
            all.samples
                .iter()
                .filter(|(k, _)| of(k))
                .filter_map(|(_, s)| s.queued)
                .collect(),
        );
        for (metric, values) in pooled {
            out.push(SummaryRow {
                variant: label.into(),
                metric,
                scope: "pooled",
                stats: summarize(values),
            });
        }

        type Get = fn(&analysis::RunRow) -> Option<f64>;
        let per_rep: [(&'static str, Get); 14] = [
            ("completion_rate", |r| Some(r.completion_rate)),
            ("throughput_wf_per_s", |r| r.throughput_wf_per_s),
            ("makespan_ms", |r| r.makespan_ms.map(|x| x as f64)),
            ("utilization", |r| r.utilization),
            ("idle_slot_ms", |r| r.idle_slot_ms.map(|x| x as f64)),
            ("wasted_execution_ms", |r| {
                Some(r.wasted_execution_ms as f64)
            }),
            ("extra_attempts", |r| Some(r.extra_attempts as f64)),
            ("activities_failed", |r| Some(r.activities_failed as f64)),
            ("workflows_failed", |r| Some(r.failed as f64)),
            ("queue_depth_max", |r| r.queue_depth_max),
            ("worker_kills_executed", |r| {
                Some(r.worker_kills_executed as f64)
            }),
            ("worker_kills_requested", |r| {
                Some(r.worker_kills_requested as f64)
            }),
            ("coordinator_kills_executed", |r| {
                Some(r.coordinator_kills_executed as f64)
            }),
            ("invariant_violations", |r| {
                Some(r.invariant_violations as f64)
            }),
        ];
        for (metric, get) in per_rep {
            out.push(SummaryRow {
                variant: label.into(),
                metric,
                scope: "per_repetition",
                stats: summarize(runs.iter().filter_map(|r| get(r))),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Row {
        z_first: &'static str,
        a_second: Option<i64>,
        nested: Value,
        ok: bool,
    }

    #[test]
    fn csv_keeps_struct_order_and_quotes_what_needs_it() {
        let rows = [
            Row {
                z_first: "plain",
                a_second: Some(3),
                nested: json!({"k": 1}),
                ok: true,
            },
            Row {
                z_first: "has, comma and \"quotes\"\nnewline",
                a_second: None,
                nested: Value::Null,
                ok: false,
            },
        ];
        let csv = to_csv(&rows).unwrap();
        let mut lines = csv.split('\n');
        assert_eq!(
            lines.next(),
            Some("z_first,a_second,nested,ok"),
            "declaration order, not alphabetical"
        );
        assert_eq!(lines.next(), Some("plain,3,\"{\"\"k\"\":1}\",true"));
        assert!(csv.contains("\"has, comma and \"\"quotes\"\"\nnewline\",,,false"));
        assert_eq!(to_csv::<Row>(&[]).unwrap(), "");
    }

    #[test]
    fn flattened_rows_serialize_flat() {
        let key = Key {
            variant: "v".into(),
            repetition: 2,
        };
        let s = Sample {
            t_ms: 5,
            queued: Some(1.0),
            running: None,
            retry_waiting: None,
            due_runs: Some(0.0),
        };
        let csv = to_csv(&[SampleRow {
            key: &key,
            sample: &s,
        }])
        .unwrap();
        assert_eq!(
            csv,
            "variant,repetition,t_ms,queued,running,retry_waiting,due_runs\nv,2,5,1.0,,,0.0\n"
        );
    }

    #[test]
    fn summary_pools_across_repetitions() {
        use analysis::WorkflowRow;
        let wf = |rep: u32, latency: i64| WorkflowRow {
            key: Key {
                variant: "v".into(),
                repetition: rep,
            },
            workflow_index: 0,
            workflow_id: None,
            state: "succeeded".into(),
            reason: None,
            submitted_at_ms: 0,
            started_at_ms: Some(0),
            finished_at_ms: Some(latency),
            latency_ms: Some(latency),
            fanout_ms: None,
            timer_lateness_ms: None,
            activities: 1,
            attempts: 1,
        };
        let mut all = Rows {
            workflows: (0..99)
                .map(|_| wf(1, 10))
                .chain((0..98).map(|_| wf(2, 10)))
                .chain([wf(2, 1000), wf(2, 1000)])
                .collect(),
            ..Rows::default()
        };
        let (plan, collected) = (
            plan::RunPlan {
                seed: 0,
                workflows: vec![],
            },
            analysis::Collected::default(),
        );
        for rep in [1, 2] {
            all.runs.extend(
                analysis::build(
                    &Key {
                        variant: "v".into(),
                        repetition: rep,
                    },
                    &plan,
                    &collected,
                )
                .runs,
            );
        }
        let rows = summary(&all);
        let lat = rows
            .iter()
            .find(|r| r.metric == "workflow_latency_ms")
            .unwrap();
        assert_eq!(
            (lat.scope, lat.stats.n, lat.stats.p99),
            ("pooled", 199, Some(1000.0))
        );
        let comp = rows.iter().find(|r| r.metric == "completion_rate").unwrap();
        assert_eq!((comp.scope, comp.stats.n), ("per_repetition", 2));
    }
}
