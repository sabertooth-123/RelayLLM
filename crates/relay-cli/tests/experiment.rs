//! `relay experiment` end to end on real processes: a clean run, a real
//! worker SIGKILL, a real coordinator SIGKILL + restart, the timeout path,
//! and an injection that can't happen being recorded as such.
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{Value, json};

fn config(name: &str, patch: Value) -> Value {
    let mut base = json!({
        "name": name, "seed": 42, "repetitions": 1, "timeout_secs": 60,
        "cluster": { "workers": 2, "concurrency": 1, "lease_secs": 2, "retry_base_ms": 100,
                     "retry_max_ms": 500, "max_active_jobs": 1000, "sqlite_synchronous": "normal" },
        "workload": { "workflows": 2, "submit_interval_ms": 0, "shape": { "width": 2, "join": true },
                      "duration": { "distribution": "fixed", "ms": 200 }, "max_attempts": 3 }
    });
    merge(&mut base, patch);
    base
}

fn merge(t: &mut Value, p: Value) {
    match (t, p) {
        (Value::Object(t), Value::Object(p)) => {
            for (k, v) in p {
                merge(t.entry(k).or_insert(Value::Null), v);
            }
        }
        (t, p) => *t = p,
    }
}

/// Run `relay experiment run`; returns the output and the run directory.
fn run(dir: &Path, config: &Value, extra: &[&str]) -> (Output, PathBuf) {
    let path = dir.join("config.json");
    std::fs::write(&path, config.to_string()).unwrap();
    let out_root = dir.join("results");
    let output = Command::new(env!("CARGO_BIN_EXE_relay"))
        .args(["experiment", "run"])
        .arg(&path)
        .arg("--out")
        .arg(&out_root)
        .args(extra)
        .env_remove("RELAY_URL")
        .output()
        .unwrap();
    let run_dir = std::fs::read_dir(&out_root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    (output, run_dir)
}

fn describe(o: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

/// Minimal RFC 4180 reader: rows of (column → cell).
fn csv(path: &Path) -> Vec<std::collections::BTreeMap<String, String>> {
    let text = std::fs::read_to_string(path).unwrap();
    let mut records: Vec<Vec<String>> = Vec::new();
    let (mut record, mut cell, mut quoted) = (Vec::new(), String::new(), false);
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, quoted) {
            ('"', true) if chars.peek() == Some(&'"') => {
                chars.next();
                cell.push('"');
            }
            ('"', _) => quoted = !quoted,
            (',', false) => record.push(std::mem::take(&mut cell)),
            ('\n', false) => {
                record.push(std::mem::take(&mut cell));
                records.push(std::mem::take(&mut record));
            }
            (c, _) => cell.push(c),
        }
    }
    let Some((header, rows)) = records.split_first() else {
        return Vec::new();
    };
    rows.iter()
        .map(|r| header.iter().cloned().zip(r.iter().cloned()).collect())
        .collect()
}

#[test]
fn a_clean_run_writes_every_raw_table_and_reproducible_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config("clean", json!({}));
    let (output, run_dir) = run(dir.path(), &cfg, &[]);
    assert!(output.status.success(), "{}", describe(&output));

    for f in [
        "runs.csv",
        "workflows.csv",
        "activities.csv",
        "attempts.csv",
        "invariants.csv",
        "samples.csv",
        "summary.csv",
    ] {
        assert!(
            run_dir.join(f).metadata().unwrap().len() > 0,
            "{f} is empty"
        );
    }
    assert_eq!(
        std::fs::read_to_string(run_dir.join("failures.csv")).unwrap(),
        "",
        "no injections planned"
    );
    let runs = csv(&run_dir.join("runs.csv"));
    assert_eq!(runs.len(), 1);
    assert_eq!(
        (
            runs[0]["succeeded"].as_str(),
            runs[0]["extra_attempts"].as_str()
        ),
        ("2", "0")
    );
    assert_eq!(
        csv(&run_dir.join("activities.csv")).len(),
        2 * 3,
        "2 workflows × (2 branches + join)"
    );
    assert!(
        csv(&run_dir.join("invariants.csv"))
            .iter()
            .all(|r| r["ok"] == "true")
    );

    let meta: Value =
        serde_json::from_str(&std::fs::read_to_string(run_dir.join("metadata.json")).unwrap())
            .unwrap();
    assert_eq!(meta["variants"][0]["resolved_config"]["seed"], 42);
    assert!(meta["variants"][0]["repetition_seeds"][0].is_u64());
    assert!(meta["environment"]["logical_cpus"].as_u64().unwrap() > 0);
    assert_eq!(meta["outcome"]["invariant_violations"], 0);
    assert!(meta.get("git").is_some());

    // The stored plan is exactly what `relay experiment plan` derives.
    let stored: Value = serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("default/rep-1/plan.json")).unwrap(),
    )
    .unwrap();
    let printed = Command::new(env!("CARGO_BIN_EXE_relay"))
        .args(["experiment", "plan"])
        .arg(dir.path().join("config.json"))
        .output()
        .unwrap();
    let printed: Value = serde_json::from_slice(&printed.stdout).unwrap();
    assert_eq!(printed["plan"], stored);
}

#[test]
fn a_worker_crash_is_a_real_kill_with_recovery_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        "worker",
        json!({
            "cluster": { "lease_secs": 1 },
            "workload": { "workflows": 1, "shape": { "width": 1, "join": false },
                          "duration": { "distribution": "fixed", "ms": 1500 } },
            "failures": { "worker_crash": { "probability": 1.0, "crash_point": 0.5 } }
        }),
    );
    let (output, run_dir) = run(dir.path(), &cfg, &[]);
    assert!(output.status.success(), "{}", describe(&output));

    let failures = csv(&run_dir.join("failures.csv"));
    assert_eq!(failures.len(), 1, "one planned crash, one record");
    let f = &failures[0];
    assert_eq!(
        (f["kind"].as_str(), f["outcome"].as_str()),
        ("worker_sigkill", "executed"),
        "{f:?}"
    );
    let lost: i64 = f["lost_execution_ms"].parse().unwrap();
    assert!(
        (700..1500).contains(&lost),
        "killed ~750 ms into the attempt, got {lost}"
    );
    let detection: i64 = f["detection_latency_ms"].parse().unwrap();
    assert!(
        (0..5_000).contains(&detection),
        "lease 1 s: detection {detection}"
    );
    assert!(f["recovery_latency_ms"].parse::<i64>().unwrap() > detection);

    let attempts = csv(&run_dir.join("attempts.csv"));
    let killed: Vec<_> = attempts
        .iter()
        .filter(|a| a["killed_by_injection"] == "true")
        .collect();
    assert_eq!(killed.len(), 1);
    assert_eq!(killed[0]["outcome"], "lease_expired");
    assert_eq!(killed[0]["wasted_execution_ms"], killed[0]["execution_ms"]);
    assert!(
        attempts
            .iter()
            .any(|a| a["attempt"] == "2" && a["outcome"] == "succeeded")
    );
    let runs = csv(&run_dir.join("runs.csv"));
    assert_eq!(
        (
            runs[0]["completion_rate"].as_str(),
            runs[0]["worker_kills_executed"].as_str()
        ),
        ("1.0", "1")
    );
}

#[test]
fn a_coordinator_crash_is_survived_without_reexecution_or_history_damage() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        "coordinator",
        json!({
            "workload": { "duration": { "distribution": "fixed", "ms": 2500 } },
            "failures": { "coordinator_crash": { "points": ["activities_running"], "downtime_ms": 500, "observe_timeout_secs": 20 } }
        }),
    );
    let (output, run_dir) = run(dir.path(), &cfg, &[]);
    assert!(output.status.success(), "{}", describe(&output));
    let f = &csv(&run_dir.join("failures.csv"))[0];
    assert_eq!(
        (f["kind"].as_str(), f["outcome"].as_str()),
        ("coordinator_sigkill", "executed"),
        "{f:?}"
    );
    assert!(f["downtime_ms"].parse::<i64>().unwrap() >= 500);
    let runs = csv(&run_dir.join("runs.csv"));
    assert_eq!(
        (
            runs[0]["succeeded"].as_str(),
            runs[0]["invariant_violations"].as_str()
        ),
        ("2", "0")
    );
    assert_eq!(
        runs[0]["extra_attempts"], "0",
        "workers outlive the coordinator; nothing reruns"
    );
    // Both coordinator lifetimes left their own log.
    assert!(
        run_dir
            .join("default/rep-1/cluster/coordinator-2.log")
            .exists()
    );
}

#[test]
fn a_timeout_is_reported_as_failure_and_every_process_is_cleaned_up() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        "timeout",
        json!({ "timeout_secs": 2, "workload": { "duration": { "distribution": "fixed", "ms": 30000 } } }),
    );
    let started = std::time::Instant::now();
    let (output, run_dir) = run(dir.path(), &cfg, &[]);
    assert!(
        !output.status.success(),
        "a timed-out run must not pass:\n{}",
        describe(&output)
    );
    assert!(
        started.elapsed().as_secs() < 30,
        "the harness waits for the timeout, not the jobs"
    );
    assert!(describe(&output).contains("hit the timeout"));
    let runs = csv(&run_dir.join("runs.csv"));
    assert_eq!(
        (
            runs[0]["timed_out"].as_str(),
            runs[0]["unfinished"].as_str()
        ),
        ("true", "2")
    );
    assert!(
        csv(&run_dir.join("workflows.csv"))
            .iter()
            .all(|w| w["state"] == "running")
    );
    // On Windows an open SQLite file can't be deleted: this only succeeds
    // if the coordinator and workers are really gone.
    std::fs::remove_dir_all(run_dir.join("default/rep-1/cluster")).unwrap();
}

#[test]
fn an_injection_that_cannot_happen_is_recorded_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    // No sleep step, so a pending timer never exists.
    let cfg = config(
        "unobservable",
        json!({ "failures": { "coordinator_crash": { "points": ["timer_pending"], "downtime_ms": 100, "observe_timeout_secs": 2 } } }),
    );
    let (output, run_dir) = run(dir.path(), &cfg, &[]);
    assert!(output.status.success(), "{}", describe(&output));
    let failures = csv(&run_dir.join("failures.csv"));
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["outcome"], "not_observed");
    assert_eq!(failures[0]["executed_at_ms"], "");
    let runs = csv(&run_dir.join("runs.csv"));
    assert_eq!(
        (
            runs[0]["coordinator_kills_requested"].as_str(),
            runs[0]["coordinator_kills_executed"].as_str()
        ),
        ("1", "0")
    );
}
