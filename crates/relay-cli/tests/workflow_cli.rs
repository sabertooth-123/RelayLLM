//! `relay workflow ...` against a real coordinator process: every
//! subcommand, the `--json` output, and a bad definition file.
#![allow(clippy::unwrap_used)]

use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct Coordinator {
    child: Child,
    url: String,
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_coordinator(dir: &std::path::Path) -> Coordinator {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_relay"))
        .args([
            "coordinator",
            "--bind",
            &format!("127.0.0.1:{port}"),
            "--db",
        ])
        .arg(dir.join("relay.db"))
        .env_remove("RELAY_URL")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let url = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(20);
    while relay(&url, &["health"]).status.code() != Some(0) {
        assert!(Instant::now() < deadline, "coordinator did not come up");
        std::thread::sleep(Duration::from_millis(100));
    }
    Coordinator { child, url }
}

fn relay(url: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_relay"))
        .arg("--server")
        .arg(url)
        .args(args)
        .output()
        .unwrap()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

#[test]
fn every_workflow_subcommand_works_against_a_real_coordinator() {
    let dir = tempfile::tempdir().unwrap();
    let c = start_coordinator(dir.path());
    let def = dir.path().join("gated.json");
    std::fs::write(
        &def,
        json!({ "name": "gated", "steps": [
            { "name": "gate", "wait_signal": "approved" },
            { "name": "publish", "activity": { "kind": "mock", "payload": { "prompt": "p" }, "inputs": ["input"] } }
        ] })
        .to_string(),
    )
    .unwrap();

    let out = relay(
        &c.url,
        &[
            "--json",
            "workflow",
            "start",
            "--file",
            def.to_str().unwrap(),
            "--input",
            "{\"k\":1}",
        ],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let started: Value = serde_json::from_str(&stdout(&out)).unwrap();
    let id = started["workflow"]["id"].as_str().unwrap().to_owned();

    let out = relay(&c.url, &["workflow", "status", &id]);
    assert!(
        stdout(&out).contains("signal \"approved\" (not received)"),
        "{}",
        stdout(&out)
    );

    let out = relay(
        &c.url,
        &[
            "workflow",
            "signal",
            &id,
            "approved",
            "--payload",
            "{\"by\":\"cli\"}",
        ],
    );
    assert_eq!(stdout(&out).trim(), "signal \"approved\" recorded");
    let out = relay(
        &c.url,
        &[
            "workflow",
            "signal",
            &id,
            "approved",
            "--payload",
            "{\"by\":\"cli\"}",
        ],
    );
    assert!(
        stdout(&out).contains("already recorded"),
        "{}",
        stdout(&out)
    );
    let out = relay(
        &c.url,
        &["workflow", "signal", &id, "approved", "--payload", "2"],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("signal_conflict"));

    // No workers: "publish" stays queued until we cancel.
    let out = relay(
        &c.url,
        &["--json", "workflow", "cancel", &id, "--reason", "cli test"],
    );
    let view: Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(
        (view["state"].as_str(), view["reason"].as_str()),
        (Some("cancelled"), Some("cli test"))
    );

    let out = relay(&c.url, &["workflow", "history", &id]);
    let text = stdout(&out);
    for t in [
        "workflow_started",
        "signal_received",
        "activity_scheduled",
        "activity_cancelled",
        "workflow_cancelled",
    ] {
        assert!(text.contains(t), "{t} missing:\n{text}");
    }
}

#[test]
fn a_bad_definition_file_is_reported_without_contacting_the_coordinator() {
    let dir = tempfile::tempdir().unwrap();
    let def = dir.path().join("bad.json");
    std::fs::write(&def, r#"{"name": "w", "steps": [], "typo": 1}"#).unwrap();
    let out = relay(
        "http://127.0.0.1:9",
        &["workflow", "start", "--file", def.to_str().unwrap()],
    );
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("is not a valid workflow definition"), "{err}");
}
