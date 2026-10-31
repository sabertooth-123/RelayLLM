//! The durable-workflow demo: two hard coordinator kills, every claim
//! checked. `relay demo-workflow` exits 1 if any check fails, so running it
//! is the test.
#![allow(clippy::unwrap_used)]

use std::process::Command;

#[test]
fn the_workflow_demo_survives_two_coordinator_crashes() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_relay"))
        .args(["demo-workflow", "--workdir"])
        .arg(dir.path().join("demo"))
        .env_remove("RELAY_URL")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "demo failed:
{stdout}
{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains(" 0 failed"), "{stdout}");
    for step in 1..=8 {
        assert!(
            stdout.contains(&format!("[{step}/8]")),
            "step {step} missing:
{stdout}"
        );
    }
}
