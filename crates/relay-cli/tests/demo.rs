//! Requirement 14: the full demo works from a clean state. `relay demo`
//! exits 1 if any of its checks fail, so running it is the test.
#![allow(clippy::unwrap_used)]

use std::process::Command;

#[test]
fn the_end_to_end_demo_passes_every_check() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_relay"))
        .args(["demo", "--jobs", "30", "--workdir"])
        .arg(dir.path().join("demo"))
        .env_remove("RELAY_URL")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "demo failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("0 failed"), "{stdout}");
    for step in 1..=10 {
        assert!(
            stdout.contains(&format!("[{step}/10]")),
            "step {step} missing:\n{stdout}"
        );
    }
}
