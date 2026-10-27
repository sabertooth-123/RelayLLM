//! Invalid input and unavailable dependencies, from the outside: the real
//! binary must exit non-zero with a message a user can act on.
#![allow(clippy::unwrap_used)]

use std::process::{Command, Output};

fn relay(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_relay"))
        .args(args)
        .env_remove("RELAY_URL")
        .output()
        .unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn dead_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("http://{}", listener.local_addr().unwrap())
}

#[test]
fn coordinator_lists_every_config_error_and_exits_1() {
    let out = relay(&[
        "coordinator",
        "--max-active-jobs",
        "0",
        "--retry-base-ms",
        "9000",
        "--retry-max-ms",
        "10",
        "--request-timeout-secs",
        "999",
    ]);
    assert_eq!(out.status.code(), Some(1));
    let err = stderr(&out);
    assert!(err.contains("invalid coordinator configuration"), "{err}");
    for flag in [
        "--max-active-jobs",
        "--retry-base-ms",
        "--request-timeout-secs",
    ] {
        assert!(err.contains(flag), "{flag} missing from:\n{err}");
    }
}

#[test]
fn coordinator_reports_an_unusable_database_path() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("no-such-dir").join("relay.db");
    let out = relay(&[
        "coordinator",
        "--db",
        &db.display().to_string(),
        "--bind",
        "127.0.0.1:0",
    ]);
    assert_eq!(out.status.code(), Some(1));
    let err = stderr(&out);
    assert!(err.contains("opening database"), "{err}");
    assert!(
        err.contains("no-such-dir"),
        "the message must name the path: {err}"
    );
}

#[test]
fn worker_lists_every_config_error_before_connecting() {
    let out = relay(&[
        "--server",
        &dead_url(),
        "worker",
        "--name",
        "has space",
        "--concurrency",
        "0",
        "--openai-base-url",
        "https://api.example.com/v1",
        "--openai-model",
        "m",
    ]);
    assert_eq!(out.status.code(), Some(1));
    let err = stderr(&out);
    assert!(err.contains("invalid worker configuration"), "{err}");
    assert!(err.contains("--name/--concurrency"), "{err}");
    assert!(err.contains("no TLS"), "{err}");
}

#[test]
fn client_commands_explain_an_unreachable_coordinator() {
    let url = dead_url();
    let id = "01a17a1d-fe55-7673-8e96-6453ea931faa";
    for args in [vec!["status", id], vec!["health"]] {
        let mut argv = vec!["--server", url.as_str()];
        argv.extend(args.iter().copied());
        let out = relay(&argv);
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        let err = stderr(&out);
        assert!(
            err.contains("could not reach the coordinator"),
            "{args:?}: {err}"
        );
        assert!(err.contains(&url), "the message must name the URL: {err}");
    }
}

#[test]
fn malformed_client_input_is_rejected_locally() {
    let out = relay(&["--server", &dead_url(), "submit", "--payload", "{not json"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("--payload is not valid JSON"),
        "{}",
        stderr(&out)
    );

    let out = relay(&["status", "not-a-uuid"]);
    assert_eq!(out.status.code(), Some(2), "clap usage error");
    assert!(stderr(&out).contains("expected a UUID"), "{}", stderr(&out));

    let out = relay(&["submit", "--prompt", "a", "--payload", "{}"]);
    assert_eq!(out.status.code(), Some(2), "conflicting payload sources");
}
