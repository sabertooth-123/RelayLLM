# Test report: v0.1.0

## Latest run

| | |
|---|---|
| Command | `cargo test --workspace` (debug build) |
| Result | **138 passed, 0 failed**, 1 ignored (an `ignore`-marked doc example in `handler.rs`) |
| Wall time | 57 s (already compiled) |
| Lint | `cargo fmt --all --check` clean · `cargo clippy --workspace --all-targets -- -D warnings` clean |
| Dashboard | `npm test`: 13 reducer tests pass (vitest) · `npm run build`: `tsc --noEmit` clean, bundle builds · `npm audit`: 0 vulnerabilities |
| Dashboard, end to end | Real coordinator (`--dev-controls`, 6 s lease) + 3 real workers, driven in a browser: empty and unreachable states; 8 jobs submitted from the UI, each failing attempt 1 and succeeding on attempt 2 (UI counts and `seq` equal a fresh snapshot: 0 duplicates, 0 stale, 0 resyncs); dev lease expiry (2 jobs → retrying, worker shown idle); a worker process killed (shown *unavailable* after one lease, *disconnected* after three); page reload (state rebuilt from the snapshot); coordinator stopped (banner, every worker *unknown*) and restarted (new epoch, one resync, durable counts restored); 375 px width without horizontal scroll |
| Environment | Windows 11 Home 10.0.26200, Intel64 Family 6 Model 183 (20 logical CPUs), rustc/cargo 1.99.1 |
| Also run | `scripts/demo.ps1` (16/16 demo checks, then the suite: all pass) and `scripts/demo-compose.ps1` (15/15 checks against Docker Compose). Both under **Windows PowerShell 5.1** |

Not run: CI on GitHub Actions (no remote yet) and Linux. The workflow
in `.github/workflows/ci.yml` runs the same commands on Ubuntu and Windows.

## Inventory

| Test binary | Tests | What it covers |
|---|---|---|
| `relay-core` unit | 26 | state machine table, validation, fingerprints, retry backoff math, monotonic clock |
| `relay-server` unit | 5 | active-state list vs state machine; clock-jump detector; one writer connection + read-only reader pool; event bus `seq` contiguity, bounded ring, per-bus epoch |
| `relay-worker` unit | 4 | mock handler determinism and scripted failures; backoff jitter |
| `relay-cli` unit | 8 | config validation (all errors at once, warnings, `--dev-controls` needs loopback); benchmark percentiles |
| `relay-server/tests/store.rs` | 7 | persistence across reopen, idempotency, capacity, durability pragmas |
| `…/api.rs` | 6 | HTTP contract and error codes, restart over HTTP |
| `…/schema.rs` | 2 | the database rejects 16 impossible job states |
| `…/crash_windows.rs` | 3 | lost submit response (fault injection), wait across a restart |
| `…/claims.rs` | 16 | claim/complete/fail: fencing, idempotent retries, races, trigger-injected atomicity |
| `…/leases.rs` | 13 | heartbeats, deadline cap, reaper, concurrent reapers, startup grace (with a control) |
| `…/cancel.rs` | 6 | cancellation and its races with complete and claim |
| `…/ops.rs` | 4 | 504 timeout ambiguity, drain deadline, `/metrics`, reads stay fast while 12 writes queue behind a held lock |
| `…/workers_api.rs` | 2 | worker protocol over raw HTTP |
| `…/dashboard.rs` | 8 | one event per committed transition, in order, contiguous `seq`; nothing for rejected/stale/replayed calls; `lease_expired` matches the committed row; dev expiry scoped to one worker; snapshot hides payload/result/key; SSE `hello` → events → visible `ping`; dev route `404` unless enabled |
| `…/multi_worker.rs` | 12 | real `Worker` loops: concurrency, retries, lost responses, heartbeats, cancel, timeouts, oversized results |
| `relay-worker/tests/openai.rs` | 7 | `chat` handler against a fake endpoint: mapping, auth, error classification |
| `relay-cli/tests/cli_errors.rs` | 5 | real binary: invalid config/input, unusable DB path, unreachable coordinator |
| `relay-cli/tests/process_failures.rs` | 3 | real processes, hard kills: worker kill, coordinator kill + restart, long job via heartbeats |
| `relay-cli/tests/demo.rs` | 1 | the full `relay demo` (all checks) |

## The 14 required scenarios

| # | Requirement | Tests |
|---|---|---|
| 1 | Submitted job survives coordinator restart | `store::submitted_jobs_survive_store_restart`, `api::job_survives_coordinator_restart`, `process_failures::coordinator_crash_keeps_results_and_in_flight_work` |
| 2 | Same idempotency key → same logical job | `store::same_key_same_request_returns_existing_job`, `store::concurrent_submits_with_one_key_create_exactly_one_job`, `api::submit_returns_201_then_200_on_replay` |
| 3 | Reused key, different payload → deterministic | `store::same_key_different_request_is_a_conflict`, `api::reused_key_with_different_payload_is_409` |
| 4 | Multiple workers process jobs concurrently | `multi_worker::three_workers_share_the_queue_concurrently`, `claims::concurrent_claims_spread_jobs_without_duplicates` |
| 5 | Two workers can't both claim one attempt | `claims::concurrent_claims_assign_a_job_exactly_once`, `claims::replaying_a_claim_id_returns_the_same_assignment`, `claims::a_claim_is_atomic_with_its_attempt_row` |
| 6 | Killed worker's job recovered after lease expiry | `process_failures::killed_worker_job_is_recovered_by_another_worker`, `leases::an_expired_lease_requeues_the_job_with_backoff` |
| 7 | Stale worker can't commit after a newer attempt | `claims::a_stale_attempt_cannot_overwrite_a_newer_one`, `leases::a_worker_that_returns_after_expiry_is_fenced_out`, demo step 9 |
| 8 | Restart preserves results and recovers eligible jobs | `process_failures::coordinator_crash_keeps_results_and_in_flight_work`, `leases::startup_grace_lets_workers_reconnect_after_an_outage` (+ control `without_grace_an_outage_reaps_healthy_jobs`) |
| 9 | Retryable errors retried within the limit | `claims::retryable_failures_retry_until_attempts_are_exhausted`, `multi_worker::handler_failures_follow_the_coordinator_retry_policy` |
| 10 | Permanent errors / exhausted retries → terminal | `claims::permanent_failure_is_terminal_immediately`, `leases::expiry_on_the_last_attempt_fails_the_job`, `multi_worker::an_oversized_result_fails_the_job_instead_of_looping` |
| 11 | Queue capacity enforced | `store::capacity_is_enforced_but_replays_still_succeed`, `store::concurrent_submits_never_exceed_capacity`, `api::full_queue_is_503_with_retry_after` |
| 12 | Cancel/complete race → valid, documented outcome | `cancel::cancel_and_complete_race_has_exactly_one_winner` (asserts **both** outcomes occur), `cancel::cancel_and_claim_race_never_leaves_an_owned_cancelled_job`, `claims::complete_and_fail_race_has_exactly_one_winner` |
| 13 | Unavailable coordinator → no uncontrolled task creation | `multi_worker::unavailable_coordinator_causes_backoff_not_task_growth`, `multi_worker::worker_connects_once_the_coordinator_comes_up` |
| 14 | Full demo works from a clean state | `demo::the_end_to_end_demo_passes_every_check` (fresh temp directory), plus both demo scripts run end to end. *Caveat:* run from this working tree, not a fresh `git clone`, since no repository exists yet |

## How the tests earn trust

* **Failing first.** The schema-invariant and lost-submit-response tests were
  written and seen failing before their fixes (correctness review, 2026-10-27).
* **Mutation checks.** I broke the code on purpose and confirmed a test
  caught each break:
  * claim's two writes split into two transactions → `a_claim_is_atomic…` fails;
  * deadline cap removed from heartbeats → `heartbeats_extend_the_lease_but_never_past_the_deadline` fails;
  * workers heartbeating 10× too slowly → `heartbeats_keep_a_long_job_alive` fails;
  * a read routed to the writer pool → `reads_stay_fast_while_writes_queue_behind_a_lock` fails;
  * startup lease grace disabled in the real binary →
    `coordinator_crash_keeps_results_and_in_flight_work` fails in 3/3 runs.

  The last one first passed *for the wrong reason*. That was a race, fixed by
  making the startup reap pass run immediately. One mutation run was briefly
  invalid: a file restored with `Copy-Item` kept its old timestamp, so cargo
  didn't rebuild.
* **Both sides of every race.** The cancel/complete race test asserts that
  *each* outcome actually happened across 30 rounds. Its first version only
  ever exercised one.
* **Deterministic time.** Lease, backoff and deadline logic is driven by a
  `ManualClock`, not `sleep`. Real-clock tests use generous bounds.
* **Real failures.** Process tests use hard kills (TerminateProcess), and the
  Compose demo uses `docker kill` (SIGKILL). Lost responses are injected by
  aborting the connection after the commit.
* **Flake checks.** The timing-sensitive suites were repeated 3–5× after each
  phase (once under concurrent Docker-build load) with no failures.

## Not covered

* A real LLM server: the `chat` handler is tested only against a fake endpoint.
* Linux, and the declared MSRV 1.85 (see the release checklist).
* Power-loss durability: `full` vs `normal` relies on SQLite's documented
  semantics; no power was actually cut.
* Clock-jump handling end to end: the detector is unit-tested, not the reaper
  path with a real wall-clock step.
* Long soak / large queues: the largest runs are 200-job bursts (benchmarks)
  and 100-job demos.
