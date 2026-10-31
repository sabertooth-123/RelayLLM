# Test report: v0.1.0 + v0.2 workflows

## Latest run

| | |
|---|---|
| Command | `cargo test --workspace` (debug build) |
| Result | **248 passed, 0 failed**, 1 ignored (an `ignore`-marked doc example in `handler.rs`). v0.1 + dashboard: 138; v0.2 workflows: 110 |
| Wall time | 96 s (already compiled; includes both process-level demos) |
| Lint | `cargo fmt --all --check` clean · `cargo clippy --workspace --all-targets -- -D warnings` clean |
| Dashboard | `npm test`: 13 reducer tests pass (vitest) · `npm run build`: `tsc --noEmit` clean, bundle builds · `npm audit`: 0 vulnerabilities |
| Dashboard, end to end | Real coordinator (`--dev-controls`, 6 s lease) + 3 real workers, driven in a browser: empty and unreachable states; 8 jobs submitted from the UI, each failing attempt 1 and succeeding on attempt 2 (UI counts and `seq` equal a fresh snapshot: 0 duplicates, 0 stale, 0 resyncs); dev lease expiry (2 jobs → retrying, worker shown idle); a worker process killed (shown *unavailable* after one lease, *disconnected* after three); page reload (state rebuilt from the snapshot); coordinator stopped (banner, every worker *unknown*) and restarted (new epoch, one resync, durable counts restored); 375 px width without horizontal scroll |
| Environment | Windows 11 Home 10.0.26200, Intel64 Family 6 Model 183 (20 logical CPUs), rustc/cargo 1.99.1 |
| Workflow demo | `relay demo-workflow`: 18/18 checks, 17.4 s. Research workflow (plan → 3 parallel branches → aggregate → 8 s timer → final) through **two hard coordinator kills**: mid fan-out and during the timer. Completed activities ran once, the in-flight branch recovered, the retrying branch kept its retry, the timer kept its deadline and fired once, every activity was scheduled exactly once |
| Also run | `scripts/demo.ps1` (16/16 demo checks, then the suite: all pass) and `scripts/demo-compose.ps1` (15/15 checks against Docker Compose). Both under **Windows PowerShell 5.1** |

Not run: CI on GitHub Actions (no remote yet) and Linux. The workflow
in `.github/workflows/ci.yml` runs the same commands on Ubuntu and Windows.

## Inventory

| Test binary | Tests | What it covers |
|---|---|---|
| `relay-core` unit | 66 | state machine table, validation, fingerprints, retry backoff math, monotonic clock; **workflows (40)**: definition validation, `fold` over normal and impossible histories, `decide` (fan-out, fail-fast, timers, signals, inputs), replay determinism, event encoding, start requests, the step view |
| `relay-server` unit | 5 | active-state list vs state machine; clock-jump detector; one writer connection + read-only reader pool; event bus `seq` contiguity, bounded ring, per-bus epoch |
| `relay-worker` unit | 5 | mock handler determinism, scripted failures, `inputs` (deterministic, not echoed); backoff jitter |
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
| `relay-cli/tests/cli_errors.rs` | 5 | real binary: invalid config/input, unusable DB path, unreachable coordinator |
| `relay-cli/tests/process_failures.rs` | 3 | real processes, hard kills: worker kill, coordinator kill + restart, long job via heartbeats |
| `relay-cli/tests/demo.rs` | 1 | the full `relay demo` (all checks) |
| `relay-server/tests/workflow_schema.rs` | 4 | migration 0005 rejects 30+ impossible rows/histories; append-only; every uniqueness rule |
| `…/workflows.rs` | 11 | sequential run on ordinary jobs; restart after completion / with activity in flight; duplicate and stale reports append nothing; idempotent start; full-queue backoff; concurrent decision passes; isolation; corrupt-history quarantine; clock floor; **completion ↔ event atomicity (fault injection)** |
| `…/workflow_failures.rs` | 12 | retry inside the job; exhaustion via `fail()` and the reaper; lease recovery fences the old worker; direct job cancel; fail-fast cancels siblings; restart mid-retry; **atomicity of fail, reap, cancel and fail-fast (fault injection)** |
| `…/workflow_parallel.rs` | 7 | fan-out/fan-in; concurrent branch completions; restart mid fan-out; retrying branch; fan-out wider than capacity; oversized inputs; end to end on real worker loops |
| `…/workflow_timers.rs` | 8 | deadline; restart keeps it; overdue fires once; clock floor ignores deadlines; back-to-back timers; decider first pass, nudges, firing |
| `…/workflow_signals.rs` | 9 | gate; early signal; survives restart; idempotent retry vs. conflict; invalid signals; finished runs; concurrent deliveries; atomicity; nudge |
| `…/workflow_cancel.rs` | 7 | cancel mid fan-out; during sleep/signal wait; idempotency; **cancel vs. completion race (both orders observed)**; cancel vs. unseen failure; restart; atomicity |
| `…/workflow_api.rs` | 7 | HTTP contract and every error code; lost start response (fault injection); full run through the client with the decider and real workers; metrics |
| `relay-worker/tests/openai.rs` (+1) | 8 | … plus `inputs` as a context block |
| `relay-cli/tests/workflow_cli.rs` | 2 | every `relay workflow` subcommand against a real coordinator; bad definition file |
| `relay-cli/tests/demo_workflow.rs` | 1 | the full `relay demo-workflow` (two coordinator kills, all checks) |

## v0.2: the 15 required workflow tests

| # | Requirement | Tests |
|---|---|---|
| 1 | Basic sequential workflow | `workflows::a_sequential_workflow_runs_end_to_end_through_ordinary_jobs` |
| 2 | Parallel fan-out/fan-in | `workflow_parallel::branches_fan_out_together_and_fan_in_only_when_all_are_done`, `…the_research_workflow_runs_on_real_workers_with_the_mock_handler` |
| 3 | Coordinator restart after activity completion | `workflows::a_restart_between_completion_and_decision_resumes_without_rerunning`, demo step 4–5 |
| 4 | Coordinator restart with activities in flight | `workflows::a_restart_with_the_activity_in_flight_keeps_the_lease_semantics`, `workflow_parallel::a_restart_mid_fan_out_keeps_each_branch_where_it_was`, demo step 5 |
| 5 | Replay does not repeat completed activities | core `replay_never_reschedules_completed_activities`; `job_attempts` counts in tests 3–4 and the demo |
| 6 | Timer survives restart | `workflow_timers::a_timer_survives_a_restart_with_its_original_deadline`, `…an_overdue_timer_fires_once_on_the_first_pass_after_downtime`, demo step 6 |
| 7 | Signal survives restart | `workflow_signals::an_acknowledged_signal_survives_a_restart` |
| 8 | Duplicate signal behavior | `workflow_signals::a_repeated_signal_is_idempotent_and_a_different_one_conflicts`, `…concurrent_deliveries_set_the_latch_exactly_once` |
| 9 | Stale activity completion stays fenced | `workflow_failures::an_expired_lease_with_attempts_left_recovers_and_fences_the_old_worker`, `workflows::duplicate_and_stale_completions_append_nothing` |
| 10 | Activity retry | `workflow_failures::a_retryable_failure_retries_the_same_job_without_touching_the_history`, demo (branch c) |
| 11 | Retry exhaustion fails the workflow | `workflow_failures::exhausting_retries_fails_the_activity_and_then_the_run`, `…an_expired_last_lease_fails_the_activity_through_the_reaper` |
| 12 | Cancellation while waiting | `workflow_cancel::cancelling_a_sleeping_or_waiting_run_needs_no_jobs_and_stops_it` |
| 13 | Cancellation/completion race | `workflow_cancel::workflow_cancel_racing_activity_completion_has_exactly_one_winner` (15/15 split; fails if either order never occurs) |
| 14 | Workflow event ordering | seq/at checks in `workflows` sequential test, `workflow_parallel::concurrent_branch_completions_each_get_their_own_event`, core framing tests, demo step 8 |
| 15 | Two concurrent runs stay isolated | `workflows::two_runs_stay_isolated`, core `two_runs_of_one_definition_are_independent` |
| + | Central invariant: terminal job transition ↔ workflow event atomic | fault injection in `workflows`, `workflow_failures` (×4), `workflow_signals`, `workflow_cancel`; mutation-checked (a non-atomic hook fails it) |

## The 14 required scenarios (v0.1)

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
