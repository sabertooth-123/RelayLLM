# Experiments

`relay experiment` runs seeded, reproducible experiments against real local
clusters (one coordinator process, N worker processes, the mock handler) and
writes raw data first, summaries second. It exists to give v0.2 a measured
baseline. It is not a benchmark of LLM inference, and nothing in RelayLLM
was changed or tuned for it.

```text
relay experiment run  <config.json> [--profile fast|full] [--out DIR] [--seed N] [--repetitions N] [--variants a,b]
relay experiment plan <config.json> [--profile P] [--variant V] [--repetition K] [--seed N]
```

`run` exits 1 if any invariant is violated or any run hits its timeout.
`plan` prints the seeded plan without running anything.

## Commands

Build in release first; debug builds print a warning and their numbers are
not representative.

```bash
cargo build --release -p relay-cli
```

The fast suite takes about 6 minutes on a laptop:

```bash
for e in lease_sweep failure_rate parallel_width durations coordinator_crash; do ./target/release/relay experiment run experiments/$e.json --profile fast; done
```

The full suite takes about 47 minutes. It uses 5 repetitions and larger workloads:

```bash
for e in lease_sweep failure_rate parallel_width durations coordinator_crash; do ./target/release/relay experiment run experiments/$e.json --profile full; done
```

Plots and Markdown tables (needs pandas and matplotlib):

```bash
python scripts/analyze_experiments.py experiments/results/<run-id> [more run dirs]
```

Results go to `experiments/results/<name>-<profile>-<unix-ms>/`, which is
gitignored.

## Architecture

| Module (`crates/relay-cli/src/experiment/`) | Role | Pure? |
|---|---|---|
| `config.rs` | file format, profiles/variants (RFC 7386 merge patch), validation | yes |
| `plan.rs` | seed → durations, crash targets, crash offsets; workflow definitions | yes |
| `runner.rs` | spawn processes, submit, inject failures, sample `/metrics`, collect via API | no |
| `analysis.rs` | observations → raw rows, metrics, invariant checks | yes |
| `stats.rs` | nearest-rank percentiles, pooled summaries | yes |
| `mod.rs` | CLI, metadata, CSV/JSON output, summary | — |

The harness reuses `cluster.rs` (the same process manager as `relay demo`
and `relay bench`). It observes RelayLLM only through the public HTTP API
and `/metrics`, the same way a user would. It never reads the database and
adds no benchmark-only code paths to the coordinator, the worker or the
store.

## Config format

A config file holds a base experiment plus optional `variants` and `profiles`.
Each variant and each profile is a JSON merge patch (RFC 7386) applied to the
base:

- An object is merged recursively.
- `null` deletes a key.
- Any other value replaces the existing one.

The profile patch is applied first, then the variant patch. A profile may
also replace the whole `variants` list. Unknown fields are rejected, and
validation reports every problem at once.

```jsonc
{
  "name": "lease_sweep", "description": "...",
  "seed": 20261031,            // master seed
  "repetitions": 3,
  "timeout_secs": 300,         // per repetition; then unfinished runs are recorded and the run fails
  "cluster": { "workers": 8, "concurrency": 1, "lease_secs": 5, "retry_base_ms": 200,
               "retry_max_ms": 2000, "max_active_jobs": 10000, "sqlite_synchronous": "full" },
  "workload": {
    "workflows": 12, "submit_interval_ms": 0,
    "shape": { "width": 4, "sleep_ms": 0, "join": false },   // parallel step "fanout" (width <= 16), optional sleep "pause", optional "join"
    "duration": { "distribution": "fixed", "ms": 3000 },      // | uniform {min_ms,max_ms} | lognormal {median_ms,sigma,max_ms}
    "max_attempts": 4
  },
  "failures": {
    "worker_crash":      { "probability": 0.25, "crash_point": 0.5, "restart_worker": true },
    "coordinator_crash": { "points": ["activities_running"], "downtime_ms": 1000, "observe_timeout_secs": 30 }
  },
  "variants": [ { "label": "lease-1s", "set": { "cluster": { "lease_secs": 1 } } } ],
  "profiles": { "fast": { "repetitions": 1, "workload": { "workflows": 4 } }, "full": { ... } }
}
```

Every workflow has the same shape:

1. a parallel step `fanout` with branches `b00..bNN`;
2. an optional `pause` sleep step;
3. an optional `join` activity whose `inputs` are all the branches.

Every activity is a `mock` job whose `delay_ms` comes from the plan.

## Reproducibility

### What the seed fixes

For each repetition `k`, the seed is `rep_seed = SplitMix64(seed + k·φ)`.
Two independent `fastrand` (wyrand) streams are derived from it:

- **The duration stream** draws one value per activity, in plan order.
- **The crash stream** draws one uniform value per activity, always, even at
  probability 0. An activity is a crash target when `u < probability`. Its
  worker is killed `round(duration × crash_point)` ms into attempt 1.

The same config, seed and repetition therefore give a byte-identical plan:
`<variant>/rep-<k>/plan.json`, which `relay experiment plan` reprints. Two
consequences follow from this design:

- **Common random numbers.** Variants that differ only in cluster or failure
  settings get identical durations. Raising the crash probability only adds
  targets; it never moves existing ones. A test checks both properties.
- **Shape changes reshuffle draws.** Variants that change the shape (width,
  join) draw a different number of values per workflow, so their durations
  differ.

### What the seed does not fix

OS scheduling, which worker claims which job, poll and claim timing, HTTP
latency, fsync time, and therefore every measured latency. Repetitions
measure that variance; the seed does not remove it.

### Metadata

`metadata.json` records:

- the run id and timestamps;
- the full config file and every resolved variant config;
- the per-repetition seeds;
- the git SHA and whether tracked files were dirty;
- the relay version and build profile;
- the OS, arch, CPU model and logical CPU count;
- the repetition order and the outcome.

### Run order

Repetitions run *repetition-major*: rep 1 of every variant, then rep 2, and
so on. This laptop's throughput drifts 30–40% between days. Interleaving
spreads that drift across variants instead of handing it all to whichever
variant runs last.

## Failure injection

All kills are real. `Child::kill` is SIGKILL on Unix and TerminateProcess on
Windows, so the process gets no drain and no cleanup. Every planned
injection produces exactly one row in `failures.csv`, whether or not it
happened. `requested_at_ms` (the harness decided to act) and
`executed_at_ms` (the process is confirmed dead) are separate columns.

### Worker crash

1. The harness polls the workflow view every 50 ms until attempt 1 of the
   target activity is running.
2. It reads the attempt's coordinator `started_at` and `worker_id`.
3. It sleeps until `started_at + after_ms`.
4. It re-checks `GET /v1/jobs/{id}`: the job must still be running, at
   attempt 1, on the same worker.
5. It kills that worker process. If `restart_worker` is set, it starts a
   replacement (`w3` → `w3r1`), so the slot count stays constant.

Outcomes:

- `executed`
- `skipped_attempt_finished`: the attempt ended before the crash point, or
  between polls.
- `skipped_attempt_never_ran`: the run ended or timed out first.
- `skipped_worker_unknown`: an earlier kill already took that process.
- `error`

### Coordinator crash

For each point in order, the harness polls every run's view until the
condition holds, then:

1. kills the coordinator;
2. waits `downtime_ms`;
3. restarts it on the same database;
4. waits until `/healthz` answers, which sets `restarted_at_ms`.

If the condition is not seen within `observe_timeout_secs`, or before every
run finishes, the outcome is `not_observed` and nothing is killed.

| point | condition (from `GET /v1/workflows/{id}`) |
|---|---|
| `activities_running` | some activity is `running` |
| `branches_partially_done` | an active parallel step has ≥ 1 succeeded and ≥ 1 unfinished branch |
| `timer_pending` | an active sleep step's `fire_at` is ≥ 1 s away |
| `decision_pending` | a running run's `wake_at` ≤ now (due but not yet decided); best effort, since the decider usually acts within milliseconds |

### Collateral damage

Killing a worker kills **every** attempt it is running. With
`concurrency > 1`, the attempts that were not targeted also lease-expire.
They appear in `attempts.csv` with `killed_by_injection = false` and
`wasted_is_upper_bound = true`. Experiments A and B use `concurrency: 1` so
that one kill hits exactly one attempt.

## Metric definitions

### Clocks

All times are Unix milliseconds. Two clocks are involved:

- **Coordinator clock:** history `at_ms`, attempt `started_at_ms` and
  `finished_at_ms`.
- **Harness clock:** submit time, `requested_at_ms`, `executed_at_ms`,
  `restarted_at_ms`.

Both read the same machine's system clock, so differences between them are
accurate to about 1 ms.

### Per workflow (`workflows.csv`)

- **`latency_ms`** = terminal event − `workflow_started`.
- **`fanout_ms`** = last `activity_completed` of a `fanout.*` branch − first
  `activity_scheduled` among them. Only set when every branch completed.
  This is the fan-out tail measure: with width *w* it is roughly the max of
  *w* draws.
- **`timer_lateness_ms`** = `timer_fired` − persisted `fire_at`.

### Per activity (`activities.csv`)

- **`scheduling_delay_ms`** = attempt 1 `started_at` − `activity_scheduled`.
  This is queue wait plus claim polling; idle workers back off up to 1 s.
- **`latency_ms`** = `activity_completed` − `activity_scheduled`.

### Per attempt (`attempts.csv`)

- **`execution_ms`**: time from `started_at` until the injected kill for
  killed attempts; otherwise `finished_at − started_at`.
- **`wasted_execution_ms`**: `execution_ms` for attempts that did **not**
  produce the activity's result (`lease_expired`, `retryable_error`,
  `permanent_error`, …); empty for the succeeded attempt and for unfinished
  ones.
- **`wasted_is_upper_bound`** is true when the end time used is the reaper's
  `finished_at` rather than an observed kill. In that case execution really
  stopped *at or before* it.

### Worker kill (`failures.csv`)

- **`lost_execution_ms`** = kill − attempt `started_at`. Equals the planned
  `after_ms` plus the poll lag.
- **`detection_latency_ms`** = killed attempt's `finished_at` (when the
  reaper expired it) − kill. Expected ≈ lease − time since the last heartbeat
  (heartbeats every lease/3), plus up to one reaper tick (lease/4, clamped to
  50 ms–5 s).
- **`reschedule_latency_ms`** = next attempt `started_at` − killed attempt
  `finished_at`. Includes the retry backoff and queueing behind other jobs.
- **`recovery_latency_ms`** = `activity_completed` − kill.

### Coordinator kill (`failures.csv`)

- **`downtime_ms`** = healthy again − kill. This is the configured downtime
  plus process start and the time `/healthz` takes to answer.
- **`first_progress_after_restart_ms`** = first history event or attempt
  start at or after the restart − restart.

### Per run (`runs.csv`)

- **`completion_rate`** = succeeded / workflows.
- **`makespan_ms`** = last terminal event − first `workflow_started`.
- **`throughput_wf_per_s`** = succeeded / makespan.
- **`extra_attempts`** = attempts − activities with an attempt. These are
  the repeated executions.
- **`wasted_execution_ms`** = the sum over attempts.
- **`busy_execution_ms`** = the sum of `execution_ms`.
- **`utilization`** = busy / (slots × makespan), where
  slots = workers × concurrency.
- **`idle_slot_ms`** = slots × makespan − busy.
- **`queue_depth_max`** and **`queue_depth_mean`** are taken over `/metrics`
  `relay_jobs{state="queued"}` samples every 250 ms, as in `samples.csv`. A
  sample taken while the coordinator is down is empty, not zero.
- **Permanent failures** are `activities_failed` and `failed`. The injector
  only ever kills attempt 1, so with `max_attempts ≥ 2` these are expected to
  be 0; a non-zero value is a finding.

### Invariants (`invariants.csv`)

These are checked per workflow on the collected history and attempts. Any
violation fails the run.

1. `seq_contiguous_and_time_monotone`: seq is 1, 2, 3, … and `at_ms` never
   decreases.
2. `each_activity_scheduled_once`: retries live inside the job, never as a
   second schedule.
3. `completed_activities_never_rerun`: no attempt starts after the
   activity's `activity_completed`.
4. `one_success_per_completed_activity`: fencing means exactly one attempt
   succeeds.
5. `timers_once_and_never_early`: one `timer_scheduled` and at most one
   `timer_fired` per sleep step, fired at or after `fire_at`.
6. `terminal_event_last_and_view_agrees`: a terminal event is the last
   event, and the view's state matches the history.

## Raw format and aggregation

The raw tables are `runs`, `workflows`, `activities`, `attempts`,
`failures`, `samples` and `invariants`. Each is an RFC 4180 CSV with a
header row, and the columns follow the struct order in `analysis.rs`. Every
row carries `variant` and `repetition`, and an empty cell means "not
applicable or not observed". The CSVs are rewritten after every repetition,
so an interrupted run keeps what finished.

`summary.csv` and `summary.json` give one row per (variant, metric) with a
`scope`:

- **`pooled`**: every observation from every repetition goes into one
  sample, then nearest-rank percentiles are taken. Percentiles of
  repetitions are never averaged, because an average of p99s is not a p99
  (a unit test shows the difference).
- **`per_repetition`**: one value per repetition; `sd` is the sample
  standard deviation (n − 1) across repetitions.

Nearest rank means the smallest value with at least p% of the sample at or
below it, with no interpolation, so every percentile is an observed value.
With n < 100, p99 is the maximum; read it with `n` beside it.

## The experiments

Each family includes a no-failure baseline variant.

| | config | varies | fixed | baseline |
|---|---|---|---|---|
| A | `lease_sweep.json` | lease 1/2/5/10/30 s | 8×1 slots, width 4, fixed 3 s, 25% of first attempts killed at 50% | lease 5 s, no kills |
| B | `failure_rate.json` | kill probability 5/10/25% | lease 2 s, 8×1 slots, width 4 + join, fixed 1 s | 0% |
| C | `parallel_width.json` | width 2/4/8/16 | 4×4 slots, join, fixed 500 ms | width 1 |
| D | `durations.json` | uniform 200–1800, lognormal (median 726, σ 0.8, cap 20 s) | equal mean ≈ 1000 ms, width 8, 4×4 slots | fixed 1000 ms |
| E | `coordinator_crash.json` | one kill at each of the 4 points, 1 s down | width 4 (uniform 0.5–3 s), 3 s sleep, join | no kill |

Notes:

- **Width 32 (C) is not run.** `WorkflowLimits` caps a parallel step at 16
  branches, and the harness does not change limits.
- **The lognormal choice (D):** heavy-tailed but with finite moments, the
  usual model for service times. Its median is set so the mean matches the
  fixed and uniform variants (726 × e^{σ²/2} ≈ 1000), and the cap keeps it
  under the mock's 60 s limit. Every generated duration is in
  `activities.csv` (`planned_duration_ms`).

## Baseline results (full profile, 2026-11-02)

The full profile has 23 variants × 5 repetitions = 115 runs and took
47 min on one Windows laptop (20 logical CPUs) in a release build. All
2,820 workflows succeeded. 957/957 planned worker kills and 20/20
coordinator kills executed. No run timed out, and there were **0
invariant violations**. Percentiles are pooled across the 5 repetitions.
The harness was uncommitted at the time (`git.dirty_tracked_files: true`
on 95c86b6), so the SHA alone does not identify the harness code.

**Every full profile runs saturated: offered work ≫ slots.** Queueing
dominates latency, which makes these results a different regime from the
fast profile. Read A in particular with that in mind.

| A, lease sweep (117 kills per variant) | detection p50 | reschedule p50 | recovery p50 | workflow p50 / p99 | throughput wf/s |
|---|---|---|---|---|---|
| baseline (5 s, no kills) | – | – | – | 18.2 / 36.3 s | 0.66 |
| 1 s | 0.97 s | 17.2 s | 21.1 s | 36.3 / 42.4 s | 0.57 |
| 2 s | 1.96 s | 16.2 s | 21.2 s | 36.3 / 42.4 s | 0.57 |
| 5 s | 4.12 s | 13.7 s | 21.1 s | 36.3 / 42.9 s | 0.57 |
| 10 s | 9.83 s | 7.9 s | 21.1 s | 36.2 / 45.5 s | 0.54 |
| 30 s | 30.8 s | 0.3 s | 34.2 s | 42.8 / 67.7 s | 0.37 |

- **Detection tracks the lease almost exactly.**
- **Under saturation, recovery is flat from 1 s to 10 s.** A shorter lease
  only moves the retried job to the back of a long FIFO queue sooner, so
  reschedule latency absorbs the difference.
- **Only 30 s exceeds the queue drain time and shows up end to end.**
- **Wasted execution is the same in every variant** (≈178 s total),
  because the kill offsets are planned, not lease-dependent.

| B, failure rate | kills | extra attempts | wasted s | workflow p50 / p99 | throughput wf/s |
|---|---|---|---|---|---|
| 0% | 0 | 0 | 0 | 23.5 / 25.6 s | 1.55 |
| 5% | 41 | 41 | 21.3 | 23.5 / 26.7 s | 1.49 |
| 10% | 90 | 90 | 46.8 | 24.0 / 28.1 s | 1.41 |
| 25% | 241 | 241 | 125.2 | 26.6 / 30.3 s | 1.32 |

- Every kill cost exactly one extra attempt.
- There were no permanent failures. That is expected, because only
  attempt 1 is ever killed.
- Throughput fell 15% at 25%.

| C, width (16 slots) | scheduling delay p50 | queue depth max | utilization | workflow p50 | throughput wf/s |
|---|---|---|---|---|---|
| 1 | 32 ms | 4 | 0.78 | 1.16 s | 12.2 |
| 2 | 503 ms | 24 | 0.88 | 2.06 s | 9.1 |
| 4 | 1.07 s | 64 | 0.87 | 3.20 s | 5.4 |
| 8 | 2.20 s | 144 | 0.91 | 5.88 s | 3.1 |
| 16 | 4.95 s | 304 | 0.94 | 11.1 s | 1.7 |

Scheduling delay grows roughly linearly with total queued work. With every
workflow submitted at once and FIFO claiming, wider fan-outs mainly mean a
longer queue.

| D, durations (mean ≈ 1 s) | generated p50 / p99 | fan-out p50 | makespan mean | idle slot time mean |
|---|---|---|---|---|
| fixed | 1.00 / 1.00 s | 10.3 s | 20.7 s | 6.7 s |
| uniform | 0.98 / 1.78 s | 10.8 s | 21.0 s | 16.6 s |
| lognormal | 0.69 / 4.14 s | 11.0 s | 22.3 s | 49.3 s |

At equal mean, the heavy tail costs about 7% in makespan and 7× the idle
slot time. Stragglers leave slots empty at the end of the run.

**E, coordinator crash.** All 20 kills executed, at all four points. Results:

- downtime p50 ≈ 1.52 s;
- first progress after restart: p50 26–182 ms, max 867 ms;
- 0 extra attempts and 0 wasted execution;
- workflow p50 within 0.5 s of the no-crash baseline.

Timer lateness stayed ≤ 24 ms everywhere except `decision-pending`, which
reached a max of 609 ms. In those runs the kill landed while a timer was
due, so the timer fired late after the restart, but exactly once and never
early.

## What these experiments DO NOT prove

- **Nothing about LLM serving.** The mock handler sleeps. There is no GPU,
  no token streaming, no model memory pressure, no provider rate limits.
  Real activity durations and failure modes differ.
- **Nothing about multi-machine deployments.** Everything runs on one
  machine: coordinator, workers, harness and SQLite share the CPUs and disk,
  and there is no network latency, partition, clock skew or packet loss.
- **Not absolute performance.** Numbers come from one laptop whose
  throughput drifts 30–40% day to day. Compare variants *within* one run
  (repetition-major order), not across runs or machines.
- **Not statistical significance.** The fast profile uses one repetition,
  and the full profile uses five. Tail percentiles of small samples are
  maxima. No confidence intervals are computed.
- **Not correctness in general.** The invariants are checked on the runs
  that happened. Passing them shows no violation was *observed* under these
  workloads, not that none can occur. Crash points are sampled by 50 ms
  polling, not exhaustively enumerated: `decision_pending` in particular is
  best effort, and in-transaction crash points (between a job update and its
  workflow event) are covered by the store's fault-injection tests, not
  here.
- **Not other failure kinds.** The harness covers process kills only. Disk
  full, slow or corrupt disks, hung (not dead) workers, a slow coordinator,
  and duplicate delivery from network retries are not exercised. Killing
  only attempt 1 means permanent-failure paths are not stressed.
- **Not optimal settings.** The sweeps describe how the current
  implementation behaves. They do not recommend a lease or a width, and the
  scheduling policy (FIFO claim, no resource awareness, no speculation) was
  deliberately left unchanged.
