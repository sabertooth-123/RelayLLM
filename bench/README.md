# Benchmarks

Everything here was produced by real runs of `relay bench`. Nothing is
estimated. Raw data:

| Directory | Durability | Code |
|---|---|---|
| `results/1793102190042/` | `full` (default) | **current**: single-connection writer pool + read-only reader pool |
| `results/1793102341482/` | `normal` | **current** |
| `results/1793090036113/` | `full` | *before*: one shared 8-connection pool |
| `results/1793090214288/` | `normal` | *before* |

Each holds `environment.json`, `scenarios.csv/json` (one row per scenario per
repetition), `jobs.csv` (every job's raw measurements) and
`recovery.csv/json`. `experiments/` holds the investigation that led to the
pool split, including the hypothesis that turned out wrong.

Reproduce with `powershell -ExecutionPolicy Bypass -File scripts\bench.ps1`.

## What is measured

* **Workload:** 200 `mock` jobs per scenario, submitted as one burst by 16
  concurrent clients. The mock handler sleeps `delay_ms` (0, 20 or 100 ms).
  5% of jobs are scripted to fail once and succeed on retry.
* **Cluster:** a fresh coordinator and database per scenario; 1, 2 or 4
  worker processes with 4 slots each; lease 10 s.
* **Sources:** all latencies come from coordinator timestamps
  (`created_at` → attempt `started_at` → `finished_at`).
  * *Queue wait* is from submission to first claim. Because the jobs arrive
    as a burst, it measures how long a backlog takes to drain, not idle
    pick-up latency. Idle pick-up is reported separately below.
  * *Execution* is from claim commit to completion commit. It includes the
    handler's sleep plus the claim and complete round trips.
  * *Peak memory* is the coordinator's peak working set, read from the OS.
* **Repetitions:** 3 per scenario. The tables show the **median**, with
  min–max for throughput.

**These are queue-overhead numbers, not LLM inference numbers.**

## Environment

| | |
|---|---|
| OS | Windows 11 Home (10.0.26200), x86_64 |
| CPU | Intel64 Family 6 Model 183 (20 logical CPUs) |
| Build | `--release`, rustc 1.99.1 |
| Storage | SQLite file on local NTFS volume `D:` |
| Topology | client, coordinator and workers all on this one machine, sharing CPU and disk |

## Current results (writer/reader pool split)

### `--sqlite-synchronous full` (default: every commit is fsynced)

| workers × slots | job delay | completed/s median [min–max] | submitted/s | queue wait p50 / p95 | exec p95 |
|---|---|---|---|---|---|
| 1 × 4 | 0 ms | 81.2 [43.7–132.8] | 177 | 1180 / 1243 ms | 45 ms |
| 1 × 4 | 20 ms | 66.7 [56.8–72.9] | 319 | 1264 / 2030 ms | 69 ms |
| 1 × 4 | 100 ms | 27.7 [26.6–30.3] | 230 | 3072 / 5397 ms | 151 ms |
| 2 × 4 | 0 ms | 77.2 [50.2–78.1] | 171 | 1257 / 1858 ms | 80 ms |
| 2 × 4 | 20 ms | 61.7 [57.1–118.0] | 176 | 1387 / 1723 ms | 119 ms |
| 2 × 4 | 100 ms | 55.4 [44.3–58.6] | 251 | 1561 / 2486 ms | 167 ms |
| 4 × 4 | 0 ms | 64.8 [58.6–75.0] | 146 | 1484 / 1814 ms | 131 ms |
| 4 × 4 | 20 ms | 98.9 [49.6–105.5] | 218 | 887 / 981 ms | 111 ms |
| 4 × 4 | 100 ms | 66.0 [57.2–88.5] | 262 | 938 / 1753 ms | 219 ms |

### `--sqlite-synchronous normal` (fsync at checkpoints only)

| workers × slots | job delay | completed/s median [min–max] | submitted/s | queue wait p50 / p95 | exec p95 |
|---|---|---|---|---|---|
| 1 × 4 | 0 ms | 596.5 [540.5–623.4] | 6833 | 172 / 216 ms | 3 ms |
| 1 × 4 | 20 ms | 115.6 [112.8–120.7] | 7050 | 795 / 1457 ms | 34 ms |
| 1 × 4 | 100 ms | 34.0 [33.6–34.1] | 6856 | 2684 / 5149 ms | 117 ms |
| 2 × 4 | 0 ms | 637.9 [571.2–916.0] | 6981 | 114 / 151 ms | 10 ms |
| 2 × 4 | 20 ms | 202.6 [199.7–210.1] | 6706 | 454 / 782 ms | 38 ms |
| 2 × 4 | 100 ms | 64.8 [63.6–65.8] | 7078 | 1407 / 2608 ms | 119 ms |
| 4 × 4 | 0 ms | 735.4 [720.3–838.5] | 6845 | 94 / 131 ms | 15 ms |
| 4 × 4 | 20 ms | 350.1 [347.4–378.9] | 6277 | 228 / 418 ms | 37 ms |
| 4 × 4 | 100 ms | 118.2 [117.1–121.8] | 6678 | 713 / 1265 ms | 115 ms |

All 10,800 jobs succeeded (54 scenario runs × 200), and every injected
failure was retried successfully. Coordinator peak memory was 12.4–18.9 MB.

### Before → after (completed jobs/s, medians)

| | 0 ms jobs (1 / 2 / 4 workers) | 20 ms jobs | 100 ms jobs |
|---|---|---|---|
| `normal` before | 65 / 65 / 328 | 61 / 70 / 135 | 32 / 57 / 67 |
| `normal` after | **597 / 638 / 735** | **116 / 203 / 350** | **34 / 65 / 118** |
| `full` before | 45 / 50 / 52 | 42 / 61 / 71 | 26 / 38 / 59 |
| `full` after | **81 / 77 / 65** | **67 / 62 / 99** | **28 / 55 / 66** |

Submissions under `normal` went from an erratic 182–1,989/s (median per
scenario) to a steady **6,277–7,078/s**.

### Worker-kill recovery (lease 2 s, reaper every 0.5 s)

A worker running a 2 s job is hard-killed at a random point. 5 trials per
durability level; times are measured from the kill.

| | detected (lease expired) | re-claimed by another worker | job completed |
|---|---|---|---|
| `full` | median 2.03 s [1.48–2.21] | median 2.47 s [2.08–2.76] | median 4.48 s |
| `normal` | median 2.06 s [1.46–2.29] | median 2.20 s [1.78–2.85] | median 4.21 s |

These are unchanged by the pool split (before: 2.02 / 2.32 / 4.34 s for
`full`), as expected, since recovery is bounded by the lease, not by the
database. Detection is about one lease: the lease runs from the *last
heartbeat* (up to lease/3 before the kill), plus up to one reaper tick.
With the default 30 s lease, expect about 30–37 s to detection.

### Idle pick-up latency

The earliest queue wait in each run (workers idle-polling when the burst
arrived): median **40 ms** (`full`) and **61 ms** (`normal`), max 108 ms.

## What the numbers say

1. **Writer-lock contention, not SQLite, was the main limit under
   `normal`.** Giving writes their own single connection, so writers queue in
   sqlx's async FIFO and are woken immediately instead of sleep-polling in
   SQLite's busy handler, raised completions **1.1–9.8×** and made submission
   throughput steady at ~7,000/s. With 100 ms jobs, 1–4 workers now reach
   74–85% of the ideal (`jobs × delay / slots`).
2. **`full` improved too (1.0–1.8×), but fsync is still its ceiling:
   ~60–100 jobs/s on this machine.** Each job needs about 3 fsynced commits
   (submit, claim, complete). Extra workers don't raise this ceiling.
   *Correction:* an earlier version of this page said the experiment left
   `full` unchanged. That was true of the 4-worker scenarios it measured, but
   the full sweep shows gains with 1–2 workers.
3. **`normal` trades durability for speed.** In WAL mode an acknowledged write
   survives a coordinator crash, which is the failure model tested throughout.
   The last few writes can be lost on power loss or an OS crash.
4. **For the intended workload, the `full` ceiling is rarely the limit.** At
   ~70 jobs/s, `full` keeps about 70 × (job seconds) slots busy: around 350
   slots for 5 s LLM calls.
5. **What's left:** each worker still claims one job per round trip, and
   `full` throughput is noisy (e.g. 1×4 at 0 ms: 44–133/s), because fsync
   latency on this disk varies. Batch claiming would be the next lever;
   it isn't implemented.

## How the bottleneck was found (`experiments/`)

| Step | Change | Result |
|---|---|---|
| Hypothesis 1: idle-poll wake-up delay | (measurement only) | **Rejected.** First pickup median 35 ms, not ~1 s |
| Hypothesis 2: Nagle + delayed ACK | `TCP_NODELAY` on accepted sockets | **Rejected.** No measurable effect; reverted |
| Diagnosis | `experiments/inflight.py` replays `job_attempts` | Workers kept only 1.2–2.9 of 4 slots busy despite a backlog → slow claim cycle |
| Hypothesis 3: SQLite busy-handler polling | whole pool limited to 1 connection | **Confirmed.** 1.6–2.1× at 4 workers under `normal`; but reads would serialize too |
| Fix | 1-connection writer pool + read-only reader pool | Results above. A test proves reads stay fast while 12 writes queue behind a held lock, and a mutation check shows it fails if a read is routed to the writer |

The `environment.json` files in `experiments/` don't record the code change
each experiment made; this table does.

## Caveats

* One machine runs everything, so client, workers and coordinator compete for
  the same CPU and disk. A real deployment spreads them out.
* fsync cost is highly OS- and disk-specific. Linux numbers will differ;
  the release checklist has an item for it.
* Three repetitions show real spread, especially under `full`. Treat
  differences inside the min–max ranges as noise.
