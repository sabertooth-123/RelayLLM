# Durable workflows: design (v0.2)

Status: **implemented** (Phases B–I, 2026-10-30/31). Sections 0–9 are the
original design; where implementation changed a decision, §10a–§10h say
what and why (§4.3 was corrected in place). The code and tests follow
this document.

## 0. Summary

A **workflow run** is a durable, ordered **event history** plus an immutable
copy of its **definition**. A pure function in `relay-core` folds the
history into the run's state and returns the next commands. The coordinator
executes those commands and appends their events in **one SQLite
transaction**. Activities are **ordinary RelayLLM jobs**, so claims, leases,
heartbeats, fencing, retries and the reaper apply unchanged.

The idea that makes it small and crash-safe:

> **Every terminal job transition that belongs to a workflow appends its
> workflow event in the same transaction as the job update.** There is no
> window in which a job has succeeded but the workflow doesn't know.

There is no in-memory workflow state, so "replay" isn't a recovery mode.
Every decision, before or after a crash, reads the history from SQLite and
folds it. Recovery after a restart is just the first decision pass.

Execution guarantee, stated honestly: **an activity that has a recorded
`activity_completed` event is never executed again for that run.** An
activity that had not completed when something crashed may execute more than
once, exactly as an ordinary job can (at-least-once, ARCHITECTURE §6). Its
result is recorded at most once.

## 1. What exists and what is reused (audit)

Verified in code and tests, not only in the docs:

| Existing primitive | Where | Reused for |
|---|---|---|
| Job state machine `queued → running → {succeeded, retry_waiting, failed, cancelled}`, terminal states absorbing, enforced by `CHECK`s and triggers | `relay-core/job.rs`, migrations 0001/0003 | Activity lifecycle, unchanged |
| `BEGIN IMMEDIATE` claim / complete / fail / cancel / reap, each closing its `job_attempts` row in the same transaction | `store/claims.rs`, `cancel.rs`, `leases.rs` | The **hook points** where workflow events are appended (§5) |
| Attempt fencing (`409 stale_attempt`), idempotent report replay (`AlreadyApplied`) | `store/claims.rs` | Duplicate or stale activity completions; nothing new is needed |
| Job retries with jittered backoff, `max_attempts` | `relay-core/retry.rs`, `store::retry_at` | Activity retries (no second retry layer) |
| Reaper with immediate first pass + startup lease grace | `reaper.rs`, `leases.rs` | Recovering in-flight activities after a coordinator restart |
| Single writer connection + read-only pool | `store.rs` | The decider writes through the same writer |
| `Clock` / `MonotonicClock` with DB-derived floor / `ManualClock` | `relay-core/clock.rs` | Timer deadlines; deterministic timer tests |
| Queue capacity in the insert (`INSERT … WHERE count < cap`) | `store.rs` `INSERT_JOB` | Backpressure on activity scheduling |
| Idempotency keys + fingerprints | `relay-core/submission.rs` | Idempotent `POST /v1/workflows` |
| `EventBus` (publish after commit, `seq`/`epoch`) | `relay-server/events.rs` | Later: dashboard workflow view (not in v0.2) |
| DB-derived Prometheus gauges/counters | `metrics.rs` | Workflow metrics |
| `mock` handler (deterministic, scripted failures), `chat` handler sends `Idempotency-Key: <job id>` | `relay-worker` | Demo + tests; honest side-effect story |

Things that are true and matter here:

* The mock payload uses `deny_unknown_fields`. Passing step results into an
  activity therefore needs a small, explicit change to the handlers (§4.4).
* Job payloads are capped at 64 KiB. An aggregate step whose inputs exceed
  that must fail deterministically, not be truncated.
* `latest_timestamp()` (the clock floor at startup) only reads `jobs` and
  `workers`. It must also read workflow events, or a backwards clock step
  during downtime could schedule a timer "in the past".

## 2. Model and names

| Term | Meaning |
|---|---|
| **Workflow definition** | A JSON document: an ordered list of **steps**. It is not stored as a reusable object. Each run stores its own validated copy, so changing a definition can never alter a run already in flight. That makes definition versioning a non-issue for v0.2. |
| **Workflow run** (`workflow_runs` row) | One execution of a definition with an input. Has an id (UUIDv7), a coarse state and a due time for the decider. "Workflow" in the API means a run. |
| **Step** | One entry in the definition, identified by a unique `name`. Kinds: `activity`, `parallel`, `sleep`, `wait_signal`. |
| **Activity** | Work done by a worker: exactly one RelayLLM job. A `parallel` step contains several named activities. |
| **Workflow event** | One immutable row in the run's history with a per-run `seq`. |
| **Activity result** | The job's `result` column. It is not copied into the event: job rows are never deleted, and a succeeded job's result is immutable (`CHECK` + absorbing state). |
| **Timer** | A `sleep` step. Its deadline is computed once and persisted in `timer_scheduled`. |
| **Signal** | A named, durable external message to a run, recorded as `signal_received`. A `wait_signal` step completes once a signal of that name exists in the history. |

**`wait_signal` is a one-shot durable latch.** Each signal name can be
received at most once per run (enforced by `wf_signal_latch`), and it stays
set forever after. A signal that arrives before the run reaches its
`wait_signal` step is kept and satisfies the step later. v0.2 does **not**
support multiple messages under one name, signal queues/mailboxes, counting,
resetting a latch, or waiting for the same name twice: two `wait_signal`
steps with the same name are rejected at validation, because one delivery
would silently satisfy both. Richer signal models are future work and would
need new event types, not a reinterpretation of `signal_received`.

### 2.1 v0.2 definition format

```json
{
  "name": "research",
  "steps": [
    { "name": "plan", "activity": { "kind": "mock", "payload": { "prompt": "plan" } } },
    { "name": "research", "parallel": [
        { "name": "a", "kind": "mock", "payload": { "prompt": "topic a" } },
        { "name": "b", "kind": "mock", "payload": { "prompt": "topic b" } },
        { "name": "c", "kind": "mock", "payload": { "prompt": "topic c" } } ] },
    { "name": "aggregate", "activity": { "kind": "mock", "payload": { "prompt": "merge" },
                                         "inputs": ["research.a", "research.b", "research.c"] } },
    { "name": "cooldown", "sleep_ms": 30000 },
    { "name": "approval", "wait_signal": "approved" },
    { "name": "final", "activity": { "kind": "mock", "payload": { "prompt": "write" },
                                     "inputs": ["aggregate"] } }
  ]
}
```

Activity fields: `kind`, `payload` (validated with the ordinary job
`SubmissionLimits`), optional `max_attempts` and `timeout_secs` (the job's
own), and optional `inputs` (§4.4). Activity addresses are `step` or
`step.branch`.

Validation (all in `relay-core`, at `POST`, before anything is stored):

* step names, and branch names within a step, are unique and valid identifiers;
* `inputs` may only reference activities in *earlier* steps, which rules out
  cycles and forward references;
* `sleep_ms` is between 1 and 7 days;
* at most 64 steps and 16 branches per `parallel`;
* the whole definition is ≤ 64 KiB.

Not supported in v0.2: conditionals, loops, nested parallel groups, child
workflows, per-step workflow-level retries, signal timeouts.

## 3. Event history

### 3.1 Event types

| `type` | Columns / data | Appended by (same transaction as) |
|---|---|---|
| `workflow_started` | `{input}` | Run creation |
| `activity_scheduled` | `step`, `job_id` | Decider, together with the job `INSERT` |
| `activity_completed` | `step`, `job_id` | `complete()` applying a success |
| `activity_failed` | `step`, `job_id`, `{error}` | `fail()` going terminal, or the reaper failing the job on its last attempt |
| `activity_cancelled` | `step`, `job_id` | Workflow cancel, or an activity job cancelled directly |
| `timer_scheduled` | `step`, `{fire_at}` | Decider |
| `timer_fired` | `step` | Decider, once `now ≥ fire_at` |
| `signal_received` | `{name, payload}` | `POST …/signals/{name}` |
| `workflow_completed` | — | Decider |
| `workflow_failed` | `{reason}` | Decider |
| `workflow_cancelled` | `{reason}` | `POST …/cancel` |

Deliberately **absent**: `activity_started`. Claims, attempts, retries and
lease expiries already live in `job_attempts` for the activity's job. Writing
them into the workflow history too would duplicate state and put workflow
writes on the claim hot path. The history API joins them in for display.

### 3.2 Ordering and transactional guarantees

1. Every event is inserted in the same transaction as the state change it
   describes. If that transaction rolls back, so does the event.
2. `seq` starts at 1 and increases by exactly 1 per event within a run
   (`UNIQUE (run_id, seq)`, assigned as `MAX(seq) + 1` under the write lock,
   which the single writer connection makes race-free).
3. History is append-only, enforced by triggers that reject `UPDATE` and
   `DELETE` on `workflow_events`.
4. Within a run, `seq` order is commit order. Across runs, no order is
   promised.
5. At most one `activity_scheduled` per (run, activity), at most one terminal
   activity event per (run, activity), at most one `timer_*` of each kind per
   step, and at most one terminal workflow event per run. Each is a partial
   `UNIQUE` index, so a buggy decider fails loudly instead of corrupting
   history.

## 4. Execution: the decider

### 4.1 Pure core (`relay-core::workflow`)

```text
fold(definition, events) -> RunState          // pure, total
decide(definition, &RunState, now) -> Decision // pure
Decision { commands: Vec<Command>, wake_at: Option<Timestamp> }
Command  = ScheduleActivity{addr, payload} | ScheduleTimer{step, fire_at}
         | FireTimer{step} | CompleteRun | FailRun{reason} | CancelActivities{reason}
```

`fold` walks the steps in order: a step is complete when all its activities
have `activity_completed`, its `timer_fired` exists, or a matching
`signal_received` exists. Signals may arrive before the run reaches the step;
they wait in the history. The first incomplete step is the cursor. `decide`
looks at the cursor:

* `activity` / `parallel`: schedule every branch without `activity_scheduled`.
  If any branch has `activity_failed` or `activity_cancelled`, the commands
  are `CancelActivities` for the remaining branches, then `FailRun` (fail fast).
* `sleep`: with no `timer_scheduled`, schedule it with `fire_at = now +
  sleep_ms`. Otherwise, if `now ≥ fire_at`, fire it; if not, set `wake_at =
  fire_at`.
* `wait_signal`: nothing; wait for the signal (`wake_at = None`).
* no cursor (every step done): `CompleteRun`.

The decider applies the resulting events to the folded state and decides
again in the same transaction, so `timer_fired → schedule next activity`
takes one transaction, not two.

`now` is the only non-history input, and it is used only to compute
`fire_at`, which is persisted at once. Replaying the same history therefore
always yields the same state; a timer is never re-armed from a new "now".

### 4.2 Driving it (`relay-server`)

`workflow_runs.wake_at` is the durable "needs a decision" flag. A run is
**due** when `wake_at ≤ now`.

* Every transaction that appends an event a run must react to sets
  `wake_at = now`. That covers: run start, activity completed, failed or
  cancelled, signal received.
* The decider sets `wake_at` to the next timer deadline, or `NULL` when the
  run is waiting on activities or a signal, or once it is terminal.

`spawn_decider` mirrors `spawn_reaper`: one task, an **immediate first
pass**, then a batch of due runs per tick. Each due run is decided in its own
`BEGIN IMMEDIATE` transaction that re-checks `wake_at ≤ now`, loads the
definition, history and the results of referenced activities, runs
`decide`, applies the commands (job `INSERT`s, event `INSERT`s), sets the
new `wake_at` and state, then commits.

Latency doesn't depend on the tick: the transactions that set `wake_at`
also notify an in-memory `tokio::sync::Notify` after commit. The notify is
only a nudge; the durable `wake_at` is the source of truth, so a lost nudge
costs one tick, never correctness. There is no task per run, no task per
timer and no unbounded queue.

Tests call `store.decide_due(limit)` directly with a `ManualClock`, the way
the lease tests call `reap_expired`. No sleeps.

### 4.3 Scheduling an activity

The job is inserted through the existing capacity-checked `INSERT_JOB`, in
the decider's transaction, followed by `activity_scheduled{job_id}`. If the
queue fills up part-way through a fan-out, the decider **keeps the branches
already scheduled**, commits them, and sets `wake_at = now + 1 s` for the
rest. The run stays `running`: workflows obey the same backpressure as
clients and never fail because the queue is busy. (Phase C first rolled the
whole decision back. Phase E found that this livelocks a step with more
branches than free slots; see §10d.)

Activity jobs get no idempotency key. Uniqueness comes from the
`activity_scheduled` partial unique index plus the shared transaction: a
second schedule attempt fails the index, and its job `INSERT` rolls back with
it.

### 4.4 Passing results between steps

`inputs: ["research.a", "research.b"]` makes the decider add
`"inputs": {"research.a": <result>, …}` to the activity's payload when it
schedules it. Results are read inside the decision transaction from
succeeded jobs, which are immutable. If the merged payload exceeds the job
payload limit, the run fails with `inputs too large for step X`:
deterministic, and visible in the history.

The reserved address `input` refers to the run's own input
(`POST /v1/workflows {input}`), so the first step can be parameterised
without a separate templating feature. No step may be named `input`.

Handler changes: `mock` accepts an optional `inputs` object and folds it into
its deterministic output. `chat` appends the inputs as a context block after
the prompt. Both are additive.

**This mechanism is deliberately temporary.** Copying earlier results into
the next job's payload is the simplest thing that works, and it is bounded by
the job payload limit (64 KiB). Every copy is a duplicate of a result already
stored in `jobs.result`. A later version may replace it with a
content-addressed artifact/result store: activities would receive references
(hashes) and fetch the content, payloads would stay small, and large
intermediate results would be stored once. That store is **not** part of
v0.2. Nothing in this design depends on the copy staying, so it can be
swapped out without a history format change: `activity_completed` already
references the job, not the result bytes.

## 5. Coupling job outcomes to the history

A new internal helper, `workflow_hook(tx, job, outcome)`, runs inside each
existing terminal transaction, right before `COMMIT`:

| Existing path | Hook appends | Sets |
|---|---|---|
| `complete()` → `Applied` | `activity_completed` | `wake_at = now` |
| `fail()` → `Applied` with `failed` | `activity_failed` | `wake_at = now` |
| `fail()` → `retry_waiting` | nothing: still the same activity | — |
| `reap_expired()` → `failed` (last attempt) | `activity_failed` | `wake_at = now` |
| `reap_expired()` → `retry_waiting` | nothing | — |
| `cancel()` of an activity job | `activity_cancelled` | `wake_at = now` |

**Required fault-injection test (central invariant).** "A terminal activity
job transition and its workflow event commit atomically" is proven, not
assumed. As with the existing `a_completion_is_atomic_with_its_attempt_row`
test, a `BEFORE INSERT` trigger on `workflow_events` is made to `RAISE(ABORT)`
for one job's `activity_completed` (and, separately, `activity_failed` from
both `fail()` and the reaper). The test then asserts:

1. the report returns an error;
2. the job is still `running` at the same attempt, its `job_attempts` row is
   still open, and no `activity_*` terminal event exists, so neither half is
   applied;
3. after the trigger is dropped, the worker's retried report succeeds and
   both halves appear together.

It lands in Phase C, with the first hook.

The helper only sets `wake_at` while the run is `running`. When the decider
or a workflow cancel cancels outstanding activities, their
`activity_cancelled` events are appended inside the same transaction that
then makes the run terminal, so the `CHECK (state = 'running' OR wake_at IS
NULL)` holds at commit.

The helper looks up `activity_scheduled` by `job_id` (unique partial index),
so a plain job costs one indexed miss. `AlreadyApplied` replays, stale
reports (`409`) and rejected cancels never reach it: they don't commit a
transition, so they can't append an event. Duplicate completions are thereby
handled by the fencing that already exists.

## 6. Failure semantics

| # | Scenario | Behaviour |
|---|---|---|
| 1 | Worker crashes before running the activity | Ordinary job: lease expires → `retry_waiting` → another attempt. History unchanged until a terminal outcome. |
| 2 | Worker crashes during the activity | Same as 1. The activity runs again (at-least-once). |
| 3 | Worker performs an external side effect, then crashes before reporting | The activity is re-run; **the side effect can happen twice.** RelayLLM can't prevent this. The job id is stable across attempts and `chat` already sends it as `Idempotency-Key`, so a backend that honours it deduplicates. Documented, not hidden. |
| 4 | Activity succeeds, coordinator crashes during workflow advancement | Two sub-cases: (a) crash before the `complete()` commit, so neither job success nor event exists and the worker's retried report or the lease path handles it; (b) crash after the commit, so `activity_completed` and `wake_at = now` are durable and the decider's first pass after restart schedules the next step. There is no state in which the job succeeded but the history lacks it. |
| 5 | Coordinator crashes while a timer is pending | `fire_at` is in the history and `wake_at` in the run. After restart the first decider pass fires it if overdue, or waits until `fire_at`. It is never re-armed with a new deadline. |
| 6 | Coordinator crashes while waiting for a signal | Nothing is in flight. A signal acknowledged before the crash is in the history (its `200` is sent after commit). A signal whose request failed or got `504` is ambiguous; resending is safe (#8). |
| 7 | Duplicate activity completion | `AlreadyApplied` (same attempt replaying) or `409 stale_attempt`. No second event; the index would reject one anyway. |
| 8 | Duplicate signal | A signal name is a **latch**: the first delivery is recorded, and a repeat with the same payload gets `200 {"recorded": false}`. A repeat with a *different* payload gets `409 signal_conflict`, mirroring idempotency-key conflicts. Signals not declared by any `wait_signal` step get `422`. Signals to a terminal run get `409 workflow_finished`. |
| 9 | Stale worker completion | Fenced exactly as today (`409`). It never reaches the hook. |
| 10 | Workflow cancelled while an activity runs | One transaction: every non-terminal activity job is cancelled through the existing cancel path (`activity_cancelled` each), then `workflow_cancelled`, state `cancelled`, `wake_at = NULL`. The worker's next heartbeat or report gets `409` and aborts, as for any cancelled job. |
| 11 | Workflow cancel races an activity completion | Both take the write lock, so one commits first. If the completion is first, `activity_completed` is recorded, then the cancel still cancels the run. If the cancel is first, the job is cancelled and the completion gets `409`. Never both, never neither. Tested with the barrier pattern from `cancel.rs`, asserting both orders occur. |
| 12 | Retry exhaustion | The job goes terminal `failed` (via `fail()` or the reaper) → `activity_failed` in the same transaction → decider cancels any sibling branches and appends `workflow_failed{reason}`. |
| 13 | Activity job cancelled directly via `/v1/jobs/{id}/cancel` | `activity_cancelled` → the run fails ("activity cancelled"). No special API: the job endpoints stay uniform. |
| 14 | Queue full when scheduling | The run stays `running` and is retried after 1 s (§4.3). |
| 15 | Two deciders / concurrent decisions | Each decision re-checks `wake_at` under `BEGIN IMMEDIATE`. The unique indexes make a double schedule impossible even if that check had a bug. |
| 16 | Coordinator restart with activities in flight | Activities are ordinary running jobs: startup lease grace, then reaper semantics. Nothing workflow-specific happens until one reaches a terminal state. |
| 17 | Malformed or oversized definition | `422` at `POST`; nothing stored. |

## 7. Schema (migration 0005)

Two tables. No link table: the job ↔ activity link *is* the
`activity_scheduled` event. No timer table: a run has at most one pending
timer, and its deadline is in the history and in `wake_at`.

```sql
CREATE TABLE workflow_runs (
    id                  TEXT    PRIMARY KEY NOT NULL,
    name                TEXT    NOT NULL,          -- from the definition; listing and metrics
    definition          TEXT    NOT NULL CHECK (json_valid(definition)),  -- immutable copy
    input               TEXT    NOT NULL CHECK (json_valid(input)),
    idempotency_key     TEXT    UNIQUE,            -- idempotent start, same rules as jobs
    request_fingerprint TEXT    NOT NULL,          -- detects key reuse with a different request
    state               TEXT    NOT NULL CHECK (state IN ('running','succeeded','failed','cancelled')),
    wake_at             INTEGER,                   -- due for a decision when <= now; NULL = not due
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    finished_at         INTEGER,
    CHECK ((state = 'running') = (finished_at IS NULL)),
    CHECK (state = 'running' OR wake_at IS NULL)   -- terminal runs are never due
);
CREATE INDEX workflow_runs_due ON workflow_runs (wake_at) WHERE wake_at IS NOT NULL;

CREATE TABLE workflow_events (
    run_id   TEXT    NOT NULL REFERENCES workflow_runs (id),
    seq      INTEGER NOT NULL CHECK (seq >= 1),
    type     TEXT    NOT NULL CHECK (type IN ('workflow_started','activity_scheduled',
             'activity_completed','activity_failed','activity_cancelled','timer_scheduled',
             'timer_fired','signal_received','workflow_completed','workflow_failed',
             'workflow_cancelled')),
    step     TEXT,                                 -- activity address or step name
    job_id   TEXT    REFERENCES jobs (id),
    data     TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(data)),
    at       INTEGER NOT NULL,
    PRIMARY KEY (run_id, seq),
    CHECK ((type LIKE 'activity_%') = (job_id IS NOT NULL AND step IS NOT NULL))
);
CREATE UNIQUE INDEX wf_scheduled_once ON workflow_events (run_id, step) WHERE type = 'activity_scheduled';
CREATE UNIQUE INDEX wf_job_link       ON workflow_events (job_id)      WHERE type = 'activity_scheduled';
CREATE UNIQUE INDEX wf_activity_end   ON workflow_events (run_id, step)
    WHERE type IN ('activity_completed','activity_failed','activity_cancelled');
CREATE UNIQUE INDEX wf_timer_sched    ON workflow_events (run_id, step) WHERE type = 'timer_scheduled';
CREATE UNIQUE INDEX wf_timer_fired    ON workflow_events (run_id, step) WHERE type = 'timer_fired';
CREATE UNIQUE INDEX wf_signal_latch   ON workflow_events (run_id, json_extract(data, '$.name'))
    WHERE type = 'signal_received';
CREATE UNIQUE INDEX wf_run_end        ON workflow_events (run_id)
    WHERE type IN ('workflow_completed','workflow_failed','workflow_cancelled');
-- + triggers rejecting UPDATE/DELETE on workflow_events (append-only)
```

Why `state` is stored even though it is derivable from the history: listing,
metrics and the `CHECK`s need it without folding every run. It is written in
the same transaction as the terminal event, and a test asserts the two agree.

## 8. API (v0.2)

| Endpoint | Purpose |
|---|---|
| `POST /v1/workflows` | `{definition, input?, idempotency_key?}` → `201`/`200` + run view |
| `GET /v1/workflows/{id}` | State plus per-step status (pending / scheduled / running / retrying / done / failed), joined with job state |
| `GET /v1/workflows/{id}/history` | Events in `seq` order, each activity event enriched with its job's attempts |
| `POST /v1/workflows/{id}/signals/{name}` | `{payload?}` → `200 {recorded}`; `409`/`422` as §6 #8 |
| `POST /v1/workflows/{id}/cancel` | `200` + run view; `409 workflow_finished` if already succeeded or failed; idempotent if already cancelled |

No list endpoint yet: the demo and tests address runs by id. CLI: `relay
workflow start|status|history|signal|cancel`, plus `relay demo-workflow`.

## 9. Metrics (DB-derived, so they survive restarts)

* `relay_workflow_runs{state}`: gauge, current runs by state.
* `relay_workflow_activity_attempts_total{outcome}`: counter, finished attempts
  of jobs that belong to a workflow.
* `relay_workflow_due_runs`: gauge, runs due now. Growth means the decider
  is falling behind.

Rejected from the proposal:

* `workflow_replays_total`: every decision folds the history, so "replay"
  has no countable boundary.
* `workflow_duration_seconds`: a histogram needs a bucket policy, and an
  in-process histogram resets on restart. Add it when there's a real need.

## 10. Phases

| Phase | Delivers | Gate |
|---|---|---|
| B | Migration 0005; `relay-core::workflow` types, validation, `fold`/`decide` with unit tests (all §6 decisions as pure tests) | fmt, clippy, all tests |
| C | Store: create run, decider transaction, hook in `complete()`; sequential workflow end to end on `ManualClock` | + basic sequential test, + the §5 atomicity fault-injection test |
| D | Hook in `fail()`, reaper and `cancel()`; restart tests (after completion, with activities in flight); "no re-execution" asserted from `job_attempts` counts | crash-window tests |
| E | Parallel fan-out/fan-in, fail-fast; inputs passing + handler changes | |
| F | Timers + decider task (`spawn_decider`, first pass, `Notify`) | timer-survives-restart |
| G | Signals (latch, conflict, early arrival) | duplicate / restart tests |
| H | Workflow cancel; cancel/complete race (both orders observed); retry exhaustion | race tests |
| I | HTTP API, client, CLI, `relay demo-workflow` (real processes, coordinator killed mid-run), metrics, docs | demo is itself a test |

Benchmark check at the end of D: the hook adds one indexed lookup per
terminal job transition. Re-run `relay bench` and record the effect on
plain jobs.

## 10a. Notes from Phase B (implementation of the pure core)

* `decide` takes a `DecideContext { now_ms, results, max_payload_bytes }`.
  Payload assembly (including `inputs`) is in the pure core, so the "inputs
  too large → fail the run" decision lives with all the other decisions. A
  missing result for a completed activity is a `DecideError::MissingResult`
  (a coordinator bug), never a workflow failure.
* `ScheduleActivity` carries a `SubmitJobRequest`, so the store validates
  activity jobs with the ordinary submission path.
* `fold` enforces more than §3.2 listed. A run cannot end while an activity
  is still outstanding; nothing is scheduled after a failure; a timer must
  fire strictly in the future and may not fire before its deadline; `at_ms`
  is non-decreasing.
* **Consequence for Phase C (clock reads).** Today `complete()`, `fail()`,
  `cancel()` and `claim()` read `now` *before* `BEGIN IMMEDIATE`. Two
  transactions can therefore commit in the opposite order to their
  timestamps. Every transaction that appends workflow events must read the
  clock *after* it holds the write lock (the hook re-reads it), so event
  `at_ms` follows commit order. `latest_timestamp()` includes
  `workflow_events.at` and `workflow_runs.updated_at` in the startup clock
  floor, but **not** `wake_at`: that is a future deadline, and including it
  would push the clock up to a week ahead after a restart.

## 10b. Notes from Phase C (store + sequential execution)

* `relay-core::workflow::run` adds `StartWorkflowRequest` (validated into
  `NewWorkflow`, fingerprint = SHA-256 of canonical `{definition, input}`),
  `WorkflowId` and `WorkflowState`.
* ~~A decision that hits a full queue rolls back *entirely* (no partial
  fan-out).~~ Superseded in Phase E (§10d): it livelocks wide fan-outs.
* A run whose stored definition, history or decision is inconsistent is
  **quarantined**: nothing is appended, `wake_at = now + 60 s`, and an error
  is logged. It never advances on a guess.
* Until Phase D, only `complete()` is hooked. An activity that fails
  terminally or is cancelled leaves its run `running` with nothing due.
  Phase D closes this.

## 10c. Notes from Phase D (failure paths)

* All four terminal paths are hooked, each inside its own transaction:
  `complete()`, `fail()` when the job becomes `failed` (not on
  `retry_waiting`), `reap_expired()` on the last attempt, and `cancel()`.
  Each path has a fault-injection test proving the job change and the
  workflow event commit together or not at all. The reaper's whole batch
  rolls back, and the next pass recovers it.
* `cancel()` was split into `cancel_in_tx` (the fenced job change, no
  history) plus the public wrapper that adds the hook. The decider's fail
  fast uses `cancel_in_tx` directly and appends `activity_cancelled` itself,
  so those events take the decision's own `seq`. If the hook appended them,
  its `MAX(seq)+1` would collide with the decision's in-memory sequence.
* Fail-fast cancels running siblings with `last_error = "cancelled: another
  activity of its workflow failed"`. Their workers get `409` on the next
  heartbeat or report, exactly as for a client cancel.
* If the history says an activity is outstanding but its job is already
  terminal, the decider quarantines the run (§10b) instead of guessing.

## 10d. Notes from Phase E (parallel steps and inputs)

* **Fan-out livelock fixed.** All-or-nothing scheduling meant a step with N
  branches needed N free queue slots *at the same moment*. With
  `max_active_jobs` below N, or on a queue kept busy by other clients, the
  run would be deferred forever. The decider now commits the branches that
  fit and retries the rest in 1 s. This is safe because the fold already
  allows the remaining branches of the cursor step to be scheduled in later
  decisions, and fail-fast refuses to schedule after a failure. A test
  covers 3 branches with room for 2, and a mutation check confirms it fails
  under all-or-nothing.
* **Handlers.** `mock` accepts `inputs`. Its output adds `inputs_seen` (the
  sorted addresses) and `context_bytes`, and **does not echo the inputs**,
  so results don't snowball along a chain. `chat` appends a "Context from
  earlier steps" block, one `### address` section per input, to the last
  user message. It uses the result's `completion` text when there is one,
  compact JSON otherwise. `inputs` with no user message to attach to is a
  permanent error.
* **Version skew (operational).** A v0.1 worker rejects payloads with an
  `inputs` field (`deny_unknown_fields`), so it fails such activities
  *permanently*, which fails the workflow. Upgrade workers before starting
  workflows that use `inputs`. Activities without `inputs` are unaffected.
* Concurrent completions of sibling branches serialize on the write lock.
  Each gets its own contiguous `seq` (tested with three concurrent
  completions, plus two concurrent decision passes scheduling the fan-in
  exactly once).

## 10e. Notes from Phase F (timers and the decider task)

* `spawn_decider` (`relay-server/src/decider.rs`) is spawned by `relay
  coordinator` next to the reaper and aborted on shutdown. It is one task for
  all runs. Each loop it decides everything due (batches of 100, repeated
  while full), then sleeps until `MIN(wake_at)`, capped at 1 s
  (`DeciderConfig::max_sleep`), or until a nudge. The first pass runs
  immediately on spawn, which is the whole restart-recovery story: overdue
  timers fire, and runs made due before a crash are decided.
* Nudges are a `tokio::sync::Notify` on the `Store`, signalled after commit
  by `start_workflow` and by each terminal hook that actually touched a
  workflow (the hook now returns whether it did). Plain jobs never wake the
  decider. `notify_one` stores a permit, so a nudge during a pass isn't lost,
  and a lost one would only cost one capped sleep.
* Timer resolution in production is "at the deadline, or within one pass
  after it". A timer is never early: `fold` rejects `timer_fired` before
  `fire_at`. An overdue timer fires once, stamped with the actual firing
  time, not backdated. This is tested after a simulated hour of downtime.
* The cap bounds lateness if the wall clock jumps, because wake times are
  wall-clock milliseconds and the sleep is computed from them.

## 10f. Notes from Phase G (signals)

* `Store::signal_workflow(id, name, payload)` runs in one `BEGIN IMMEDIATE`
  transaction. It appends `signal_received`, sets `wake_at = now`, commits,
  *then* acknowledges and nudges the decider. A test makes the `wake_at`
  update fail and shows the event rolls back with it.
* Checks happen in this order, and the order is deliberate:
  1. payload size;
  2. run exists (`WorkflowNotFound`, `404` in Phase I);
  3. the name is awaited by some `wait_signal` step (`UnknownSignal`, `422`);
  4. latch already set: equal payload is `AlreadyReceived`, a different one
     is `SignalConflict` (`409`);
  5. only then, run finished (`WorkflowFinished`, `409`).

  Step 4 runs before step 5 so that a sender retrying after a lost response
  still gets success even if the run has since finished. A mutation test
  pins this order.
* Payload equality is JSON equality, so key order doesn't matter. This
  mirrors the request fingerprints for jobs.
* The one-shot rule is enforced twice. The application check gives callers
  the friendly outcomes, and the `wf_signal_latch` unique index refuses a
  second delivery even if that check were removed.
* Concurrent deliveries of one name serialize on the write lock. Exactly one
  is `Recorded`, equal payloads get `AlreadyReceived`, and different ones get
  `SignalConflict` (tested with three concurrent deliveries).

## 10g. Notes from Phase H (cancellation)

* `Store::cancel_workflow(id, reason)` runs in one `BEGIN IMMEDIATE`
  transaction. For every outstanding activity it calls `cancel_in_tx`
  (`last_error = "cancelled: its workflow was cancelled"`; its worker gets
  `409` and aborts), then appends one `activity_cancelled` per job and
  `workflow_cancelled{reason}` in the run's own sequence. It sets the run to
  `cancelled` with `wake_at = NULL` and commits. If any part fails, all of it
  rolls back: a fault-injection test shows the running job is still owned
  and can complete normally.
* The outcome is idempotent on a cancelled run (`AlreadyCancelled`).
  Succeeded or failed runs give `WorkflowFinished` (`409`), and an unknown
  run gives `WorkflowNotFound`. A pending timer simply never fires, and a
  pending signal wait rejects later signals with `WorkflowFinished`.
* **Cancel vs. completion race** (§6 #11). Tested over 30 rounds with a
  barrier and alternating spawn order. Workflow cancel won 15 and the
  completion won 15, so both orders are exercised (the test fails if either
  never happens). In every round the job state, its attempt row and the
  history agree with the winner, and nothing further is ever scheduled.
* **Cancel vs. a failure the decider hasn't seen yet**: retries exhausted,
  then cancel. The run ends `cancelled`, and the history shows
  `activity_failed` then `workflow_cancelled`.
* The core backs this up: `fold` rejects `workflow_cancelled` while any
  activity is outstanding. A cancel that forgot to cancel its jobs fails
  instead of committing a history with an orphaned running job (a mutation
  test shows this).

## 10h. Notes from Phase I (API, CLI, demo, metrics)

* **HTTP**: five routes (`relay-server/src/workflow_api.rs`) with typed
  codes: `invalid_request`, `invalid_workflow_id`, `workflow_not_found`,
  `idempotency_conflict`, `workflow_finished`, `unknown_signal`,
  `signal_conflict` and `signal_too_large`. Signal and cancel bodies are
  optional (empty = default). The history endpoint returns events only;
  per-attempt detail stays on `/v1/jobs/{id}/attempts` rather than being
  joined in (a deviation from §8's "enriched with its job's attempts",
  which would have duplicated that endpoint).
* **One source of limits.** The coordinator builds `StoreOptions.workflow`
  from its API `Limits`, so activities obey exactly the job submission
  limits (risk from §10b closed).
* **The step view** (`relay-core::workflow::view::build_view`) is pure and
  derived, never stored. It reads the run row, the folded history and the
  activity jobs' current states in one read transaction.
* **Client**: `start_workflow` generates an idempotency key when none is
  given, exactly like `submit`. A fault-injection test drops the first
  response after the commit and shows the retry returns the same run, and
  that an unkeyed raw retry would have started a second one.
* **`relay demo-workflow`**: 8 steps, 18 checks, two hard coordinator
  kills. It is a test (`crates/relay-cli/tests/demo_workflow.rs`). The
  definition, `crates/relay-cli/workflows/research.json`, is also the
  documented example.
* **Metrics** (§9): the three DB-derived metrics, tested against a run with
  a retried activity, a cancelled run, and a plain job that must not count.

## 11. Out of scope (v0.2)

User-code replay, a DSL, conditionals or loops, child workflows, cron,
versioning or migration of running definitions, multiple coordinators,
workflow-level retry policies, signal timeouts, dashboard redesign (the
`GET /v1/workflows/{id}` view is shaped so the dashboard can render it later).
