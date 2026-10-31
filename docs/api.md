# Coordinator HTTP API (v1)

Base URL defaults to `http://127.0.0.1:7070`. All bodies are JSON. There is
no authentication in v0.1 (see `ARCHITECTURE.md` §8).

Every non-2xx response has this body:

```json
{ "error": { "code": "queue_full", "message": "queue is full (10000 active jobs); retry later" } }
```

| `code` | HTTP | Meaning |
|---|---|---|
| `invalid_request` | 422 | Body parsed but failed validation (kind, payload, limits) |
| `bad_body` | 400 / 413 / 415 / 422 | Malformed JSON, too large, wrong content type, unknown field |
| `invalid_job_id` | 400 | Path id is not a UUID |
| `job_not_found` | 404 | No such job |
| `worker_not_registered` | 404 | Claim from an unknown worker id; `PUT /v1/workers/{id}` first |
| `idempotency_conflict` | 409 | Key already bound to a *different* request |
| `stale_attempt` | 409 | Report for an attempt that no longer owns the job; discard the result |
| `not_cancellable` | 409 | Cancel of a job that already succeeded or failed |
| `request_timeout` | 504 | Request exceeded the coordinator's request timeout. **Ambiguous**: the operation may still have been applied. Retry only idempotently (same idempotency key / claim id / report) |
| `queue_full` | 503 | Active-job capacity reached; has `Retry-After: 1` |
| `internal` | 500 | Server-side failure (details only in coordinator log) |

## `POST /v1/jobs`: submit

```json
{
  "kind": "mock",
  "payload": { "prompt": "Summarize the CAP theorem" },
  "idempotency_key": "order-42",
  "max_attempts": 3,
  "timeout_secs": 60
}
```

| Field | Required | Rules |
|---|---|---|
| `kind` | yes | 1–64 chars of `[a-z0-9_.-]`; selects the worker handler |
| `payload` | yes | JSON **object**, ≤ 64 KiB compact-serialized |
| `idempotency_key` | no | 1–128 printable ASCII chars, no spaces |
| `max_attempts` | no | 1–20, default 3 |
| `timeout_secs` | no | 1–3600, default 60 (per-attempt execution limit) |

Unknown fields are rejected, so a typo like `idempotencyKey` is never silently ignored.

Responses:
* `201 Created` with `{"created": true, "job": <JobStatus>}`: a new job.
* `200 OK` with `{"created": false, "job": <JobStatus>}`: the idempotency key
  matched an earlier submission of the **same** request (same kind, payload,
  max_attempts and timeout_secs; JSON key order doesn't matter). This
  succeeds even when the queue is full.
* `409 idempotency_conflict`: same key, different request. The original job is unchanged.
* `503 queue_full`.

The request body is limited to 256 KiB. Larger bodies are rejected before parsing.

**Retrying a submit.** If you get no response (timeout, dropped connection,
500/502/504), the job may or may not have been committed. Retry only with the
same `idempotency_key`. Without a key, a retry can create a duplicate.
`relay-client` and the CLI do this automatically. A key names one logical
job forever: replaying it after the job failed or was cancelled returns that
job. Use a new key to run the work again.

## `GET /v1/jobs/{id}`: status

`200` with a **JobStatus**. Never contains the payload or result.

```json
{
  "id": "01a17a1d-fe55-7673-8e96-6453ea931faa",
  "kind": "mock",
  "state": "queued",
  "attempt": 0,
  "max_attempts": 3,
  "idempotency_key": "order-42",
  "worker_id": null,
  "last_error": null,
  "created_at_ms": 1793050148437,
  "updated_at_ms": 1793050148437,
  "finished_at_ms": null
}
```

`state` ∈ `queued`, `running`, `retry_waiting`, `succeeded`, `failed`,
`cancelled`. Timestamps are milliseconds since the Unix epoch, taken from the
coordinator clock.

## `GET /v1/jobs/{id}/result`: result

```json
{ "id": "...", "state": "succeeded", "result": { "text": "..." }, "error": null }
```

* `200` when the job is terminal: `result` is set for `succeeded`; `error` is set for `failed`/`cancelled`.
* `202 Accepted` while the job is still `queued`/`running`/`retry_waiting`. Poll again later.

## `POST /v1/jobs/{id}/cancel`

No body. Returns `200` with the **JobStatus** (state `cancelled`), also if
it was already cancelled (cancelling is idempotent).

* `queued` / `retry_waiting` → `cancelled`; it is never claimed.
* `running` → `cancelled` at once, and the attempt is recorded as
  `cancelled`. The worker learns at its next heartbeat (`409`), aborts the
  handler, and its report, if any, gets `409 stale_attempt`.
* `succeeded` / `failed` → `409 not_cancellable`; the result stands.

**Race with completion.** Cancel and complete both take the database write
lock and check the job's current state, so exactly one wins:
cancel-then-complete gives `200` + `409 stale_attempt`, and
complete-then-cancel gives `200` + `409 not_cancellable`.
`tests/cancel.rs` runs the race 30 times with both orderings and asserts
that each happens.

A cancelled job keeps its idempotency key: resubmitting with the same key
returns the cancelled job. Use a new key to run the work again.

## `GET /healthz`

`200 {"status":"ok"}` when the coordinator can query its database.
(`relay health` exits 0/1 accordingly; the Docker healthcheck uses it.)

## `GET /metrics`

Prometheus text format (`text/plain; version=0.0.4`).

| Metric | Type | Source |
|---|---|---|
| `relay_jobs{state}` | gauge | database |
| `relay_attempts_total{outcome}` | counter | database (attempt rows are never deleted, so it survives restarts) |
| `relay_queue_capacity` | gauge | config |
| `relay_oldest_eligible_job_age_seconds` | gauge | database: how long the oldest *claimable* job has waited (jobs in backoff excluded). Rising = workers can't keep up |
| `relay_workers{status="live"\|"stale"}` | gauge | database: live = seen within 3 leases |
| `relay_http_requests_total{route,status}` | counter | in-process (resets on restart); `route` is the template, e.g. `/v1/jobs/{id}` |
| `relay_http_request_timeouts_total` | counter | in-process |

## Request limits

* Body ≤ 256 KiB (`413`), job payload ≤ 64 KiB, worker result ≤ 128 KiB (`422`).
* Every request must finish within `--request-timeout-secs` (default 10 s),
  including reading the body, or it gets `504 request_timeout`. A `504` is
  *not* "nothing happened": a database write queued behind a lock can commit
  after the timeout fired (`tests/ops.rs` demonstrates this). `relay-client`
  therefore treats 500/502/504 as ambiguous and retries only under
  idempotency.

## `GET /v1/dashboard/snapshot`: observer snapshot

`200` with everything a dashboard needs to (re)build its view. Read-only.
Counts, workers and jobs come from **one** read transaction, so they agree.

```json
{
  "epoch": "01a17ea8-…",          // this coordinator run; changes on restart
  "seq": 56,                      // taken BEFORE the reads below
  "server_time_ms": 1793126099084,
  "lease_ms": 30000,
  "max_active_jobs": 10000,
  "counts": [{ "state": "queued", "count": 0 }, …],   // all six states, exact
  "workers": [ /* as GET /v1/workers */ ],
  "jobs": [ /* JobSummary: active (≤ 500, oldest first), then the 100 most recently finished */ ],
  "active_jobs_truncated": false,
  "recent_events": [ /* up to 200 events with seq ≤ snapshot seq */ ],
  "dev_controls": false
}
```

A `JobSummary` has `id, kind, state, attempt, max_attempts, worker_id,
has_idempotency_key, last_error (≤ 300 chars), created_at_ms, updated_at_ms,
finished_at_ms, available_at_ms, lease_expires_at_ms`. It never contains the
payload, the result or the idempotency key itself.

## `GET /v1/events`: live event stream (SSE)

`text/event-stream`. Message names:

| `event:` | `data:` |
|---|---|
| `hello` | `{"epoch", "seq"}`: always first; `seq` is the last event published before this subscription |
| `job` | a job transition: `{"seq", "epoch", "at_ms", "type", "from", "job": JobSummary, …}` |
| `worker` | `worker_registered {worker}` or `worker_heartbeat {worker_id, job_id, attempt, lease_expires_at_ms}` |
| `ping` | `{}` every 5 s, so clients can detect a dead connection |
| `resync` | the subscriber fell > 1,024 events behind; the stream then ends |

Job `type`s and their transitions: `job_submitted` (`null` → `queued`),
`job_claimed` (`queued`/`retry_waiting` → `running`), `job_succeeded`,
`job_retry_scheduled` (`running` → `retry_waiting`), `job_failed`,
`job_cancelled`, `lease_expired` (`running` → `retry_waiting`/`failed`, plus
the `worker_id` that lost the job and a `reason`).

Guarantees and non-guarantees:

* Events are published only **after** their transaction commits, one per
  applied transition. Rejected, stale and idempotently replayed requests
  publish nothing.
* Within an epoch `seq` increases by exactly 1 per event.
* The stream is in memory: a restart starts a new epoch at `seq` 1, and a
  slow subscriber is dropped (`resync`). Two transactions that commit
  back-to-back can be *published* in the opposite order. Clients must check
  each transition's `from` against the state they hold and re-fetch the
  snapshot when it doesn't fit. `dashboard/src/state.ts` is a reference
  implementation.

Recommended client sequence: open the stream, wait for `hello`, fetch the
snapshot, apply buffered and live events with `seq` > snapshot `seq`.

## `POST /v1/dev/workers/{worker_id}/expire-leases` (development only)

Exists only when the coordinator runs with `--dev-controls`, which it refuses
on a non-loopback `--bind`; otherwise `404`. Ends the lease of every job the
worker is running and runs the reaper immediately, exactly as if the worker
had stopped heartbeating (`lease_expired` attempts, retry or fail). The
worker process is not touched; its next report for those attempts gets
`409 stale_attempt`.

```json
{ "worker_id": "alpha-01a17ea4-…", "expired_jobs": 2 }
```

## `GET /v1/jobs/{id}/attempts`: attempt history

`200` with one entry per claim, oldest first:

```json
[ { "attempt": 1, "worker_id": "w1-01a1...", "started_at_ms": 1793050148437,
    "finished_at_ms": 1793050148759, "outcome": "succeeded", "error": null } ]
```

`outcome` ∈ `succeeded`, `retryable_error`, `permanent_error`, `lease_expired`,
`cancelled`, or `null` while the attempt is running.

---

# Worker protocol

Workers are ordinary HTTP clients. The protocol can be implemented in any
language. The Rust worker in `crates/relay-worker` is one implementation.

```
PUT  /v1/workers/{worker_id}                       register (idempotent)
POST /v1/workers/{worker_id}/claims                 → 200 Assignment | 204
POST /v1/jobs/{id}/attempts/{attempt}/heartbeat     → 200 | 409 stale_attempt
POST /v1/jobs/{id}/attempts/{attempt}/complete      → 200 | 409 stale_attempt
POST /v1/jobs/{id}/attempts/{attempt}/fail          → 200 | 409 stale_attempt
```

Worker ids and claim ids: 1–128 characters from `[A-Za-z0-9_.-]`.

## `PUT /v1/workers/{worker_id}`: register

```json
{ "name": "box-a", "concurrency": 4, "kinds": ["mock"] }
```

The **worker** chooses its id (the Rust worker uses `<name>-<uuid>`, new per
process). Registration is an upsert, so a retry after a lost response can't
create a duplicate worker. `kinds` (1–32 distinct job kinds) limits what
claims return. Returns a **WorkerView**; `GET /v1/workers` lists them all:

```json
{ "id": "box-a-01a1...", "name": "box-a", "concurrency": 4, "kinds": ["mock"],
  "registered_at_ms": 1793050148000, "last_seen_at_ms": 1793050150000, "running_jobs": 2 }
```

## `POST /v1/workers/{worker_id}/claims`: claim one job

```json
{ "claim_id": "c-01a1..." }
```

* `200` with an **Assignment**: the oldest eligible job of a registered kind.
  ```json
  { "job_id": "...", "attempt": 1, "claim_id": "c-01a1...", "kind": "mock",
    "payload": { "prompt": "..." }, "timeout_ms": 60000,
    "lease_expires_at_ms": 1793050178437, "heartbeat_interval_ms": 10000 }
  ```
  Heartbeat every `heartbeat_interval_ms`. Use the interval, not
  `lease_expires_at_ms`: that timestamp is on the coordinator's clock, and
  comparing it to your own clock is exactly the clock-skew bug the protocol
  avoids.
* `204 No Content`: nothing eligible. Back off before polling again.
* `404 worker_not_registered`: register, then retry.

**Retry rule.** Generate a new `claim_id` for every claim. If a claim fails
*ambiguously* (timeout, dropped connection, 5xx), retry with the **same**
`claim_id`. If the first request did claim a job, the retry returns that same
assignment and no second job is taken. If the assignment has meanwhile
ended, the retry returns `204`. After any definite response, use a fresh
`claim_id`.

The claim (job update plus attempt-history row) is one `BEGIN IMMEDIATE`
transaction.

## `POST /v1/jobs/{id}/attempts/{attempt}/heartbeat`

```json
{ "worker_id": "box-a-01a1..." }
```

* `200` with `{ "lease_expires_at_ms": ..., "attempt_deadline_ms": ... }`. The
  lease becomes `min(now + lease, attempt_deadline)`, and is never shortened.
  `attempt_deadline = claim time + timeout + deadline grace` is fixed: a
  handler that hangs while its heartbeat thread keeps running is still
  recovered once the deadline passes.
* `409 stale_attempt`: this attempt no longer owns the job (its lease was
  reaped, or a newer attempt took over). **Stop working on it** and don't
  report; the Rust worker aborts the handler.
* If the coordinator is unreachable, keep working and keep heartbeating. Only
  the coordinator decides when a lease is gone. After a coordinator restart,
  running jobs get one full lease of grace to reconnect.

If no heartbeat arrives for a full lease, the reaper moves the job to
`retry_waiting` (with backoff) or `failed` (out of attempts), and records the
attempt as `lease_expired`.

## `POST /v1/jobs/{id}/attempts/{attempt}/complete`

```json
{ "worker_id": "box-a-01a1...", "result": { "completion": "..." } }
```

`result` is any JSON value ≤ 128 KiB. Accepted only if the job is `running`
at exactly `{attempt}` and owned by `worker_id`. The attempt number is the
fencing token. Returns the new **JobStatus**.

## `POST /v1/jobs/{id}/attempts/{attempt}/fail`

```json
{ "worker_id": "box-a-01a1...", "error": "upstream 503", "retryable": true }
```

* `retryable: true` and attempts remain → `retry_waiting`, claimable again
  after a backoff of `min(base·2^(attempt-1), max)` with jitter into `[d/2, d]`
  (coordinator flags `--retry-base-ms`, `--retry-max-ms`).
* Otherwise → `failed` (terminal). `error` is truncated to 4096 bytes.

The worker reports *what* happened; the coordinator decides whether the job
runs again.

**Retry rule for complete/fail.** Retrying the *identical* report for an
attempt is always safe. If it already landed, the response is `200`, even if
the job has since moved on to a newer attempt. `409 stale_attempt` means the
attempt lost ownership (a different outcome was recorded, or another attempt
owns the job). Discard the result.

A late report for an attempt that was already reaped gets `409`, even if no
newer attempt has started yet. The job has left `running`, and
`retry_waiting → succeeded` is not a legal transition.

If the coordinator refuses a completion because of its content (`422`
result too large, or `413` body too large), the Rust worker reports a
**permanent** failure instead. Re-running would produce the same result.

# Workflows

Durable multi-step runs (v0.2). Design: `docs/WORKFLOWS_DESIGN.md`. Every
activity is an ordinary job (visible under `/v1/jobs`), so claims, leases,
retries and fencing apply unchanged.

## `POST /v1/workflows`: start a run

```json
{ "definition": { "name": "research", "steps": [ ... ] },
  "input": { "topic": "rust" },          // optional; the "input" address
  "idempotency_key": "req-123" }          // optional; same rules as jobs
```

Step kinds (each step sets exactly one):

| Field | Meaning |
|---|---|
| `activity: {kind, payload, inputs?, max_attempts?, timeout_secs?}` | One job. Address = step name. |
| `parallel: [{name, kind, payload, inputs?, ...}, ...]` | 1–16 jobs scheduled together; the step is done when all succeed. Addresses = `step.branch`. If one fails, the in-flight others are cancelled and the run fails. |
| `sleep_ms: N` | Durable timer, 1 ms – 7 days. The deadline is computed once and survives restarts. |
| `wait_signal: "name"` | Wait until signal `name` is delivered (one-shot latch; may arrive early). |

`inputs` lists earlier activity addresses (or `"input"`). Their results are
added to the job's payload as `"inputs": {address: result}`. That is bounded
by the 64 KiB payload limit: exceeding it fails the run with a clear
reason. Limits: ≤ 64 steps, names `[A-Za-z0-9_-]{1,64}`, definition ≤ 64 KiB,
unknown fields rejected.

`201 {"created": true, "workflow": <view>}`, or `200` with `created: false`
when the key matched a run started with the same request. Errors: `422
invalid_request` (definition), `422 bad_body`, `409 idempotency_conflict`.

## `GET /v1/workflows/{id}`: run and step view

```json
{ "id": "...", "name": "research", "state": "running",
  "reason": null, "created_at_ms": 0, "updated_at_ms": 0, "finished_at_ms": null,
  "wake_at_ms": null,
  "steps": [
    { "name": "plan", "kind": "activity", "status": "done",
      "activities": [{ "address": "plan", "kind": "mock", "status": "succeeded",
                       "job_id": "...", "attempt": 1, "max_attempts": 3 }] },
    { "name": "research", "kind": "parallel", "status": "active", "activities": [ ... ] },
    { "name": "cooldown", "kind": "sleep", "status": "pending" },
    { "name": "approval", "kind": "wait_signal", "status": "pending",
      "signal": "approved", "signal_received": false } ] }
```

* `state`: `running`, `succeeded`, `failed` or `cancelled`. `reason` explains
  the last two.
* Step `status`: `pending`, `active`, `done`, `failed` or `cancelled`.
* Activity `status`: `pending` (no job yet), `queued`, `running`, `retrying`,
  `succeeded`, `failed` or `cancelled`.
* `fire_at_ms` appears on an armed `sleep`.

Errors: `400 invalid_workflow_id`, `404 workflow_not_found`.

## `GET /v1/workflows/{id}/history`

`{"id", "events": [{"seq", "at_ms", "type", ...}]}`, the append-only event
log in `seq` order: `workflow_started`, `activity_scheduled`,
`activity_completed`, `activity_failed`, `activity_cancelled`,
`timer_scheduled`, `timer_fired`, `signal_received`, `workflow_completed`,
`workflow_failed`, `workflow_cancelled`. Per-attempt detail lives on each
activity's job (`/v1/jobs/{job_id}/attempts`).

## `POST /v1/workflows/{id}/signals/{name}`

Optional body `{"payload": <any JSON>}` (empty body = `null`). Returns `200
{"recorded": true}` on first delivery, or `{"recorded": false}` when the same
payload was already delivered (a safe retry).

| Error | When |
|---|---|
| `404 workflow_not_found` | no such run |
| `422 unknown_signal` | no `wait_signal` step awaits this name |
| `409 signal_conflict` | already delivered with a *different* payload |
| `409 workflow_finished` | the run ended without this signal |
| `413 signal_too_large` | payload over 64 KiB |

## `POST /v1/workflows/{id}/cancel`

Optional body `{"reason": "..."}`. Cancels every outstanding activity job
(its worker gets `409` and aborts) and ends the run, in one transaction.
Returns `200` with the view. Idempotent on a cancelled run; `409
workflow_finished` on a succeeded or failed one.

## Metrics

All read from the database (exact, survive restarts):

| Metric | |
|---|---|
| `relay_workflow_runs{state}` | gauge, runs by state |
| `relay_workflow_activity_attempts_total{outcome}` | finished attempts of workflow jobs; `retryable_error` + `lease_expired` are activity retries |
| `relay_workflow_due_runs` | gauge, runs due for a decision now; growth means the decider is behind |

# Job kinds shipped with the worker

* `mock`: deterministic stand-in for an LLM; see `crates/relay-worker/src/mock.rs`.
* `chat`: OpenAI-compatible `/chat/completions` (Ollama, llama.cpp, vLLM, …).
  Enabled with `relay worker --openai-base-url http://host:port/v1
  --openai-model NAME [--openai-api-key-env VAR]`. Payload
  `{"prompt": ...}` or `{"messages": [...], "system"?, "model"?,
  "max_tokens"?, "temperature"?}`. Upstream 408/409/425/429/5xx and
  connection errors are retryable; other 4xx are permanent. Each request
  carries `Idempotency-Key: <job id>`. This build has no TLS, so endpoints must
  be `http://`.
