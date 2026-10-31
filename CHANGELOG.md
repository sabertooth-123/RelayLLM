# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [0.2.0] - unreleased

Durable multi-step LLM workflows on top of the v0.1 job queue
(design: `docs/WORKFLOWS_DESIGN.md`).

### Added
- **Workflows**: declarative definitions with `activity`, `parallel`
  (fan-out/fan-in, fail-fast), `sleep_ms` (durable timers) and
  `wait_signal` (one-shot durable latches). Earlier results are passed in
  with `inputs`. Each activity is an ordinary job, so leases, retries,
  fencing and cancellation apply unchanged.
- **Durable event history** per run (migration `0005`): append-only,
  contiguous `seq`, guarded by `CHECK`s, partial unique indexes and triggers.
  Every terminal job transition appends its workflow event **in the same
  transaction**. Fault-injection tests cover `complete`, `fail`, the reaper,
  direct cancel, fail-fast, signals and workflow cancel.
- **Pure core** (`relay-core::workflow`): definition validation, `fold`
  (rejects impossible histories with typed errors) and `decide`. No I/O and
  no clock, so replay is deterministic and completed activities are never
  rescheduled.
- **Decider task** in the coordinator: one task for all runs, an immediate
  first pass (restart recovery), commit nudges, and sleep until the next
  timer.
- **API**: `POST /v1/workflows`, `GET /v1/workflows/{id}` (per-step view),
  `GET .../history`, `POST .../signals/{name}`, `POST .../cancel`, with typed
  error codes. `RelayClient` methods; idempotent writes retry ambiguous
  failures.
- **CLI**: `relay workflow start|status|history|signal|cancel`, and
  **`relay demo-workflow`**: a research workflow through two hard
  coordinator kills, 18 checks (also a test).
- **Metrics**: `relay_workflow_runs{state}`,
  `relay_workflow_activity_attempts_total{outcome}`, `relay_workflow_due_runs`.
- `mock` and `chat` handlers accept `inputs`.

### Fixed during development
- A full queue no longer livelocks a fan-out wider than the free capacity:
  branches that fit are scheduled, and the rest a second later.

### Upgrade notes
- **Upgrade workers before starting workflows that use `inputs`**: a v0.1
  worker rejects the unknown payload field and fails the activity
  permanently.

## [0.1.0] - unreleased

First release: a fault-tolerant, durable job queue for LLM workloads.

### Added
- **Coordinator** (`relay coordinator`): Axum HTTP API over SQLite (WAL).
  Atomic submission with queue-capacity backpressure (`503 queue_full`).
  Idempotency keys with request fingerprints (`409 idempotency_conflict` on a
  reused key with a different request).
- **Workers** (`relay worker`): pull-based claiming with worker-generated
  `claim_id`s (a lost claim response never strands a job), semaphore-bounded
  concurrency, jittered backoff, and a pluggable `JobHandler` trait.
- **Fault tolerance**: leases and heartbeats; attempt-number fencing on every
  worker write (`409 stale_attempt`); an absolute per-attempt deadline that
  heartbeats can't extend; a batched, concurrency-safe lease reaper; a lease
  grace period on coordinator startup; a non-decreasing clock with forward-jump
  detection; and exponential retry backoff with jitter.
- **Cancellation** (`relay cancel`) with defined race semantics against completion.
- **Operations**: request timeout (`504`, documented as ambiguous), graceful
  shutdown with a drain deadline and SIGTERM handling, Prometheus `/metrics`,
  structured request logs, all-at-once config validation, `relay health`.
- **Handlers**: deterministic `mock` LLM; `chat` for OpenAI-compatible local
  endpoints (Ollama, llama.cpp, vLLM) with retryable/permanent error classification.
- **Durability setting** `--sqlite-synchronous full|normal` (default `full`).
- **Single-connection writer pool + read-only reader pool**, after the
  benchmark traced lost throughput to SQLite busy-handler polling. Measured
  gains: up to 9.8× under `normal`, 1.0–1.8× under `full`.
- **Live dashboard** (`dashboard/`, React + TypeScript + Vite): counts, a
  coordinator–worker network view, job table with filters, job inspector,
  event timeline, mock-job submission. Backed by new read-only endpoints
  `GET /v1/dashboard/snapshot` and `GET /v1/events` (SSE). Store operations
  publish an event after each committed transition, with a per-run `epoch`,
  contiguous `seq` and the transition's `from` state, so clients can detect
  duplicates, staleness and gaps and resync from the snapshot.
- **`--dev-controls`** (loopback only): `POST
  /v1/dev/workers/{id}/expire-leases` to simulate a worker failure.
- Migration `0004`: partial index on `finished_at` for "recently finished" queries.
- **Tooling**: `relay demo` (checked ten-step failure-recovery demo), `relay
  bench` (CSV/JSON output), PowerShell scripts for both, Dockerfile and Compose
  (1 coordinator + 3 workers).
- **Docs**: architecture (with SVG diagram), API reference, ADRs, a correctness
  review, test report, benchmark report.

### Known limitations
- Single coordinator: a single point of failure for *availability*, not durability.
- No authentication; trusted networks only. No TLS (the `chat` handler needs `http://`).
- At-least-once execution; handlers with side effects must be idempotent.
- With `--sqlite-synchronous full`, throughput is bounded by fsync latency:
  ~60–100 jobs/s on the benchmark machine (see `bench/README.md`).
- Idempotency keys never expire.
