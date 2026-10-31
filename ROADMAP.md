# Roadmap

Each phase leaves the workspace building, formatted, Clippy-clean and tested.

## Phase 0: Plan ✅
- [x] Workspace layout, CI, `ARCHITECTURE.md`, ADRs
- [x] State machine, schema, API contract

## Phase 1: Durable single-node queue
- [x] Typed job model and state-transition table (unit tested)
- [x] SQLite schema and embedded migrations
- [x] Submit / get, with atomic queue-capacity check
- [x] Idempotency keys + request fingerprints (replay → same job, different request → 409)
- [x] Minimal HTTP API (`POST /v1/jobs`, `GET /v1/jobs/{id}`, `GET /v1/jobs/{id}/result`, `GET /healthz`)
- [x] CLI: `relay coordinator`, `relay submit`, `relay status`, `relay result [--wait]`
- [x] Tests: persistence across restart (store + HTTP), concurrent same-key submits, capacity

## Correctness review (2026-10-27) ✅
- [x] DB `CHECK` constraints for job invariants (`tests/schema.rs`)
- [x] Lost submit response: client retries safely under a generated idempotency key (`tests/crash_windows.rs`)
- [x] `result --wait` survives a coordinator restart
- [x] Absolute DB path + durability verified at startup
- [x] Phase 2/3 crash windows written into ARCHITECTURE §5.1

## Phase 2: Coordinator and workers ✅
- [x] `workers` table, idempotent registration (worker-chosen id, upsert)
- [x] Atomic claim endpoint returning `(job, attempt, lease)`; idempotent per `claim_id` (ARCHITECTURE §5.1 #1)
- [x] Claim/complete/fail + `job_attempts` row in one `BEGIN IMMEDIATE` transaction (§5.1 #4), atomicity tested with fault-injecting triggers
- [x] `relay-worker` crate: poll loop, `JobHandler` trait, semaphore-bounded concurrency, jittered backoff
- [x] Deterministic mock LLM handler (configurable delay, scripted retryable/permanent failures)
- [x] `relay worker`, `relay workers`, `relay attempts`
- [x] Integration tests with 3 workers; double-claim race; lost claim/complete responses end to end
- Pulled forward from later phases because the protocol needed them:
  - [x] Attempt fencing on complete/fail; `complete`/`fail` idempotent per attempt (§5.1 #2)
  - [x] Retryable vs. permanent failures, max attempts (backoff still pending)
  - [x] `job_attempts` history + `GET /v1/jobs/{id}/attempts`
  - [x] Worker-side execution timeout (handler aborted); graceful worker drain
  - [x] Unavailable-coordinator test (bounded worker tasks)

## Phase 3: Fault tolerance ✅
- [x] Heartbeats renewing leases (fenced; `409` makes the worker abort the handler)
- [x] Absolute per-attempt deadline that heartbeats can't extend past (§5.1 #3); migration 0003 + triggers
- [x] Reaper (batched, `IMMEDIATE`, concurrency-safe) + immediate startup pass after a lease grace period (§5.1 #5)
- [x] Non-decreasing coordinator clock with a DB-derived floor; forward-jump detection in the reaper (§5.1 #6)
- [x] Exponential backoff + equal jitter for `retry_waiting` (failures and reaped leases)
- [x] Failure tests: worker hard-kill and coordinator hard-kill/restart (real processes), expired lease, stale completion and heartbeat after expiry, concurrent reapers
- [x] Mutation-checked: disabling the startup grace, the deadline cap, or heartbeat cadence each makes a test fail

## Phase 4: Operational quality
- [x] Cancellation (`relay cancel`); cancel/complete race test asserting *both* outcomes occur; cancel/claim race; worker aborts on cancel
- [x] Server request timeout → `504 request_timeout` (ambiguous; proven by test); graceful-shutdown drain deadline; SIGTERM handling
- [x] `/metrics` (Prometheus text): durable job/attempt counts from the DB, worker liveness, oldest-eligible-job age, HTTP counters
- [x] Structured request logging (route templates, status, latency; never bodies)
- [x] Config validation listing every error at once (coordinator + worker); warnings for risky-but-legal settings
- [x] `relay health`, `relay cancel`
- [x] OpenAI-compatible `chat` handler (local endpoints): retryable vs. permanent classification, key from env var, `Idempotency-Key`
- [x] Worker turns a coordinator-rejected result (413/422) into a permanent failure instead of an endless lease-expiry loop
- [x] Tests: invalid input and unavailable dependencies (process-level), endpoint failure modes
- [x] Docker (multi-stage, non-root, 134 MB) + Compose (1 coordinator, 3 workers, healthcheck via `relay health`). Verified: SIGKILL of a busy worker container → job recovered on another worker (attempt 2, 18 s with a 10 s lease); `docker compose stop` → SIGTERM drain, in-flight job finished as attempt 1, all exit 0. The optional `ollama` profile is not exercised (it needs a model download).

## Phase 5: Release readiness
- [x] `relay demo`: ten checked steps on real processes; exits 1 on any failed check; itself a test (`tests/demo.rs`)
- [x] `scripts/demo.ps1` (native + test suite) and `scripts/demo-compose.ps1` (Docker, `docker kill`)
- [x] `relay bench` + `scripts/bench.ps1`: fresh cluster per scenario, repetitions, recovery trials, peak memory from the OS; CSV/JSON + environment in `bench/results/`; real runs only
- [x] Found and added `--sqlite-synchronous full|normal`, after the benchmark showed fsync bounds throughput
- [x] Test report, SVG architecture diagram + sequence diagrams, quick start
- [x] License (MIT OR Apache-2.0), CHANGELOG, crate metadata, `cargo package --workspace`, release checklist
- [ ] 👤 Repository, tag `v0.1.0`, GitHub release (see `docs/RELEASE_CHECKLIST.md`)

## v0.2: Durable workflows ✅ (2026-10-30/31; design `docs/WORKFLOWS_DESIGN.md`)
- [x] A: audit + design (stateless decider, same-transaction events, 2 tables)
- [x] B: migration 0005, pure `fold`/`decide`, impossible-history tests
- [x] C: store + decider transaction; atomicity fault injection
- [x] D: hooks in fail/reaper/cancel; fail-fast; benchmark check (no measurable hook cost)
- [x] E: parallel fan-out/fan-in, `inputs` in handlers; fan-out livelock fixed
- [x] F: durable timers + decider task (first pass, nudges)
- [x] G: signals (one-shot latch, conflict, early arrival)
- [x] H: workflow cancel; cancel/complete race exercised both ways
- [x] I: API, client, CLI, `relay demo-workflow`, metrics, docs
- Not done (v0.2 scope cuts): workflow list endpoint, dashboard workflow view, content-addressed result store, signal timeouts, versioning of running definitions

## Deliberately out of scope for v0.1
- Multiple coordinators / consensus (single coordinator is the documented SPOF)
- Authentication (trusted local network only)
- Web dashboard
- Idempotency-key expiry
