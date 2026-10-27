# Correctness review: 2026-10-27 (after Phase 1)

Scope: everything in the repo after Phase 1, plus the Phase 2/3 design in
`ARCHITECTURE.md`. The review focused on what happens when a process dies or
a packet is lost *between* two steps.

## Fixed in code (each has a test that failed before the fix)

| # | Finding | Severity | Failure scenario | Fix | Test |
|---|---|---|---|---|---|
| 1 | Schema accepted 12 of 14 impossible job states | High (latent) | A future claim/complete/reap bug writes e.g. `succeeded` with no result, or attempt 4 of 3. The row persists, and a restart "recovers" it as truth. | `CHECK` constraints in migration 0001 | `tests/schema.rs` |
| 2 | Lost submit response caused a duplicate job or a false error | High | Coordinator commits, then the connection drops before `201`. The client reports failure, the user resubmits, and the work runs twice (paid LLM call twice). | `relay-client` always sends an idempotency key (generated if absent) and retries ambiguous failures with it | `tests/crash_windows.rs::lost_submit_response_*` |

## Fixed in code (robustness; regression tests added)

| # | Finding | Fix | Test |
|---|---|---|---|
| 3 | `relay result --wait` aborted on the first refused connection during a coordinator restart | `RelayClient::wait_for_result` retries ambiguous errors until the deadline; 404 etc. still fail fast | `crash_windows.rs::waiting_for_a_result_survives_a_coordinator_restart` (new API, so this test was never red) |
| 4 | Relative `--db` silently created an empty DB when started from another folder | Absolute path, existing-file flag and per-state counts logged at startup | manual (CLI) |
| 5 | `synchronous=FULL` was claimed but never verified | `Store::durability()`; coordinator refuses to start otherwise | `store.rs::every_pooled_connection_is_wal_and_synchronous_full` (already green: verifies the claim) |

## Design flaws in the unbuilt Phase 2/3 (documented in ARCHITECTURE §5.1, added to ROADMAP)

1. Claim committed, response lost → attempt burned; repeated, the job is failed for nothing. Fix: idempotent claim via `claim_id`.
2. Completion committed, response lost → the retry gets a spurious `409`. Fix: `complete` is idempotent for the same attempt.
3. Hung handler with a live heartbeat task → job stuck forever. Fix: absolute per-attempt deadline.
4. Multi-row state changes split by a crash. Fix: one `BEGIN IMMEDIATE` transaction each.
5. Coordinator downtime longer than the lease → every in-flight job is requeued on restart and runs twice. Fix: startup lease grace period.
6. Wall-clock jumps expire leases early. Fix: non-decreasing clock + grace period.

## Checked and found correct

* **Concurrent same-key submits.** `UNIQUE` + `ON CONFLICT DO NOTHING` in one statement; tested with 16 tasks.
* **Capacity under concurrency.** Count and insert are in one write statement; tested with 20 tasks against capacity 5.
* **Autocommit write statements and `SQLITE_BUSY`.** A single write statement takes the write lock at its start, and SQLite invokes the busy handler when starting from no transaction, so the 5 s busy timeout applies. Only multi-statement *deferred* transactions are exposed (see design flaw 4).
* **Hard kill during a write.** SQLite commits are atomic; the WAL is replayed on the next open. Verified manually with `Stop-Process`.
* **Crash during migration.** sqlx runs each migration in a transaction, and SQLite DDL is transactional.
* **Replay while the queue is full** returns the existing job (tested).

## Accepted limitations (documented, not fixed)

* A *separate* CLI invocation without `--idempotency-key` is a new job by design.
* Fingerprints compare JSON as parsed: `1` vs `1.0`, or Unicode normalization differences, count as different requests.
* Idempotency keys are a single global namespace with no expiry.
* Graceful shutdown has no deadline, so a stalled client connection can delay it. Safe (Ctrl-C twice / kill is atomic) but slow. Phase 4 (timeouts).
* No request timeout on the server (slow-loris). Phase 4.
