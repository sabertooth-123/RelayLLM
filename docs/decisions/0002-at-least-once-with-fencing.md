# ADR 0002: At-least-once execution with attempt-number fencing

**Status:** accepted (Phase 0)

## Context
Workers crash, stall and get partitioned. The coordinator cannot tell a dead
worker from a slow one, so it must eventually give the job to someone else.
The slow worker may then come back.

## Decision
* Each claim increments `jobs.attempt`. The new value is the worker's
  **fencing token**.
* Every worker write (heartbeat, complete, fail) is conditional on
  `attempt = :token AND state = 'running'`. A stale token matches 0 rows and
  the API returns `409 stale_attempt`.
* We promise **at-least-once execution** and **at most one recorded result**.

## Alternatives rejected
* *Worker id as owner check.* A worker that restarts under the same id could
  match its own previous, expired attempt. A monotonic counter cannot repeat.
* *Exactly-once.* This is impossible when the handler calls an external system
  that doesn't take part in our transaction (e.g. a hosted LLM API).

## Consequences
Handlers with side effects must be idempotent themselves. `ARCHITECTURE.md` §6
documents this. Duplicate LLM calls can cost money; lease duration is a
trade-off between recovery speed and the chance of duplicate execution.
