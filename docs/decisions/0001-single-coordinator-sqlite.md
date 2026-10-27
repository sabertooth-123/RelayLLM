# ADR 0001: Single coordinator, SQLite as the source of truth

**Status:** accepted (Phase 0)

## Context
Jobs must survive coordinator restarts, and state changes must be atomic with
respect to concurrent workers. Options considered:

1. **SQLite owned by one coordinator.** Zero ops, ACID, file-based.
2. **PostgreSQL.** Supports multiple coordinators (`SELECT ... FOR UPDATE SKIP LOCKED`),
   but every user must run a database server first.
3. **Embedded Raft (multiple coordinators).** Removes the single point of
   failure, but most of the project would become a consensus implementation.

## Decision
SQLite with WAL + `synchronous=FULL`, accessed only by the coordinator.
Workers never touch the database; they go through the HTTP API.

## Consequences
* The coordinator is a single point of failure for *availability*, not for
  *durability*: a restart loses no acknowledged jobs.
* All cross-process correctness comes from conditional SQL statements, so it
  would carry over to Postgres if we ever add multi-coordinator support.
* SQLite allows one writer at a time. That suits LLM jobs, which take seconds
  each, but it limits the throughput of tiny jobs. Phase 5 benchmarks will
  measure the limit.
* The DB file must live on a local disk. Network and sync filesystems break
  SQLite locking.
