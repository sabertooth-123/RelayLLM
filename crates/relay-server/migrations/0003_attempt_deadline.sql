-- Phase 3: absolute per-attempt deadline (ARCHITECTURE §5.1 #3).
--
-- A worker whose handler is stuck can keep heartbeating from a separate task,
-- so leases alone can't detect it. The deadline (claim time + job timeout +
-- grace) caps how far heartbeats may extend the lease.

ALTER TABLE jobs ADD COLUMN attempt_deadline INTEGER;

-- Jobs that were already running when this migration runs get a deadline
-- derived from their current lease.
UPDATE jobs SET attempt_deadline = lease_expires_at + timeout_ms WHERE state = 'running';

-- SQLite can't add a table-level CHECK with ALTER TABLE, so the invariant
-- "running <=> has a deadline" is enforced by triggers. The message keeps the
-- "CHECK constraint failed" prefix so it reads like the other invariants.
CREATE TRIGGER jobs_deadline_on_insert BEFORE INSERT ON jobs
WHEN (NEW.state = 'running') != (NEW.attempt_deadline IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'CHECK constraint failed: attempt_deadline is set iff state = running');
END;

CREATE TRIGGER jobs_deadline_on_update BEFORE UPDATE ON jobs
WHEN (NEW.state = 'running') != (NEW.attempt_deadline IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'CHECK constraint failed: attempt_deadline is set iff state = running');
END;
