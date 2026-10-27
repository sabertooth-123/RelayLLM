-- Phase 2: worker registry and idempotent claims.

CREATE TABLE workers (
    id             TEXT    PRIMARY KEY NOT NULL,   -- chosen by the worker process
    name           TEXT    NOT NULL,
    concurrency    INTEGER NOT NULL CHECK (concurrency >= 1),
    kinds          TEXT    NOT NULL                -- JSON array of job kinds
                       CHECK (json_valid(kinds) AND json_type(kinds) = 'array'),
    registered_at  INTEGER NOT NULL,
    last_seen_at   INTEGER NOT NULL
);

-- The worker-generated id of the claim request that created each attempt.
-- Retrying a claim with the same (worker_id, claim_id) finds this row and
-- returns the same assignment instead of claiming another job.
ALTER TABLE job_attempts ADD COLUMN claim_id TEXT;
CREATE UNIQUE INDEX job_attempts_claim ON job_attempts (worker_id, claim_id);

-- "Running jobs per worker" for GET /v1/workers.
CREATE INDEX jobs_worker ON jobs (worker_id);
