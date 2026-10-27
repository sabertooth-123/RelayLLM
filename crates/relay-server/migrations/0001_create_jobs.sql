-- All timestamps are integer milliseconds since the Unix epoch, taken from the
-- coordinator's clock. Once v0.1 is released, never edit this file; add a new
-- migration instead (sqlx checksums applied migrations).
--
-- The CHECK constraints encode the job invariants from ARCHITECTURE.md §2.
-- They are the last line of defence: a buggy store operation fails at the
-- write instead of persisting a state that a restart would then "recover".

CREATE TABLE jobs (
    id                   TEXT    PRIMARY KEY NOT NULL,
    kind                 TEXT    NOT NULL,
    payload              TEXT    NOT NULL              -- JSON object
                             CHECK (json_valid(payload) AND json_type(payload) = 'object'),
    request_fingerprint  TEXT    NOT NULL,             -- SHA-256 hex of the canonical request
    idempotency_key      TEXT    UNIQUE,               -- NULL = no key; NULLs never collide
    state                TEXT    NOT NULL CHECK (state IN
                             ('queued', 'running', 'retry_waiting', 'succeeded', 'failed', 'cancelled')),
    attempt              INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),   -- fencing token
    max_attempts         INTEGER NOT NULL CHECK (max_attempts >= 1),
    timeout_ms           INTEGER NOT NULL CHECK (timeout_ms > 0),
    available_at         INTEGER NOT NULL,             -- earliest claim time
    worker_id            TEXT,
    lease_expires_at     INTEGER,
    result               TEXT                          -- JSON, set only on success
                             CHECK (result IS NULL OR json_valid(result)),
    last_error           TEXT,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL,
    finished_at          INTEGER,
    -- A running job always has an owner and a lease; nothing else does.
    CHECK ((state = 'running') = (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)),
    -- A result exists exactly when the job succeeded.
    CHECK ((state = 'succeeded') = (result IS NOT NULL)),
    -- Terminal exactly when finished.
    CHECK ((state IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL)),
    -- Claims never exceed the budget; queued means never claimed; a running
    -- or retrying job has been claimed at least once.
    CHECK (attempt <= max_attempts),
    CHECK (state != 'queued' OR attempt = 0),
    CHECK (state NOT IN ('running', 'retry_waiting') OR attempt >= 1)
);

-- Claim scans: "oldest eligible queued/retry_waiting job".
CREATE INDEX jobs_claimable ON jobs (state, available_at);
-- Reaper scans: "running jobs whose lease has expired".
CREATE INDEX jobs_lease ON jobs (state, lease_expires_at);

CREATE TABLE job_attempts (
    job_id       TEXT    NOT NULL REFERENCES jobs (id),
    attempt      INTEGER NOT NULL CHECK (attempt >= 1),
    worker_id    TEXT    NOT NULL,
    started_at   INTEGER NOT NULL,
    finished_at  INTEGER,
    outcome      TEXT CHECK (outcome IN
                     ('succeeded', 'retryable_error', 'permanent_error', 'lease_expired', 'cancelled')),
    error        TEXT,
    PRIMARY KEY (job_id, attempt)
);
