-- v0.2: durable workflows (docs/WORKFLOWS_DESIGN.md §7).
--
-- A run is an immutable definition plus an append-only event history.
-- Activities are ordinary rows in `jobs`; the link between a job and its
-- activity is the `activity_scheduled` event, not a separate table.
-- As in 0001, the constraints are the last line of defence: a buggy decider
-- must fail at the write instead of persisting an impossible history.

CREATE TABLE workflow_runs (
    id                  TEXT    PRIMARY KEY NOT NULL,
    -- From the definition; for listing and metrics without parsing JSON.
    name                TEXT    NOT NULL,
    -- The validated definition, copied per run so a changed definition can
    -- never alter a run in flight. Never updated.
    definition          TEXT    NOT NULL CHECK (json_valid(definition)),
    input               TEXT    NOT NULL CHECK (json_valid(input)),
    -- Idempotent start, same semantics as jobs.idempotency_key.
    idempotency_key     TEXT    UNIQUE,
    request_fingerprint TEXT    NOT NULL,
    -- Cache of the terminal event, written in the same transaction.
    state               TEXT    NOT NULL CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
    -- Due for a decision when wake_at <= now; NULL = nothing to decide.
    wake_at             INTEGER,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    finished_at         INTEGER,
    CHECK ((state = 'running') = (finished_at IS NULL)),
    CHECK (state = 'running' OR wake_at IS NULL)
);

-- The decider's scan: "runs due now", oldest first.
CREATE INDEX workflow_runs_due ON workflow_runs (wake_at) WHERE wake_at IS NOT NULL;

CREATE TABLE workflow_events (
    run_id  TEXT    NOT NULL REFERENCES workflow_runs (id),
    seq     INTEGER NOT NULL CHECK (seq >= 1),
    type    TEXT    NOT NULL CHECK (type IN (
                'workflow_started', 'activity_scheduled', 'activity_completed',
                'activity_failed', 'activity_cancelled', 'timer_scheduled', 'timer_fired',
                'signal_received', 'workflow_completed', 'workflow_failed', 'workflow_cancelled')),
    -- Activity address ("step" or "step.branch") or, for timers, the step name.
    step    TEXT,
    -- The activity's job; set exactly for activity_* events.
    job_id  TEXT    REFERENCES jobs (id),
    data    TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(data) AND json_type(data) = 'object'),
    at      INTEGER NOT NULL,
    PRIMARY KEY (run_id, seq),
    CHECK ((type IN ('activity_scheduled', 'activity_completed', 'activity_failed', 'activity_cancelled'))
           = (job_id IS NOT NULL)),
    CHECK ((type IN ('activity_scheduled', 'activity_completed', 'activity_failed',
                     'activity_cancelled', 'timer_scheduled', 'timer_fired'))
           = (step IS NOT NULL))
);

-- Each activity is scheduled at most once per run (its job does the retrying).
CREATE UNIQUE INDEX wf_scheduled_once ON workflow_events (run_id, step)
    WHERE type = 'activity_scheduled';
-- A job belongs to at most one activity; also the hook's job -> run lookup.
CREATE UNIQUE INDEX wf_job_link ON workflow_events (job_id)
    WHERE type = 'activity_scheduled';
-- At most one terminal outcome per activity.
CREATE UNIQUE INDEX wf_activity_end ON workflow_events (run_id, step)
    WHERE type IN ('activity_completed', 'activity_failed', 'activity_cancelled');
CREATE UNIQUE INDEX wf_timer_scheduled ON workflow_events (run_id, step)
    WHERE type = 'timer_scheduled';
CREATE UNIQUE INDEX wf_timer_fired ON workflow_events (run_id, step)
    WHERE type = 'timer_fired';
-- wait_signal is a one-shot latch: one delivery per name per run.
CREATE UNIQUE INDEX wf_signal_latch ON workflow_events (run_id, json_extract(data, '$.name'))
    WHERE type = 'signal_received';
-- At most one terminal workflow event per run.
CREATE UNIQUE INDEX wf_run_end ON workflow_events (run_id)
    WHERE type IN ('workflow_completed', 'workflow_failed', 'workflow_cancelled');

-- History is append-only. The message keeps the "CHECK constraint failed"
-- prefix so it reads like the other invariants.
CREATE TRIGGER workflow_events_no_update BEFORE UPDATE ON workflow_events
BEGIN
    SELECT RAISE(ABORT, 'CHECK constraint failed: workflow_events is append-only');
END;

CREATE TRIGGER workflow_events_no_delete BEFORE DELETE ON workflow_events
BEGIN
    SELECT RAISE(ABORT, 'CHECK constraint failed: workflow_events is append-only');
END;
