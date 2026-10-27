-- Dashboard: "most recently finished jobs". Only terminal jobs have
-- finished_at, so the partial index stays as small as the history needs.
CREATE INDEX jobs_finished ON jobs (finished_at) WHERE finished_at IS NOT NULL;
