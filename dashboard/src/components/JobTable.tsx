import { useMemo } from "react";

import { ago, since, STATE_LABEL, workerLabel } from "../format";
import { short, type Model } from "../state";
import { JOB_STATES, TERMINAL, type JobState, type JobSummary } from "../types";

export type Filter = JobState | "active" | "all";

interface Props {
  model: Model;
  serverNow: number;
  filter: Filter;
  onFilter: (f: Filter) => void;
  selectedJob: string | null;
  onSelectJob: (id: string) => void;
}

const LIMIT = 200;
const MAX_PIPS = 8;

/** One pip per allowed attempt: past ones amber, the current one in state colour. */
function Pips({ job }: { job: JobSummary }) {
  return (
    <span>
      {Array.from({ length: Math.min(job.max_attempts, MAX_PIPS) }, (_, i) => {
        const k = i + 1;
        const cls = k < job.attempt ? "pip past" : k === job.attempt ? "pip now" : "pip";
        return <span key={k} className={cls} />;
      })}
    </span>
  );
}

function leaseCell(j: JobSummary, serverNow: number, leaseMs: number) {
  if (j.state === "running" && j.lease_expires_at_ms) {
    const low = j.lease_expires_at_ms - serverNow < leaseMs * 0.4;
    return { text: `exp ${ago(j.lease_expires_at_ms, serverNow)}`, color: low ? "var(--warn)" : "var(--text-2)" };
  }
  if (j.state === "retry_waiting") {
    return { text: `next ${ago(j.available_at_ms, serverNow)}`, color: "var(--warn)" };
  }
  return { text: "—", color: "var(--text-3)" };
}

export function JobTable({ model, serverNow, filter, onFilter, selectedJob, onSelectJob }: Props) {
  const rows = useMemo(() => {
    const picked = Object.values(model.jobs).filter((j) =>
      filter === "all" ? true : filter === "active" ? !TERMINAL.has(j.state) : j.state === filter,
    );
    // Active jobs oldest first (queue order); history newest first.
    picked.sort((a, b) => {
      const ta = TERMINAL.has(a.state);
      const tb = TERMINAL.has(b.state);
      if (ta !== tb) return ta ? 1 : -1;
      return ta ? (b.finished_at_ms ?? 0) - (a.finished_at_ms ?? 0) : a.created_at_ms - b.created_at_ms;
    });
    return picked;
  }, [model.jobs, filter]);

  const filters: Filter[] = ["active", "all", ...JOB_STATES];
  const noun = filter === "all" ? "" : filter === "active" ? "active " : `${STATE_LABEL[filter].toLowerCase()} `;

  return (
    <section>
      <div className="section-head">
        <h2>Jobs</h2>
        <span className="note" style={{ fontSize: 11.5 }}>
          {rows.length.toLocaleString()} shown · every active job
          {model.activeTruncated ? " (first 500)" : ""} plus recent history
        </span>
        <span className="grow" />
        <div className="chips" role="tablist" aria-label="Filter jobs by status">
          {filters.map((f) => (
            <button
              key={f}
              role="tab"
              aria-selected={filter === f}
              className={`chip${filter === f ? " on" : ""}`}
              onClick={() => onFilter(f)}
            >
              {f === "active" ? "Active" : f === "all" ? "All" : STATE_LABEL[f]}
            </button>
          ))}
        </div>
      </div>
      <div className="jobs-scroll">
        <div className="jgrid jhead">
          <span>Job</span>
          <span>Status</span>
          <span>Attempt</span>
          <span>Worker</span>
          <span>Updated</span>
          <span>Lease / next</span>
          <span>Last error</span>
        </div>
        {rows.length === 0 ? (
          <div className="jempty">No {noun}jobs.</div>
        ) : (
          <div className="jbody">
            {rows.slice(0, LIMIT).map((j) => {
              const lease = leaseCell(j, serverNow, model.leaseMs);
              const worker = j.worker_id ? model.workers[j.worker_id] : undefined;
              return (
                <div
                  key={j.id}
                  className={`jgrid jrow s-${j.state}${selectedJob === j.id ? " selected" : ""}`}
                  onClick={() => onSelectJob(j.id)}
                  tabIndex={0}
                  onKeyDown={(e) => e.key === "Enter" && onSelectJob(j.id)}
                >
                  <span className="nowrap" title={j.id}>
                    …{short(j.id)} <span className="muted">{j.kind}</span>
                  </span>
                  <span>
                    <span className="pill">
                      <span className="dot" style={{ width: 5, height: 5 }} />
                      {STATE_LABEL[j.state]}
                    </span>
                  </span>
                  <span className="pips">
                    <Pips job={j} />
                    <span style={{ color: "var(--text-mid)" }}>
                      {j.attempt}/{j.max_attempts}
                    </span>
                  </span>
                  <span className="nowrap" title={j.worker_id ?? undefined} style={{ color: j.worker_id ? "var(--text-2)" : "var(--text-3)" }}>
                    {j.worker_id ? workerLabel(j.worker_id, worker?.name) : "—"}
                  </span>
                  <span className="nowrap" style={{ color: "var(--text-mid)" }}>
                    {since(j.updated_at_ms, serverNow)}
                  </span>
                  <span className="nowrap" style={{ color: lease.color }}>
                    {lease.text}
                  </span>
                  <span className="nowrap err-text" title={j.last_error ?? undefined}>
                    {j.last_error ?? ""}
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </div>
      {rows.length > LIMIT ? (
        <p className="jfoot">{(rows.length - LIMIT).toLocaleString()} more not shown; narrow the filter.</p>
      ) : null}
    </section>
  );
}
