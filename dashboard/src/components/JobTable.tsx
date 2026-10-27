import { useMemo } from "react";

import { ago, STATE_LABEL } from "../format";
import { short, type Model } from "../state";
import { JOB_STATES, TERMINAL, type JobState } from "../types";

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

export function JobTable({ model, serverNow, filter, onFilter, selectedJob, onSelectJob }: Props) {
  const rows = useMemo(() => {
    const all = Object.values(model.jobs);
    const picked = all.filter((j) =>
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

  return (
    <div className="panel jobs">
      <div className="panel-head">
        <h2>Jobs</h2>
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
      <p className="muted small">
        Showing jobs this dashboard holds: every active job
        {model.activeTruncated ? " (snapshot truncated at 500)" : ""} plus the most recent finished ones.
      </p>
      {rows.length === 0 ? (
        <div className="empty">No {filter === "all" ? "" : filter === "active" ? "active " : `${STATE_LABEL[filter].toLowerCase()} `}jobs.</div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Job</th>
                <th>Status</th>
                <th>Attempt</th>
                <th>Worker</th>
                <th>Updated</th>
                <th className="wide">Last error</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, LIMIT).map((j) => (
                <tr
                  key={j.id}
                  className={selectedJob === j.id ? "selected" : undefined}
                  onClick={() => onSelectJob(j.id)}
                  tabIndex={0}
                  onKeyDown={(e) => e.key === "Enter" && onSelectJob(j.id)}
                >
                  <td className="mono" title={j.id}>
                    …{short(j.id)} <span className="muted">{j.kind}</span>
                  </td>
                  <td>
                    <span className={`pill p-${j.state}`}>{STATE_LABEL[j.state]}</span>
                  </td>
                  <td className="mono">
                    {j.attempt}/{j.max_attempts}
                  </td>
                  <td className="mono">{j.worker_id ?? <span className="muted">—</span>}</td>
                  <td className="muted">{ago(j.updated_at_ms, serverNow)}</td>
                  <td className="wide err" title={j.last_error ?? undefined}>
                    {j.last_error ?? ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length > LIMIT ? (
            <p className="muted small">
              {rows.length - LIMIT} more not shown; narrow the filter.
            </p>
          ) : null}
        </div>
      )}
    </div>
  );
}
