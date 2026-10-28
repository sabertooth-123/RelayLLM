import { STATE_LABEL } from "../format";
import type { Model } from "../state";
import { JOB_STATES, type JobState } from "../types";
import type { Filter } from "./JobTable";

/** One cell per state, doubling as the job-table filter, then capacity. */
export function SummaryCards({
  model,
  filter,
  onFilter,
}: {
  model: Model;
  filter: Filter;
  onFilter: (s: JobState) => void;
}) {
  const active = model.counts.queued + model.counts.running + model.counts.retry_waiting;
  const approx = model.activeTruncated ? "≈" : "";
  const pct = Math.min(100, (100 * active) / Math.max(1, model.maxActiveJobs));
  return (
    <section className="counts" aria-label="Job counts">
      {JOB_STATES.map((s) => {
        const n = model.counts[s];
        const tone = n === 0 ? " zero" : s === "failed" || s === "retry_waiting" ? " hot" : "";
        return (
          <button
            key={s}
            className={`count s-${s}${filter === s ? " on" : ""}`}
            onClick={() => onFilter(s)}
            title="Show these jobs in the table"
            aria-pressed={filter === s}
          >
            <span className="count-label">{STATE_LABEL[s]}</span>
            <span className={`count-value${tone}`}>
              {approx}
              {n.toLocaleString()}
            </span>
          </button>
        );
      })}
      <div className="capacity" title="queued + running + retrying, against --max-active-jobs">
        <div className="capacity-head">
          <span>Capacity</span>
        </div>
        <div className="capacity-value">
          {approx}
          {active.toLocaleString()} <span className="muted">/ {model.maxActiveJobs.toLocaleString()} active</span>
        </div>
        <div className="capacity-bar">
          <span style={{ width: `${pct}%` }} />
        </div>
      </div>
    </section>
  );
}
