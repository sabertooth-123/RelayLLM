import { STATE_LABEL } from "../format";
import type { Model } from "../state";
import type { JobState } from "../types";

const CARDS: JobState[] = ["queued", "running", "retry_waiting", "succeeded", "failed"];

export function SummaryCards({ model, onFilter }: { model: Model; onFilter: (s: JobState) => void }) {
  const active = model.counts.queued + model.counts.running + model.counts.retry_waiting;
  const approx = model.activeTruncated ? "≈" : "";
  return (
    <section className="cards" aria-label="Job counts">
      {CARDS.map((s) => (
        <button key={s} className={`card card-${s}`} onClick={() => onFilter(s)} title={`Show ${STATE_LABEL[s].toLowerCase()} jobs`}>
          <span className="card-label">{STATE_LABEL[s]}</span>
          <span className="card-value">
            {approx}
            {model.counts[s].toLocaleString()}
          </span>
        </button>
      ))}
      <div className="card card-capacity" title="queued + running + retrying, against --max-active-jobs">
        <span className="card-label">Capacity</span>
        <span className="card-value small-value">
          {approx}
          {active.toLocaleString()} <span className="muted">/ {model.maxActiveJobs.toLocaleString()}</span>
        </span>
        <span className="bar">
          <span style={{ width: `${Math.min(100, (100 * active) / Math.max(1, model.maxActiveJobs))}%` }} />
        </span>
        {model.counts.cancelled > 0 ? (
          <span className="muted small">{model.counts.cancelled.toLocaleString()} cancelled</span>
        ) : null}
      </div>
    </section>
  );
}
