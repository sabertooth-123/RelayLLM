import { useState } from "react";

import { clock } from "../format";
import { short } from "../state";
import type { DashboardEvent } from "../types";

const LABEL: Record<DashboardEvent["type"], string> = {
  job_submitted: "submitted",
  job_claimed: "claimed",
  job_succeeded: "succeeded",
  job_retry_scheduled: "retry scheduled",
  job_failed: "failed",
  job_cancelled: "cancelled",
  lease_expired: "lease expired",
  worker_registered: "worker registered",
  worker_heartbeat: "heartbeat",
};

function describe(e: DashboardEvent): string {
  switch (e.type) {
    case "worker_registered":
      return `${e.worker.id} · ${e.worker.kinds.join(", ")} · concurrency ${e.worker.concurrency}`;
    case "worker_heartbeat":
      return `${e.worker_id} renewed …${short(e.job_id)} (attempt ${e.attempt})`;
    case "lease_expired":
      return `…${short(e.job.id)} taken from ${e.worker_id} → ${e.job.state}`;
    case "job_claimed":
      return `…${short(e.job.id)} by ${e.job.worker_id} (attempt ${e.job.attempt}/${e.job.max_attempts})`;
    case "job_retry_scheduled":
    case "job_failed":
      return `…${short(e.job.id)} attempt ${e.job.attempt}: ${e.job.last_error ?? ""}`;
    default:
      return `…${short(e.job.id)} ${e.job.kind}`;
  }
}

export function EventTimeline({ events, onSelectJob }: { events: DashboardEvent[]; onSelectJob: (id: string) => void }) {
  const [heartbeats, setHeartbeats] = useState(false);
  const shown = events.filter((e) => heartbeats || e.type !== "worker_heartbeat").slice(-150).reverse();
  return (
    <div className="panel timeline">
      <div className="panel-head">
        <h2>Events</h2>
        <label className="small muted toggle">
          <input type="checkbox" checked={heartbeats} onChange={(e) => setHeartbeats(e.target.checked)} /> heartbeats
        </label>
      </div>
      {shown.length === 0 ? (
        <div className="empty">No events yet. They appear as the coordinator commits changes.</div>
      ) : (
        <ol className="events">
          {shown.map((e) => (
            <li
              key={`${e.epoch}:${e.seq}`}
              className={`ev ev-${e.type}`}
              onClick={"job" in e ? () => onSelectJob(e.job.id) : "job_id" in e ? () => onSelectJob(e.job_id) : undefined}
            >
              <span className="mono muted seq">#{e.seq}</span>
              <span className="mono muted">{clock(e.at_ms)}</span>
              <span className="ev-type">{LABEL[e.type]}</span>
              <span className="ev-desc">{describe(e)}</span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
