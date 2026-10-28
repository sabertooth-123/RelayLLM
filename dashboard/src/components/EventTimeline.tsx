import { useState } from "react";

import { clock, workerLabel } from "../format";
import { short } from "../state";
import type { DashboardEvent, WorkerView } from "../types";

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

interface Props {
  events: DashboardEvent[];
  workers: Record<string, WorkerView>;
  height: number;
  stats: string;
  onSelectJob: (id: string) => void;
  onSelectWorker: (id: string) => void;
}

export function EventTimeline({ events, workers, height, stats, onSelectJob, onSelectWorker }: Props) {
  const [heartbeats, setHeartbeats] = useState(false);
  const w = (id: string | null) => (id ? workerLabel(id, workers[id]?.name) : "—");

  const describe = (e: DashboardEvent): string => {
    switch (e.type) {
      case "worker_registered":
        return `${w(e.worker.id)} · ${e.worker.kinds.join(", ")} · concurrency ${e.worker.concurrency}`;
      case "worker_heartbeat":
        return `${w(e.worker_id)} renewed …${short(e.job_id)} (attempt ${e.attempt})`;
      case "lease_expired":
        return `…${short(e.job.id)} taken from ${w(e.worker_id)} → ${e.job.state}`;
      case "job_claimed":
        return `…${short(e.job.id)} by ${w(e.job.worker_id)} (attempt ${e.job.attempt}/${e.job.max_attempts})`;
      case "job_retry_scheduled":
      case "job_failed":
        return `…${short(e.job.id)} attempt ${e.job.attempt}: ${e.job.last_error ?? ""}`;
      default:
        return `…${short(e.job.id)} ${e.job.kind}`;
    }
  };

  const target = (e: DashboardEvent) =>
    "job" in e
      ? () => onSelectJob(e.job.id)
      : e.type === "worker_heartbeat"
        ? () => onSelectJob(e.job_id)
        : () => onSelectWorker(e.worker.id);

  const shown = events
    .filter((e) => heartbeats || e.type !== "worker_heartbeat")
    .slice(-150)
    .reverse();

  return (
    <section>
      <div className="section-head" style={{ paddingBottom: 8 }}>
        <h2>Events</h2>
        <span className="sub" style={{ fontSize: 11 }}>
          SSE /v1/events
        </span>
        <span className="grow" />
        <label className="toggle">
          <input type="checkbox" checked={heartbeats} onChange={(e) => setHeartbeats(e.target.checked)} />
          heartbeats
        </label>
      </div>
      <div className="events" style={{ height }}>
        {shown.length === 0 ? (
          <p className="jempty">No events yet. They appear as the coordinator commits changes.</p>
        ) : (
          shown.map((e) => (
            <div key={`${e.epoch}:${e.seq}`} className={`ev clickable e-${e.type}`} onClick={target(e)}>
              <span className="ev-seq">#{e.seq}</span>
              <span className="ev-time">{clock(e.at_ms)}</span>
              <span className="ev-sq" />
              <span className="ev-text" title={describe(e)}>
                <b>{LABEL[e.type]}</b> <span>{describe(e)}</span>
              </span>
            </div>
          ))
        )}
      </div>
      <div className="stream-stats">{stats}</div>
    </section>
  );
}
