import { useMemo } from "react";

import { short, workerHealth, type Model, type WorkerHealth } from "../state";
import type { DashboardEvent, JobSummary } from "../types";

interface Props {
  model: Model;
  live: boolean;
  serverNow: number;
  running: Record<string, JobSummary[]>;
  selectedWorker: string | null;
  onSelectWorker: (id: string) => void;
  onSelectJob: (id: string) => void;
}

const W = 760;
const H = 420;
const CX = W / 2;
const CY = H / 2;

interface Pulse {
  seq: number;
  kind: "claim" | "heartbeat" | "expired";
}

/** The newest recent event that involved each worker. */
function recentPulses(events: DashboardEvent[], serverNow: number): Record<string, Pulse> {
  const out: Record<string, Pulse> = {};
  for (let i = events.length - 1; i >= 0; i--) {
    const e = events[i]!;
    if (serverNow - e.at_ms > 2_000) break;
    let worker: string | null = null;
    let kind: Pulse["kind"] = "heartbeat";
    if (e.type === "job_claimed") [worker, kind] = [e.job.worker_id, "claim"];
    else if (e.type === "worker_heartbeat") worker = e.worker_id;
    else if (e.type === "lease_expired") [worker, kind] = [e.worker_id, "expired"];
    if (worker && !out[worker]) out[worker] = { seq: e.seq, kind };
  }
  return out;
}

export function NetworkView(props: Props) {
  const { model, live, serverNow, running, selectedWorker } = props;
  const workers = useMemo(
    () => Object.values(model.workers).sort((a, b) => a.registered_at_ms - b.registered_at_ms || a.id.localeCompare(b.id)),
    [model.workers],
  );
  const pulses = recentPulses(model.events, serverNow);
  const n = workers.length;
  const rx = Math.min(300, 140 + n * 18);
  const ry = Math.min(165, 110 + n * 8);
  const waiting = model.counts.queued + model.counts.retry_waiting;

  return (
    <div className="panel network">
      <div className="panel-head">
        <h2>Cluster</h2>
        <span className="muted small">
          {n} worker{n === 1 ? "" : "s"} · lease {Math.round(model.leaseMs / 1000)} s
        </span>
      </div>
      {n === 0 ? (
        <div className="empty">
          <p>No workers have registered with this coordinator.</p>
          <code>relay worker --server http://127.0.0.1:7070</code>
        </div>
      ) : null}
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Coordinator and worker network">
        <defs>
          <radialGradient id="core" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.35" />
            <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
          </radialGradient>
        </defs>

        {workers.map((w, i) => {
          const angle = -Math.PI / 2 + (2 * Math.PI * i) / Math.max(n, 1);
          const x = CX + rx * Math.cos(angle);
          const y = CY + ry * Math.sin(angle);
          const jobs = running[w.id] ?? [];
          const health: WorkerHealth = workerHealth(w, jobs.length, serverNow, model.leaseMs, live);
          const pulse = pulses[w.id];
          return (
            <g key={w.id}>
              <line x1={CX} y1={CY} x2={x} y2={y} className={`edge edge-${health}`} />
              {pulse ? (
                <circle key={pulse.seq} r="4" className={`packet packet-${pulse.kind}`}>
                  <animateMotion
                    dur="0.9s"
                    fill="freeze"
                    path={pulse.kind === "claim" ? `M${CX},${CY} L${x},${y}` : `M${x},${y} L${CX},${CY}`}
                  />
                  <animate attributeName="opacity" values="1;1;0" dur="0.9s" fill="freeze" />
                </circle>
              ) : null}
              <g
                transform={`translate(${x},${y})`}
                className={`wnode wnode-${health}${selectedWorker === w.id ? " selected" : ""}`}
                onClick={() => props.onSelectWorker(w.id)}
                role="button"
                tabIndex={0}
                aria-label={`Worker ${w.id}, ${health}`}
                onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && props.onSelectWorker(w.id)}
              >
                <rect x={-58} y={-30} width={116} height={60} rx={10} className="wbox" />
                <text y={-11} className="wname">
                  <title>{w.id}</title>
                  {w.name.length > 10 ? `${w.name.slice(0, 9)}…` : w.name}
                  <tspan className="wid"> ·{w.id.slice(-4)}</tspan>
                </text>
                <text y={4} className={`wstate t-${health}`}>
                  {health}
                </text>
                {Array.from({ length: Math.min(w.concurrency, 8) }, (_, s) => {
                  const job = jobs[s];
                  const total = Math.min(w.concurrency, 8);
                  const sx = (s - (total - 1) / 2) * 12;
                  return (
                    <circle
                      key={s}
                      cx={sx}
                      cy={18}
                      r={4}
                      className={job ? "slot slot-busy" : "slot"}
                      onClick={(e) => {
                        if (!job) return;
                        e.stopPropagation();
                        props.onSelectJob(job.id);
                      }}
                    >
                      <title>{job ? `job …${short(job.id)} attempt ${job.attempt}` : "free slot"}</title>
                    </circle>
                  );
                })}
                {w.concurrency > 8 ? (
                  <text x={56} y={22} className="wmore">
                    +{w.concurrency - 8}
                  </text>
                ) : null}
              </g>
            </g>
          );
        })}

        <circle cx={CX} cy={CY} r={70} fill="url(#core)" />
        <g transform={`translate(${CX},${CY})`} className={`core${live ? "" : " core-down"}`}>
          <rect x={-72} y={-38} width={144} height={76} rx={14} className="cbox" />
          <text y={-16} className="cname">
            coordinator
          </text>
          <text y={4} className="cqueue">
            {waiting} waiting
          </text>
          <g transform="translate(0,22)">
            {Array.from({ length: Math.min(waiting, 12) }, (_, i) => (
              <rect key={i} x={-47 + i * 8} y={-3} width={6} height={6} rx={1.5} className="qdot" />
            ))}
            {waiting > 12 ? (
              <text x={56} y={3} className="wmore">
                +{waiting - 12}
              </text>
            ) : null}
          </g>
        </g>
      </svg>
      <div className="legend small">
        {(["busy", "idle", "unavailable", "disconnected"] as const).map((h) => (
          <span key={h}>
            <i className={`dot d-${h}`} /> {h}
          </span>
        ))}
        <span className="muted">health is derived from the coordinator's last-seen time, not the browser</span>
      </div>
    </div>
  );
}
