import { useState } from "react";

import { api } from "../api";
import { ago, dateTime, duration } from "../format";
import { short, workerHealth, type Model } from "../state";
import type { JobSummary } from "../types";

interface Props {
  model: Model;
  workerId: string;
  running: JobSummary[];
  serverNow: number;
  live: boolean;
  onSelectJob: (id: string) => void;
  onClose: () => void;
}

export function WorkerPanel({ model, workerId, running, serverNow, live, onSelectJob, onClose }: Props) {
  const worker = model.workers[workerId];
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  if (!worker) {
    return (
      <aside className="panel side">
        <div className="panel-head">
          <h2>Worker</h2>
          <button className="ghost" onClick={onClose} aria-label="Close">✕</button>
        </div>
        <div className="empty">Worker {workerId} is not known to this coordinator.</div>
      </aside>
    );
  }

  const health = workerHealth(worker, running.length, serverNow, model.leaseMs, live);
  const silence = serverNow - worker.last_seen_at_ms;

  const expire = async () => {
    if (!window.confirm(`Expire the leases of every job running on ${worker.id}?\n\nThe jobs are reaped as if the worker had stopped heartbeating, and its reports for them will be rejected.`)) return;
    setBusy(true);
    setMessage(null);
    try {
      const r = await api.expireLeases(worker.id);
      setMessage(`Expired ${r.expired_jobs} lease${r.expired_jobs === 1 ? "" : "s"}.`);
    } catch (e) {
      setMessage(`Failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <aside className="panel side">
      <div className="panel-head">
        <h2>
          Worker <span className="mono">{worker.id}</span>
        </h2>
        <button className="ghost" onClick={onClose} aria-label="Close">✕</button>
      </div>
      <p>
        <span className={`pill h-${health}`}>{health}</span>{" "}
        <span className="muted small">
          {health === "unknown"
            ? "dashboard not connected; last known state shown"
            : health === "unavailable" || health === "disconnected"
              ? `no claim, poll or heartbeat for ${duration(silence)} (lease ${duration(model.leaseMs)})`
              : `last seen ${ago(worker.last_seen_at_ms, serverNow)}`}
        </span>
      </p>
      <dl className="kv">
        <dt>Name</dt>
        <dd>{worker.name}</dd>
        <dt>Kinds</dt>
        <dd className="mono">{worker.kinds.join(", ")}</dd>
        <dt>Concurrency</dt>
        <dd>
          {running.length} / {worker.concurrency} slots busy
        </dd>
        <dt>Registered</dt>
        <dd>{dateTime(worker.registered_at_ms)}</dd>
        <dt>Last seen</dt>
        <dd>
          {dateTime(worker.last_seen_at_ms)} <span className="muted">({ago(worker.last_seen_at_ms, serverNow)})</span>
        </dd>
      </dl>

      <h3>Active jobs</h3>
      {running.length === 0 ? (
        <p className="muted">None.</p>
      ) : (
        <ul className="plain">
          {running.map((j) => (
            <li key={j.id}>
              <button className="link mono" onClick={() => onSelectJob(j.id)}>
                …{short(j.id)}
              </button>{" "}
              attempt {j.attempt}/{j.max_attempts}
              {j.lease_expires_at_ms ? (
                <span className="muted"> · lease {ago(j.lease_expires_at_ms, serverNow)}</span>
              ) : null}
            </li>
          ))}
        </ul>
      )}

      {model.devControls ? (
        <div className="dev">
          <h3>Development controls</h3>
          <p className="muted small">
            Enabled with <code>--dev-controls</code> (loopback only). This does not stop the worker process; it expires
            its leases so the coordinator recovers the jobs exactly as after a crash.
          </p>
          <button className="danger" disabled={busy || running.length === 0} onClick={expire}>
            Simulate failure: expire leases
          </button>
          {message ? <p className="small">{message}</p> : null}
        </div>
      ) : null}
    </aside>
  );
}
