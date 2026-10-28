import { useEffect, useState } from "react";

import { api } from "../api";
import { ago, clock, dateTime, duration, since, workerLabel } from "../format";
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
  const [armed, setArmed] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    setArmed(false);
    setMessage(null);
  }, [workerId]);

  if (!worker) {
    return (
      <section className="panel">
        <div className="panel-head">
          <h2>Worker</h2>
          <span className="grow" />
          <button className="close" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        <p className="muted">Worker {workerId} is not known to this coordinator.</p>
      </section>
    );
  }

  const health = workerHealth(worker, running.length, serverNow, model.leaseMs, live);
  const silence = Math.max(0, serverNow - worker.last_seen_at_ms);

  // Two clicks instead of a browser dialog: arm, then confirm.
  const expire = async () => {
    if (!armed) {
      setArmed(true);
      setMessage(null);
      return;
    }
    setArmed(false);
    setBusy(true);
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
    <section className="panel">
      <div className="panel-head">
        <h2>
          Worker <span className="mono" style={{ fontWeight: 500 }}>{workerLabel(worker.id, worker.name)}</span>
        </h2>
        <span className={`tag h-${health}`}>{health}</span>
        <span className="grow" />
        <button className="close" onClick={onClose} aria-label="Close">
          ✕
        </button>
      </div>
      <p style={{ margin: "0 0 12px", fontSize: 11.5, color: "var(--text-mid)" }}>
        {health === "unknown"
          ? "Dashboard not connected; last known state shown."
          : health === "unavailable" || health === "disconnected"
            ? `No claim, poll or heartbeat for ${duration(silence)} (lease ${duration(model.leaseMs)}).`
            : `Last seen ${since(worker.last_seen_at_ms, serverNow)} via claim poll or heartbeat.`}
      </p>
      <div className="kv">
        <span>ID</span>
        <span className="v">{worker.id}</span>
        <span>Kinds</span>
        <span className="v">{worker.kinds.join(", ")}</span>
        <span>Concurrency</span>
        <span className="v">
          {running.length} / {worker.concurrency} slots busy
        </span>
        <span>Registered</span>
        <span className="v">{dateTime(worker.registered_at_ms)}</span>
        <span>Last seen</span>
        <span className="v">
          {clock(worker.last_seen_at_ms)} ({since(worker.last_seen_at_ms, serverNow)})
        </span>
      </div>

      <h3>Active jobs</h3>
      {running.length === 0 ? (
        <p className="muted" style={{ fontSize: 12 }}>
          None.
        </p>
      ) : (
        <div className="stack">
          {running.map((j) => {
            const left = (j.lease_expires_at_ms ?? serverNow) - serverNow;
            const pct = Math.max(0, Math.min(100, (left / model.leaseMs) * 100));
            const low = pct < 40;
            return (
              <button key={j.id} className="wjob" onClick={() => onSelectJob(j.id)}>
                <span className="wjob-top">
                  <span>
                    …{short(j.id)}{" "}
                    <span className="muted">
                      attempt {j.attempt}/{j.max_attempts}
                    </span>
                  </span>
                  <span style={{ color: low ? "var(--warn)" : "var(--text-2)" }}>
                    {j.lease_expires_at_ms ? `lease ${ago(j.lease_expires_at_ms, serverNow)}` : "no lease"}
                  </span>
                </span>
                <span className={`lease${low ? " low" : ""}`}>
                  <span style={{ width: `${pct}%` }} />
                </span>
              </button>
            );
          })}
        </div>
      )}

      {model.devControls ? (
        <div className="dev">
          <div className="dev-title">Development controls</div>
          <p>
            Expires this worker's leases so the coordinator recovers its jobs exactly as it would after a crash. The
            worker process keeps running, and the coordinator rejects its reports for those jobs.
          </p>
          <button
            className={`danger${armed ? " armed" : ""}`}
            disabled={busy || running.length === 0}
            onClick={expire}
            onBlur={() => setArmed(false)}
          >
            {armed
              ? `Confirm: expire ${running.length} lease${running.length === 1 ? "" : "s"}`
              : "Simulate failure: expire leases"}
          </button>
          {message ? <p className="danger-msg">{message}</p> : null}
        </div>
      ) : null}
    </section>
  );
}
