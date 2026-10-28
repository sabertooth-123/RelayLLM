import { useEffect, useMemo, useRef, useState, type CSSProperties } from "react";

import { ago, duration, since } from "../format";
import { short, workerHealth, type ActivityKind, type Model } from "../state";
import type { DashboardEvent, JobSummary } from "../types";

interface Props {
  model: Model;
  live: boolean;
  /** Local clock + offset; the component re-reads it on its own tick. */
  clockOffsetMs: number;
  running: Record<string, JobSummary[]>;
  selectedWorker: string | null;
  onSelectWorker: (id: string) => void;
  onSelectJob: (id: string) => void;
}

/** Slots drawn per worker; more are summarised as "+N". */
const MAX_SLOTS = 6;
/** Packet animation length; matches the CSS keyframes. */
const PACKET_MS = 900;

const PACKET: Record<ActivityKind, { color: string; size: number; dir: "out" | "in" | "flash" }> = {
  claim: { color: "var(--accent)", size: 6, dir: "out" },
  heartbeat: { color: "var(--hb)", size: 4, dir: "in" },
  done: { color: "var(--ok)", size: 6, dir: "in" },
  retry: { color: "var(--warn)", size: 6, dir: "in" },
  fail: { color: "var(--err)", size: 6, dir: "in" },
  expired: { color: "var(--err)", size: 10, dir: "flash" },
};

function useTick(ms: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), ms);
    return () => window.clearInterval(t);
  }, [ms]);
  return now;
}

/** Height and colour class of one SQLite commit tick. */
function tick(e: DashboardEvent) {
  return { height: e.type === "worker_heartbeat" ? "45%" : "100%", cls: `e-${e.type}` };
}

export function NetworkView(props: Props) {
  const { model, live, running, selectedWorker } = props;
  // Lease bars and packets move faster than the app's 1 s tick.
  const localNow = useTick(250);
  const serverNow = localNow + props.clockOffsetMs;

  // Packets are animated from when this browser first saw the exchange,
  // not the coordinator's timestamp: clock offset would skew them.
  const firstSeen = useRef(new Map<number, number>());
  for (const a of model.activity) {
    if (!firstSeen.current.has(a.seq)) firstSeen.current.set(a.seq, localNow);
  }
  if (firstSeen.current.size > 400) {
    for (const [seq, at] of firstSeen.current) if (localNow - at > PACKET_MS) firstSeen.current.delete(seq);
  }

  const workers = useMemo(
    () =>
      Object.values(model.workers).sort(
        (a, b) => a.registered_at_ms - b.registered_at_ms || a.id.localeCompare(b.id),
      ),
    [model.workers],
  );

  const waiting = useMemo(
    () =>
      Object.values(model.jobs)
        .filter((j) => j.state === "queued" || j.state === "retry_waiting")
        .sort(
          (a, b) =>
            (a.state === b.state ? 0 : a.state === "queued" ? -1 : 1) || a.available_at_ms - b.available_at_ms,
        ),
    [model.jobs],
  );
  const queueShown = Math.max(5, Math.min(12, workers.length + 2));
  const waitingN = model.counts.queued + model.counts.retry_waiting;

  let busyN = 0;
  let slotsUsed = 0;
  let slotsTotal = 0;
  const rows = workers.map((w) => {
    const jobs = running[w.id] ?? [];
    const health = workerHealth(w, jobs.length, serverNow, model.leaseMs, live);
    if (health === "busy") busyN += 1;
    slotsUsed += jobs.length;
    slotsTotal += w.concurrency;
    return { w, jobs, health };
  });

  const lastEvent = model.events[model.events.length - 1];

  return (
    <section className="topology">
      <div className="section-head">
        <h2>Topology</h2>
        <span className="sub">
          {workers.length} worker{workers.length === 1 ? "" : "s"} · {busyN} busy · {slotsUsed}/{slotsTotal} slots
        </span>
        <span className="grow" />
        <div className="legend">
          <span>
            <i style={{ width: 6, height: 6, background: "var(--accent)" }} />
            claim →
          </span>
          <span>
            <i style={{ width: 4, height: 4, background: "var(--hb)" }} />← heartbeat
          </span>
          <span>
            <i style={{ width: 6, height: 6, background: "var(--ok)" }} />← done
          </span>
          <span>
            <i style={{ width: 6, height: 6, background: "var(--warn)" }} />← retry
          </span>
          <span>
            <i style={{ width: 7, height: 7, border: "1.5px solid var(--err)" }} />
            lease expired
          </span>
        </div>
      </div>

      {workers.length === 0 ? (
        <div className="empty-box">
          <p>No workers have registered with this coordinator.</p>
          <code>relay worker --server http://127.0.0.1:7070</code>
        </div>
      ) : null}

      <div className="stage">
        <div className={`coord${live ? "" : " down"}`}>
          <div className="coord-head">
            <div className="coord-title">
              <span className={`dot conn-${live ? "live" : "reconnecting"}`} />
              coordinator
            </div>
            <div className="coord-sub">pull-based claiming · one lease = {duration(model.leaseMs)}</div>
          </div>
          <div className="coord-queue">
            <div className="kv-line">
              <span>Waiting</span>
              <span className="big-num">
                {model.activeTruncated ? "≈" : ""}
                {waitingN.toLocaleString()}
              </span>
            </div>
            <div className="queue-sub">
              {model.counts.queued} queued · {model.counts.retry_waiting} backing off
            </div>
            <div>
              {waiting.slice(0, queueShown).map((j) => {
                const retry = j.state === "retry_waiting";
                const wait = j.available_at_ms - serverNow;
                return (
                  <button
                    key={j.id}
                    className={`queue-row s-${j.state}`}
                    onClick={() => props.onSelectJob(j.id)}
                    title={`job ${j.id}`}
                  >
                    <i />
                    <span>…{short(j.id)}</span>
                    <span className="t">
                      {retry
                        ? wait > 0
                          ? `retry ${ago(j.available_at_ms, serverNow)}`
                          : "retry ready"
                        : duration(Math.max(0, serverNow - j.created_at_ms))}
                    </span>
                  </button>
                );
              })}
              {waitingN === 0 ? <div className="queue-note">Queue empty</div> : null}
              {waiting.length > queueShown ? (
                <div className="queue-note mono" style={{ fontSize: 11 }}>
                  +{waiting.length - queueShown} more
                </div>
              ) : null}
            </div>
          </div>
          <div className="coord-db">
            <div className="kv-line">
              <span className="db-label">
                <span className="db-icon" />
                SQLite
              </span>
              <span className="tiny">{lastEvent ? `last ${since(lastEvent.at_ms, serverNow)}` : "no commits yet"}</span>
            </div>
            <div className="ticks" aria-hidden>
              {model.events.slice(-40).map((e) => {
                const t = tick(e);
                return <span key={`${e.epoch}:${e.seq}`} className={t.cls} style={{ height: t.height }} />;
              })}
            </div>
            <div className="tiny">committed transitions → seq {model.seq.toLocaleString()}</div>
          </div>
        </div>

        <div className="workers">
          {rows.map(({ w, jobs, health }) => {
            const packets = model.activity.filter((a) => {
              if (a.workerId !== w.id) return false;
              const seen = firstSeen.current.get(a.seq);
              return seen !== undefined && localNow - seen < PACKET_MS;
            });
            const shown = Math.min(w.concurrency, MAX_SLOTS);
            return (
              <div key={w.id} className={`wrow h-${health}`}>
                <div className="bus" aria-hidden>
                  {packets.map((p) => {
                    const d = PACKET[p.kind];
                    return (
                      <span
                        key={p.seq}
                        className={`packet ${d.dir}`}
                        style={
                          {
                            "--pc": d.color,
                            width: d.size,
                            height: d.size,
                            marginTop: d.dir === "flash" ? -5 : -d.size / 2,
                          } as CSSProperties
                        }
                      />
                    );
                  })}
                </div>
                <div
                  className={`wcard${selectedWorker === w.id ? " selected" : ""}`}
                  role="button"
                  tabIndex={0}
                  aria-label={`Worker ${w.name}, ${health}`}
                  title={w.id}
                  onClick={() => props.onSelectWorker(w.id)}
                  onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && props.onSelectWorker(w.id)}
                >
                  <div className="wname">
                    <b>{w.name}</b>
                    <small>·{w.id.slice(-4)}</small>
                  </div>
                  <div className="whealth">
                    <span className="dot" />
                    {health}
                  </div>
                  <div className="slots">
                    {Array.from({ length: shown }, (_, i) => {
                      const j = jobs[i];
                      if (!j) {
                        return (
                          <span key={i} className="slot free" title="free slot">
                            free
                          </span>
                        );
                      }
                      const left = (j.lease_expires_at_ms ?? serverNow) - serverNow;
                      const pct = Math.max(0, Math.min(100, (left / model.leaseMs) * 100));
                      return (
                        <button
                          key={j.id}
                          className="slot busy"
                          title={`job …${short(j.id)} · attempt ${j.attempt}/${j.max_attempts} · lease ${ago(
                            j.lease_expires_at_ms ?? serverNow,
                            serverNow,
                          )}`}
                          onClick={(e) => {
                            e.stopPropagation();
                            props.onSelectJob(j.id);
                          }}
                        >
                          <span className="slot-top">
                            <span className="nowrap">…{short(j.id).slice(-6)}</span>
                            <span>a{j.attempt}</span>
                          </span>
                          <span className={`lease${pct < 40 ? " low" : ""}`}>
                            <span style={{ width: `${pct}%` }} />
                          </span>
                        </button>
                      );
                    })}
                    {w.concurrency > MAX_SLOTS ? <span className="more">+{w.concurrency - MAX_SLOTS}</span> : null}
                  </div>
                  <div className="wseen" title="coordinator's last_seen_at_ms">
                    {since(w.last_seen_at_ms, serverNow)}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
      <p className="topo-note">
        Health comes from the coordinator's last-seen time. A worker is unavailable after one lease of silence and
        disconnected after three. Lease bars drain until a heartbeat renews them.
      </p>
    </section>
  );
}
