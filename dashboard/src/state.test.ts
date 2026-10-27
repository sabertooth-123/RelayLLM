import { describe, expect, it } from "vitest";

import { applyEvent, applyWorkers, fromSnapshot, workerHealth, type Model } from "./state";
import type { DashboardEvent, DashboardSnapshot, JobState, JobSummary, WorkerView } from "./types";

const EPOCH = "epoch-1";
const T = 1_700_000_000_000;

function job(id: string, state: JobState, attempt: number, at: number, worker: string | null = null): JobSummary {
  return {
    id,
    kind: "mock",
    state,
    attempt,
    max_attempts: 3,
    worker_id: worker,
    has_idempotency_key: false,
    last_error: null,
    created_at_ms: T,
    updated_at_ms: at,
    finished_at_ms: ["succeeded", "failed", "cancelled"].includes(state) ? at : null,
    available_at_ms: T,
    lease_expires_at_ms: state === "running" ? at + 30_000 : null,
  };
}

const worker: WorkerView = {
  id: "w1",
  name: "w1",
  concurrency: 2,
  kinds: ["mock"],
  registered_at_ms: T,
  last_seen_at_ms: T,
  running_jobs: 0,
};

function snapshot(seq: number, jobs: JobSummary[], extra: Partial<DashboardSnapshot> = {}): DashboardSnapshot {
  const counts = new Map<JobState, number>();
  for (const j of jobs) counts.set(j.state, (counts.get(j.state) ?? 0) + 1);
  return {
    epoch: EPOCH,
    seq,
    server_time_ms: T,
    lease_ms: 30_000,
    max_active_jobs: 100,
    counts: [...counts].map(([state, count]) => ({ state, count })),
    workers: [worker],
    jobs,
    active_jobs_truncated: false,
    recent_events: [],
    dev_controls: false,
    ...extra,
  };
}

function ev(seq: number, type: string, from: JobState | null, j: JobSummary, epoch = EPOCH): DashboardEvent {
  return { seq, epoch, at_ms: j.updated_at_ms, type, from, job: j } as DashboardEvent;
}

const count = (m: Model, s: JobState) => m.counts[s];

describe("applyEvent", () => {
  it("applies a clean submit → claim → succeed sequence", () => {
    let m = fromSnapshot(snapshot(0, []), T);
    m = applyEvent(m, ev(1, "job_submitted", null, job("a", "queued", 0, T + 1)));
    m = applyEvent(m, ev(2, "job_claimed", "queued", job("a", "running", 1, T + 2, "w1")));
    expect(count(m, "running")).toBe(1);
    expect(m.workers.w1!.last_seen_at_ms).toBe(T + 2);
    m = applyEvent(m, ev(3, "job_succeeded", "running", job("a", "succeeded", 1, T + 3)));
    expect(m.resync).toBeNull();
    expect(m.seq).toBe(3);
    expect([count(m, "queued"), count(m, "running"), count(m, "succeeded")]).toEqual([0, 0, 1]);
    expect(m.jobs.a!.state).toBe("succeeded");
    expect(m.stats.applied).toBe(3);
  });

  it("ignores duplicate events without double counting", () => {
    let m = fromSnapshot(snapshot(0, []), T);
    const submitted = ev(1, "job_submitted", null, job("a", "queued", 0, T + 1));
    m = applyEvent(m, submitted);
    m = applyEvent(m, submitted);
    expect(count(m, "queued")).toBe(1);
    expect(m.stats.duplicates).toBe(1);
    expect(m.events).toHaveLength(1);
  });

  it("treats events the snapshot already reflects as stale, not as new transitions", () => {
    // Snapshot taken at seq 1 but read after the claim (seq 2) committed.
    const running = job("a", "running", 1, T + 2, "w1");
    let m = fromSnapshot(snapshot(1, [running]), T);
    m = applyEvent(m, ev(2, "job_claimed", "queued", running));
    expect(m.resync).toBeNull();
    expect(count(m, "running")).toBe(1);
    expect(count(m, "queued")).toBe(0);
    expect(m.stats.stale).toBe(1);
  });

  it("ignores an event the held job has already moved past", () => {
    // Snapshot read after the job had already succeeded (seq 3), with the
    // stream still delivering the older claim (seq 2).
    let m = fromSnapshot(snapshot(1, [job("a", "succeeded", 1, T + 3)]), T);
    m = applyEvent(m, ev(2, "job_claimed", "queued", job("a", "running", 1, T + 2, "w1")));
    expect(m.resync).toBeNull();
    expect(m.jobs.a!.state).toBe("succeeded");
    expect(count(m, "running")).toBe(0);
  });

  it("resyncs on a gap in seq", () => {
    let m = fromSnapshot(snapshot(4, []), T);
    m = applyEvent(m, ev(6, "job_submitted", null, job("a", "queued", 0, T + 1)));
    expect(m.resync).toMatch(/missed events 5\.\.5/);
    expect(m.seq).toBe(4);
    // Nothing more is applied until a new snapshot.
    const after = applyEvent(m, ev(5, "job_submitted", null, job("b", "queued", 0, T + 1)));
    expect(after).toBe(m);
  });

  it("resyncs when the coordinator restarted", () => {
    const m = applyEvent(
      fromSnapshot(snapshot(10, []), T),
      ev(1, "job_submitted", null, job("a", "queued", 0, T + 1), "epoch-2"),
    );
    expect(m.resync).toMatch(/epoch/);
  });

  it("resyncs when a newer transition doesn't fit the held state", () => {
    let m = fromSnapshot(snapshot(0, [job("a", "queued", 0, T)]), T);
    m = applyEvent(m, ev(1, "job_succeeded", "running", job("a", "succeeded", 1, T + 5)));
    expect(m.resync).toMatch(/held queued/);
  });

  it("keeps an unseen job without counting it twice when the snapshot was complete", () => {
    // Not held, snapshot not truncated: the event committed before the
    // snapshot's reads, so the counts already include it.
    let m = fromSnapshot(snapshot(0, []), T);
    m = { ...m, counts: { ...m.counts, succeeded: 1 } };
    m = applyEvent(m, ev(1, "job_succeeded", "running", job("z", "succeeded", 1, T + 1)));
    expect(m.resync).toBeNull();
    expect(count(m, "succeeded")).toBe(1);
    expect(m.jobs.z).toBeDefined();
  });

  it("handles retry and lease expiry transitions", () => {
    let m = fromSnapshot(snapshot(0, [job("a", "running", 1, T, "w1")]), T);
    m = applyEvent(m, {
      ...ev(1, "lease_expired", "running", job("a", "retry_waiting", 1, T + 1)),
      worker_id: "w1",
      reason: "lease expired",
    } as DashboardEvent);
    m = applyEvent(m, ev(2, "job_claimed", "retry_waiting", job("a", "running", 2, T + 2, "w1")));
    m = applyEvent(m, ev(3, "job_retry_scheduled", "running", job("a", "retry_waiting", 2, T + 3)));
    m = applyEvent(m, ev(4, "job_claimed", "retry_waiting", job("a", "running", 3, T + 4, "w1")));
    m = applyEvent(m, ev(5, "job_failed", "running", job("a", "failed", 3, T + 5)));
    expect(m.resync).toBeNull();
    expect([count(m, "running"), count(m, "retry_waiting"), count(m, "failed")]).toEqual([0, 0, 1]);
  });

  it("applies heartbeats only to the current attempt", () => {
    let m = fromSnapshot(snapshot(0, [job("a", "running", 2, T, "w1")]), T);
    const hb = (seq: number, attempt: number, lease: number): DashboardEvent => ({
      seq,
      epoch: EPOCH,
      at_ms: T + seq,
      type: "worker_heartbeat",
      worker_id: "w1",
      job_id: "a",
      attempt,
      lease_expires_at_ms: lease,
    });
    m = applyEvent(m, hb(1, 1, T + 999_999));
    expect(m.jobs.a!.lease_expires_at_ms).toBe(T + 30_000);
    m = applyEvent(m, hb(2, 2, T + 40_000));
    expect(m.jobs.a!.lease_expires_at_ms).toBe(T + 40_000);
    expect(m.workers.w1!.last_seen_at_ms).toBe(T + 2);
  });
});

describe("applyWorkers", () => {
  it("never moves last_seen backwards", () => {
    let m = fromSnapshot(snapshot(0, []), T);
    m = applyWorkers(m, [{ ...worker, last_seen_at_ms: T + 50 }]);
    m = applyWorkers(m, [{ ...worker, last_seen_at_ms: T + 10 }]);
    expect(m.workers.w1!.last_seen_at_ms).toBe(T + 50);
  });
});

describe("workerHealth", () => {
  const lease = 30_000;
  it("derives health only from the coordinator's last_seen", () => {
    expect(workerHealth(worker, 1, T + 1_000, lease, true)).toBe("busy");
    expect(workerHealth(worker, 0, T + 1_000, lease, true)).toBe("idle");
    expect(workerHealth(worker, 1, T + lease + 1, lease, true)).toBe("unavailable");
    expect(workerHealth(worker, 0, T + 3 * lease + 1, lease, true)).toBe("disconnected");
  });
  it("is unknown while the dashboard itself is disconnected", () => {
    expect(workerHealth(worker, 0, T, lease, false)).toBe("unknown");
  });
});
