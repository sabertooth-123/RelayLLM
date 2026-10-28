// The dashboard's model of the coordinator, and the rules for keeping it in
// step with the event stream. Pure functions, unit-tested in state.test.ts.
//
// The coordinator's snapshot is the source of truth. Events only move the
// model forward from a snapshot, and only when they provably fit:
//
//   * a different epoch (coordinator restarted) or a gap in seq => resync;
//   * seq <= the last one applied => duplicate, ignored;
//   * a job transition whose `from` matches the job's held state => applied;
//   * a transition the held job has already moved past (snapshot raced the
//     event, or events arrived out of commit order) => stale, ignored;
//   * anything else => resync.
//
// "Resync" means: stop applying events, fetch a fresh snapshot, start over.

import type {
  DashboardEvent,
  DashboardSnapshot,
  JobEvent,
  JobState,
  JobSummary,
  WorkerView,
} from "./types";
import { JOB_STATES, TERMINAL, isJobEvent } from "./types";

/** Events kept for the timeline. */
export const EVENT_LOG_LIMIT = 500;
/** Terminal jobs kept in memory (active jobs are never evicted). */
export const TERMINAL_JOB_LIMIT = 300;

export interface SyncStats {
  applied: number;
  duplicates: number;
  stale: number;
}

export type ActivityKind = "claim" | "heartbeat" | "done" | "retry" | "fail" | "expired";

/** One worker ↔ coordinator exchange, for the topology animation. */
export interface Activity {
  seq: number;
  workerId: string;
  kind: ActivityKind;
}

export const ACTIVITY_LIMIT = 200;

export interface Model {
  epoch: string;
  /** Last applied seq. */
  seq: number;
  /** server clock - local clock, measured when the snapshot arrived. */
  clockOffsetMs: number;
  leaseMs: number;
  maxActiveJobs: number;
  devControls: boolean;
  /**
   * The snapshot didn't include every active job. Counts may then drift by
   * a few until the next snapshot, and the UI marks them as approximate.
   */
  activeTruncated: boolean;
  counts: Record<JobState, number>;
  jobs: Record<string, JobSummary>;
  workers: Record<string, WorkerView>;
  /** Oldest first. */
  events: DashboardEvent[];
  /**
   * Applied exchanges with the worker involved, oldest first. Completion
   * events carry `worker_id: null` (the job no longer has an owner), so the
   * worker comes from the job as held just before the transition.
   */
  activity: Activity[];
  /** Set when the stream can no longer be applied; the hook resyncs. */
  resync: string | null;
  stats: SyncStats;
}

export function fromSnapshot(snap: DashboardSnapshot, localNowMs: number): Model {
  const counts = Object.fromEntries(JOB_STATES.map((s) => [s, 0])) as Record<JobState, number>;
  for (const c of snap.counts) counts[c.state] = c.count;
  return {
    epoch: snap.epoch,
    seq: snap.seq,
    clockOffsetMs: snap.server_time_ms - localNowMs,
    leaseMs: snap.lease_ms,
    maxActiveJobs: snap.max_active_jobs,
    devControls: snap.dev_controls,
    activeTruncated: snap.active_jobs_truncated,
    counts,
    jobs: Object.fromEntries(snap.jobs.map((j) => [j.id, j])),
    workers: Object.fromEntries(snap.workers.map((w) => [w.id, w])),
    events: snap.recent_events.slice(-EVENT_LOG_LIMIT),
    activity: [],
    resync: null,
    stats: { applied: 0, duplicates: 0, stale: 0 },
  };
}

export function applyEvent(model: Model, event: DashboardEvent): Model {
  if (model.resync) return model;
  if (event.epoch !== model.epoch) {
    return { ...model, resync: "coordinator restarted (new epoch)" };
  }
  if (event.seq <= model.seq) {
    return { ...model, stats: { ...model.stats, duplicates: model.stats.duplicates + 1 } };
  }
  if (event.seq !== model.seq + 1) {
    return { ...model, resync: `missed events ${model.seq + 1}..${event.seq - 1}` };
  }

  const next: Model = {
    ...model,
    seq: event.seq,
    events: appendCapped(model.events, event, EVENT_LOG_LIMIT),
  };
  if (isJobEvent(event)) return applyTransition(next, event);

  switch (event.type) {
    case "worker_registered":
      return counted({
        ...next,
        workers: { ...next.workers, [event.worker.id]: mergeWorker(next.workers[event.worker.id], event.worker) },
      });
    case "worker_heartbeat": {
      const workers = touchWorker(next.workers, event.worker_id, event.at_ms);
      const held = next.jobs[event.job_id];
      // Only the current attempt's heartbeat says anything about the lease.
      const jobs =
        held && held.state === "running" && held.attempt === event.attempt
          ? {
              ...next.jobs,
              [held.id]: {
                ...held,
                lease_expires_at_ms: Math.max(held.lease_expires_at_ms ?? 0, event.lease_expires_at_ms),
                updated_at_ms: Math.max(held.updated_at_ms, event.at_ms),
              },
            }
          : next.jobs;
      const activity = appendCapped<Activity>(
        next.activity,
        { seq: event.seq, workerId: event.worker_id, kind: "heartbeat" },
        ACTIVITY_LIMIT,
      );
      return counted({ ...next, workers, jobs, activity });
    }
  }
}

const TRANSITION_ACTIVITY: Partial<Record<JobEvent["type"], ActivityKind>> = {
  job_claimed: "claim",
  job_succeeded: "done",
  job_retry_scheduled: "retry",
  job_failed: "fail",
  lease_expired: "expired",
};

function activityFor(event: JobEvent, held: JobSummary | undefined): Activity | null {
  const kind = TRANSITION_ACTIVITY[event.type];
  if (!kind) return null;
  const workerId =
    event.type === "job_claimed"
      ? event.job.worker_id
      : event.type === "lease_expired"
        ? event.worker_id
        : (held?.worker_id ?? null);
  return workerId ? { seq: event.seq, workerId, kind } : null;
}

function applyTransition(model: Model, event: JobEvent): Model {
  const { from, job } = event;
  const held = model.jobs[job.id];
  let { counts, workers } = model;

  if (held) {
    if (sameVersion(held, job)) {
      // The snapshot was read after this event committed.
      return stale(model);
    }
    if (held.state !== from || held.attempt > job.attempt) {
      if (held.updated_at_ms >= job.updated_at_ms || held.attempt > job.attempt) {
        // The held job is already further along.
        return stale(model);
      }
      return { ...model, resync: `job ${short(job.id)}: ${event.type} from ${from} but held ${held.state}` };
    }
    counts = shift(counts, from, job.state);
  } else if (from === null || model.activeTruncated) {
    // A new job; or, with a truncated snapshot, a job we never saw. In the
    // latter case the counts may already include it (marked approximate).
    counts = shift(counts, from, job.state);
  }
  // else: not held although the snapshot listed every active job, so this
  // event committed before the snapshot read it (counts include it). The
  // job is history now; keep it without touching counts.

  if (event.type === "job_claimed" && job.worker_id) {
    workers = touchWorker(workers, job.worker_id, event.at_ms);
  }
  const act = activityFor(event, held);
  return counted({
    ...model,
    counts,
    workers,
    jobs: evictTerminal({ ...model.jobs, [job.id]: job }),
    activity: act ? appendCapped(model.activity, act, ACTIVITY_LIMIT) : model.activity,
  });
}

/** Merge a polled worker list: the newer `last_seen_at_ms` wins. */
export function applyWorkers(model: Model, polled: WorkerView[]): Model {
  const workers = { ...model.workers };
  for (const w of polled) workers[w.id] = mergeWorker(workers[w.id], w);
  return { ...model, workers };
}

function mergeWorker(held: WorkerView | undefined, fresh: WorkerView): WorkerView {
  if (!held) return fresh;
  return { ...fresh, last_seen_at_ms: Math.max(held.last_seen_at_ms, fresh.last_seen_at_ms) };
}

function touchWorker(workers: Record<string, WorkerView>, id: string, at: number) {
  const w = workers[id];
  if (!w || w.last_seen_at_ms >= at) return workers;
  return { ...workers, [id]: { ...w, last_seen_at_ms: at } };
}

function sameVersion(a: JobSummary, b: JobSummary): boolean {
  return a.state === b.state && a.attempt === b.attempt && a.updated_at_ms === b.updated_at_ms;
}

function shift(counts: Record<JobState, number>, from: JobState | null, to: JobState) {
  const next = { ...counts, [to]: counts[to] + 1 };
  if (from) next[from] = Math.max(0, next[from] - 1);
  return next;
}

function stale(model: Model): Model {
  return { ...model, stats: { ...model.stats, stale: model.stats.stale + 1 } };
}

function counted(model: Model): Model {
  return { ...model, stats: { ...model.stats, applied: model.stats.applied + 1 } };
}

function appendCapped<T>(list: T[], item: T, limit: number): T[] {
  const next = list.length >= limit ? list.slice(list.length - limit + 1) : list.slice();
  next.push(item);
  return next;
}

function evictTerminal(jobs: Record<string, JobSummary>): Record<string, JobSummary> {
  const terminal = Object.values(jobs).filter((j) => TERMINAL.has(j.state));
  if (terminal.length <= TERMINAL_JOB_LIMIT) return jobs;
  terminal.sort((a, b) => (a.finished_at_ms ?? 0) - (b.finished_at_ms ?? 0));
  const next = { ...jobs };
  for (const j of terminal.slice(0, terminal.length - TERMINAL_JOB_LIMIT)) delete next[j.id];
  return next;
}

export function short(id: string): string {
  return id.slice(-8);
}

// ---- derived views -------------------------------------------------------

export type WorkerHealth = "busy" | "idle" | "unavailable" | "disconnected" | "unknown";

/**
 * Health from the coordinator's own `last_seen_at_ms` (bumped by claims,
 * claim polls and heartbeats), never from missing browser events:
 *
 *   * unknown       — the dashboard itself is not live, so we can't tell;
 *   * disconnected  — not seen for over 3 leases (the /metrics "live" window);
 *   * unavailable   — not seen for over one lease: it missed heartbeats;
 *   * busy / idle   — seen recently, with / without running jobs.
 */
export function workerHealth(
  worker: WorkerView,
  running: number,
  serverNowMs: number,
  leaseMs: number,
  live: boolean,
): WorkerHealth {
  if (!live) return "unknown";
  const silence = serverNowMs - worker.last_seen_at_ms;
  if (silence > 3 * leaseMs) return "disconnected";
  if (silence > leaseMs) return "unavailable";
  return running > 0 ? "busy" : "idle";
}

/** Running jobs per worker, from the jobs held. */
export function runningByWorker(model: Model): Record<string, JobSummary[]> {
  const out: Record<string, JobSummary[]> = {};
  for (const j of Object.values(model.jobs)) {
    if (j.state === "running" && j.worker_id) (out[j.worker_id] ??= []).push(j);
  }
  return out;
}
