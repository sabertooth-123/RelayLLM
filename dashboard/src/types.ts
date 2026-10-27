// Mirrors of the coordinator's JSON types. Source of truth:
// crates/relay-core/src/api.rs and crates/relay-core/src/events.rs.

export type JobState =
  | "queued"
  | "running"
  | "retry_waiting"
  | "succeeded"
  | "failed"
  | "cancelled";

export const JOB_STATES: readonly JobState[] = [
  "queued",
  "running",
  "retry_waiting",
  "succeeded",
  "failed",
  "cancelled",
];

export const TERMINAL: ReadonlySet<JobState> = new Set(["succeeded", "failed", "cancelled"]);

export type AttemptOutcome =
  | "succeeded"
  | "retryable_error"
  | "permanent_error"
  | "lease_expired"
  | "cancelled";

/** A job as broadcast to observers: no payload, result or idempotency key. */
export interface JobSummary {
  id: string;
  kind: string;
  state: JobState;
  /** Current attempt number; also the fencing token. */
  attempt: number;
  max_attempts: number;
  worker_id: string | null;
  has_idempotency_key: boolean;
  last_error: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  finished_at_ms: number | null;
  available_at_ms: number;
  lease_expires_at_ms: number | null;
}

export interface WorkerView {
  id: string;
  name: string;
  concurrency: number;
  kinds: string[];
  registered_at_ms: number;
  last_seen_at_ms: number;
  running_jobs: number;
}

interface Transition<T extends string> {
  type: T;
  from: JobState | null;
  job: JobSummary;
}

export type EventKind =
  | Transition<"job_submitted">
  | Transition<"job_claimed">
  | Transition<"job_succeeded">
  | Transition<"job_retry_scheduled">
  | Transition<"job_failed">
  | Transition<"job_cancelled">
  | (Transition<"lease_expired"> & { worker_id: string; reason: string })
  | { type: "worker_registered"; worker: WorkerView }
  | {
      type: "worker_heartbeat";
      worker_id: string;
      job_id: string;
      attempt: number;
      lease_expires_at_ms: number;
    };

export type DashboardEvent = EventKind & { seq: number; epoch: string; at_ms: number };

export type JobEvent = Extract<DashboardEvent, { job: JobSummary }>;

export function isJobEvent(e: DashboardEvent): e is JobEvent {
  return "job" in e;
}

export interface StateCount {
  state: JobState;
  count: number;
}

export interface DashboardSnapshot {
  epoch: string;
  /** Every event up to this seq is reflected below; later ones may be. */
  seq: number;
  server_time_ms: number;
  lease_ms: number;
  max_active_jobs: number;
  counts: StateCount[];
  workers: WorkerView[];
  jobs: JobSummary[];
  active_jobs_truncated: boolean;
  recent_events: DashboardEvent[];
  dev_controls: boolean;
}

export interface Hello {
  epoch: string;
  seq: number;
}

// Existing per-job endpoints, used by the inspector.

export interface JobStatusView {
  id: string;
  kind: string;
  state: JobState;
  attempt: number;
  max_attempts: number;
  idempotency_key: string | null;
  worker_id: string | null;
  last_error: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  finished_at_ms: number | null;
}

export interface AttemptView {
  attempt: number;
  worker_id: string;
  started_at_ms: number;
  finished_at_ms: number | null;
  outcome: AttemptOutcome | null;
  error: string | null;
}

export interface JobResultView {
  id: string;
  state: JobState;
  result: unknown;
  error: string | null;
}

export interface SubmitJobRequest {
  kind: string;
  payload: unknown;
  idempotency_key?: string;
  max_attempts?: number;
  timeout_secs?: number;
}

export interface SubmitJobResponse {
  created: boolean;
  job: JobStatusView;
}

export interface ApiErrorBody {
  error: { code: string; message: string };
}
