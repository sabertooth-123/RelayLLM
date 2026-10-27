// Thin client for the coordinator. All paths are relative: the Vite dev
// server proxies /v1 to the coordinator (see vite.config.ts).

import type {
  ApiErrorBody,
  AttemptView,
  DashboardSnapshot,
  JobResultView,
  JobStatusView,
  SubmitJobRequest,
  SubmitJobResponse,
  WorkerView,
} from "./types";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      ...init,
      headers: { "content-type": "application/json", ...init?.headers },
    });
  } catch (e) {
    throw new ApiError(0, "network", `coordinator unreachable: ${String(e)}`);
  }
  const text = await response.text();
  let body: unknown = undefined;
  try {
    body = text ? JSON.parse(text) : undefined;
  } catch {
    // Not JSON (e.g. the dev proxy's own error page).
  }
  if (!response.ok) {
    const err = (body as ApiErrorBody | undefined)?.error;
    throw new ApiError(
      response.status,
      err?.code ?? "http_error",
      err?.message ?? `${response.status} ${response.statusText}`,
    );
  }
  return body as T;
}

export const api = {
  snapshot: () => request<DashboardSnapshot>("/v1/dashboard/snapshot"),
  workers: () => request<WorkerView[]>("/v1/workers"),
  job: (id: string) => request<JobStatusView>(`/v1/jobs/${encodeURIComponent(id)}`),
  attempts: (id: string) =>
    request<AttemptView[]>(`/v1/jobs/${encodeURIComponent(id)}/attempts`),
  result: (id: string) =>
    request<JobResultView>(`/v1/jobs/${encodeURIComponent(id)}/result`),
  submit: (body: SubmitJobRequest) =>
    request<SubmitJobResponse>("/v1/jobs", { method: "POST", body: JSON.stringify(body) }),
  cancel: (id: string) =>
    request<JobStatusView>(`/v1/jobs/${encodeURIComponent(id)}/cancel`, { method: "POST" }),
  expireLeases: (workerId: string) =>
    request<{ worker_id: string; expired_jobs: number }>(
      `/v1/dev/workers/${encodeURIComponent(workerId)}/expire-leases`,
      { method: "POST" },
    ),
};

export const EVENTS_URL = "/v1/events";
