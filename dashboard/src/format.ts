import type { JobState } from "./types";

export const STATE_LABEL: Record<JobState, string> = {
  queued: "Queued",
  running: "Running",
  retry_waiting: "Retrying",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
};

/** "3.2 s", "4 min", ... for a duration in ms. Whole seconds drop the ".0". */
export function duration(ms: number): string {
  const abs = Math.abs(ms);
  if (abs < 1_000) return `${Math.round(ms)} ms`;
  if (abs < 60_000) {
    const s = ms / 1_000;
    return `${Number.isInteger(s) || abs >= 10_000 ? Math.round(s) : s.toFixed(1)} s`;
  }
  if (abs < 3_600_000) return `${Math.round(ms / 60_000)} min`;
  return `${(ms / 3_600_000).toFixed(1)} h`;
}

/** Relative to the coordinator's clock: "4.1 s ago", "in 12 s". */
export function ago(atMs: number, serverNowMs: number): string {
  const d = serverNowMs - atMs;
  return d >= 0 ? `${duration(d)} ago` : `in ${duration(-d)}`;
}

/**
 * For past events only ("updated", "last seen"). The local estimate of the
 * coordinator's clock is a few ms off, which made fresh updates read
 * "in 67 ms"; a past event is never in the future.
 */
export function since(atMs: number, serverNowMs: number): string {
  return `${duration(Math.max(0, serverNowMs - atMs))} ago`;
}

/** "alpha·f6a8": worker ids are `<name>-<uuid>`, unreadable in full. */
export function workerLabel(id: string, name?: string): string {
  const base = name ?? id.replace(/-[0-9a-f]{8}-[0-9a-f-]+$/i, "");
  return `${base}·${id.slice(-4)}`;
}

export function clock(atMs: number): string {
  const d = new Date(atMs);
  return `${d.toLocaleTimeString([], { hour12: false })}.${String(d.getMilliseconds()).padStart(3, "0")}`;
}

export function dateTime(atMs: number): string {
  return `${new Date(atMs).toLocaleDateString()} ${clock(atMs)}`;
}

/** Show enough of a secret to recognise it, never all of it. */
export function mask(secret: string): string {
  if (secret.length <= 4) return "••••";
  return `${secret.slice(0, 2)}${"•".repeat(Math.min(8, secret.length - 4))}${secret.slice(-2)}`;
}
