// Connection lifecycle: SSE first, then the snapshot, then live events.
//
//   open /v1/events ──hello{epoch,seq}──▶ GET /v1/dashboard/snapshot
//        │ events are buffered until the snapshot is in, then replayed
//        ▼ through the same reducer (duplicates are dropped by seq)
//   live ──(resync / gap / epoch / error)──▶ close, back off, start over
//
// Worker liveness also needs claim polls, which emit no events, so
// /v1/workers is polled every few seconds while live.

import { useCallback, useEffect, useReducer, useRef } from "react";

import { api, EVENTS_URL } from "./api";
import { applyEvent, applyWorkers, fromSnapshot, type Model } from "./state";
import type { DashboardEvent, DashboardSnapshot, Hello, WorkerView } from "./types";

export type Connection = "connecting" | "syncing" | "live" | "reconnecting";

export interface DashboardState {
  connection: Connection;
  model: Model | null;
  /** The last connection problem, cleared once live again. */
  error: string | null;
  /** Number of times the model was rebuilt from a snapshot. */
  resyncs: number;
  lastSyncAt: number | null;
}

type Action =
  | { type: "connection"; connection: Connection; error?: string | null }
  | { type: "snapshot"; snapshot: DashboardSnapshot; buffered: DashboardEvent[] }
  | { type: "event"; event: DashboardEvent }
  | { type: "workers"; workers: WorkerView[] };

const initial: DashboardState = {
  connection: "connecting",
  model: null,
  error: null,
  resyncs: 0,
  lastSyncAt: null,
};

function reducer(state: DashboardState, action: Action): DashboardState {
  switch (action.type) {
    case "connection":
      return {
        ...state,
        connection: action.connection,
        error: action.error === undefined ? state.error : action.error,
      };
    case "snapshot": {
      let model = fromSnapshot(action.snapshot, Date.now());
      for (const event of action.buffered) model = applyEvent(model, event);
      return {
        ...state,
        model,
        connection: "live",
        error: null,
        resyncs: state.model ? state.resyncs + 1 : state.resyncs,
        lastSyncAt: Date.now(),
      };
    }
    case "event":
      return state.model && state.connection === "live"
        ? { ...state, model: applyEvent(state.model, action.event) }
        : state;
    case "workers":
      return state.model ? { ...state, model: applyWorkers(state.model, action.workers) } : state;
  }
}

const WORKER_POLL_MS = 5_000;
const MAX_BACKOFF_MS = 10_000;
const SILENCE_LIMIT_MS = 15_000;

export function useDashboard() {
  const [state, dispatch] = useReducer(reducer, initial);
  const generation = useRef(0);
  const source = useRef<EventSource | null>(null);
  const retryTimer = useRef<number | undefined>(undefined);
  const failures = useRef(0);

  const connect = useCallback(() => {
    const gen = ++generation.current;
    source.current?.close();
    window.clearTimeout(retryTimer.current);
    dispatch({ type: "connection", connection: "connecting" });

    const es = new EventSource(EVENTS_URL);
    source.current = es;
    let buffered: DashboardEvent[] | null = [];
    const current = () => gen === generation.current;

    let stopped = false;
    const restart = (reason: string) => {
      if (!current() || stopped) return;
      stopped = true;
      es.close();
      failures.current += 1;
      const delay = Math.min(MAX_BACKOFF_MS, 250 * 2 ** Math.min(failures.current, 6));
      dispatch({ type: "connection", connection: "reconnecting", error: reason });
      retryTimer.current = window.setTimeout(connect, delay);
    };

    // The coordinator sends `ping` every 5 s. If nothing at all arrives for
    // three of those, the connection is dead even if the browser (or a
    // proxy in between) hasn't noticed.
    let lastMessage = Date.now();
    const seen = () => {
      lastMessage = Date.now();
    };
    const watchdog = window.setInterval(() => {
      if (!current() || stopped) return window.clearInterval(watchdog);
      if (Date.now() - lastMessage > SILENCE_LIMIT_MS) restart("no data from the coordinator for 15 s");
    }, 1_000);
    es.addEventListener("ping", seen);

    es.addEventListener("hello", (e) => {
      if (!current()) return;
      seen();
      const hello = JSON.parse((e as MessageEvent<string>).data) as Hello;
      dispatch({ type: "connection", connection: "syncing" });
      api
        .snapshot()
        .then((snapshot) => {
          if (!current()) return;
          if (snapshot.epoch !== hello.epoch || snapshot.seq < hello.seq) {
            restart("coordinator restarted during sync");
            return;
          }
          failures.current = 0;
          const events = buffered ?? [];
          buffered = null;
          dispatch({ type: "snapshot", snapshot, buffered: events });
        })
        .catch((err: unknown) => restart(`snapshot failed: ${String(err)}`));
    });

    const onEvent = (e: Event) => {
      if (!current()) return;
      seen();
      const event = JSON.parse((e as MessageEvent<string>).data) as DashboardEvent;
      if (buffered) buffered.push(event);
      else dispatch({ type: "event", event });
    };
    es.addEventListener("job", onEvent);
    es.addEventListener("worker", onEvent);
    es.addEventListener("resync", () => restart("event stream fell behind"));
    // EventSource retries some failures itself, but not all (e.g. a proxy
    // error page). Always take over so the snapshot handshake re-runs.
    es.onerror = () => restart("lost connection to the coordinator");
  }, []);

  // Open on mount; close on unmount.
  useEffect(() => {
    connect();
    return () => {
      generation.current += 1;
      source.current?.close();
      window.clearTimeout(retryTimer.current);
    };
  }, [connect]);

  // The reducer flagged the stream as unusable: rebuild from a snapshot.
  const resyncReason = state.model?.resync ?? null;
  useEffect(() => {
    if (resyncReason) {
      dispatch({ type: "connection", connection: "reconnecting", error: `resync: ${resyncReason}` });
      connect();
    }
  }, [resyncReason, connect]);

  // Poll workers while live: idle polling bumps last_seen without events.
  const live = state.connection === "live";
  useEffect(() => {
    if (!live) return;
    const timer = window.setInterval(() => {
      api
        .workers()
        .then((workers) => dispatch({ type: "workers", workers }))
        .catch(() => {
          // The event stream reports connection problems.
        });
    }, WORKER_POLL_MS);
    return () => window.clearInterval(timer);
  }, [live]);

  // A truncated snapshot's counts can drift; refresh it once a minute.
  const truncated = state.model?.activeTruncated ?? false;
  useEffect(() => {
    if (!live || !truncated) return;
    const timer = window.setTimeout(connect, 60_000);
    return () => window.clearTimeout(timer);
  }, [live, truncated, connect, state.lastSyncAt]);

  return { ...state, reconnect: connect };
}
