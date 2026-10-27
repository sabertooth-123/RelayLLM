# RelayLLM live dashboard

A browser view of a running coordinator: job counts, the coordinator–worker
network, the job queue and history, an event timeline, and a per-job
inspector. Every number comes from the coordinator. Nothing is simulated.

React + TypeScript + Vite. No UI framework, no state library, no chart
library. It talks only to the coordinator's HTTP API.

## Run it

```powershell
# 1. A coordinator (add --dev-controls for the failure button; loopback only)
cargo run -p relay-cli -- coordinator --dev-controls

# 2. Some workers, each in its own terminal
cargo run -p relay-cli -- worker --name alpha --concurrency 2
cargo run -p relay-cli -- worker --name beta --concurrency 2

# 3. The dashboard (Node 20+)
cd dashboard
npm install
npm run dev          # http://localhost:5173
```

The Vite dev server proxies `/v1` to `http://127.0.0.1:7070`, so the browser
stays same-origin and the coordinator needs no CORS setup. If the
coordinator listens elsewhere, set `RELAY_COORDINATOR` first, e.g.
`$env:RELAY_COORDINATOR = "http://127.0.0.1:7071"`.

Other scripts: `npm test` (reducer tests, vitest), `npm run typecheck`,
`npm run build` (type-check, then a static bundle in `dist/`).

## What it shows

| Area | Source |
|---|---|
| Summary cards | exact per-state counts from the snapshot, then moved by each applied transition |
| Network view | workers from the snapshot, `worker_registered` events, and `GET /v1/workers` every 5 s; slots filled from running jobs |
| Worker health | coordinator's `last_seen_at_ms` (claims, claim polls, heartbeats) vs. the lease. **busy/idle** if seen within one lease, **unavailable** after one lease, **disconnected** after three. While the dashboard itself is disconnected, every worker is **unknown**: a missing browser event never marks a worker as crashed |
| Jobs table | every active job (up to 500) plus the 100 most recently finished, then live events. Filter by status |
| Job inspector | `GET /v1/jobs/{id}`, `/attempts` and, on request, `/result`. The idempotency key is masked (with an explicit reveal). Payloads are never shown; the API doesn't expose them |
| Event timeline | the stream itself; heartbeats hidden by default |
| Submit form | `POST /v1/jobs` with `kind: "mock"` (delay, scripted failures, max attempts, count) |
| Failure button | `POST /v1/dev/workers/{id}/expire-leases`, only if the coordinator runs with `--dev-controls` |

Not offered: **pause/resume submissions**. The coordinator has no safe pause
mechanism, and faking one in the browser would lie about the system. It
applies backpressure instead (`503 queue_full` at `--max-active-jobs`).

## How it stays correct

The coordinator is the source of truth; the browser only holds a copy.

1. Open `GET /v1/events` (SSE). The first message, `hello`, carries the
   coordinator's `epoch` and the `seq` of the last event published before
   this subscription.
2. Fetch `GET /v1/dashboard/snapshot`. Events arriving meanwhile are
   buffered.
3. Build the model from the snapshot, then replay the buffer through the same
   reducer as live events (`src/state.ts`):
   * different epoch (coordinator restarted) or a gap in `seq` → **resync**;
   * `seq` already applied → **duplicate**, dropped;
   * a job transition whose `from` equals the held state → **applied**,
     counts moved `from` → `to`;
   * a transition the held job has already moved past (the snapshot read it
     after it committed, or events were published out of commit order) →
     **stale**, dropped;
   * anything else → **resync**.
4. Resync means: close the stream, back off, start again at 1.

The stream is **not** gap-free by design: it is in memory, restarts with the
coordinator, and drops a subscriber that falls more than 1,024 events behind
(it sends `resync` and closes). Every one of those cases ends in a fresh
snapshot, so the model can be stale for a moment but is never silently wrong.
The footer shows how many events were applied, dropped as duplicate or stale,
and how many resyncs happened.

Liveness: the coordinator sends a named `ping` every 5 s. If nothing arrives
for 15 s the dashboard reconnects. This matters because a proxy that loses the
upstream (Vite's does) can keep the browser's connection open indefinitely,
and SSE comments are invisible to `EventSource`.

Limits worth knowing:

* With more than 500 active jobs the snapshot is truncated; counts are then
  marked `≈` and the snapshot is refreshed every 60 s.
* Worker registrations are never deleted, so every past worker process stays
  in the view as `disconnected`. That is the coordinator's real state.
* Endpoints are unauthenticated, like the rest of the API. Keep the
  coordinator on loopback or a trusted network.
