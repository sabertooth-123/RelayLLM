# ADR 0003: Workers pull jobs over HTTP

**Status:** accepted (Phase 0)

## Context
The coordinator can push jobs to workers, or workers can pull them.

## Decision
Workers pull. A worker POSTs a claim request and gets a job or `204 No Content`.
It claims at most as many jobs as it has free execution slots.

## Why
* **Backpressure comes for free.** A busy worker simply doesn't ask. The
  coordinator never needs to know worker capacity, and memory can't grow
  without bound because jobs are never sent to a worker that has no slot.
* **No inbound connections to workers.** Workers can sit behind NAT or in
  containers without exposed ports.
* **A plain HTTP contract**, testable with `curl`.

## Costs
* Idle polling. This is mitigated by exponential backoff when the queue is empty
  (and possibly long-polling later).
* Claim latency of up to one poll interval.
