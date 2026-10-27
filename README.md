# RelayLLM

A fault-tolerant, durable job queue for LLM workloads, written in Rust.

A single **coordinator** accepts jobs over HTTP and persists them in SQLite.
Independent **worker** processes claim jobs, run them, and report results.
Abandoned work is recovered through leases, and stale workers are fenced out
with attempt numbers. The point of the project is the distributed-systems
engineering: durability, idempotency, failure detection, backpressure and
recovery. LLM calls are just the workload.

> **Status: v0.1.0 release candidate.** All five roadmap phases are done.
> Remaining steps (repository, tag, publishing) are in
> [`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md).

![RelayLLM architecture](docs/architecture.svg)

## See it work in one command

```powershell
powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
```

This builds `relay` and runs `relay demo`. The demo starts a coordinator and
three worker processes, then:

1. submits 100 jobs and shows them running concurrently;
2. hard-kills a worker mid-job and shows the lease expiring and another worker
   finishing it (attempt 2);
3. hard-kills and restarts the coordinator and shows every result intact;
4. checks idempotency;
5. replays a "zombie" worker whose late result is rejected
   (`409 stale_attempt`).

Then it runs the whole test suite. Every step is a check, and the script
fails if any check fails. `scripts\demo-compose.ps1` does the same against the
Docker Compose deployment, using `docker kill` for the failures.

## Guarantees

* **Durable acceptance.** A `201` means the job is committed to disk (`synchronous=FULL`).
* **Idempotent submission.** The same `idempotency_key` + same request → the same job.
  Same key + different request → `409`.
* **At-least-once execution.** A job can run more than once if a worker
  crashes after doing the work but before reporting it. **Exactly-once execution
  is not promised.** Handlers that call paid APIs or cause side effects should be idempotent.
* **At most one recorded result** per job: every report is fenced by its
  attempt number, so a stale attempt gets `409` and can't overwrite a newer one.
* **No double claims.** Claims are atomic, and a claim retried after a lost
  response returns the same job instead of taking another.
* **Crashed workers are recovered.** If a worker stops heartbeating, its job
  is retried after one lease (default 30 s), with exponential backoff. A
  handler that hangs while its heartbeats continue is cut off at its absolute
  deadline (timeout + grace).
* **Coordinator restarts lose nothing and duplicate nothing avoidable.**
  Acknowledged jobs and results survive a hard kill. Running jobs get one
  lease of grace, so workers that kept going through the outage can still
  deliver.
* **Bounded queue.** Over capacity → `503 queue_full`, never unbounded growth.

Limitations: one coordinator (an availability single point of failure),
no authentication (trusted local network only), idempotency keys never expire.

## Quick start (Windows PowerShell)

Prerequisites: Rust stable (via [rustup](https://rustup.rs)) and, on Windows,
Visual Studio Build Tools with the C++ workload (needed for linking and for the
bundled SQLite).

```powershell
cargo build --release
$relay = ".\target\release\relay.exe"

# Terminal 1: coordinator (creates relay.db in the current folder)
& $relay coordinator --db relay.db

# Terminals 2-4: workers (each an independent process)
& $relay worker --name w1 --concurrency 2
& $relay worker --name w2 --concurrency 2
& $relay worker --name w3 --concurrency 2

# Terminal 5: client
& $relay submit --prompt "Summarize the CAP theorem" --idempotency-key demo-1
& $relay submit --prompt "Summarize the CAP theorem" --idempotency-key demo-1   # same job, nothing new
& $relay result <job-id> --wait
& $relay status <job-id>
& $relay attempts <job-id>          # which worker ran each attempt, and how it ended
& $relay cancel <job-id>            # queued/running/retrying jobs; exit 2 if it already finished
& $relay workers                    # registered workers and their running jobs
& $relay health                     # exit 0 if the coordinator and its DB are up
& $relay --json status <job-id>     # machine-readable
curl.exe http://127.0.0.1:7070/metrics   # Prometheus metrics
```

### With Docker Compose (1 coordinator + 3 workers)

```powershell
docker compose up --build -d
docker compose ps                    # coordinator healthy, 3 workers
& $relay submit --prompt "hello from compose"
docker compose logs -f worker
docker compose down                  # keeps the database volume; add -v to delete it
```

The coordinator is published on `127.0.0.1:7070` only, because there is no
authentication. `docker compose stop` sends SIGTERM: the coordinator drains
requests for up to 10 s, and workers finish and report in-flight jobs.

### Real LLM jobs (`chat`)

Any OpenAI-compatible server works (Ollama, llama.cpp `llama-server`, vLLM,
LM Studio). The endpoint must be `http://`, because this build has no TLS.

```powershell
& $relay worker --name gpu1 --openai-base-url http://127.0.0.1:11434/v1 --openai-model llama3.2:1b
& $relay submit --kind chat --prompt "Explain leases in one sentence."
```

If the server needs a key, pass the *name* of an environment variable:
`--openai-api-key-env OPENAI_API_KEY`. The key is never accepted as a flag,
and it is never logged. Compose has an optional profile for this:
`docker compose --profile ollama up -d`.

The built-in `mock` handler is a deterministic stand-in for an LLM (no model
or API key needed). Its payload is `{"prompt": ..., "delay_ms": ...,
"fail_attempts": ..., "fail_kind": "retryable"|"permanent"}`. The last two
script failures, so you can watch the retry policy work.

Use `--prompt` or `--payload-file payload.json` rather than inline `--payload`
JSON. Windows PowerShell 5.1 mangles embedded double quotes when it passes
arguments to native programs.

`relay result` exit codes: `0` succeeded, `1` failed/cancelled/error, `2` still in progress.

### Configuration

| Flag | Env var | Default |
|---|---|---|
| `--server` (client cmds) | `RELAY_URL` | `http://127.0.0.1:7070` |
| `--db` | `RELAY_DB` | `relay.db` |
| `--bind` | `RELAY_BIND` | `127.0.0.1:7070` |
| `--max-active-jobs` | `RELAY_MAX_ACTIVE_JOBS` | `10000` |
| `--lease-secs` | `RELAY_LEASE_SECS` | `30` |
| `--retry-base-ms` / `--retry-max-ms` | `RELAY_RETRY_BASE_MS` / `RELAY_RETRY_MAX_MS` | `1000` / `60000` |
| `--deadline-grace-secs` | `RELAY_DEADLINE_GRACE_SECS` | `30` |
| `--request-timeout-secs` | `RELAY_REQUEST_TIMEOUT_SECS` | `10` |
| `--shutdown-drain-secs` | `RELAY_SHUTDOWN_DRAIN_SECS` | `10` |
| `worker --name` | `RELAY_WORKER_NAME` | `worker` |
| `worker --concurrency` | `RELAY_WORKER_CONCURRENCY` | `4` |
| `worker --mock-delay-ms` | `RELAY_MOCK_DELAY_MS` | `200` |
| `worker --openai-base-url` / `--openai-model` | `RELAY_OPENAI_BASE_URL` / `RELAY_OPENAI_MODEL` | unset (no `chat` jobs) |
| `worker --openai-api-key-env` | `RELAY_OPENAI_API_KEY_ENV` | unset |
| `--sqlite-synchronous full\|normal` | `RELAY_SQLITE_SYNCHRONOUS` | `full` |
| `--log-format text\|json` | `RELAY_LOG_FORMAT` | `text` |
| log filter | `RELAY_LOG` | `info` |

Invalid settings are reported all at once, and the process exits with code 1.
Risky-but-legal ones (e.g. binding a non-loopback address) log a warning.

Keep the database on a local disk. Network drives and sync folders (OneDrive,
Dropbox) break SQLite locking.

## Performance

Measured, not estimated. Full numbers, method and caveats are in
[`bench/README.md`](bench/README.md); raw CSV/JSON is in `bench/results/`.
The short version, on one Windows laptop running everything:

* With the default `--sqlite-synchronous full`, every commit is an fsync, and
  **fsync latency caps throughput at roughly 60–100 jobs/s**. Adding workers
  doesn't raise that ceiling. For real LLM jobs, which take seconds each, it
  is rarely the bottleneck.
* With `normal` (see the configuration table for the trade-off), the
  coordinator completes up to **~735 zero-work jobs/s** and accepts ~7,000
  submissions/s.
* Writes go through a dedicated single-connection pool and reads through a
  separate read-only pool. That split alone raised `normal` throughput by
  up to 9.8×, because colliding writers had been sleep-polling inside
  SQLite's busy handler.

## Development

```powershell
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
powershell -ExecutionPolicy Bypass -File scripts\bench.ps1   # reproduce the benchmark
```

## Documentation

* [`ARCHITECTURE.md`](ARCHITECTURE.md): components, state machine, claim/lease/fencing design, sequence diagrams, risks
* [`docs/api.md`](docs/api.md): HTTP contract
* [`docs/TEST_REPORT.md`](docs/TEST_REPORT.md): what is tested, how, and the latest results
* [`bench/README.md`](bench/README.md): benchmark method and results
* [`docs/decisions/`](docs/decisions): architecture decision records
* [`docs/reviews/`](docs/reviews): correctness review
* [`docs/RELEASE_CHECKLIST.md`](docs/RELEASE_CHECKLIST.md), [`CHANGELOG.md`](CHANGELOG.md), [`ROADMAP.md`](ROADMAP.md)

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
