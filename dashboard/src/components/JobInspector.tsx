import { useEffect, useState } from "react";

import { api } from "../api";
import { ago, dateTime, duration, mask, STATE_LABEL } from "../format";
import type { Model } from "../state";
import { TERMINAL, type AttemptView, type JobResultView, type JobStatusView } from "../types";

interface Props {
  model: Model;
  jobId: string;
  serverNow: number;
  onSelectWorker: (id: string) => void;
  onClose: () => void;
}

interface Loaded {
  status: JobStatusView;
  attempts: AttemptView[];
}

const NA = <span className="muted">unavailable</span>;

export function JobInspector({ model, jobId, serverNow, onSelectWorker, onClose }: Props) {
  const summary = model.jobs[jobId];
  const [loaded, setLoaded] = useState<Loaded | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<JobResultView | null>(null);
  const [showResult, setShowResult] = useState(false);
  const [revealKey, setRevealKey] = useState(false);

  // Re-read the authoritative per-job endpoints whenever the job changes.
  const version = summary ? `${summary.state}:${summary.attempt}:${summary.updated_at_ms}` : "";
  useEffect(() => {
    let cancelled = false;
    setError(null);
    Promise.all([api.job(jobId), api.attempts(jobId)])
      .then(([status, attempts]) => !cancelled && setLoaded({ status, attempts }))
      .catch((e: unknown) => !cancelled && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      cancelled = true;
    };
  }, [jobId, version]);

  useEffect(() => {
    setShowResult(false);
    setResult(null);
    setRevealKey(false);
  }, [jobId]);

  useEffect(() => {
    if (!showResult) return;
    let cancelled = false;
    api
      .result(jobId)
      .then((r) => !cancelled && setResult(r))
      .catch((e: unknown) => !cancelled && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      cancelled = true;
    };
  }, [jobId, showResult, version]);

  const status = loaded?.status.id === jobId ? loaded.status : null;
  const attempts = loaded?.status.id === jobId ? loaded.attempts : [];
  const state = status?.state ?? summary?.state;
  const terminal = state ? TERMINAL.has(state) : false;

  const cancel = async () => {
    if (!window.confirm("Cancel this job? A running attempt is fenced out.")) return;
    try {
      await api.cancel(jobId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <aside className="panel side">
      <div className="panel-head">
        <h2>
          Job <span className="mono">…{jobId.slice(-8)}</span>
        </h2>
        <button className="ghost" onClick={onClose} aria-label="Close">✕</button>
      </div>
      {error ? <p className="err">{error}</p> : null}
      {!status && !summary && !error ? <p className="muted">Loading…</p> : null}

      {state ? (
        <p>
          <span className={`pill p-${state}`}>{STATE_LABEL[state]}</span>
          {!terminal ? (
            <button className="ghost small-btn" onClick={cancel}>
              Cancel job
            </button>
          ) : null}
        </p>
      ) : null}

      <dl className="kv">
        <dt>ID</dt>
        <dd className="mono wrap">{jobId}</dd>
        <dt>Kind</dt>
        <dd className="mono">{status?.kind ?? summary?.kind ?? NA}</dd>
        <dt>Attempt</dt>
        <dd>
          {status || summary ? (
            <>
              {(status ?? summary)!.attempt} of {(status ?? summary)!.max_attempts}{" "}
              <span className="muted small">(the attempt number is the fencing token)</span>
            </>
          ) : (
            NA
          )}
        </dd>
        <dt>Worker</dt>
        <dd>
          {status?.worker_id ? (
            <button className="link mono" onClick={() => onSelectWorker(status.worker_id!)}>
              {status.worker_id}
            </button>
          ) : (
            <span className="muted">{terminal ? "none (finished)" : "not assigned"}</span>
          )}
        </dd>
        <dt>Created</dt>
        <dd>{status ? dateTime(status.created_at_ms) : NA}</dd>
        <dt>Updated</dt>
        <dd>{status ? `${dateTime(status.updated_at_ms)} (${ago(status.updated_at_ms, serverNow)})` : NA}</dd>
        <dt>Finished</dt>
        <dd>{status?.finished_at_ms ? dateTime(status.finished_at_ms) : <span className="muted">not finished</span>}</dd>
        {state === "retry_waiting" && summary ? (
          <>
            <dt>Next try</dt>
            <dd>{ago(summary.available_at_ms, serverNow)}</dd>
          </>
        ) : null}
        {state === "running" ? (
          <>
            <dt>Lease</dt>
            <dd>
              {summary?.lease_expires_at_ms ? `expires ${ago(summary.lease_expires_at_ms, serverNow)}` : NA}
            </dd>
          </>
        ) : null}
        <dt>Idempotency</dt>
        <dd>
          {status ? (
            status.idempotency_key ? (
              <>
                <span className="mono">{revealKey ? status.idempotency_key : mask(status.idempotency_key)}</span>{" "}
                <button className="link small" onClick={() => setRevealKey((v) => !v)}>
                  {revealKey ? "hide" : "reveal"}
                </button>
              </>
            ) : (
              <span className="muted">no key</span>
            )
          ) : (
            NA
          )}
        </dd>
        <dt>Last error</dt>
        <dd className="err wrap">{status?.last_error ?? <span className="muted">none</span>}</dd>
      </dl>

      <h3>Attempts</h3>
      {attempts.length === 0 ? (
        <p className="muted">{status ? "Not claimed yet." : "…"}</p>
      ) : (
        <div className="table-wrap">
          <table className="compact">
            <thead>
              <tr>
                <th>#</th>
                <th>Worker</th>
                <th>Outcome</th>
                <th>Took</th>
              </tr>
            </thead>
            <tbody>
              {attempts.map((a) => (
                <tr key={a.attempt} title={a.error ?? undefined}>
                  <td className="mono">{a.attempt}</td>
                  <td className="mono">{a.worker_id}</td>
                  <td>
                    {a.outcome ? (
                      <span className={`pill o-${a.outcome}`}>{a.outcome.replace("_", " ")}</span>
                    ) : (
                      <span className="pill p-running">in progress</span>
                    )}
                    {a.error ? <div className="err small wrap">{a.error}</div> : null}
                  </td>
                  <td className="muted">
                    {a.finished_at_ms ? duration(a.finished_at_ms - a.started_at_ms) : ago(a.started_at_ms, serverNow)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <h3>Result</h3>
      {!terminal ? (
        <p className="muted">Unavailable until the job finishes.</p>
      ) : !showResult ? (
        <button className="ghost" onClick={() => setShowResult(true)}>
          Load result (may contain model output)
        </button>
      ) : result ? (
        result.result !== null && result.result !== undefined ? (
          <pre className="result">{JSON.stringify(result.result, null, 2)}</pre>
        ) : (
          <p className="muted">No result ({result.error ?? STATE_LABEL[result.state].toLowerCase()}).</p>
        )
      ) : (
        <p className="muted">Loading…</p>
      )}
      <p className="muted small">Payloads are never shown here; the coordinator does not expose them.</p>
    </aside>
  );
}
