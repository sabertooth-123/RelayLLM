import { useEffect, useState } from "react";

import { api } from "../api";
import { ago, clock, dateTime, duration, mask, since, STATE_LABEL, workerLabel } from "../format";
import { short, type Model } from "../state";
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
const errMsg = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function JobInspector({ model, jobId, serverNow, onSelectWorker, onClose }: Props) {
  const summary = model.jobs[jobId];
  const [loaded, setLoaded] = useState<Loaded | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<JobResultView | null>(null);
  const [showResult, setShowResult] = useState(false);
  const [revealKey, setRevealKey] = useState(false);
  const [confirmCancel, setConfirmCancel] = useState(false);

  // Re-read the authoritative per-job endpoints whenever the job changes.
  const version = summary ? `${summary.state}:${summary.attempt}:${summary.updated_at_ms}` : "";
  useEffect(() => {
    let cancelled = false;
    setError(null);
    Promise.all([api.job(jobId), api.attempts(jobId)])
      .then(([status, attempts]) => !cancelled && setLoaded({ status, attempts }))
      .catch((e: unknown) => !cancelled && setError(errMsg(e)));
    return () => {
      cancelled = true;
    };
  }, [jobId, version]);

  useEffect(() => {
    setShowResult(false);
    setResult(null);
    setRevealKey(false);
    setConfirmCancel(false);
  }, [jobId]);

  useEffect(() => {
    if (!showResult) return;
    let cancelled = false;
    api
      .result(jobId)
      .then((r) => !cancelled && setResult(r))
      .catch((e: unknown) => !cancelled && setError(errMsg(e)));
    return () => {
      cancelled = true;
    };
  }, [jobId, showResult, version]);

  const status = loaded?.status.id === jobId ? loaded.status : null;
  const attempts = loaded?.status.id === jobId ? loaded.attempts : [];
  const state = status?.state ?? summary?.state;
  const terminal = state ? TERMINAL.has(state) : false;
  const label = (id: string) => workerLabel(id, model.workers[id]?.name);
  const maxAttempts = status?.max_attempts ?? summary?.max_attempts ?? 0;

  const cancel = async () => {
    if (!confirmCancel) {
      setConfirmCancel(true);
      return;
    }
    setConfirmCancel(false);
    try {
      await api.cancel(jobId);
    } catch (e) {
      setError(errMsg(e));
    }
  };

  // Attempt timeline: width = time each attempt held a worker.
  const held = (a: AttemptView) => Math.max(0, (a.finished_at_ms ?? serverNow) - a.started_at_ms);
  const total = attempts.reduce((n, a) => n + held(a), 0) || 1;
  const unused = terminal ? 0 : Math.max(0, maxAttempts - attempts.length);

  let timing: { label: string; text: string; color: string } | null = null;
  if (state === "running" && summary?.lease_expires_at_ms) {
    const low = summary.lease_expires_at_ms - serverNow < model.leaseMs * 0.4;
    timing = {
      label: "Lease",
      text: `expires ${ago(summary.lease_expires_at_ms, serverNow)}`,
      color: low ? "var(--warn)" : "var(--text)",
    };
  } else if (state === "retry_waiting" && summary) {
    timing = { label: "Next try", text: ago(summary.available_at_ms, serverNow), color: "var(--warn)" };
  }

  return (
    <section className="panel scroll">
      <div className="panel-head">
        <h2>
          Job <span className="mono" style={{ fontWeight: 500 }}>…{short(jobId)}</span>
        </h2>
        {state ? <span className={`tag s-${state}`}>{STATE_LABEL[state]}</span> : null}
        <span className="grow" />
        {state && !terminal ? (
          <button className={`cancel-btn${confirmCancel ? " armed" : ""}`} onClick={cancel} onBlur={() => setConfirmCancel(false)}>
            {confirmCancel ? "Confirm cancel" : "Cancel job"}
          </button>
        ) : null}
        <button className="close" onClick={onClose} aria-label="Close">
          ✕
        </button>
      </div>
      {error ? <p className="err-text" style={{ marginBottom: 10 }}>{error}</p> : null}
      {!status && !summary && !error ? <p className="muted">Loading…</p> : null}

      <div className="segments">
        {attempts.map((a) => (
          <span
            key={a.attempt}
            className={`seg o-${a.outcome ?? "in_progress"}`}
            style={{ flex: Math.max(0.08, held(a) / total) }}
            title={`attempt ${a.attempt}: ${a.outcome ?? "in progress"}`}
          >
            #{a.attempt} {duration(held(a))}
          </span>
        ))}
        {Array.from({ length: unused }, (_, i) => (
          <span key={`u${i}`} className="seg unused" style={{ flex: 0.12 }} title={`attempt ${attempts.length + i + 1} unused`} />
        ))}
      </div>
      <p className="seg-note">
        {attempts.length
          ? `${attempts.length} attempt${attempts.length === 1 ? "" : "s"}, width = time held by a worker`
          : "Waiting in the queue for its first claim"}
      </p>

      <div className="kv">
        <span>ID</span>
        <span className="v">{jobId}</span>
        <span>Kind</span>
        <span className="v">{status?.kind ?? summary?.kind ?? NA}</span>
        <span>Attempt</span>
        <span>
          {status || summary ? (
            <>
              <span className="v">
                {(status ?? summary)!.attempt} of {maxAttempts}
              </span>{" "}
              <span className="note">also the fencing token</span>
            </>
          ) : (
            NA
          )}
        </span>
        <span>Worker</span>
        <span>
          {status?.worker_id ? (
            <button className="link mono" style={{ fontSize: 11.5 }} title={status.worker_id} onClick={() => onSelectWorker(status.worker_id!)}>
              {label(status.worker_id)}
            </button>
          ) : (
            <span className="muted">{terminal ? "none (finished)" : "not assigned"}</span>
          )}
        </span>
        <span>Created</span>
        <span className="v">{status ? dateTime(status.created_at_ms) : NA}</span>
        <span>Updated</span>
        <span className="v">
          {status ? `${clock(status.updated_at_ms)} (${since(status.updated_at_ms, serverNow)})` : NA}
        </span>
        <span>Finished</span>
        <span className="v">{status?.finished_at_ms ? dateTime(status.finished_at_ms) : <span className="muted">not finished</span>}</span>
        {timing ? (
          <>
            <span>{timing.label}</span>
            <span className="v" style={{ color: timing.color }}>
              {timing.text}
            </span>
          </>
        ) : null}
        <span>Idempotency</span>
        <span style={{ display: "flex", gap: 8, alignItems: "baseline" }}>
          {status ? (
            status.idempotency_key ? (
              <>
                <span className="v">{revealKey ? status.idempotency_key : mask(status.idempotency_key)}</span>
                <button className="link" style={{ fontSize: 11 }} onClick={() => setRevealKey((v) => !v)}>
                  {revealKey ? "hide" : "reveal"}
                </button>
              </>
            ) : (
              <span className="v muted">no key</span>
            )
          ) : (
            NA
          )}
        </span>
        <span>Last error</span>
        <span className={status?.last_error ? "err-text" : "muted"} style={{ fontSize: 11.5 }}>
          {status?.last_error ?? "none"}
        </span>
      </div>

      <h3>Attempts</h3>
      {attempts.length === 0 ? (
        <p className="muted" style={{ fontSize: 12 }}>
          {status ? "Not claimed yet." : "…"}
        </p>
      ) : (
        <div>
          {attempts.map((a) => (
            <div key={a.attempt} className="attempt">
              <span>{a.attempt}</span>
              <span className="attempt-main">
                <span>
                  <span className={`tag o-${a.outcome ?? "in_progress"}`}>
                    {a.outcome ? a.outcome.replace("_", " ") : "in progress"}
                  </span>
                  <span className="mono nowrap" style={{ color: "var(--text-2)" }} title={a.worker_id}>
                    {label(a.worker_id)}
                  </span>
                </span>
                {a.error ? <span className="err-text">{a.error}</span> : null}
              </span>
              <span>{duration(held(a))}</span>
            </div>
          ))}
        </div>
      )}

      <h3>Result</h3>
      {!terminal ? (
        <p className="muted" style={{ fontSize: 12 }}>
          Available once the job finishes.
        </p>
      ) : !showResult ? (
        <button className="btn" onClick={() => setShowResult(true)}>
          Load result (may contain model output)
        </button>
      ) : result ? (
        result.result !== null && result.result !== undefined ? (
          <pre className="result">{JSON.stringify(result.result, null, 2)}</pre>
        ) : (
          <p className="muted" style={{ fontSize: 12 }}>
            No result ({result.error ?? STATE_LABEL[result.state].toLowerCase()}).
          </p>
        )
      ) : (
        <p className="muted">Loading…</p>
      )}
      <p className="note" style={{ marginTop: 10 }}>
        Payloads aren't shown because the coordinator doesn't expose them.
      </p>
    </section>
  );
}
