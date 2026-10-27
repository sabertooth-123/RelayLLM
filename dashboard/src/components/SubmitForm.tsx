import { useState, type FormEvent } from "react";

import { api, ApiError } from "../api";
import { short } from "../state";

/** Submits real jobs through the existing POST /v1/jobs. */
export function SubmitForm({ onSubmitted }: { onSubmitted: (id: string) => void }) {
  const [prompt, setPrompt] = useState("hello from the dashboard");
  const [delayMs, setDelayMs] = useState(2000);
  const [failAttempts, setFailAttempts] = useState(0);
  const [failKind, setFailKind] = useState<"retryable" | "permanent">("retryable");
  const [maxAttempts, setMaxAttempts] = useState(3);
  const [count, setCount] = useState(1);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setMessage(null);
    let created = 0;
    let last = "";
    try {
      for (let i = 0; i < count; i++) {
        const payload: Record<string, unknown> = { prompt: count > 1 ? `${prompt} #${i + 1}` : prompt, delay_ms: delayMs };
        if (failAttempts > 0) Object.assign(payload, { fail_attempts: failAttempts, fail_kind: failKind });
        const r = await api.submit({ kind: "mock", payload, max_attempts: maxAttempts });
        created += 1;
        last = r.job.id;
      }
      setMessage({ ok: true, text: count === 1 ? `Submitted …${short(last)}` : `Submitted ${created} jobs` });
      if (count === 1) onSubmitted(last);
    } catch (err) {
      const text =
        err instanceof ApiError ? `${err.code}: ${err.message}` : err instanceof Error ? err.message : String(err);
      setMessage({ ok: false, text: created ? `Submitted ${created}, then ${text}` : text });
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="panel side submit" onSubmit={submit}>
      <div className="panel-head">
        <h2>Submit mock job</h2>
      </div>
      <p className="muted small">
        Real jobs of kind <code>mock</code>, handled by the built-in mock LLM on any worker that registered for it.
      </p>
      <label>
        Prompt
        <input value={prompt} onChange={(e) => setPrompt(e.target.value)} required maxLength={2000} />
      </label>
      <div className="row">
        <label>
          Delay (ms)
          <input type="number" min={0} max={60000} step={100} value={delayMs} onChange={(e) => setDelayMs(Number(e.target.value))} />
        </label>
        <label>
          Max attempts
          <input type="number" min={1} max={20} value={maxAttempts} onChange={(e) => setMaxAttempts(Number(e.target.value))} />
        </label>
      </div>
      <div className="row">
        <label>
          Fail first N attempts
          <input type="number" min={0} max={20} value={failAttempts} onChange={(e) => setFailAttempts(Number(e.target.value))} />
        </label>
        <label>
          Failure kind
          <select value={failKind} onChange={(e) => setFailKind(e.target.value as "retryable" | "permanent")} disabled={failAttempts === 0}>
            <option value="retryable">retryable</option>
            <option value="permanent">permanent</option>
          </select>
        </label>
      </div>
      <label>
        How many
        <input type="number" min={1} max={200} value={count} onChange={(e) => setCount(Math.max(1, Math.min(200, Number(e.target.value))))} />
      </label>
      <button className="primary" disabled={busy}>
        {busy ? "Submitting…" : count === 1 ? "Submit job" : `Submit ${count} jobs`}
      </button>
      {message ? <p className={message.ok ? "ok small" : "err small"}>{message.text}</p> : null}
      <p className="muted small">
        Pausing submissions isn't offered: the coordinator has no safe pause mechanism. It applies backpressure
        instead (<code>503 queue_full</code> at the capacity limit).
      </p>
    </form>
  );
}
