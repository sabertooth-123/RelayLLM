import { useEffect, useMemo, useState } from "react";

import { EventTimeline } from "./components/EventTimeline";
import { JobInspector } from "./components/JobInspector";
import { JobTable, type Filter } from "./components/JobTable";
import { NetworkView } from "./components/NetworkView";
import { SubmitForm } from "./components/SubmitForm";
import { SummaryCards } from "./components/SummaryCards";
import { WorkerPanel } from "./components/WorkerPanel";
import { duration, since } from "./format";
import { runningByWorker } from "./state";
import { useDashboard } from "./useDashboard";

type Selection = { kind: "job"; id: string } | { kind: "worker"; id: string } | null;

function useTick(ms: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), ms);
    return () => window.clearInterval(t);
  }, [ms]);
  return now;
}

export function App() {
  const dash = useDashboard();
  const { model, connection } = dash;
  const localNow = useTick(1_000);
  const [filter, setFilter] = useState<Filter>("active");
  const [selection, setSelection] = useState<Selection>(null);

  const offset = model?.clockOffsetMs ?? 0;
  const serverNow = localNow + offset;
  const live = connection === "live";
  const running = useMemo(() => (model ? runningByWorker(model) : {}), [model]);

  const selectJob = (id: string) => setSelection({ kind: "job", id });
  const selectWorker = (id: string) => setSelection({ kind: "worker", id });

  const header = (
    <header className="topbar">
      <div className="brand">
        <span className="brand-mark" aria-hidden />
        RelayLLM
        <span className="brand-sub">dashboard</span>
      </div>
      <div className="meta">
        {model ? (
          <>
            <span>lease {duration(model.leaseMs)}</span>
            <span title={`epoch ${model.epoch}`}>epoch …{model.epoch.slice(-6)}</span>
            <span>
              seq <strong>{model.seq.toLocaleString()}</strong>
            </span>
            {model.devControls ? <span className="badge">--dev-controls</span> : null}
          </>
        ) : null}
        <span className={`conn conn-${connection}`}>
          <span className="dot" />
          {connection}
        </span>
      </div>
    </header>
  );

  if (!model) {
    return (
      <>
        {header}
        <main className="center">
          {connection === "reconnecting" ? (
            <div className="center-box">
              <h2>Can't reach the coordinator</h2>
              <p className="err-text">{dash.error}</p>
              <p>
                Start one with <code>relay coordinator</code> (default <code>127.0.0.1:7070</code>). Set{" "}
                <code>RELAY_COORDINATOR</code> before <code>npm run dev</code> if it listens elsewhere. Retrying
                automatically.
              </p>
              <button className="btn" onClick={dash.reconnect}>
                Retry now
              </button>
            </div>
          ) : (
            <div className="center-box">
              <div className="spinner" aria-hidden />
              <p>{connection === "syncing" ? "Loading snapshot…" : "Connecting to the event stream…"}</p>
            </div>
          )}
        </main>
      </>
    );
  }

  const stats = `stream: ${model.stats.applied} applied · ${model.stats.duplicates} duplicate · ${model.stats.stale} stale · ${dash.resyncs} resync${dash.resyncs === 1 ? "" : "s"}`;

  return (
    <>
      {header}
      {!live ? (
        <div className="banner" role="status">
          <strong>{connection === "syncing" ? "Resynchronising." : "Disconnected from the coordinator."}</strong>
          <span>
            {dash.error ? `${dash.error}. ` : ""}
            Showing the last known state from {dash.lastSyncAt ? since(dash.lastSyncAt, localNow) : "earlier"}; worker
            health is unknown until the stream is back.
          </span>
          <button onClick={dash.reconnect}>Retry now</button>
        </div>
      ) : null}

      <div className={`body${live ? "" : " stale"}`}>
        <SummaryCards model={model} filter={filter} onFilter={setFilter} />
        <div className="columns">
          <div className="main">
            <NetworkView
              model={model}
              live={live}
              clockOffsetMs={offset}
              running={running}
              selectedWorker={selection?.kind === "worker" ? selection.id : null}
              onSelectWorker={selectWorker}
              onSelectJob={selectJob}
            />
            <JobTable
              model={model}
              serverNow={serverNow}
              filter={filter}
              onFilter={setFilter}
              selectedJob={selection?.kind === "job" ? selection.id : null}
              onSelectJob={selectJob}
            />
          </div>
          <aside className="rail">
            {selection?.kind === "worker" ? (
              <WorkerPanel
                model={model}
                workerId={selection.id}
                running={running[selection.id] ?? []}
                serverNow={serverNow}
                live={live}
                onSelectJob={selectJob}
                onClose={() => setSelection(null)}
              />
            ) : selection?.kind === "job" ? (
              <JobInspector
                model={model}
                jobId={selection.id}
                serverNow={serverNow}
                onSelectWorker={selectWorker}
                onClose={() => setSelection(null)}
              />
            ) : (
              <SubmitForm onSubmitted={selectJob} />
            )}
            {selection ? (
              <button className="back" onClick={() => setSelection(null)}>
                + Submit job
              </button>
            ) : null}
            <EventTimeline
              events={model.events}
              workers={model.workers}
              height={selection ? 360 : 420}
              stats={stats}
              onSelectJob={selectJob}
              onSelectWorker={selectWorker}
            />
          </aside>
        </div>
      </div>
    </>
  );
}
