import { useEffect, useMemo, useState } from "react";

import { EventTimeline } from "./components/EventTimeline";
import { JobInspector } from "./components/JobInspector";
import { JobTable, type Filter } from "./components/JobTable";
import { NetworkView } from "./components/NetworkView";
import { SubmitForm } from "./components/SubmitForm";
import { SummaryCards } from "./components/SummaryCards";
import { WorkerPanel } from "./components/WorkerPanel";
import { ago } from "./format";
import { runningByWorker } from "./state";
import { useDashboard, type Connection } from "./useDashboard";

type Selection = { kind: "job"; id: string } | { kind: "worker"; id: string } | null;

function useTick(ms: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), ms);
    return () => window.clearInterval(t);
  }, [ms]);
  return now;
}

const CONNECTION_LABEL: Record<Connection, string> = {
  connecting: "connecting",
  syncing: "syncing",
  live: "live",
  reconnecting: "reconnecting",
};

export function App() {
  const dash = useDashboard();
  const { model, connection } = dash;
  const localNow = useTick(1_000);
  const [filter, setFilter] = useState<Filter>("active");
  const [selection, setSelection] = useState<Selection>(null);

  const serverNow = localNow + (model?.clockOffsetMs ?? 0);
  const live = connection === "live";
  const running = useMemo(() => (model ? runningByWorker(model) : {}), [model]);

  const selectJob = (id: string) => setSelection({ kind: "job", id });
  const selectWorker = (id: string) => setSelection({ kind: "worker", id });

  return (
    <div className="app">
      <header className="top">
        <div className="brand">
          <span className="logo" aria-hidden>
            ◆
          </span>
          RelayLLM <span className="muted">live</span>
        </div>
        <div className="status">
          <span className={`conn conn-${connection}`}>
            <i /> {CONNECTION_LABEL[connection]}
          </span>
          {model ? (
            <span className="muted small mono" title={`epoch ${model.epoch}`}>
              epoch …{model.epoch.slice(-6)} · seq {model.seq}
            </span>
          ) : null}
        </div>
      </header>

      {!live && model ? (
        <div className="banner" role="status">
          <strong>{connection === "syncing" ? "Resynchronising" : "Disconnected from the coordinator."}</strong>{" "}
          {dash.error ? <span>{dash.error}. </span> : null}
          Showing the last known state from {dash.lastSyncAt ? ago(dash.lastSyncAt, localNow) : "earlier"}; worker health is
          unknown until the connection is back.
          <button className="ghost" onClick={dash.reconnect}>
            Retry now
          </button>
        </div>
      ) : null}

      {!model ? (
        <main className="loading">
          {connection === "reconnecting" ? (
            <div className="panel center">
              <h2>Can't reach the coordinator</h2>
              <p className="err">{dash.error}</p>
              <p className="muted">
                Start one with <code>relay coordinator</code> (default <code>127.0.0.1:7070</code>). Set{" "}
                <code>RELAY_COORDINATOR</code> before <code>npm run dev</code> if it listens elsewhere. Retrying
                automatically.
              </p>
              <button className="primary" onClick={dash.reconnect}>
                Retry now
              </button>
            </div>
          ) : (
            <div className="panel center">
              <div className="spinner" aria-hidden />
              <p className="muted">{connection === "syncing" ? "Loading snapshot…" : "Connecting to the event stream…"}</p>
            </div>
          )}
        </main>
      ) : (
        <main className={`grid${live ? "" : " stale"}`}>
          <SummaryCards
            model={model}
            onFilter={(s) => {
              setFilter(s);
              document.getElementById("jobs")?.scrollIntoView({ behavior: "smooth" });
            }}
          />
          <NetworkView
            model={model}
            live={live}
            serverNow={serverNow}
            running={running}
            selectedWorker={selection?.kind === "worker" ? selection.id : null}
            onSelectWorker={selectWorker}
            onSelectJob={selectJob}
          />
          <div className="rail">
          <div className="sidecol">
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
          </div>
          <div className="eventscol">
            <EventTimeline events={model.events} onSelectJob={selectJob} />
            <p className="muted small sync">
              stream: {model.stats.applied} applied · {model.stats.duplicates} duplicate · {model.stats.stale} stale ·{" "}
              {dash.resyncs} resync{dash.resyncs === 1 ? "" : "s"}
            </p>
          </div>
          </div>
          <div id="jobs" className="jobscol">
            <JobTable
              model={model}
              serverNow={serverNow}
              filter={filter}
              onFilter={setFilter}
              selectedJob={selection?.kind === "job" ? selection.id : null}
              onSelectJob={selectJob}
            />
          </div>
          {selection ? (
            <button className="fab" onClick={() => setSelection(null)}>
              + Submit job
            </button>
          ) : null}
        </main>
      )}
    </div>
  );
}
