#!/usr/bin/env python3
"""Plots and tables for `relay experiment run` output.

    python scripts/analyze_experiments.py experiments/results/<run-id> [<run-id> ...]

For each run directory it writes `plots/*.png` and prints a Markdown table
of the headline metrics. Everything is recomputed from summary.csv (pooled
nearest-rank percentiles, written by the harness) and the raw CSVs; nothing
here averages percentiles. Needs pandas and matplotlib.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# Validated categorical order (dataviz reference palette, light mode).
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SURFACE, INK, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": MUTED, "xtick.color": MUTED, "ytick.color": MUTED,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 10,
    "axes.titlesize": 11, "axes.titleweight": "bold", "legend.frameon": False,
})


def read_csv(path: Path) -> pd.DataFrame:
    """An empty file means "no rows" (e.g. failures.csv with no injections)."""
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def stat(summary: pd.DataFrame, variant: str, metric: str, col: str) -> float:
    row = summary[(summary.variant == variant) & (summary.metric == metric)]
    return float(row[col].iloc[0]) if len(row) and pd.notna(row[col].iloc[0]) else math.nan


def percentile_lines(ax, summary, variants, metric, title, ylabel):
    """p50 / p95 / p99 of a pooled metric across variants: three series.

    No direct labels: with small n, p95 and p99 are often the same value
    and the labels would collide; the legend carries identity."""
    x = range(len(variants))
    for i, p in enumerate(["p50", "p95", "p99"]):
        y = [stat(summary, v, metric, p) for v in variants]
        ax.plot(x, y, color=SERIES[i], linewidth=2, marker="o", markersize=6, label=p)
    ax.set_xticks(list(x), variants, rotation=20, ha="right")
    ax.set_title(title, loc="left")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left")


def per_rep_bars(ax, runs, variants, column, title, ylabel):
    """Mean across repetitions as a bar; each repetition as a dot on it."""
    means = [runs[runs.variant == v][column].mean() for v in variants]
    ax.bar(range(len(variants)), means, color=SERIES[0], width=0.6, edgecolor=SURFACE, linewidth=2)
    for i, v in enumerate(variants):
        reps = runs[runs.variant == v][column].dropna()
        ax.scatter([i] * len(reps), reps, color=INK, s=12, zorder=3)
    ax.set_xticks(range(len(variants)), variants, rotation=20, ha="right")
    ax.set_title(title, loc="left")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)


def ecdf(ax, df, variants, column, title, xlabel):
    for i, v in enumerate(variants):
        s = df[df.variant == v][column].dropna().sort_values()
        if s.empty:
            continue
        ax.step(s, [(k + 1) / len(s) for k in range(len(s))], where="post",
                color=SERIES[i % len(SERIES)], linewidth=2, label=f"{v} (n={len(s)})")
    ax.set_title(title, loc="left")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("fraction ≤ x")
    ax.legend(loc="lower right")


def save(fig, out: Path, name: str) -> None:
    fig.tight_layout()
    fig.savefig(out / f"{name}.png", dpi=130)
    plt.close(fig)
    print(f"  wrote plots/{name}.png")


def analyze(run_dir: Path) -> None:
    meta = json.loads((run_dir / "metadata.json").read_text())
    summary = read_csv(run_dir / "summary.csv")
    runs = read_csv(run_dir / "runs.csv")
    failures = read_csv(run_dir / "failures.csv")
    activities = read_csv(run_dir / "activities.csv")
    workflows = read_csv(run_dir / "workflows.csv")
    samples = read_csv(run_dir / "samples.csv")
    variants = list(dict.fromkeys(runs.variant))
    name = meta["experiment"]
    out = run_dir / "plots"
    out.mkdir(exist_ok=True)
    print(f"\n## {name} ({meta['profile']}, {meta['build_profile']} build, git {str(meta['git']['sha'])[:9]})\n")

    # Headline table.
    head = ["variant", "reps", "completion", "wf p50", "wf p95", "wf p99", "throughput wf/s",
            "extra attempts", "wasted ms", "kills exec/req", "violations"]
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    for v in variants:
        r = runs[runs.variant == v]
        kills_e = int(r.worker_kills_executed.sum() + r.coordinator_kills_executed.sum())
        kills_r = int(r.worker_kills_requested.sum() + r.coordinator_kills_requested.sum())
        cells = [v, len(r), f"{r.succeeded.sum()}/{r.workflows.sum()}",
                 *(f"{stat(summary, v, 'workflow_latency_ms', p):.0f}" for p in ("p50", "p95", "p99")),
                 f"{r.throughput_wf_per_s.mean():.2f}", int(r.extra_attempts.sum()),
                 int(r.wasted_execution_ms.sum()), f"{kills_e}/{kills_r}", int(r.invariant_violations.sum())]
        print("| " + " | ".join(str(c) for c in cells) + " |")

    # Every family: workflow latency percentiles + per-repetition scalars.
    fig, ax = plt.subplots(figsize=(8, 4.2))
    percentile_lines(ax, summary, variants, "workflow_latency_ms", f"{name}: workflow latency (pooled)", "ms")
    save(fig, out, "workflow_latency")

    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    per_rep_bars(axes[0], runs, variants, "throughput_wf_per_s", "Throughput", "workflows / s")
    per_rep_bars(axes[1], runs, variants, "wasted_execution_ms", "wasted_execution_ms", "ms per repetition")
    per_rep_bars(axes[2], runs, variants, "extra_attempts", "Re-executions (extra attempts)", "attempts")
    save(fig, out, "per_repetition")

    if not failures.empty and (failures.kind == "worker_sigkill").any():
        wk = failures[(failures.kind == "worker_sigkill") & (failures.outcome == "executed")]
        killed = [v for v in variants if v in set(wk.variant)]
        fig, axes = plt.subplots(1, 3, figsize=(13, 4))
        for ax, metric, title in zip(axes, ["detection_latency_ms", "reschedule_latency_ms", "recovery_latency_ms"],
                                     ["Detection (reaped − kill)", "Reschedule (next start − reaped)",
                                      "Recovery (activity done − kill)"]):
            percentile_lines(ax, summary, killed, metric, title, "ms")
        save(fig, out, "recovery")

    if not failures.empty and (failures.kind == "coordinator_sigkill").any():
        ck = failures[failures.kind == "coordinator_sigkill"]
        print("\n| variant | crash point | outcome | downtime ms | first progress after restart ms |\n|---|---|---|---|---|")
        for _, f in ck.iterrows():
            print(f"| {f.variant} | {f.crash_point} | {f.outcome} | {f.downtime_ms} | {f.first_progress_after_restart_ms} |")

    if "timer_lateness_ms" in workflows and workflows.timer_lateness_ms.notna().any():
        fig, ax = plt.subplots(figsize=(8, 4))
        ecdf(ax, workflows, variants, "timer_lateness_ms", "Timer lateness (fired − fire_at)", "ms")
        save(fig, out, "timer_lateness")

    if True:  # scheduling and queueing: every family
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        percentile_lines(axes[0], summary, variants, "scheduling_delay_ms", "Scheduling delay (start − scheduled)", "ms")
        percentile_lines(axes[1], summary, variants, "activity_latency_ms", "Activity latency (done − scheduled)", "ms")
        save(fig, out, "activity_latency")
        if not samples.empty:
            fig, ax = plt.subplots(figsize=(8, 4))
            for i, v in enumerate(variants):
                s = samples[(samples.variant == v) & (samples.repetition == 1)].dropna(subset=["queued"])
                if not s.empty:
                    ax.plot((s.t_ms - s.t_ms.iloc[0]) / 1000, s.queued, color=SERIES[i % len(SERIES)],
                            linewidth=2, label=v)
            ax.set_title("Queued jobs over time (repetition 1)", loc="left")
            ax.set_xlabel("s since first sample")
            ax.set_ylabel("relay_jobs{state=queued}")
            ax.legend(loc="upper right")
            save(fig, out, "queue_depth")

    if name == "durations":
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        ecdf(axes[0], activities, variants, "planned_duration_ms", "Generated activity durations", "ms")
        ecdf(axes[1], workflows, variants, "fanout_ms", "Fan-out step time (last − first branch)", "ms")
        save(fig, out, "durations")
        print("\n| variant | activity p50 | activity p99 | fan-out p50 | fan-out p50 / activity p50 | idle slot s |\n|---|---|---|---|---|---|")
        for v in variants:
            a50 = stat(summary, v, "activity_latency_ms", "p50")
            f50 = stat(summary, v, "fanout_ms", "p50")
            idle = runs[runs.variant == v].idle_slot_ms.mean() / 1000
            print(f"| {v} | {a50:.0f} | {stat(summary, v, 'activity_latency_ms', 'p99'):.0f} | {f50:.0f} | "
                  f"{f50 / a50:.2f} | {idle:.1f} |")


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    for arg in argv:
        analyze(Path(arg))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
