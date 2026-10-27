"""Replay job_attempts to see how full each worker's slots actually were."""
import shutil, sqlite3, sys, tempfile, os, statistics

src = sys.argv[1]
tmp = tempfile.mkdtemp()
for suffix in ("", "-wal", "-shm"):
    if os.path.exists(src + suffix):
        shutil.copy(src + suffix, os.path.join(tmp, "relay.db" + suffix))
db = sqlite3.connect(os.path.join(tmp, "relay.db"))

rows = db.execute(
    "SELECT a.worker_id, a.started_at, a.finished_at, j.created_at "
    "FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE a.finished_at IS NOT NULL"
).fetchall()
t0 = min(r[1] for r in rows)
t1 = max(r[2] for r in rows)
last_created = db.execute("SELECT MAX(created_at) FROM jobs").fetchone()[0]
print(f"attempts={len(rows)} window={(t1 - t0)} ms  (submission ended at +{last_created - t0} ms)")

def busy_profile(intervals, start, end):
    """Time-weighted distribution of in-flight count over [start, end)."""
    events = []
    for s, f in intervals:
        events.append((max(s, start), +1))
        events.append((min(f, end), -1))
    events.sort()
    level, prev, share = 0, start, {}
    for t, d in events:
        if t > prev:
            share[level] = share.get(level, 0) + (t - prev)
            prev = t
        level += d
    total = sum(share.values()) or 1
    mean = sum(k * v for k, v in share.items()) / total
    return mean, {k: round(100 * v / total) for k, v in sorted(share.items())}

by_worker = {}
for w, s, f, c in rows:
    by_worker.setdefault(w, []).append((s, f))
for w, iv in sorted(by_worker.items()):
    mean, dist = busy_profile(iv, t0, t1)
    iv.sort()
    exec_ms = [f - s for s, f in iv]
    # Gap between a slot freeing up and this worker's next claim.
    starts = sorted(s for s, _ in iv)
    refill = []
    for _, f in iv:
        nxt = next((s for s in starts if s >= f), None)
        if nxt is not None:
            refill.append(nxt - f)
    print(f"{w[:14]}: jobs={len(iv):3} mean_in_flight={mean:.2f}/4 "
          f"time_at_level%={dist} exec_med={statistics.median(exec_ms)}ms "
          f"refill_med={statistics.median(refill) if refill else '-'}ms "
          f"refill_p90={sorted(refill)[int(0.9 * len(refill))] if refill else '-'}ms")
