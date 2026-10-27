//! Time source abstraction.
//!
//! Every timestamp the coordinator stores comes from a [`Clock`]. Production
//! code uses [`SystemClock`]; tests use [`ManualClock`] so lease expiry and
//! backoff can be driven deterministically instead of with `sleep`.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
pub type Timestamp = i64;

pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> Timestamp;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> Timestamp {
        // A system clock set before 1970 is clamped to the epoch rather than
        // crashing the coordinator.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

/// Wraps a clock so it never goes backwards (ARCHITECTURE §5.1 #6).
///
/// Leases and backoff deadlines are stored as wall-clock times, because they
/// must survive a restart. If NTP steps the system clock back 10 s, a raw
/// clock would stamp new rows *earlier* than existing ones, and every lease
/// would silently gain 10 s. This wrapper returns
/// `max(inner.now(), highest value returned so far, floor)`.
///
/// It can't make *forward* jumps harmless; the coordinator's reaper detects
/// those separately. Across restarts, raise the floor to the newest timestamp
/// in the database with [`MonotonicClock::raise_floor`].
#[derive(Debug)]
pub struct MonotonicClock<C> {
    inner: C,
    last: AtomicI64,
}

impl<C: Clock> MonotonicClock<C> {
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            last: AtomicI64::new(i64::MIN),
        }
    }

    /// Never return anything earlier than `floor_ms` from now on.
    pub fn raise_floor(&self, floor_ms: Timestamp) {
        self.last.fetch_max(floor_ms, Ordering::SeqCst);
    }
}

impl<C: Clock> Clock for MonotonicClock<C> {
    fn now_ms(&self) -> Timestamp {
        let raw = self.inner.now_ms();
        // fetch_max returns the previous maximum; the result is the larger of
        // the two, and concurrent callers can never observe a decrease.
        let previous = self.last.fetch_max(raw, Ordering::SeqCst);
        previous.max(raw)
    }
}

/// A clock that only moves when told to. Cloning shares the same time.
#[derive(Debug, Clone)]
pub struct ManualClock {
    now: Arc<AtomicI64>,
}

impl ManualClock {
    pub fn new(start_ms: Timestamp) -> Self {
        Self {
            now: Arc::new(AtomicI64::new(start_ms)),
        }
    }

    pub fn advance_ms(&self, delta_ms: i64) {
        self.now.fetch_add(delta_ms, Ordering::SeqCst);
    }

    pub fn set_ms(&self, now_ms: Timestamp) {
        self.now.store(now_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> Timestamp {
        self.now.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_advanced_and_clones_share_time() {
        let clock = ManualClock::new(1_000);
        let other = clock.clone();
        assert_eq!(clock.now_ms(), 1_000);
        other.advance_ms(250);
        assert_eq!(clock.now_ms(), 1_250);
        clock.set_ms(5);
        assert_eq!(other.now_ms(), 5);
    }

    #[test]
    fn monotonic_clock_never_goes_backwards() {
        let raw = ManualClock::new(10_000);
        let clock = MonotonicClock::new(raw.clone());
        assert_eq!(clock.now_ms(), 10_000);
        raw.set_ms(4_000); // NTP steps the clock back 6 s
        assert_eq!(clock.now_ms(), 10_000);
        raw.set_ms(10_500);
        assert_eq!(clock.now_ms(), 10_500, "resumes once real time catches up");
    }

    #[test]
    fn monotonic_clock_respects_a_floor_from_a_previous_run() {
        let raw = ManualClock::new(1_000);
        let clock = MonotonicClock::new(raw.clone());
        clock.raise_floor(5_000); // newest timestamp already in the database
        assert_eq!(clock.now_ms(), 5_000);
        clock.raise_floor(3_000); // lowering is a no-op
        raw.set_ms(6_000);
        assert_eq!(clock.now_ms(), 6_000);
    }

    #[test]
    fn system_clock_is_after_2020() {
        assert!(SystemClock.now_ms() > 1_577_836_800_000);
    }
}
