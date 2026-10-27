//! Coordinator-side retry backoff.
//!
//! When an attempt fails retryably (or its lease expires), the job waits in
//! `retry_waiting` until `available_at = now + delay`. Without the delay, a
//! job hitting a rate-limited upstream would be retried instantly by
//! whichever worker polls next, which hammers the upstream and burns the
//! attempt budget in milliseconds.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Delay before the 2nd attempt (before jitter).
    pub base: Duration,
    /// Cap on the pre-jitter delay.
    pub max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }
}

impl RetryPolicy {
    /// No delay at all. For tests that don't exercise backoff.
    pub const IMMEDIATE: RetryPolicy = RetryPolicy {
        base: Duration::ZERO,
        max: Duration::ZERO,
    };

    /// Delay after attempt `failed_attempt` (1-based) failed:
    /// `min(base * 2^(failed_attempt - 1), max)`, then "equal jitter" into
    /// `[d/2, d]`. `unit` is a uniform random number in `[0, 1)`; it is a
    /// parameter so tests can pin it.
    ///
    /// Jitter matters when many jobs fail together (an upstream outage):
    /// without it they all become eligible at the same instant and stampede
    /// the upstream again when it recovers.
    pub fn delay(&self, failed_attempt: u32, unit: f64) -> Duration {
        let exponent = failed_attempt.saturating_sub(1).min(31);
        let uncapped = self.base.saturating_mul(1u32 << exponent);
        let capped = uncapped.min(self.max);
        let half = capped / 2;
        half + (capped - half).mul_f64(unit.clamp(0.0, 1.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn doubles_per_attempt_and_caps() {
        let policy = RetryPolicy {
            base: ms(100),
            max: ms(500),
        };
        // unit = 1.0 gives the top of the jitter range: the pre-jitter delay.
        assert_eq!(policy.delay(1, 1.0), ms(100));
        assert_eq!(policy.delay(2, 1.0), ms(200));
        assert_eq!(policy.delay(3, 1.0), ms(400));
        assert_eq!(policy.delay(4, 1.0), ms(500));
        assert_eq!(policy.delay(30, 1.0), ms(500));
        assert_eq!(policy.delay(u32::MAX, 1.0), ms(500), "no overflow");
    }

    #[test]
    fn jitter_stays_within_half_to_full() {
        let policy = RetryPolicy {
            base: ms(1_000),
            max: ms(60_000),
        };
        assert_eq!(policy.delay(1, 0.0), ms(500));
        assert_eq!(policy.delay(1, 0.5), ms(750));
        for i in 0..100 {
            let d = policy.delay(3, f64::from(i) / 100.0);
            assert!(d >= ms(2_000) && d <= ms(4_000), "{d:?}");
        }
    }

    #[test]
    fn immediate_policy_never_waits() {
        assert_eq!(RetryPolicy::IMMEDIATE.delay(5, 0.9), Duration::ZERO);
    }
}
