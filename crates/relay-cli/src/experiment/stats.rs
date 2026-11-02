//! Descriptive statistics with explicit semantics (docs/EXPERIMENTS.md
//! §Aggregation).
//!
//! * Percentiles are **nearest-rank** on the sorted sample: the smallest
//!   value with at least p% of the sample at or below it. No interpolation,
//!   so every reported percentile is a value that was actually observed.
//! * Distributions are **pooled** across repetitions before taking
//!   percentiles. Averaging per-repetition p99s is not a p99.
//! * `sd` is the sample standard deviation (n − 1), and it is reported only
//!   for per-repetition scalars (throughput, makespan...), where the
//!   repetitions are the samples.

use serde::Serialize;

/// Nearest-rank percentile; `None` for an empty sample.
pub fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    Some(sorted[rank.clamp(1, sorted.len()) - 1])
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Summary {
    pub n: usize,
    pub mean: Option<f64>,
    /// Sample standard deviation; `None` below 2 values.
    pub sd: Option<f64>,
    pub min: Option<f64>,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

pub fn summarize(values: impl IntoIterator<Item = f64>) -> Summary {
    let mut v: Vec<f64> = values.into_iter().filter(|x| x.is_finite()).collect();
    v.sort_by(f64::total_cmp);
    let n = v.len();
    let mean = (n > 0).then(|| v.iter().sum::<f64>() / n as f64);
    let sd = mean
        .filter(|_| n > 1)
        .map(|m| (v.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (n - 1) as f64).sqrt());
    Summary {
        n,
        mean,
        sd,
        min: v.first().copied(),
        p50: percentile(&v, 50.0),
        p95: percentile(&v, 95.0),
        p99: percentile(&v, 99.0),
        max: v.last().copied(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_percentiles_are_observed_values() {
        let v: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile(&v, 50.0), Some(50.0));
        assert_eq!(percentile(&v, 95.0), Some(95.0));
        assert_eq!(percentile(&v, 99.0), Some(99.0));
        assert_eq!(percentile(&v, 100.0), Some(100.0));
        assert_eq!(percentile(&[7.0], 99.0), Some(7.0));
        assert_eq!(percentile(&[], 50.0), None);
        // 10 values: p95 rank = ceil(9.5) = 10 → the max; no interpolation.
        let ten: Vec<f64> = (1..=10).map(f64::from).collect();
        assert_eq!(percentile(&ten, 95.0), Some(10.0));
        assert_eq!(percentile(&ten, 50.0), Some(5.0));
    }

    #[test]
    fn summary_has_sample_sd_and_handles_small_samples() {
        let s = summarize([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert_eq!(
            (s.n, s.mean, s.min, s.max),
            (8, Some(5.0), Some(2.0), Some(9.0))
        );
        assert!((s.sd.unwrap() - 2.138).abs() < 0.001, "sample sd, n − 1");
        let one = summarize([3.0]);
        assert_eq!((one.mean, one.sd, one.p99), (Some(3.0), None, Some(3.0)));
        let none = summarize(Vec::<f64>::new());
        assert_eq!((none.n, none.mean, none.p50), (0, None, None));
        assert_eq!(
            summarize([1.0, f64::NAN, 3.0]).n,
            2,
            "non-finite values are dropped"
        );
    }

    #[test]
    fn pooling_differs_from_averaging_per_repetition_percentiles() {
        // Rep A: 99 fast values. Rep B: 98 fast, 2 slow (100 values).
        let a = [10.0; 99];
        let mut b = vec![10.0; 98];
        b.extend([1000.0, 1000.0]);
        let (pa, pb) = (percentile(&a, 99.0).unwrap(), percentile(&b, 99.0).unwrap());
        assert_eq!((pa, pb), (10.0, 1000.0));
        // Averaging per-repetition p99s yields a latency nobody observed.
        assert_eq!((pa + pb) / 2.0, 505.0);
        // Pooled: 2 of 199 values (1.0%) are slow, so p99 is an observed 1000.
        let pooled = summarize(a.iter().chain(b.iter()).copied()).p99.unwrap();
        assert_eq!(pooled, 1000.0);
    }
}
