//! Retry policy for the mover's RPCs — the ONE place that decides what is
//! transient and how long to wait. Shared by the BigQuery read path
//! (`bqread.rs`) and the Bigtable write path (`write.rs`).
//!
//! Names `tonic::Code` — a gRPC status enum, not a client/transport type;
//! the swap boundary (README "Swap boundaries") stays: `bqread.rs` is still
//! the only module naming BQ Storage Read / `tonic::transport` types.
//!
//! Why retry matters here: Google frontends close every connection at ~60
//! min of age, and the first request dispatched on a dead channel fails
//! fast (`connection closed`) before tonic reconnects. The backoff below is
//! what gives the reconnect time to happen — measured 2026-08-20, a lane
//! saw three instant failures in 150 ms and tripped its breaker.

use std::future::Future;
use std::time::Duration;

use crate::manifest::ShardLabel;

/// Attempts per RPC (first try + retries) before the failure is real. The
/// four backoffs between five attempts sum to ~7.5 s (0.5+1+2+4), plus
/// RPC_ATTEMPTS × the caller's own per-RPC deadline — e.g. 5 × 120 s for
/// `write_rows` is ~10 min for one hung flush, bounded, where before this
/// module existed a stuck RPC could hang the lane indefinitely — and, at the
/// `write_flush` call site, that whole budget is again multiplied by
/// `× (ENTRY_RETRY_LIMIT+1)` (the outer in-place entry-retry loop).
pub const RPC_ATTEMPTS: u32 = 5;

/// Statuses a fresh connection or a moment's patience can cure. tonic maps
/// transport and GOAWAY failures to Unknown / Internal / Unavailable, so
/// those must be in. Everything else (InvalidArgument, NotFound,
/// PermissionDenied, Unauthenticated, OutOfRange, FailedPrecondition, …) is
/// an answer and fails the slice on the first try.
///
/// `Cancelled` is included for the server side: a graceful frontend
/// teardown can hand back CANCELLED for an in-flight RPC, which is
/// transient; a client-side cancel never produces a status that reaches
/// this function. `ResourceExhausted` is quota pushback — retried on the
/// fixed schedule below with no `RetryInfo` parsing, bounded by
/// [`RPC_ATTEMPTS`] the same as any other transient code.
pub fn retryable(code: tonic::Code) -> bool {
    use tonic::Code;
    matches!(
        code,
        Code::Unavailable
            | Code::Internal
            | Code::Unknown
            | Code::Cancelled
            | Code::Aborted
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
    )
}

/// [`retryable`] for a wire-level google.rpc.Code as carried by
/// `Error::BigtableRpc`. An integer outside the enum maps to `Unknown`, i.e.
/// retryable (the client's `-1` no-status sentinel is a
/// per-entry status, never an RPC error, so it does not reach here).
pub fn retryable_code(code: i32) -> bool {
    retryable(tonic::Code::from(code))
}

/// 500 ms · 2^attempt, capped at 16 s: 0.5, 1, 2, 4, 8, 16, 16, …
pub fn backoff(attempt: u32) -> Duration {
    const BASE_MS: u64 = 500;
    const CAP: Duration = Duration::from_secs(16);
    let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    Duration::from_millis(BASE_MS.saturating_mul(factor)).min(CAP)
}

/// Runs `op` up to [`RPC_ATTEMPTS`] times, sleeping [`backoff`] between
/// attempts that failed with an error `is_retryable` accepts. `op` receives
/// the 0-based attempt index (callers that need a fresh client per attempt
/// use it). Returns the value and the number of retries made; callers add
/// the count to their own counter. A non-retryable error, or the last
/// attempt's error, is returned as-is.
///
/// On the failure path the retry count is NOT reported — the `Err` carries
/// only the error, not how many attempts preceded it. A `rpc_retries`
/// counter that reads 0 on a Failed verdict does not mean no retries
/// happened; the per-attempt `warn` lines below are the record of those.
pub async fn retry_rpc<T, E, F, Fut>(
    what: &str,
    label: ShardLabel,
    is_retryable: impl Fn(&E) -> bool,
    mut op: F,
) -> std::result::Result<(T, u64), E>
where
    E: std::fmt::Display,
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
    let mut attempt = 0u32;
    loop {
        match op(attempt).await {
            Ok(v) => return Ok((v, u64::from(attempt))),
            Err(e) if !is_retryable(&e) => return Err(e),
            Err(e) if attempt + 1 >= RPC_ATTEMPTS => {
                log::warn!("{label} {what} gave up after {RPC_ATTEMPTS} attempts: {e}");
                return Err(e);
            }
            Err(e) => {
                let wait = backoff(attempt);
                log::warn!(
                    "{label} {what} attempt {}/{RPC_ATTEMPTS} failed, retrying in {wait:?}: {e}",
                    attempt + 1
                );
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
// tests; the workspace denies unwrap_used
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn label() -> ShardLabel {
        ShardLabel { shard: 1, lane: 0 }
    }

    #[test]
    fn retryable_splits_transient_from_fatal_codes() {
        use tonic::Code;
        for c in [
            Code::Unavailable,
            Code::Internal,
            Code::Unknown,
            Code::Cancelled,
            Code::Aborted,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
        ] {
            assert!(retryable(c), "{c:?} must be retryable");
        }
        for c in [
            Code::Ok,
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::FailedPrecondition,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::DataLoss,
        ] {
            assert!(!retryable(c), "{c:?} must NOT be retryable");
        }
        assert!(retryable_code(14)); // UNAVAILABLE
        assert!(retryable_code(4)); // DEADLINE_EXCEEDED
        assert!(!retryable_code(3)); // INVALID_ARGUMENT
    }

    #[test]
    fn backoff_doubles_from_500ms_and_caps_at_16s() {
        assert_eq!(backoff(0), Duration::from_millis(500));
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(4), Duration::from_secs(8));
        assert_eq!(backoff(5), Duration::from_secs(16));
        assert_eq!(backoff(9), Duration::from_secs(16));
        assert_eq!(backoff(63), Duration::from_secs(16));
        assert_eq!(backoff(200), Duration::from_secs(16));
    }

    #[derive(Debug)]
    struct E(bool);
    impl std::fmt::Display for E {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "E(retryable={})", self.0)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_rpc_succeeds_after_two_retries_and_reports_them() {
        let calls = AtomicU32::new(0);
        let t0 = tokio::time::Instant::now();
        let (v, retries) = retry_rpc(
            "op",
            label(),
            |e: &E| e.0,
            |attempt| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(E(true))
                    } else {
                        Ok(attempt)
                    }
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(v, 2);
        assert_eq!(retries, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        // backoff(0) + backoff(1) = 0.5s + 1s; paused clock makes this exact.
        assert_eq!(t0.elapsed(), Duration::from_millis(1500));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_rpc_does_not_retry_a_non_retryable_error() {
        let calls = AtomicU32::new(0);
        let err = retry_rpc(
            "op",
            label(),
            |e: &E| e.0,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), E>(E(false)) }
            },
        )
        .await
        .unwrap_err();
        assert!(!err.0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_rpc_exhausts_at_rpc_attempts() {
        let calls = AtomicU32::new(0);
        let t0 = tokio::time::Instant::now();
        let err = retry_rpc(
            "op",
            label(),
            |e: &E| e.0,
            |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), E>(E(true)) }
            },
        )
        .await
        .unwrap_err();
        assert!(err.0);
        assert_eq!(calls.load(Ordering::SeqCst), RPC_ATTEMPTS);
        // backoff(0..=3) = 0.5+1+2+4s; paused clock makes this exact.
        assert_eq!(t0.elapsed(), Duration::from_millis(7500));
    }
}
