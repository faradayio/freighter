//! Lanes and the per-lane circuit breaker: shards sharing a lane are
//! written in listed order with each shard's read overlapping the previous
//! shard's write, distinct lanes run concurrently, and a lane stops itself
//! after consecutive shard failures.

use crate::error::{Error, Result};
use crate::manifest::{Outcome, ShardSpec, ShardVerdict};

/// N consecutive shard failures in one lane STOP THAT LANE — the
/// pod exits non-zero at the end naming every incomplete lane, so a
/// systemic failure still surfaces as one failed Job without discarding
/// the healthy lanes' in-flight work.
/// A lone failure does not trip it; any success resets it.
pub struct CircuitBreaker {
    limit: u32,
    consecutive: u32,
}

impl CircuitBreaker {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            consecutive: 0,
        }
    }
    /// Records a shard outcome (`ok` = the shard completed); returns true
    /// when the breaker trips on consecutive failures.
    pub fn record(&mut self, ok: bool) -> bool {
        if ok {
            self.consecutive = 0;
        } else {
            self.consecutive += 1;
        }
        self.consecutive >= self.limit
    }
}

pub const CONSECUTIVE_FAILURE_LIMIT: u32 = 3;

/// Groups shards by lane, preserving listed order within each lane.
/// Indexable by lane value because manifest validation guarantees lanes
/// are dense from 0 (single-lane manifests trivially so).
fn lane_groups(shards: &[ShardSpec]) -> Vec<Vec<ShardSpec>> {
    let n_lanes = shards.iter().map(|s| s.lane).max().unwrap_or(0) as usize + 1;
    let mut groups: Vec<Vec<ShardSpec>> = vec![Vec::new(); n_lanes];
    for s in shards {
        groups[s.lane as usize].push(s.clone());
    }
    groups
}

/// How one lane ended.
#[derive(Debug)]
enum LaneEnd {
    Completed,
    /// The per-lane breaker tripped at this shard.
    Tripped {
        shard: u32,
    },
    /// A fatal, non-shard error (e.g. verdict upload) at this shard.
    Failed {
        shard: u32,
        error: Error,
    },
}

#[derive(Debug)]
struct LaneOutcome {
    lane: u32,
    end: LaneEnd,
}

/// Walks one lane's shards in listed order with its own consecutive-failure
/// breaker, reading shard N+1 while shard N writes: a lane's wall is
/// max(read, write) per shard, not their sum, at the same write concurrency.
/// A trip or a fatal error stops THIS lane only — the job's other lanes keep
/// running: fifteen lanes of in-flight work are not thrown away to report
/// one bad lane. The lane's remaining shards (including the one already
/// read) have no verdict, so the fence re-runs them.
///
/// `read` yields a loaded shard or the error that stopped its read; `write`
/// takes that result, so a failed read reaches the verdict path as a Failed
/// verdict in listed order, and only `write`'s own error (verdict upload)
/// is fatal to the lane.
///
/// Lane futures are unspawned — `run_lanes` polls them all from one task.
/// Per shard, reads are bounded by `READ_REOPEN_LIMIT × READ_IDLE_TIMEOUT`
/// and writes by `(ENTRY_RETRY_LIMIT+1) × (RPC_ATTEMPTS × WRITE_RPC_TIMEOUT
/// plus backoff)`; the outer wall is the Job's `activeDeadlineSeconds`. The
/// operational trade this makes: an early trip does not shorten the pod,
/// so a pod with one dead lane runs until its slowest healthy lane drains
/// and burns toward that deadline, rather than exiting early for a fresh
/// budget on relaunch. Verdicts land per shard, so a deadline kill loses
/// only in-flight shards and the fence re-runs them.
async fn run_lane<L, R, RF, W, WF>(
    lane: u32,
    shards: Vec<ShardSpec>,
    read: &R,
    write: &W,
) -> LaneOutcome
where
    R: Fn(ShardSpec) -> RF,
    RF: std::future::Future<Output = Result<L>>,
    W: Fn(ShardSpec, Result<L>) -> WF,
    WF: std::future::Future<Output = Result<ShardVerdict>>,
{
    let mut breaker = CircuitBreaker::new(CONSECUTIVE_FAILURE_LIMIT);
    let mut shards = shards.into_iter();
    let Some(first) = shards.next() else {
        log::info!("lane {lane} finished");
        return LaneOutcome {
            lane,
            end: LaneEnd::Completed,
        };
    };
    let mut current = (first.clone(), read(first).await);
    loop {
        let (shard, loaded) = current;
        let shard_index = shard.index;
        let next = shards.next();
        let (written, next_loaded) = match next.clone() {
            Some(next_shard) => {
                let (w, l) = tokio::join!(write(shard, loaded), read(next_shard));
                (w, Some(l))
            }
            None => (write(shard, loaded).await, None),
        };
        let verdict = match written {
            Ok(v) => v,
            Err(error) => {
                log::error!("lane {lane} shard {shard_index}: fatal error, lane stops: {error}");
                return LaneOutcome {
                    lane,
                    end: LaneEnd::Failed {
                        shard: shard_index,
                        error,
                    },
                };
            }
        };
        // record takes ok, not !ok: the breaker counts FAILURES internally,
        // so an inverted argument would trip the lane on three successes.
        if let Some(ok) = breaker_signal(&verdict) {
            if breaker.record(ok) {
                log::error!(
                    "lane {lane} shard {shard_index}: {CONSECUTIVE_FAILURE_LIMIT} consecutive shard failures — breaker tripped, lane stops (siblings continue)"
                );
                return LaneOutcome {
                    lane,
                    end: LaneEnd::Tripped { shard: shard_index },
                };
            }
        }
        let (Some(next_shard), Some(loaded)) = (next, next_loaded) else {
            break;
        };
        current = (next_shard, loaded);
    }
    log::info!("lane {lane} finished");
    LaneOutcome {
        lane,
        end: LaneEnd::Completed,
    }
}

/// Ok iff every lane completed; otherwise one error naming every tripped
/// and failed lane — the pod exits non-zero and the harness's verify-first
/// relaunch picks up the unverdicted shards.
fn lanes_result(outcomes: Vec<LaneOutcome>) -> Result<()> {
    let mut tripped = Vec::new();
    let mut failed = Vec::new();
    for o in outcomes {
        match o.end {
            LaneEnd::Completed => {}
            LaneEnd::Tripped { shard } => tripped.push(format!("lane {} at shard {shard}", o.lane)),
            LaneEnd::Failed { shard, error } => {
                failed.push(format!("lane {} at shard {shard}: {error}", o.lane))
            }
        }
    }
    if tripped.is_empty() && failed.is_empty() {
        return Ok(());
    }
    Err(Error::Write(format!(
        "lanes tripped [{}]; lanes failed [{}]",
        tripped.join("; "),
        failed.join("; ")
    )))
}

/// Drives a manifest's shards as lanes: shards sharing a lane run in
/// listed order, each read overlapping the previous write; distinct lanes
/// run concurrently and are ALL joined — a lane that trips or fails stops
/// itself and the others finish. The result names every lane that did not
/// complete. A single-lane manifest yields one lane, one breaker, one
/// walk.
pub(crate) async fn run_lanes<L, R, RF, W, WF>(
    shards: &[ShardSpec],
    read: R,
    write: W,
) -> Result<()>
where
    R: Fn(ShardSpec) -> RF,
    RF: std::future::Future<Output = Result<L>>,
    W: Fn(ShardSpec, Result<L>) -> WF,
    WF: std::future::Future<Output = Result<ShardVerdict>>,
{
    let (read, write) = (&read, &write);
    let outcomes = futures::future::join_all(
        lane_groups(shards)
            .into_iter()
            .enumerate()
            .map(|(lane, lane_shards)| run_lane(lane as u32, lane_shards, read, write)),
    )
    .await;
    lanes_result(outcomes)
}

/// The breaker's view of one verdict: `Some(true)` records a success,
/// `Some(false)` a failure, `None` skips the breaker entirely.
///
/// Complete-with-zero-rows can only be an `expect_empty` shard (a
/// non-expected empty read raises `EmptyShard` before any verdict exists),
/// and it must NOT reset the breaker: it proves nothing about the write
/// path — no cell touched the destination. During a systemic destination
/// outage (the exact hazard the breaker exists for), interleaved
/// expected-empty shards would otherwise keep resetting the failure streak
/// below the trip limit forever. Skipping preserves the current streak,
/// counting the shard as neither success nor failure.
fn breaker_signal(v: &ShardVerdict) -> Option<bool> {
    match v.outcome {
        Outcome::Failed => Some(false),
        Outcome::Complete if v.rows_read == 0 => None,
        Outcome::Complete => Some(true),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests; the workspace denies unwrap_used
mod tests {
    #[test]
    fn circuit_breaker_trips_on_consecutive_failures() {
        use super::CircuitBreaker;
        let mut cb = CircuitBreaker::new(3);
        assert!(!cb.record(false)); // one failure
        assert!(!cb.record(false));
        cb.record(true); // success resets
        assert!(!cb.record(false));
        assert!(!cb.record(false));
        assert!(cb.record(false)); // third consecutive -> trip
    }

    #[test]
    fn expected_empty_shards_neither_reset_nor_advance_the_breaker() {
        use super::breaker_signal;
        use crate::manifest::Outcome;
        let verdict = |outcome: Outcome, rows_read: u64| test_verdict(0, outcome, rows_read);
        // Complete with writes: success. Failed: failure — regardless of
        // rows_read (a shard can fail after reading rows, or before any).
        assert_eq!(breaker_signal(&verdict(Outcome::Complete, 10)), Some(true));
        assert_eq!(breaker_signal(&verdict(Outcome::Failed, 0)), Some(false));
        assert_eq!(breaker_signal(&verdict(Outcome::Failed, 10)), Some(false));
        // Complete with zero rows == an expect_empty shard: skip, so an
        // outage's failure streak survives interleaved empty shards.
        assert_eq!(breaker_signal(&verdict(Outcome::Complete, 0)), None);
    }

    fn test_verdict(
        index: u32,
        outcome: crate::manifest::Outcome,
        rows: u64,
    ) -> crate::manifest::ShardVerdict {
        crate::manifest::ShardVerdict {
            manifest_version: 3,
            shard_index: index,
            rows_read: rows,
            cells_encoded: 0,
            cells_acknowledged: 0,
            cells_failed: 0,
            row_deletes_acknowledged: 0,
            row_deletes_failed: 0,
            entry_retries: 0,
            entries: 0,
            requests: 0,
            read_reopens: 0,
            rpc_retries: 0,
            read_seconds: 0.0,
            sort_seconds: 0.0,
            write_seconds: 0.0,
            stalled_seconds: 0.0,
            wall_seconds: 0.0,
            latency_ms_p50: 0.0,
            latency_ms_p99: 0.0,
            outcome,
        }
    }

    fn test_shard(index: u32, lane: u32) -> crate::manifest::ShardSpec {
        crate::manifest::ShardSpec {
            index,
            range: crate::manifest::ShardRange::Int {
                lo: i64::from(index) * 10,
                hi: i64::from(index) * 10 + 9,
            },
            expect_empty: false,
            lane,
        }
    }

    #[test]
    fn lane_groups_single_lane_is_one_group_in_listed_order() {
        use super::lane_groups;
        // Stride order (non-ascending) — preserved verbatim.
        let shards = vec![test_shard(2, 0), test_shard(0, 0), test_shard(1, 0)];
        let groups = lane_groups(&shards);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].iter().map(|s| s.index).collect::<Vec<_>>(),
            vec![2, 0, 1]
        );
    }

    /// The lane walk with a trivial read phase: every test below is about
    /// the write-side sequence (order, breaker, siblings).
    async fn writing<F, Fut>(
        shards: &[crate::manifest::ShardSpec],
        per_shard: F,
    ) -> crate::error::Result<()>
    where
        F: Fn(crate::manifest::ShardSpec) -> Fut,
        Fut: std::future::Future<Output = crate::error::Result<crate::manifest::ShardVerdict>>,
    {
        super::run_lanes(
            shards,
            |_| async { Ok(()) },
            |s, _loaded: crate::error::Result<()>| per_shard(s),
        )
        .await
    }

    #[tokio::test]
    async fn run_lanes_reads_the_next_shard_while_writing_the_current_one() {
        use crate::manifest::Outcome;
        // Shard 0's WRITE waits for shard 1's READ to fire a oneshot. A lane
        // that read and wrote each shard in turn would deadlock; the timeout
        // converts that regression into a failure.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tx = std::sync::Mutex::new(Some(tx));
        let rx = std::sync::Mutex::new(Some(rx));
        let shards = vec![test_shard(0, 0), test_shard(1, 0)];
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            super::run_lanes(
                &shards,
                |s| {
                    let fire = if s.index == 1 {
                        tx.lock().unwrap().take()
                    } else {
                        None
                    };
                    async move {
                        if let Some(tx) = fire {
                            let _ = tx.send(());
                        }
                        Ok(s.index)
                    }
                },
                |s, loaded: crate::error::Result<u32>| {
                    let wait = if s.index == 0 {
                        rx.lock().unwrap().take()
                    } else {
                        None
                    };
                    async move {
                        assert_eq!(loaded.unwrap(), s.index, "a shard writes what it read");
                        if let Some(rx) = wait {
                            rx.await.expect("shard 1 reads while shard 0 writes");
                        }
                        Ok(test_verdict(s.index, Outcome::Complete, 1))
                    }
                },
            ),
        )
        .await
        .expect("the next read must overlap the current write")
        .unwrap();
    }

    #[tokio::test]
    async fn run_lanes_hands_a_failed_read_to_the_write_side_in_listed_order() {
        use crate::error::Error;
        use crate::manifest::Outcome;
        // Shard 1's read fails. Its write side still runs, in order, with
        // the error, so it can produce that shard's Failed verdict; the lane
        // goes on to shard 2.
        let shards = vec![test_shard(0, 0), test_shard(1, 0), test_shard(2, 0)];
        let seen = std::sync::Mutex::new(Vec::<(u32, bool)>::new());
        let seen_ref = &seen;
        super::run_lanes(
            &shards,
            |s| async move {
                if s.index == 1 {
                    Err(Error::BqRead("stream died".into()))
                } else {
                    Ok(())
                }
            },
            |s, loaded: crate::error::Result<()>| async move {
                seen_ref.lock().unwrap().push((s.index, loaded.is_ok()));
                let outcome = if loaded.is_ok() {
                    Outcome::Complete
                } else {
                    Outcome::Failed
                };
                Ok(test_verdict(s.index, outcome, 1))
            },
        )
        .await
        .unwrap();
        assert_eq!(
            seen.into_inner().unwrap(),
            vec![(0, true), (1, false), (2, true)]
        );
    }

    #[tokio::test]
    async fn run_lanes_walks_each_lane_sequentially_in_listed_order() {
        use crate::manifest::Outcome;
        let shards = vec![
            test_shard(0, 0),
            test_shard(1, 1),
            test_shard(2, 0),
            test_shard(3, 1),
        ];
        let seen = std::sync::Mutex::new(Vec::<(u32, u32)>::new());
        let seen_ref = &seen;
        writing(&shards, |s| async move {
            seen_ref.lock().unwrap().push((s.lane, s.index));
            Ok(test_verdict(s.index, Outcome::Complete, 1))
        })
        .await
        .unwrap();
        let seen = seen.into_inner().unwrap();
        let lane0: Vec<u32> = seen
            .iter()
            .filter(|(l, _)| *l == 0)
            .map(|(_, i)| *i)
            .collect();
        let lane1: Vec<u32> = seen
            .iter()
            .filter(|(l, _)| *l == 1)
            .map(|(_, i)| *i)
            .collect();
        assert_eq!(lane0, vec![0, 2], "lane 0 in listed order");
        assert_eq!(lane1, vec![1, 3], "lane 1 in listed order");
    }

    #[tokio::test]
    async fn run_lanes_runs_lanes_concurrently() {
        use crate::manifest::Outcome;
        // Lane 0's only shard WAITS for lane 1's shard to fire a oneshot.
        // Sequential lane execution would deadlock; the timeout converts
        // that regression into a failure.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tx = std::sync::Mutex::new(Some(tx));
        let rx = std::sync::Mutex::new(Some(rx));
        let shards = vec![test_shard(0, 0), test_shard(1, 1)];
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            writing(&shards, |s| {
                let fire = if s.index == 1 {
                    tx.lock().unwrap().take()
                } else {
                    None
                };
                let wait = if s.index == 0 {
                    rx.lock().unwrap().take()
                } else {
                    None
                };
                async move {
                    if let Some(tx) = fire {
                        let _ = tx.send(());
                    }
                    if let Some(rx) = wait {
                        rx.await.expect("lane 1 fires while lane 0 waits");
                    }
                    Ok(test_verdict(s.index, Outcome::Complete, 1))
                }
            }),
        )
        .await
        .expect("lanes must run concurrently, not sequentially")
        .unwrap();
    }

    #[tokio::test]
    async fn run_lanes_per_lane_breaker_trips_on_one_bad_lane() {
        use crate::manifest::Outcome;
        // The blindness case: lane 0 fails every shard while lane 1
        // succeeds. A pod-wide completion-order streak could interleave and
        // never trip; the per-lane breaker must stop its lane and fail the
        // pod.
        let shards = vec![
            test_shard(0, 0),
            test_shard(1, 1),
            test_shard(2, 0),
            test_shard(3, 1),
            test_shard(4, 0),
            test_shard(5, 1),
        ];
        let err = writing(&shards, |s| async move {
            tokio::task::yield_now().await; // interleave lanes, as real move_shard awaits do
            let outcome = if s.lane == 0 {
                Outcome::Failed
            } else {
                Outcome::Complete
            };
            Ok(test_verdict(s.index, outcome, 1))
        })
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("lanes tripped [lane 0 at shard 4"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn run_lanes_expected_empty_preserves_the_lane_streak() {
        use crate::manifest::Outcome;
        // Complete-with-zero-rows (expect_empty) records neither success
        // nor failure: two failures, an interleaved empty, then a third
        // failure still trips (breaker_signal contract carried into lanes).
        let shards = vec![
            test_shard(0, 0),
            test_shard(1, 0),
            test_shard(2, 0),
            test_shard(3, 0),
        ];
        let err = writing(&shards, |s| async move {
            let (outcome, rows) = match s.index {
                2 => (Outcome::Complete, 0), // expected-empty: skip
                _ => (Outcome::Failed, 1),
            };
            Ok(test_verdict(s.index, outcome, rows))
        })
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("lanes tripped [lane 0 at shard 3"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn run_lanes_tripped_lane_leaves_sibling_lanes_running() {
        use crate::manifest::Outcome;
        // Lane 0 fails every shard and trips after 3; lanes 1 and 2 must
        // still walk ALL their shards, and the result names lane 0.
        let mut shards = Vec::new();
        for i in 0..12u32 {
            shards.push(test_shard(i, i % 3));
        }
        let seen = std::sync::Mutex::new(Vec::<u32>::new());
        let seen_ref = &seen;
        let err = writing(&shards, |s| async move {
            tokio::task::yield_now().await;
            seen_ref.lock().unwrap().push(s.index);
            let outcome = if s.lane == 0 {
                Outcome::Failed
            } else {
                Outcome::Complete
            };
            Ok(test_verdict(s.index, outcome, 1))
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("lane 0"), "{err}");
        let seen = seen.into_inner().unwrap();
        for i in (0..12u32).filter(|i| i % 3 != 0) {
            assert!(seen.contains(&i), "sibling shard {i} must have run");
        }
        // Lane 0 stopped at its third failure: shards 0, 3, 6 ran; 9 did not.
        assert!(seen.contains(&6));
        assert!(!seen.contains(&9), "tripped lane must stop walking");
        // Siblings don't just run — they run PAST the point lane 0 tripped.
        let pos = |x: u32| seen.iter().position(|&v| v == x).unwrap();
        assert!(
            pos(10) > pos(6),
            "sibling shard 10 must run after lane 0's trip at shard 6: {seen:?}"
        );
    }

    #[tokio::test]
    async fn run_lanes_fatal_error_stops_only_its_lane() {
        use crate::manifest::Outcome;
        let shards = vec![
            test_shard(0, 0),
            test_shard(1, 1),
            test_shard(2, 0),
            test_shard(3, 1),
        ];
        let seen = std::sync::Mutex::new(Vec::<u32>::new());
        let seen_ref = &seen;
        let err = writing(&shards, |s| async move {
            tokio::task::yield_now().await;
            seen_ref.lock().unwrap().push(s.index);
            if s.index == 0 {
                Err(crate::error::Error::Write(
                    "verdict upload failed".to_owned(),
                ))
            } else {
                Ok(test_verdict(s.index, Outcome::Complete, 1))
            }
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("upload"), "{err}");
        let seen = seen.into_inner().unwrap();
        assert!(
            seen.contains(&1) && seen.contains(&3),
            "lane 1 finished: {seen:?}"
        );
        assert!(
            !seen.contains(&2),
            "lane 0 stopped after its fatal error: {seen:?}"
        );
        // Lane 1 doesn't just finish — its second shard runs PAST the point
        // lane 0's fatal error stopped it.
        let pos = |x: u32| seen.iter().position(|&v| v == x).unwrap();
        assert!(
            pos(3) > pos(0),
            "lane 1's second shard must run after lane 0's fatal shard: {seen:?}"
        );
    }

    #[test]
    fn lanes_result_is_ok_only_when_every_lane_completed() {
        use super::{lanes_result, LaneEnd, LaneOutcome};
        assert!(lanes_result(vec![
            LaneOutcome {
                lane: 0,
                end: LaneEnd::Completed
            },
            LaneOutcome {
                lane: 1,
                end: LaneEnd::Completed
            },
        ])
        .is_ok());
        let err = lanes_result(vec![
            LaneOutcome {
                lane: 0,
                end: LaneEnd::Completed,
            },
            LaneOutcome {
                lane: 1,
                end: LaneEnd::Tripped { shard: 17 },
            },
            LaneOutcome {
                lane: 2,
                end: LaneEnd::Failed {
                    shard: 40,
                    error: crate::error::Error::Write("boom".into()),
                },
            },
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("lane 1") && err.contains("17"), "{err}");
        assert!(err.contains("lane 2") && err.contains("boom"), "{err}");
    }
}
