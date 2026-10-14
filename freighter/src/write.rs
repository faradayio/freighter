//! The ordered, budgeted write of one shard's rows: a whole-shard sort,
//! disjoint consecutive key slices of `flush_cells` rows, one mutation
//! permit per row acquired BEFORE a slice's flush is spawned (that is the
//! backpressure), bounded per-entry retries, retry-proof counters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::{mpsc, Semaphore};

use crate::client::{BtClient, EntryStatus, RowChange, RowWrite};
use crate::error::{Error, Result};
use crate::manifest::ShardLabel;
use crate::retry::{retry_rpc, retryable_code};

/// Bounded in-place retries for failed entries. Idempotency makes retries
/// safe; the bound keeps a dead destination from retrying forever.
const ENTRY_RETRY_LIMIT: u32 = 3;

/// What the write path needs from a Bigtable client: exactly
/// `BtClient::write_rows`. The generic lets the emulator suite inject
/// transient failures ahead of the real emulator (one production impl, one
/// fault-injecting test impl that delegates to it). Generic, not `dyn`: an
/// async fn in a trait returns an anonymous `impl Future`, which is not
/// dyn-compatible without boxing every call.
pub trait WriteRows: Clone + Send + Sync + 'static {
    fn write_rows(
        &self,
        table: &str,
        rows: Vec<RowWrite>,
    ) -> impl std::future::Future<Output = Result<Vec<EntryStatus>>> + Send;
}

impl WriteRows for BtClient {
    fn write_rows(
        &self,
        table: &str,
        rows: Vec<RowWrite>,
    ) -> impl std::future::Future<Output = Result<Vec<EntryStatus>>> + Send {
        BtClient::write_rows(self, table, rows) // inherent method; the trait method would recurse
    }
}

#[derive(Debug)]
pub struct Counters {
    /// Which slice these counters belong to — printed on every write-side
    /// retry line.
    label: ShardLabel,
    started: Instant,
    rows_read: AtomicU64,
    cells_encoded: AtomicU64,
    /// Each entry at most once, on FINAL success (retry-proof).
    cells_acknowledged: AtomicU64,
    /// TERMINALLY failed after retries.
    cells_failed: AtomicU64,
    row_deletes_acknowledged: AtomicU64,
    row_deletes_failed: AtomicU64,
    entry_retries: AtomicU64,
    requests: AtomicU64,
    /// MutateRows entries sent, counted once per flush (retries re-send a
    /// subset, not new entries). entries/requests is the request-shape
    /// metric: Dataflow's reference shape is ~21 contiguous entries/request
    /// — subtract THIS STRUCT's rpc_retries (write-side only) from requests
    /// first, since requests also counts whole-batch RPC retries. Once this
    /// is folded into a `ShardVerdict`, that subtraction no longer works:
    /// the verdict's `rpc_retries` sums this write-side count with the
    /// read-side unary-RPC retries from `ReadStats`, and the two
    /// populations can't be split back out from the verdict alone.
    entries: AtomicU64,
    /// Whole-request `write_rows` retries on transient errors.
    rpc_retries: AtomicU64,
    /// The shard-wide sort's wall time.
    sort_micros: AtomicU64,
    /// Micros since `started` of the first flush issued: starts at
    /// `u64::MAX`, fetch_min'd at first flush.
    write_first_micros: AtomicU64,
    /// Micros since `started` of the last MutateRows acknowledged: 0 until
    /// the first ack sets it.
    write_last_ack_micros: AtomicU64,
    /// One elapsed-micros sample per logical flush (a whole `retry_rpc`
    /// sequence — up to `RPC_ATTEMPTS` RPCs plus their backoff — not per
    /// underlying RPC): `latencies_micros.len()` == flush attempts, not
    /// RPCs, and p99 on a retrying slice is retry-inclusive.
    /// std::sync::Mutex on purpose: critical sections are a push or a
    /// clone, with no await held across the lock.
    latencies_micros: Mutex<Vec<u64>>,
}

impl Default for Counters {
    /// Emulator-suite affordance: label 0/0. Production's only constructor
    /// is `drain_shard` via `for_shard`.
    fn default() -> Self {
        Self::for_shard(ShardLabel { shard: 0, lane: 0 })
    }
}

impl Counters {
    pub fn for_shard(label: ShardLabel) -> Self {
        Self {
            label,
            started: Instant::now(),
            rows_read: AtomicU64::new(0),
            cells_encoded: AtomicU64::new(0),
            cells_acknowledged: AtomicU64::new(0),
            cells_failed: AtomicU64::new(0),
            row_deletes_acknowledged: AtomicU64::new(0),
            row_deletes_failed: AtomicU64::new(0),
            entry_retries: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            entries: AtomicU64::new(0),
            rpc_retries: AtomicU64::new(0),
            sort_micros: AtomicU64::new(0),
            write_first_micros: AtomicU64::new(u64::MAX),
            write_last_ack_micros: AtomicU64::new(0),
            latencies_micros: Mutex::new(Vec::new()),
        }
    }
    pub fn label(&self) -> ShardLabel {
        self.label
    }
    fn elapsed_micros(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
    pub fn rpc_retries(&self) -> u64 {
        self.rpc_retries.load(Ordering::Relaxed)
    }
    pub fn sort_seconds(&self) -> f64 {
        self.sort_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
    /// First flush → last ack; 0 when nothing was written.
    pub fn write_seconds(&self) -> f64 {
        let first = self.write_first_micros.load(Ordering::Relaxed);
        let last = self.write_last_ack_micros.load(Ordering::Relaxed);
        if first == u64::MAX || last < first {
            0.0
        } else {
            (last - first) as f64 / 1_000_000.0
        }
    }
    pub fn rows_read(&self) -> u64 {
        self.rows_read.load(Ordering::Relaxed)
    }
    pub fn cells_encoded(&self) -> u64 {
        self.cells_encoded.load(Ordering::Relaxed)
    }
    pub fn cells_acknowledged(&self) -> u64 {
        self.cells_acknowledged.load(Ordering::Relaxed)
    }
    pub fn cells_failed(&self) -> u64 {
        self.cells_failed.load(Ordering::Relaxed)
    }
    pub fn row_deletes_acknowledged(&self) -> u64 {
        self.row_deletes_acknowledged.load(Ordering::Relaxed)
    }
    pub fn row_deletes_failed(&self) -> u64 {
        self.row_deletes_failed.load(Ordering::Relaxed)
    }
    fn record_acknowledged(&self, change: &RowChange) {
        match change {
            RowChange::Set(cells) => self
                .cells_acknowledged
                .fetch_add(cells.len() as u64, Ordering::Relaxed),
            RowChange::Delete => self
                .row_deletes_acknowledged
                .fetch_add(1, Ordering::Relaxed),
        };
    }
    fn record_failed(&self, change: &RowChange) {
        match change {
            RowChange::Set(cells) => self
                .cells_failed
                .fetch_add(cells.len() as u64, Ordering::Relaxed),
            RowChange::Delete => self.row_deletes_failed.fetch_add(1, Ordering::Relaxed),
        };
    }
    pub fn entry_retries(&self) -> u64 {
        self.entry_retries.load(Ordering::Relaxed)
    }
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }
    pub fn entries(&self) -> u64 {
        self.entries.load(Ordering::Relaxed)
    }
    /// Quantiles of per-flush `write_rows` latency, in milliseconds — one
    /// result per requested `q`, from a SINGLE snapshot + sort (the
    /// histogram is one sample per logical flush, so a large shard's is
    /// worth cloning and sorting exactly once, not once per percentile). A
    /// sample spans one whole `retry_rpc` sequence, so p99 on a retrying
    /// slice is retry-inclusive, not per-RPC. Sort, round-index,
    /// micros / 1000.0. An empty histogram reports 0.0 for every quantile.
    pub fn latency_percentiles(&self, qs: &[f64]) -> Vec<f64> {
        let mut sorted = self
            .latencies_micros
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        sorted.sort_unstable();
        qs.iter()
            .map(|&q| {
                if sorted.is_empty() {
                    0.0
                } else {
                    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
                    sorted[idx] as f64 / 1000.0
                }
            })
            .collect()
    }
    fn record_latency_micros(&self, micros: u64) {
        self.latencies_micros
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(micros);
    }
}

/// Writes one flushed batch of rows: MutateRows, per-entry status, bounded
/// in-place retry of failed entries, counters updated only on terminal
/// outcomes.
///
/// A `write_rows` Err leaves the whole batch outcome UNKNOWN. If it is a
/// transient RPC failure (a transport-class gRPC code — Google frontends
/// close connections at ~60 min of age and the first request on a dead
/// channel fails fast), the WHOLE batch is re-sent with backoff, up to
/// [`crate::retry::RPC_ATTEMPTS`]−1 times (`RPC_ATTEMPTS` total attempts):
/// SetCell at explicit timestamps is idempotent. Anything else propagates
/// as a task-level failure (`?`) — the shard retry owns it. A per-entry
/// `Failed` (including the local `code: -1` no-status sentinel) gets the
/// bounded in-place retry with backoff, up to `ENTRY_RETRY_LIMIT` times —
/// composing the two: worst case (ENTRY_RETRY_LIMIT+1) ×
/// (RPC_ATTEMPTS × WRITE_RPC_TIMEOUT + backoff) ≈ 40 min per flush, permits
/// held throughout; iterations after the first re-send only the failed
/// subset, so the practical tail is ~10 min. Bounded and loud (`retry_rpc`
/// warns per attempt), never indefinite.
async fn write_flush<W: WriteRows>(
    bt: &W,
    dest: &str,
    mut rows: Vec<RowWrite>,
    counters: &Counters,
) -> Result<()> {
    counters
        .entries
        .fetch_add(rows.len() as u64, Ordering::Relaxed);
    counters
        .write_first_micros
        .fetch_min(counters.elapsed_micros(), Ordering::Relaxed);
    let mut attempt = 0u32;
    loop {
        let started = Instant::now();
        let (statuses, retries) = retry_rpc(
            "write_rows",
            counters.label(),
            |e: &Error| matches!(e, Error::BigtableRpc { code, .. } if retryable_code(*code)),
            |_| {
                counters.requests.fetch_add(1, Ordering::Relaxed);
                bt.write_rows(dest, rows.clone())
            },
        )
        .await
        .map_err(|e| Error::Write(e.to_string()))?;
        counters.rpc_retries.fetch_add(retries, Ordering::Relaxed);
        counters.record_latency_micros(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
        counters
            .write_last_ack_micros
            .fetch_max(counters.elapsed_micros(), Ordering::Relaxed);
        // write_rows returns exactly one status per entry; zip would
        // TRUNCATE silently if that invariant ever regressed.
        debug_assert_eq!(rows.len(), statuses.len());
        let mut failed: Vec<RowWrite> = Vec::new();
        for (row, status) in rows.into_iter().zip(statuses) {
            match status {
                EntryStatus::Ok => counters.record_acknowledged(&row.change),
                EntryStatus::Failed { .. } => failed.push(row),
            }
        }
        if failed.is_empty() {
            return Ok(());
        }
        attempt += 1;
        if attempt > ENTRY_RETRY_LIMIT {
            for row in &failed {
                counters.record_failed(&row.change);
            }
            return Ok(()); // terminal; verdict carries the failure
        }
        // Counted AFTER the limit check so entry_retries is exactly the
        // number of entry-retry attempts made, not attempts-plus-one for
        // terminally failed entries.
        counters
            .entry_retries
            .fetch_add(failed.len() as u64, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(200 * 2u64.pow(attempt))).await;
        rows = failed;
    }
}

/// Acquires MUTATION permits (one per row: a row is one cell or one delete) and spawns
/// one `write_flush` task. Permit BEFORE spawn: this await is the
/// backpressure that stalls decode, which stalls the bounded read channel.
/// A slice is at most `flush_cells` rows and the manifest guarantees
/// `flush_cells <= inflight_mutations`, the semaphore's size, so the
/// acquisition is always satisfiable.
async fn spawn_flush<W: WriteRows>(
    sem: &Arc<Semaphore>,
    rows: Vec<RowWrite>,
    bt: &W,
    dest: &str,
    counters: &Arc<Counters>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let n = u32::try_from(rows.len())
        .map_err(|_| Error::Write("mutation permit count overflows u32".to_owned()))?;
    let permit = Arc::clone(sem)
        .acquire_many_owned(n)
        .await
        .map_err(|_| Error::Write("semaphore closed".to_owned()))?;
    let bt = bt.clone();
    let dest = dest.to_owned();
    let c = Arc::clone(counters);
    Ok(tokio::spawn(async move {
        let _permit = permit;
        write_flush(&bt, &dest, rows, &c).await
    }))
}

/// Joins every spawned write task unconditionally. First error wins, with
/// precedence: loop error, then a task's own error, then a join error.
/// `write_sorted` exits only through here, so no exit detaches live
/// writers.
async fn join_writes(
    tasks: Vec<tokio::task::JoinHandle<Result<()>>>,
    loop_result: Result<()>,
) -> Result<()> {
    let mut task_err: Option<Error> = None;
    let mut join_err: Option<Error> = None;
    for t in tasks {
        match t.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if task_err.is_none() {
                    task_err = Some(e);
                }
            }
            Err(e) => {
                if join_err.is_none() {
                    join_err = Some(Error::Write(format!("write task join: {e}")));
                }
            }
        }
    }
    loop_result?;
    if let Some(e) = task_err {
        return Err(e);
    }
    if let Some(e) = join_err {
        return Err(e);
    }
    Ok(())
}

/// The shard's write path, in two halves: `collect_sorted` buffers the
/// WHOLE shard and sorts it once; `write_sorted` writes disjoint
/// consecutive key slices. Each tablet then sees append-order ingest — the
/// LSM-native fast path — instead of re-interleaving every request into
/// ranges it already wrote. The slices are issued in key order through one
/// cursor, as fast as the mutation budget admits them; concurrency across
/// distant key regions comes from lanes.
///
/// Memory: the shard's decoded cells are resident at once (~200B/cell) —
/// size pod memory and shard rung accordingly. A lane reads its next shard
/// while writing this one, so it is TWO shards per concurrent lane.
///
/// A key the source repeats is written once per occurrence, in arrival
/// order (the sort is stable): MutateRows applies a request's entries in
/// order and every cell carries the load's one timestamp, so the row ends
/// as one cell holding the last arrival's value.
///
/// The testability seam: emulator tests feed `collect_sorted` a channel of
/// decoded rows — no BQ required. `counters` is an Arc because flush tasks
/// outlive the loop body.
/// Drain the channel entirely, then ONE global sort, so every request of
/// `write_sorted` is a disjoint consecutive slice of this ordering. No
/// writes yet — a decode failure upstream surfaces at the readers' join and
/// fails the shard before any cell is sent. The sort runs on a blocking
/// thread: every lane is polled from one task, and a multi-second inline
/// sort would stall them all.
pub async fn collect_sorted(
    mut rx: mpsc::Receiver<Vec<RowWrite>>,
    counters: &Arc<Counters>,
) -> Result<Vec<RowWrite>> {
    let mut all: Vec<RowWrite> = Vec::new();
    while let Some(rows) = rx.recv().await {
        counters
            .rows_read
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        let cells: usize = rows
            .iter()
            .map(|r| match &r.change {
                RowChange::Set(cells) => cells.len(),
                RowChange::Delete => 0,
            })
            .sum();
        counters
            .cells_encoded
            .fetch_add(cells as u64, Ordering::Relaxed);
        all.extend(rows);
    }
    let sort_started = Instant::now();
    let all = tokio::task::spawn_blocking(move || {
        all.sort_by(|a, b| a.rowkey.cmp(&b.rowkey));
        all
    })
    .await
    .map_err(|e| Error::Write(format!("sort task: {e}")))?;
    counters.sort_micros.fetch_add(
        u64::try_from(sort_started.elapsed().as_micros()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    Ok(all)
}

/// Consecutive slices of `flush_cells` rows, in key order, as fast as the
/// mutation budget admits them.
pub async fn write_sorted<W: WriteRows>(
    all: Vec<RowWrite>,
    bt: &W,
    dest: &str,
    flush_cells: usize,
    counters: Arc<Counters>,
    sem: &Arc<Semaphore>,
) -> Result<()> {
    let mut rows = all.into_iter();
    let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::new();
    let loop_result: Result<()> = async {
        loop {
            let slice: Vec<RowWrite> = rows.by_ref().take(flush_cells.max(1)).collect();
            if slice.is_empty() {
                break;
            }
            tasks.push(spawn_flush(sem, slice, bt, dest, &counters).await?);
        }
        Ok(())
    }
    .await;
    join_writes(tasks, loop_result).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests; the workspace denies unwrap_used
mod tests {
    #[test]
    fn counters_write_seconds_is_zero_until_a_flush_is_acknowledged() {
        use super::Counters;
        use std::sync::atomic::Ordering;
        let fresh = Counters::default();
        assert_eq!(fresh.write_seconds(), 0.0, "nothing written yet");

        let c = Counters::default();
        c.write_first_micros.fetch_min(1_000_000, Ordering::Relaxed);
        c.write_last_ack_micros
            .fetch_max(3_500_000, Ordering::Relaxed);
        assert_eq!(c.write_seconds(), 2.5);

        // write_first_micros still u64::MAX (default, unset) with an ack
        // timestamp set: the `first == u64::MAX` guard must still read 0.0,
        // not underflow computing `last - first`.
        let unset_first = Counters::default();
        unset_first
            .write_last_ack_micros
            .fetch_max(3_500_000, Ordering::Relaxed);
        assert_eq!(unset_first.write_seconds(), 0.0);
    }
}
