//! The move loop: bounded mpsc, per-stream decode tasks (decode
//! parallelism is read_streams-wide, not 1 — vCPU is a real lever), a
//! shard's read phase and its write phase (a lane overlaps the next read
//! with the current write), empty-shard raise, the shard's verdict and its
//! upload, channel rotation, and the pod's walk of the manifest's lanes.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::{mpsc, Semaphore};

use crate::bqread::{BqReader, ReadStats};
use crate::cells::TableConfig;
use crate::client::{BtClient, RowWrite};
use crate::error::{Error, Result};
use crate::lanes::run_lanes;
use crate::lookup::{decode_lookup, lookup_selected_fields};
use crate::manifest::{MoveManifest, Outcome, ShardLabel, ShardRange, ShardSpec, ShardVerdict};
use crate::write::{collect_sorted, write_sorted, Counters, WriteRows};

/// One stream's decode task: drains `rx`, decodes each RecordBatch into
/// rows — one per BQ row, one cell each — and forwards them. One of these
/// runs per read stream, which is what makes decode parallelism
/// read_streams-wide instead of 1.
///
/// A closed downstream receiver means the writer failed; the writer's error
/// is the one that surfaces, so the decoder stops quietly (mirrors
/// `read_stream`'s contract).
pub async fn decode_stream(
    mut rx: mpsc::Receiver<RecordBatch>,
    shape: &TableConfig,
    tx: mpsc::Sender<Vec<RowWrite>>,
) -> Result<()> {
    while let Some(batch) = rx.recv().await {
        if tx.send(decode_lookup(&batch, shape)?).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// How often the pod swaps in fresh BigQuery and Bigtable connection
/// pools. Google frontends close connections at ~60 min of age; at 30
/// min, with reads silent ≤ 90 s before re-open and writes bounded at 120
/// s, no unary REQUEST starts on a connection that can reach that wall in
/// flight. A long-running read STREAM still can: it picks its channel once
/// at open and a healthy stream never re-opens, so a stream opened just
/// under the period can outlive its channel — Google closes it, and the
/// status re-open resumes it at offset on the rotated pool (the backstop
/// rotation is belt-and-braces for).
pub const CHANNEL_ROTATE_PERIOD: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// After a failed rotation the old pool is still in use and a whole
/// `period` later it would sit on Google's ~60-min wall — so retry sooner.
pub const ROTATE_RETRY_AFTER_FAILURE: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Rotates both pools every `period` until aborted. A failed rotation keeps
/// the old pool (rotate() contract), is logged, and is retried after
/// `ROTATE_RETRY_AFTER_FAILURE` instead of a full period; this never errors.
async fn rotate_pools_forever(bq: BqReader, bt: BtClient, period: std::time::Duration) {
    let mut wait = period;
    loop {
        tokio::time::sleep(wait).await;
        let mut all_ok = true;
        match bq.rotate().await {
            Ok(()) => log::info!("rotation done: bigquery pool"),
            Err(e) => {
                all_ok = false;
                log::warn!("rotation failed, keeping bigquery pool: {e}");
            }
        }
        match bt.rotate().await {
            Ok(()) => log::info!("rotation done: bigtable pool"),
            Err(e) => {
                all_ok = false;
                log::warn!("rotation failed, keeping bigtable pool: {e}");
            }
        }
        wait = if all_ok {
            period
        } else {
            ROTATE_RETRY_AFTER_FAILURE
        };
    }
}

/// The empty-shard rule, then the counters out of their Arc, after every
/// write task of the shard is joined.
fn finish_shard(counters: Arc<Counters>, expect_empty: bool) -> Result<Counters> {
    if counters.rows_read() == 0 && !expect_empty {
        return Err(Error::EmptyShard(
            "shard read zero rows and the plan did not mark it expected — wrong table, wrong range, or expired input".to_owned(),
        ));
    }
    if expect_empty && counters.rows_read() > 0 {
        // Not an error — the rows are moved normally — but a mis-specced
        // plan should surface early, not be discovered from verdict counts.
        log::warn!(
            "shard marked expect_empty read {} rows — the plan mis-specced this shard",
            counters.rows_read()
        );
    }
    // Hard error, never a silent default: a zeroed Counters would produce
    // a verdict with acknowledged=0, the caller's count check would then
    // fail forever, and the shard would re-run in a loop. All write tasks
    // are joined inside write_sorted, so this succeeds by construction —
    // the error is the tripwire for that invariant breaking.
    Arc::try_unwrap(counters).map_err(|_| {
        Error::Write("counters still shared after write_sorted joined all tasks".to_owned())
    })
}

/// Everything after decode, for one shard: collect, sort, write, verify the
/// empty-shard rule. Emulator tests enter here without BigQuery.
pub async fn drain_shard<W: WriteRows>(
    rx: mpsc::Receiver<Vec<RowWrite>>,
    bt: &W,
    dest: &str,
    flush_cells: usize,
    expect_empty: bool,
    label: ShardLabel,
    sem: &Arc<Semaphore>,
) -> Result<Counters> {
    let counters = Arc::new(Counters::for_shard(label));
    let rows = collect_sorted(rx, &counters).await?;
    write_sorted(rows, bt, dest, flush_cells, Arc::clone(&counters), sem).await?;
    finish_shard(counters, expect_empty)
}

/// The Storage Read row restriction for one shard: a BETWEEN on the
/// manifest's range column. Pure so it is unit-tested without a read
/// session.
fn shard_restriction(m: &MoveManifest, range: ShardRange) -> String {
    let ShardRange::Int { lo, hi } = range;
    format!("{} BETWEEN {lo} AND {hi}", m.range_column)
}

/// One shard, read and sorted, waiting for its write phase. Resident in
/// full: a lane holds one of these while it writes the previous shard.
pub struct LoadedShard {
    rows: Vec<RowWrite>,
    counters: Arc<Counters>,
    stats: Arc<ReadStats>,
    started: Instant,
}

/// The read phase of one shard: session, streams, decode, collect, sort.
/// A reader or decoder that failed fails the shard here, before any cell
/// is sent: the collected rows would be short.
pub async fn read_shard(bq: &BqReader, m: &MoveManifest, shard: &ShardSpec) -> Result<LoadedShard> {
    let shape = TableConfig::from_manifest(m);
    let restriction = shard_restriction(m, shard.range);
    let started = Instant::now();
    let label = shard.label();
    let stats = Arc::new(ReadStats::start(label));
    log::info!("{label} row restriction: {restriction:?}");
    let session = bq
        .create_session(
            &m.billing_project,
            &m.source_table,
            lookup_selected_fields(&shape),
            restriction,
            m.read_streams,
            &stats,
        )
        .await?;
    log::info!(
        "{label} session open: {} streams, est {} rows",
        session.streams.len(),
        session.estimated_rows
    );
    // Reader -> per-stream channel -> decode task -> ONE shared bounded
    // channel -> collector. The shared channel is what keeps backpressure
    // global: a stalled collector fills it, which stalls every decoder,
    // which fills each per-stream channel, which stalls every reader — no
    // stream can run away with memory. The per-stream hop is what lets a
    // stream's network read proceed while its previous batch is being
    // decoded. Capacity scales with producers: 64 decoders squeezing
    // through 8 slots thrash on wakeups; one slot per stream keeps handoff
    // smooth while still bounding buffered decoded batches (memory ≈ one
    // batch per stream beyond the per-stream raw channels).
    let (dtx, drx) = mpsc::channel::<Vec<RowWrite>>(session.streams.len().max(8));
    let shape = Arc::new(shape);
    let schema = Arc::new(session.arrow_schema);
    let mut readers = Vec::new();
    let mut decoders = Vec::new();
    for stream in session.streams {
        let (btx, brx) = mpsc::channel::<RecordBatch>(2);
        let bq = bq.clone();
        let schema = Arc::clone(&schema);
        let st = Arc::clone(&stats);
        readers.push(tokio::spawn(async move {
            bq.read_stream(&stream, &schema, btx, &st).await
        }));
        let shape = Arc::clone(&shape);
        let dtx = dtx.clone();
        let st = Arc::clone(&stats);
        decoders.push(tokio::spawn(async move {
            let r = decode_stream(brx, &shape, dtx).await;
            st.mark_decode_done();
            r
        }));
    }
    drop(dtx); // decoders hold the only remaining senders

    let counters = Arc::new(Counters::for_shard(label));
    let collected = collect_sorted(drx, &counters).await;
    // Readers and decoders are joined BEFORE the collected rows are trusted:
    // a stream that died mid-shard closes the channel early and the
    // collector sees a short count when the truth is a read failure.
    //
    // A decoder parked in `rx.recv()` holds its per-stream channel open, so
    // a reader that's stalled (not yet errored) can keep this join waiting
    // up to READ_REOPEN_LIMIT × READ_IDLE_TIMEOUT (~6 min) before it
    // finally errors out and the shard's verdict lands.
    let mut read_err: Option<Error> = None;
    for r in readers {
        let outcome = r
            .await
            .unwrap_or_else(|e| Err(Error::BqRead(format!("reader join: {e}"))));
        if let Err(e) = outcome {
            log::warn!("{label} read error: {e}");
            read_err.get_or_insert(e);
        }
    }
    for d in decoders {
        let outcome = d
            .await
            .unwrap_or_else(|e| Err(Error::Decode(format!("decoder join: {e}"))));
        if let Err(e) = outcome {
            log::warn!("{label} decode error: {e}");
            read_err.get_or_insert(e);
        }
    }
    let rows = match (collected, read_err) {
        (_, Some(e)) | (Err(e), None) => {
            log::warn!(
                "{label} read failed: err={e} wall={:.1}s read={:.1}s stalled={:.0}s reopens={} rpc_retries={}",
                started.elapsed().as_secs_f64(),
                stats.read_seconds(),
                stats.stalled_seconds(),
                stats.read_reopens(),
                stats.rpc_retries(),
            );
            return Err(e);
        }
        (Ok(rows), None) => rows,
    };
    log::info!(
        "{label} read: {} rows in {:.1}s (sort {:.1}s)",
        rows.len(),
        started.elapsed().as_secs_f64(),
        counters.sort_seconds(),
    );
    Ok(LoadedShard {
        rows,
        counters,
        stats,
        started,
    })
}

/// The write phase of one shard: the sorted rows to Bigtable, the
/// empty-shard rule, the verdict.
pub async fn write_shard(
    bt: &BtClient,
    m: &MoveManifest,
    shard: &ShardSpec,
    loaded: LoadedShard,
    sem: &Arc<Semaphore>,
) -> Result<ShardVerdict> {
    let LoadedShard {
        rows,
        counters,
        stats,
        started,
    } = loaded;
    let label = shard.label();
    let written = write_sorted(
        rows,
        bt,
        &m.destination_table,
        m.flush_cells,
        Arc::clone(&counters),
        sem,
    )
    .await;
    let counters = match written.and_then(|()| finish_shard(counters, shard.expect_empty)) {
        Ok(c) => c,
        Err(e) => {
            log::warn!(
                "{label} done: outcome=Failed err={e} wall={:.1}s read={:.1}s stalled={:.0}s reopens={} rpc_retries={}",
                started.elapsed().as_secs_f64(),
                stats.read_seconds(),
                stats.stalled_seconds(),
                stats.read_reopens(),
                stats.rpc_retries(),
            );
            return Err(e);
        }
    };
    let ok = counters.cells_failed() == 0 && counters.row_deletes_failed() == 0;
    let percentiles = counters.latency_percentiles(&[0.50, 0.99]);
    let verdict = ShardVerdict {
        manifest_version: m.version,
        shard_index: shard.index,
        rows_read: counters.rows_read(),
        cells_encoded: counters.cells_encoded(),
        cells_acknowledged: counters.cells_acknowledged(),
        cells_failed: counters.cells_failed(),
        row_deletes_acknowledged: counters.row_deletes_acknowledged(),
        row_deletes_failed: counters.row_deletes_failed(),
        entry_retries: counters.entry_retries(),
        entries: counters.entries(),
        requests: counters.requests(),
        wall_seconds: started.elapsed().as_secs_f64(),
        latency_ms_p50: percentiles[0],
        latency_ms_p99: percentiles[1],
        read_reopens: stats.read_reopens(),
        // disjoint populations: read-side unary RPCs vs write-side whole-batch re-sends
        rpc_retries: stats.rpc_retries() + counters.rpc_retries(),
        read_seconds: stats.read_seconds(),
        sort_seconds: counters.sort_seconds(),
        write_seconds: counters.write_seconds(),
        stalled_seconds: stats.stalled_seconds(),
        outcome: if ok {
            Outcome::Complete
        } else {
            Outcome::Failed
        },
    };
    log::info!(
        "{label} done: outcome={:?} rows_read={} cells_ack={} cells_failed={} deletes_ack={} deletes_failed={} wall={:.1}s read={:.1}s sort={:.1}s write={:.1}s stalled={:.0}s reopens={} rpc_retries={} requests={} p50={:.0}ms p99={:.0}ms",
        verdict.outcome,
        verdict.rows_read,
        verdict.cells_acknowledged,
        verdict.cells_failed,
        verdict.row_deletes_acknowledged,
        verdict.row_deletes_failed,
        verdict.wall_seconds,
        verdict.read_seconds,
        verdict.sort_seconds,
        verdict.write_seconds,
        verdict.stalled_seconds,
        verdict.read_reopens,
        verdict.rpc_retries,
        verdict.requests,
        verdict.latency_ms_p50,
        verdict.latency_ms_p99,
    );
    Ok(verdict)
}

/// The verdict of a shard that failed before it had counters: nothing
/// acknowledged, so the launcher's count check re-runs it.
fn failed_verdict(m: &MoveManifest, shard: &ShardSpec, e: &Error) -> ShardVerdict {
    log::error!("{} failed: {e}", shard.label());
    ShardVerdict {
        manifest_version: m.version,
        shard_index: shard.index,
        rows_read: 0,
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
        outcome: Outcome::Failed,
    }
}

/// The binary's whole job: connect once, build the pod-wide mutation
/// budget, then walk the manifest's lanes (all parallelism is intra-shard,
/// across lanes, and a lane's next read against its current write). Each
/// shard's verdict uploads as it completes; per-shard failures become
/// Failed verdicts (the caller re-runs them); a tripped or
/// failed lane stops itself; the pod exits non-zero at the end if any lane
/// did not complete.
pub async fn run_move(m: MoveManifest, store: Arc<dyn ObjectStore>) -> Result<()> {
    let bq = BqReader::connect().await?;
    // channel_size 4: the write path's connection pool.
    let bt = BtClient::connect(&m.bt_project, &m.bt_instance, &m.app_profile, 4).await?;
    // ONE budget for the whole pod. Manifest validation guarantees
    // flush_cells <= inflight_mutations, so spawn_flush's acquire of one
    // permit per row of a slice is always satisfiable. Shard-scoped budgets
    // would multiply the server queue by the concurrent-lane count.
    let sem = Arc::new(Semaphore::new(m.inflight_mutations));
    let ticker = tokio::spawn(rotate_pools_forever(
        bq.clone(),
        bt.clone(),
        CHANNEL_ROTATE_PERIOD,
    ));
    // Reborrow as Copy refs: the Fn closures run once per shard, so each
    // async move block must capture references, not move the owned values.
    let (m, bq, bt, store, sem) = (&m, &bq, &bt, &store, &sem);
    let result = run_lanes(
        &m.shards,
        |shard| async move { read_shard(bq, m, &shard).await },
        |shard, loaded| async move {
            let verdict = match loaded {
                Ok(loaded) => match write_shard(bt, m, &shard, loaded, sem).await {
                    Ok(v) => v,
                    Err(e) => failed_verdict(m, &shard, &e),
                },
                Err(e) => failed_verdict(m, &shard, &e),
            };
            upload_verdict(store, &verdict).await?;
            Ok(verdict)
        },
    )
    .await;
    ticker.abort();
    result
}

async fn upload_verdict(store: &Arc<dyn ObjectStore>, v: &ShardVerdict) -> Result<()> {
    // The store is rooted at the MANIFEST's location by main.rs, so
    // verdicts land beside the manifest.
    let path = StorePath::from(format!("shard_{:04}.json", v.shard_index));
    let body = serde_json::to_vec(v).map_err(|e| Error::Storage(format!("verdict json: {e}")))?;
    store
        .put(&path, Bytes::from(body).into())
        .await
        .map_err(|e| Error::Storage(format!("verdict upload {path} beside the manifest: {e}")))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests; the workspace denies unwrap_used
mod tests {
    const LOOKUP_MANIFEST_FIXTURE: &str =
        include_str!("../fixtures/freighter-lookup-manifest-example.json");

    #[test]
    fn shard_restriction_is_built_on_the_range_column() {
        use super::shard_restriction;
        use crate::manifest::MoveManifest;
        let lookup = MoveManifest::from_json(LOOKUP_MANIFEST_FIXTURE).unwrap();
        assert_eq!(
            shard_restriction(&lookup, lookup.shards[0].range),
            "bucket BETWEEN 0 AND 53"
        );
    }
}
