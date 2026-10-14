//! The write path against a real emulator: slicing, repeated keys, the
//! mutation budget, retries, and the lookup round trip.

#![allow(clippy::unwrap_used)] // tests; same allowance as main.rs

use std::io::Read;
use std::sync::Arc;

use freighter::write::Counters;
use tokio::sync::mpsc;

use crate::support;
use crate::support::write_batches;

#[tokio::test]
async fn six_scrambled_rows_at_flush_cells_three_go_out_as_two_requests() {
    // Six distinct rowkeys arriving scrambled, flush_cells=3: exactly two
    // MutateRows requests carry the six entries, and every cell is
    // acknowledged. The counts are what is asserted; which keys share a
    // request is not observable here.
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("mover_ordered_slices").await;
    let shape = support::lookup_config();
    let (tx, rx) = mpsc::channel(8);
    tx.send(support::decoded(
        &support::lookup_keys(&["k5", "k1", "k3", "k6", "k2", "k4"]),
        &shape,
    ))
    .await
    .unwrap();
    drop(tx);
    let counters = Arc::new(Counters::default());
    write_batches(
        rx,
        &client,
        &table,
        3,
        Arc::clone(&counters),
        &Arc::new(tokio::sync::Semaphore::new(100)),
    )
    .await
    .unwrap();
    assert_eq!(counters.cells_acknowledged(), 6);
    assert_eq!(counters.cells_failed(), 0);
    assert_eq!(counters.requests(), 2);
    assert_eq!(counters.entries(), 6);
}

#[tokio::test]
async fn empty_shard_raises_unless_expected() {
    support::require_emulator();
    // move_shard over a channel that closes with no batches.
    let (client, table) = support::fresh_lookup_table("mover_empty").await;
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<freighter::client::RowWrite>>(1);
    drop(tx);
    let err = freighter::pipeline::drain_shard(
        rx,
        &client,
        &table,
        3000,
        false,
        freighter::manifest::ShardLabel { shard: 0, lane: 0 },
        &Arc::new(tokio::sync::Semaphore::new(3000)),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, freighter::error::Error::EmptyShard(_)));

    let (tx2, rx2) = tokio::sync::mpsc::channel::<Vec<freighter::client::RowWrite>>(1);
    drop(tx2);
    freighter::pipeline::drain_shard(
        rx2,
        &client,
        &table,
        3000,
        true,
        freighter::manifest::ShardLabel { shard: 0, lane: 0 },
        &Arc::new(tokio::sync::Semaphore::new(3000)),
    )
    .await
    .expect("expect_empty=true tolerates an empty read");
}

#[tokio::test]
async fn concurrent_drains_share_one_pod_semaphore_without_deadlock() {
    // One budget per pod means concurrent lanes contend on it. Two
    // drains sharing a tight budget (3 permits, flush 3; five single-cell
    // rows each — a slice of 3 taking the whole budget, then a slice of 2,
    // interleaving across the drains) must complete — a double-acquire or
    // permit-leak regression deadlocks, which the timeout converts into a
    // failure. Counters stay per-drain exact.
    support::require_emulator();
    let (client_a, table_a) = support::fresh_lookup_table("mover_shared_sem_a").await;
    let (client_b, table_b) = support::fresh_lookup_table("mover_shared_sem_b").await;
    let shape = support::lookup_config();
    let sem = Arc::new(tokio::sync::Semaphore::new(3));

    let (tx_a, rx_a) = mpsc::channel(8);
    tx_a.send(support::decoded(
        &support::lookup_keys(&["a1", "a2", "a3", "a4", "a5"]),
        &shape,
    ))
    .await
    .unwrap();
    drop(tx_a);
    let (tx_b, rx_b) = mpsc::channel(8);
    tx_b.send(support::decoded(
        &support::lookup_keys(&["b1", "b2", "b3", "b4", "b5"]),
        &shape,
    ))
    .await
    .unwrap();
    drop(tx_b);

    let counters_a = Arc::new(Counters::default());
    let counters_b = Arc::new(Counters::default());
    let (ra, rb) = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        futures::future::join(
            write_batches(rx_a, &client_a, &table_a, 3, Arc::clone(&counters_a), &sem),
            write_batches(rx_b, &client_b, &table_b, 3, Arc::clone(&counters_b), &sem),
        ),
    )
    .await
    .expect("two drains sharing one pod semaphore must not deadlock");
    ra.unwrap();
    rb.unwrap();
    assert_eq!(counters_a.cells_acknowledged(), 5);
    assert_eq!(counters_b.cells_acknowledged(), 5);
    assert_eq!(counters_a.cells_failed() + counters_b.cells_failed(), 0);
}

#[tokio::test]
async fn write_batches_retries_transient_rpc_errors_and_acks_once() {
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("mover_flaky").await;
    let shape = support::lookup_config();
    // Six single-cell rows; flush_cells 3 → 2 slices of 3 cells, and the
    // 3000-mutation budget admits both at once, so both flushes run as
    // concurrent tasks. `fail_first` is ONE counter
    // shared across both tasks, not a per-flush budget — either flush can
    // consume 0, 1, or both injected failures depending on scheduling. The
    // assertions below hold under any split because they are TOTALS:
    // rpc_retries, requests, and cells_acknowledged are invariant to which
    // flush happened to eat the failures. Do not add a per-flush assertion
    // here; it would flake on the race.
    let keys = ["k1", "k2", "k3", "k4", "k5", "k6"];
    let batch = support::lookup_keys(&keys);
    let writer = support::FlakyWriter::new(client.clone(), 2, 14);
    let (tx, rx) = mpsc::channel(8);
    tx.send(support::decoded(&batch, &shape)).await.unwrap();
    drop(tx);
    let counters = Arc::new(Counters::default());
    write_batches(
        rx,
        &writer,
        &table,
        3,
        Arc::clone(&counters),
        &Arc::new(tokio::sync::Semaphore::new(3000)),
    )
    .await
    .unwrap();
    assert_eq!(counters.cells_acknowledged(), 6);
    assert_eq!(counters.cells_failed(), 0);
    assert_eq!(
        counters.rpc_retries(),
        2,
        "two injected failures were retried"
    );
    assert_eq!(counters.requests(), 4, "2 flushes + 2 retries");
    assert_eq!(
        counters.entry_retries(),
        0,
        "whole-batch RPC retry, not per-entry retry"
    );
    let keys: Vec<Vec<u8>> = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
    let rows = support::read_back_all(&client, &table, &keys)
        .await
        .unwrap();
    assert_eq!(rows.len(), 6);
    assert_eq!(rows.iter().map(|r| r.cells.len()).sum::<usize>(), 6);
}

#[tokio::test]
async fn write_batches_does_not_retry_a_non_transient_rpc_error() {
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("mover_flaky_fatal").await;
    let shape = support::lookup_config();
    let batch = support::lookup_keys(&["k1", "k2"]);
    let writer = support::FlakyWriter::new(client, 1, 3); // INVALID_ARGUMENT
    let (tx, rx) = mpsc::channel(8);
    tx.send(support::decoded(&batch, &shape)).await.unwrap();
    drop(tx);
    let counters = Arc::new(Counters::default());
    let err = write_batches(
        rx,
        &writer,
        &table,
        3000,
        Arc::clone(&counters),
        &Arc::new(tokio::sync::Semaphore::new(3000)),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, freighter::error::Error::Write(_)), "{err}");
    assert!(
        err.to_string().contains("injected"),
        "must be the injected error, not some other Write failure: {err}"
    );
    assert_eq!(counters.rpc_retries(), 0);
    assert_eq!(counters.requests(), 1);
}

#[tokio::test]
async fn rotate_against_emulator_keeps_writes_flowing() {
    // rotate() re-dials and the pool it leaves in place still serves real
    // MutateRows traffic. It would pass if rotate() were a no-op.
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("mover_rotate").await;
    let shape = support::lookup_config();
    for key in ["k1", "k2"] {
        let batch = support::lookup_keys(&[key]);
        let (tx, rx) = mpsc::channel(8);
        tx.send(support::decoded(&batch, &shape)).await.unwrap();
        drop(tx);
        write_batches(
            rx,
            &client,
            &table,
            3000,
            Arc::new(Counters::default()),
            &Arc::new(tokio::sync::Semaphore::new(3000)),
        )
        .await
        .unwrap();
        if key == "k1" {
            client.rotate().await.expect("rotate against the emulator");
        }
    }
    let keys = [b"k1".to_vec(), b"k2".to_vec()];
    let rows = support::read_back_all(&client, &table, &keys)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "both writes landed, one on each side of the rotation"
    );
}

#[tokio::test]
async fn lookup_batch_round_trips() {
    // The write side end to end: (key, json) rows in,
    // one gzipped data:jsonz cell per row out, at the config's timestamp.
    // Written TWICE through the same config: a relaunched shard re-writes
    // cells it already wrote, and the single load timestamp is what makes
    // that a storage no-op (one cell, not two versions).
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("mover_lookup").await;
    let config = support::lookup_config();
    let source = [
        ("p/t/5005", r#"{"score":0.91}"#),
        ("p/t/1001", r#"{"score":0.42}"#),
        ("p/t/05401", r#"{"count":17,"town":"Winooski — café"}"#),
        ("p/t/3003", r#"{"score":0.07}"#),
        ("p/t/2002", "{}"),
    ];
    let batch = support::lookup_cells(&source);

    for attempt in 0..2 {
        let (tx, rx) = mpsc::channel(8);
        tx.send(support::decoded(&batch, &config)).await.unwrap();
        drop(tx);
        let counters = Arc::new(Counters::default());
        write_batches(
            rx,
            &client,
            &table,
            2,
            Arc::clone(&counters),
            &Arc::new(tokio::sync::Semaphore::new(100)),
        )
        .await
        .expect("write_batches");
        assert_eq!(counters.cells_failed(), 0, "attempt {attempt}");
        assert_eq!(counters.rows_read(), 5, "attempt {attempt}");
        assert_eq!(counters.cells_acknowledged(), 5, "one cell per row");
        assert_eq!(counters.entries(), 5, "one entry per row");
        assert_eq!(counters.requests(), 3, "5 rows in sorted slices of 2");
    }

    for (key, json) in source {
        let rows = support::read_back_all(&client, &table, &[key.as_bytes().to_vec()])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "row {key} present");
        assert_eq!(rows[0].rowkey, key.as_bytes());
        assert_eq!(rows[0].cells.len(), 1, "exactly one cell in row {key}");
        let cell = &rows[0].cells[0];
        assert_eq!(cell.family, "data");
        assert_eq!(cell.qualifier, b"jsonz".to_vec());
        assert_eq!(cell.timestamp_micros, support::LOOKUP_TIMESTAMP_MICROS);
        let mut decompressed = String::new();
        flate2::read::GzDecoder::new(cell.value.as_slice())
            .read_to_string(&mut decompressed)
            .unwrap();
        assert_eq!(decompressed, json);
    }
}

#[tokio::test]
async fn a_null_json_deletes_its_row_and_the_rest_are_set() {
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("mover_deletes").await;
    let loads = [
        (
            support::lookup_config(),
            support::lookup_cells(&[("k1", r#"{"v":1}"#), ("k2", r#"{"v":1}"#), ("k3", "{}")]),
        ),
        (
            support::deleting_config(),
            support::lookup_changes(&[("k1", None), ("k2", Some(r#"{"v":2}"#)), ("k4", None)]),
        ),
    ];
    let mut last_load = None;
    for (config, batch) in &loads {
        let (tx, rx) = mpsc::channel(8);
        tx.send(support::decoded(batch, config)).await.unwrap();
        drop(tx);
        let counters = Arc::new(Counters::default());
        write_batches(
            rx,
            &client,
            &table,
            2,
            Arc::clone(&counters),
            &Arc::new(tokio::sync::Semaphore::new(100)),
        )
        .await
        .unwrap();
        last_load = Some(counters);
    }
    let counters = last_load.unwrap();
    assert_eq!(counters.rows_read(), 3);
    assert_eq!(counters.cells_acknowledged(), 1);
    assert_eq!(
        counters.row_deletes_acknowledged(),
        2,
        "deleting an absent row still succeeds"
    );
    assert_eq!(counters.cells_failed() + counters.row_deletes_failed(), 0);

    let keys: Vec<Vec<u8>> = ["k1", "k2", "k3", "k4"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect();
    let rows = support::read_back_all(&client, &table, &keys)
        .await
        .unwrap();
    let survivors: Vec<&[u8]> = rows.iter().map(|r| r.rowkey.as_slice()).collect();
    assert_eq!(survivors, [b"k2".as_slice(), b"k3".as_slice()]);
    let mut json = String::new();
    flate2::read::GzDecoder::new(rows[0].cells[0].value.as_slice())
        .read_to_string(&mut json)
        .unwrap();
    assert_eq!(json, r#"{"v":2}"#);
}

#[tokio::test]
async fn a_repeated_key_is_written_per_occurrence_and_ends_as_one_cell() {
    // The source repeats `k1` across two batches with different values.
    // Both occurrences ship (two entries, two acknowledged cells); the row
    // ends as ONE cell holding the later arrival's value, because the sort
    // is stable and every cell carries the same timestamp.
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("mover_repeated_key").await;
    let shape = support::lookup_config();
    let (tx, rx) = mpsc::channel(8);
    tx.send(support::decoded(
        &support::lookup_cells(&[("k1", r#"{"v":1}"#), ("k2", "{}")]),
        &shape,
    ))
    .await
    .unwrap();
    tx.send(support::decoded(
        &support::lookup_cells(&[("k1", r#"{"v":2}"#)]),
        &shape,
    ))
    .await
    .unwrap();
    drop(tx);
    let counters = Arc::new(Counters::default());
    write_batches(
        rx,
        &client,
        &table,
        100,
        Arc::clone(&counters),
        &Arc::new(tokio::sync::Semaphore::new(100)),
    )
    .await
    .unwrap();
    assert_eq!(counters.rows_read(), 3);
    assert_eq!(counters.cells_acknowledged(), 3);
    assert_eq!(counters.cells_failed(), 0);
    assert_eq!(counters.entries(), 3);
    assert_eq!(counters.requests(), 1);

    let rows = support::read_back_all(&client, &table, &[b"k1".to_vec()])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cells.len(), 1, "one cell, not two versions");
    let mut json = String::new();
    flate2::read::GzDecoder::new(rows[0].cells[0].value.as_slice())
        .read_to_string(&mut json)
        .unwrap();
    assert_eq!(json, r#"{"v":2}"#);
}
