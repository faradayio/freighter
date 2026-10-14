//! `write_rows` regression: per-entry statuses, batch round-trip, the
//! 100k-mutations guard.

#![allow(clippy::unwrap_used)] // tests; same allowance as main.rs

use freighter::client::{CellWrite, EntryStatus, RowChange, RowWrite};

use crate::support;

fn row(key: &[u8], n_cells: usize) -> RowWrite {
    RowWrite {
        rowkey: key.to_vec(),
        change: RowChange::Set(cells(n_cells)),
    }
}

fn cells(n_cells: usize) -> Vec<CellWrite> {
    (0..n_cells)
        .map(|i| CellWrite {
            family: "data".to_owned(),
            qualifier: format!("q{i:04}").into_bytes(),
            timestamp_micros: 1_700_000_000_000_000,
            value: vec![1, 0, 0, 0, 0, 0, 0, 0, i as u8],
        })
        .collect()
}

#[tokio::test]
async fn writes_batch_and_reports_per_entry_ok() {
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("write_rows_ok").await;
    let statuses = client
        .write_rows(&table, vec![row(b"aaaa", 3), row(b"bbbb", 2)])
        .await
        .expect("write_rows");
    assert_eq!(statuses.len(), 2);
    assert!(statuses.iter().all(|s| matches!(s, EntryStatus::Ok)));
    // Round-trip: both rows readable with all cells.
    let rows = support::read_back_all(&client, &table, &[b"aaaa".to_vec(), b"bbbb".to_vec()])
        .await
        .expect("read back");
    assert_eq!(rows.len(), 2);
    // read_rows returns Bigtable's lexicographic row-key order, not
    // request order — assert the keys so that reliance stays visible.
    assert_eq!(rows[0].rowkey, b"aaaa");
    assert_eq!(rows[1].rowkey, b"bbbb");
    assert_eq!(rows[0].cells.len(), 3);
    assert_eq!(rows[1].cells.len(), 2);
}

#[tokio::test]
async fn a_bad_entry_fails_alone_while_the_rest_land() {
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("write_rows_mixed").await;
    // Middle row targets a column family the table does not have; the
    // RPC itself should succeed with per-entry statuses.
    let mut bad_cells = cells(2);
    for cell in &mut bad_cells {
        cell.family = "nope".to_owned();
    }
    let bad = RowWrite {
        rowkey: b"bbbb".to_vec(),
        change: RowChange::Set(bad_cells),
    };
    let statuses = client
        .write_rows(&table, vec![row(b"aaaa", 1), bad, row(b"cccc", 1)])
        .await
        .expect("write_rows RPC itself succeeds; failure is per-entry");
    assert_eq!(statuses.len(), 3);
    assert_eq!(statuses[0], EntryStatus::Ok);
    assert!(
        matches!(&statuses[1], EntryStatus::Failed { code, .. } if *code != 0),
        "bad-family entry must fail with a nonzero code: {:?}",
        statuses[1]
    );
    assert_eq!(statuses[2], EntryStatus::Ok);
    // The Ok entries actually landed; the failed one wrote nothing.
    let rows = support::read_back_all(
        &client,
        &table,
        &[b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()],
    )
    .await
    .expect("read back");
    assert_eq!(rows.len(), 2, "only the two Ok rows exist");
    assert_eq!(rows[0].rowkey, b"aaaa");
    assert_eq!(rows[1].rowkey, b"cccc");
}

#[tokio::test]
async fn refuses_over_100k_mutations() {
    support::require_emulator();
    let (client, table) = support::fresh_lookup_table("write_rows_guard").await;
    // 34 rows x 3000 cells = 102,000 mutations > 100,000.
    let rows: Vec<RowWrite> = (0..34)
        .map(|i| row(format!("k{i:04}").as_bytes(), 3000))
        .collect();
    let err = client.write_rows(&table, rows).await.unwrap_err();
    assert!(
        err.to_string().contains("100000"),
        "guard names the limit: {err}"
    );
}
