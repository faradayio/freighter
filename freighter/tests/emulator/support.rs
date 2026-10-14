//! Emulator harness for the mover suite: env gate, per-test table
//! provisioning via `cbt`, and `(key, json)` source batches.

use std::process::Command;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use freighter::cells::TableConfig;
use freighter::client::{BtClient, RawRow, RowWrite};
use freighter::error::Result;
use freighter::manifest::CellCompression;
use freighter::write::{collect_sorted, write_sorted, Counters, WriteRows};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::Filter;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::RowFilter;
use tokio::sync::{mpsc, Semaphore};

/// Emulator project/instance. `cbtemulator` accepts any names; these only
/// have to agree between `cbt` and `BtClient`.
pub const PROJECT: &str = "bt-pit-emulator";
pub const INSTANCE: &str = "bt-pit-emulator";
pub const APP_PROFILE: &str = "default";

/// Asserts the suite is pointed at an emulator and returns its host:port.
///
/// PANICS rather than skipping. A Bigtable regression test that silently
/// no-ops when the emulator is absent is indistinguishable from a passing
/// test.
pub fn require_emulator() -> String {
    match std::env::var("BIGTABLE_EMULATOR_HOST") {
        Ok(host) if !host.is_empty() => host,
        _ => panic!("emulator tests require BIGTABLE_EMULATOR_HOST — run scripts/test-emulator.sh"),
    }
}

/// A table name no other test will collide with, so tests stay independent
/// even though the emulator process is shared.
fn unique_table(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{prefix}_{nanos}_{n}")
}

/// Creates `table` with the comma-separated `families` via `cbt`
/// (`bigtable_rs` ships no admin API).
fn create_table(table: &str, families: &str) {
    let families = format!("families={families}");
    let output = Command::new("cbt")
        .args([
            "-project",
            PROJECT,
            "-instance",
            INSTANCE,
            "createtable",
            table,
            &families,
        ])
        .output()
        .expect("cbt must be on PATH — run: gcloud components install cbt");
    assert!(
        output.status.success(),
        "cbt createtable {table} failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Gates on the emulator, provisions a fresh table with the one family,
/// `data`, and connects. Returns `(client, table_name)`. No GC policy
/// on purpose: with none, a row that
/// still holds one cell after a rewrite got there by timestamp identity,
/// not by garbage collection.
pub async fn fresh_lookup_table(prefix: &str) -> (BtClient, String) {
    require_emulator();
    let table = unique_table(prefix);
    create_table(&table, "data");
    let client = BtClient::connect(PROJECT, INSTANCE, APP_PROFILE, 1)
        .await
        .expect("connect to the bigtable emulator");
    (client, table)
}

/// A batch as the write loop receives it — the same transformation
/// `decode_stream` applies.
pub fn decoded(batch: &RecordBatch, shape: &TableConfig) -> Vec<RowWrite> {
    freighter::lookup::decode_lookup(batch, shape).expect("test batch decodes")
}

/// A `WriteRows` that fails its first `fail_first` calls with an injected
/// `BigtableRpc { code }` — the caller picks the code, transient or not —
/// and then delegates to the REAL emulator client: a fault injector in
/// front of the emulator, not a mock of it.
#[derive(Clone)]
pub struct FlakyWriter {
    pub inner: BtClient,
    pub fail_first: Arc<AtomicU32>,
    pub code: i32,
}

impl FlakyWriter {
    pub fn new(inner: BtClient, fail_first: u32, code: i32) -> Self {
        Self {
            inner,
            fail_first: Arc::new(AtomicU32::new(fail_first)),
            code,
        }
    }
}

impl freighter::write::WriteRows for FlakyWriter {
    fn write_rows(
        &self,
        table: &str,
        rows: Vec<freighter::client::RowWrite>,
    ) -> impl std::future::Future<
        Output = freighter::error::Result<Vec<freighter::client::EntryStatus>>,
    > + Send {
        let remaining = self.fail_first.clone();
        let code = self.code;
        let inner = self.inner.clone();
        let table = table.to_owned();
        async move {
            // A compare-exchange loop rather than fetch_update (deprecated on
            // current stable) or try_update (newer than rust-version).
            let mut n = remaining.load(Ordering::SeqCst);
            let took_a_failure = loop {
                if n == 0 {
                    break false;
                }
                match remaining.compare_exchange(n, n - 1, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(_) => break true,
                    Err(current) => n = current,
                }
            };
            if took_a_failure {
                return Err(freighter::error::Error::BigtableRpc {
                    code,
                    message: format!("injected failure code {code}"),
                });
            }
            inner.write_rows(&table, rows).await
        }
    }
}

/// The cell timestamp [`lookup_config`] declares: 2026-09-17T18:30:00.123Z,
/// whole milliseconds as the manifest validator requires.
pub const LOOKUP_TIMESTAMP_MICROS: i64 = 1_789_669_800_123_000;

/// The table config: rowkey column `key`, family `data`, every cell at
/// [`LOOKUP_TIMESTAMP_MICROS`].
pub fn lookup_config() -> TableConfig {
    TableConfig {
        rowkey_column: "key".into(),
        family: "data".into(),
        qualifier: b"jsonz".to_vec(),
        compression: CellCompression::Gzip,
        cell_timestamp_micros: LOOKUP_TIMESTAMP_MICROS,
        null_json_deletes_row: false,
    }
}

/// [`lookup_config`], where a NULL json deletes its row.
pub fn deleting_config() -> TableConfig {
    TableConfig {
        null_json_deletes_row: true,
        ..lookup_config()
    }
}

/// A lookup source batch: one BQ row per `(key, json)` pair.
pub fn lookup_cells(rows: &[(&str, &str)]) -> RecordBatch {
    lookup_changes(&rows.iter().map(|(k, j)| (*k, Some(*j))).collect::<Vec<_>>())
}

/// A lookup source batch where a `None` json marks a row to delete.
pub fn lookup_changes(rows: &[(&str, Option<&str>)]) -> RecordBatch {
    let mut key = StringBuilder::new();
    let mut json = StringBuilder::new();
    for (k, j) in rows {
        key.append_value(*k);
        json.append_option(*j);
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("json", DataType::Utf8, true),
        ])),
        vec![Arc::new(key.finish()), Arc::new(json.finish())],
    )
    .expect("lookup batch is valid")
}

/// [`lookup_cells`] for tests that only care about the keys: every json is
/// `{}`.
pub fn lookup_keys(keys: &[&str]) -> RecordBatch {
    lookup_cells(&keys.iter().map(|k| (*k, "{}")).collect::<Vec<_>>())
}

/// Every cell of `rowkeys`, every family and version, unfiltered — so a
/// one-cell-per-row assertion is a real one.
pub async fn read_back_all(
    client: &BtClient,
    table: &str,
    rowkeys: &[Vec<u8>],
) -> freighter::error::Result<Vec<RawRow>> {
    let mut rows = Vec::new();
    let pass_all = RowFilter {
        filter: Some(Filter::PassAllFilter(true)),
    };
    client
        .read_rows(table, rowkeys, pass_all, |row| {
            rows.push(row);
            Ok(())
        })
        .await?;
    Ok(rows)
}

/// The shard's write path over a channel of decoded rows, as the mover
/// runs it minus the read session: collect and sort, then write.
pub async fn write_batches<W: WriteRows>(
    rx: mpsc::Receiver<Vec<RowWrite>>,
    bt: &W,
    dest: &str,
    flush_cells: usize,
    counters: Arc<Counters>,
    sem: &Arc<Semaphore>,
) -> Result<()> {
    let all = collect_sorted(rx, &counters).await?;
    write_sorted(all, bt, dest, flush_cells, counters, sem).await
}
