//! Env gates, the synthetic source, the binary runner, and cleanup.

use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use freighter::bqjobs::BqJobs;
use freighter::source::TableRef;

/// `<project>.<dataset>` the suite may write to. PANICS rather than skipping.
pub fn dataset() -> (String, String) {
    let v = std::env::var("FREIGHTER_TEST_DATASET").unwrap_or_default();
    match v.split_once('.') {
        Some((p, d)) if !p.is_empty() && !d.is_empty() => (p.to_owned(), d.to_owned()),
        _ => panic!(
            "the BigQuery suite needs FREIGHTER_TEST_DATASET=<project>.<dataset> — run scripts/test-bigquery.sh"
        ),
    }
}

pub fn require_emulator() -> String {
    match std::env::var("BIGTABLE_EMULATOR_HOST") {
        Ok(host) if !host.is_empty() => host,
        _ => {
            panic!("the BigQuery suite writes to the Bigtable emulator; set BIGTABLE_EMULATOR_HOST")
        }
    }
}

/// A table name no other run collides with.
pub fn unique_table(prefix: &str) -> TableRef {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let (project, dataset) = dataset();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{project}.{dataset}.freighter_test_{prefix}_{nanos}_{n}")
        .parse()
        .unwrap()
}

/// The synthetic target: 1,000,000 distinct keys; a third of the rows share
/// one mid-range key, so several consecutive quantile cuts land on it and
/// deduplicate to one; a NULL key and a NULL json that the filter drops.
/// Expires in a day in case cleanup never runs.
pub fn synthetic_ddl(table: &TableRef) -> String {
    format!(
        "CREATE TABLE {} OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS
SELECT CONCAT('k/', CAST(n AS STRING)) AS key,
       TO_JSON_STRING(STRUCT(n AS n, REPEAT('x', 200) AS pad)) AS json
FROM UNNEST(GENERATE_ARRAY(1, 1000000)) AS n
UNION ALL
SELECT 'k/500000x', TO_JSON_STRING(STRUCT(0 AS n, REPEAT('x', 200) AS pad))
FROM UNNEST(GENERATE_ARRAY(1, 500000))
UNION ALL SELECT CAST(NULL AS STRING), '{{}}'
UNION ALL SELECT 'k/null-json', CAST(NULL AS STRING)",
        table.sql()
    )
}

/// Connects with the TLS provider installed. The binary does this in
/// `main`; these tests call the library in-process, and reqwest panics
/// without a provider.
pub async fn jobs() -> BqJobs {
    let _ = rustls::crypto::ring::default_provider().install_default();
    BqJobs::connect().await.expect("connect to BigQuery")
}

pub async fn drop_table(jobs: &BqJobs, table: &TableRef) {
    let (project, _) = dataset();
    if let Err(e) = jobs
        .query(&project, &format!("DROP TABLE IF EXISTS {}", table.sql()))
        .await
    {
        eprintln!("cleanup: could not drop {table}: {e}");
    }
}

/// Runs the built `freighter` binary with the emulator and ADC of this process.
pub fn run_freighter(args: &[&str]) -> Output {
    let out = Command::new(env!("CARGO_BIN_EXE_freighter"))
        .args(args)
        .output()
        .expect("run freighter");
    eprintln!(
        "freighter {}:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Emulator project/instance; `cbtemulator` accepts any names.
pub const BT_PROJECT: &str = "example-project";
pub const BT_INSTANCE: &str = "example-instance";

/// Creates `table` with family `data` in the emulator via `cbt`.
pub fn create_bt_table(table: &str) {
    let output = Command::new("cbt")
        .args([
            "-project",
            BT_PROJECT,
            "-instance",
            BT_INSTANCE,
            "createtable",
            table,
            "families=data",
        ])
        .output()
        .expect("cbt must be on PATH — run: gcloud components install cbt");
    assert!(
        output.status.success(),
        "cbt createtable {table} failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
