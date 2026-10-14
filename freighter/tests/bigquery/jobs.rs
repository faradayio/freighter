//! The jobs client against the live API, five tests:
//! - a result over 10 MB is paged and reassembled;
//! - a query that fails at once is an error, not an empty result;
//! - a query that outlasts the 10 s long-poll is polled to completion;
//! - a query that fails after the long-poll is still an error;
//! - a missing table is `None`, and a LIMIT 0 probe reports column types.

use std::time::{Duration, Instant};

use crate::support;

/// The long-poll window `bqjobs.rs` asks `jobs.query` to wait.
const LONG_POLL: Duration = Duration::from_secs(10);

/// 100M SHA-256s over a cross join of generated arrays: no table is
/// scanned, so it costs nothing. Slot speed varies run to run: 100M took
/// 67 s once, and 40M once took under 9 s, short of the long-poll.
/// GENERATE_ARRAY caps its length, hence the cross join; COUNT(DISTINCT)
/// over it exceeds the query's memory, hence the SUM. The nonce in the
/// hashed value keeps a rerun out of the query cache. The sum is 100M
/// digests of 32 bytes each: 3,200,000,000.
fn slow_sum_sql(select: impl Fn(&str) -> String) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sum = format!("SUM(LENGTH(SHA256(CONCAT('{nonce}/', CAST(a * 100000 + b AS STRING)))))");
    format!(
        "SELECT {} AS n
FROM UNNEST(GENERATE_ARRAY(1, 10000)) AS a, UNNEST(GENERATE_ARRAY(1, 10000)) AS b",
        select(&sum)
    )
}

#[tokio::test]
async fn a_result_over_ten_megabytes_is_paged_and_reassembled() {
    let (project, _) = support::dataset();
    let jobs = support::jobs().await;
    // 150 rows x 100 KB = 15 MB: over the 10 MB response cap, so the first
    // page carries a pageToken.
    let r = jobs
        .query(
            &project,
            "SELECT REPEAT('x', 100000) AS s FROM UNNEST(GENERATE_ARRAY(1, 150))",
        )
        .await
        .expect("query");
    assert_eq!(r.rows.len(), 150);
    assert_eq!(r.rows[149].str(0).unwrap().len(), 100_000);
    assert_eq!(r.schema[0].name, "s");
}

#[tokio::test]
async fn a_failing_query_is_an_error_not_an_empty_result() {
    let (project, _) = support::dataset();
    let jobs = support::jobs().await;
    let err = jobs
        .query(&project, "SELECT ERROR('freighter test failure') AS x")
        .await
        .expect_err("a failing query must not look like success");
    assert!(err.to_string().contains("freighter test failure"), "{err}");
}

/// The first `jobs.query` page comes back incomplete, so `getQueryResults`
/// is what carries the result.
#[tokio::test]
async fn a_query_outlasting_the_long_poll_is_polled_to_completion() {
    let (project, _) = support::dataset();
    let jobs = support::jobs().await;
    let started = Instant::now();
    let r = jobs
        .query(&project, &slow_sum_sql(str::to_owned))
        .await
        .expect("slow query");
    let elapsed = started.elapsed();
    assert!(
        elapsed > LONG_POLL,
        "finished in {elapsed:?}: no longer outlasts the long-poll; raise the row count"
    );
    assert_eq!(r.rows[0].i64(0).unwrap(), 3_200_000_000);
}

/// The one assumption in `bqjobs.rs` only a live server can confirm: a job
/// that fails after the first long-poll surfaces as an error from
/// `getQueryResults`, never as a complete page with no rows.
#[tokio::test]
async fn a_failing_query_that_outlasts_the_long_poll_is_still_an_error() {
    let (project, _) = support::dataset();
    let jobs = support::jobs().await;
    let started = Instant::now();
    let err = jobs
        .query(
            &project,
            &slow_sum_sql(|sum| format!("IF({sum} > 0, ERROR('freighter slow failure'), 0)")),
        )
        .await
        .expect_err("a job failing after the long-poll must not look like success");
    let elapsed = started.elapsed();
    assert!(err.to_string().contains("freighter slow failure"), "{err}");
    assert!(
        elapsed > LONG_POLL,
        "failed in {elapsed:?}: no longer outlasts the long-poll; raise the row count"
    );
}

#[tokio::test]
async fn a_missing_table_is_none_and_a_probe_reports_types() {
    let (project, _) = support::dataset();
    let jobs = support::jobs().await;
    let missing = support::unique_table("missing");
    assert_eq!(jobs.table(&missing).await.expect("tables.get"), None);
    let r = jobs
        .query(&project, "SELECT 'a' AS key, '{}' AS json, 1 AS n LIMIT 0")
        .await
        .expect("probe");
    assert!(r.rows.is_empty());
    let types: Vec<(&str, &str)> = r
        .schema
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.as_str()))
        .collect();
    assert_eq!(
        types,
        [("key", "STRING"), ("json", "STRING"), ("n", "INTEGER")]
    );
}
