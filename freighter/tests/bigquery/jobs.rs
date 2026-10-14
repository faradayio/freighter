//! The jobs client against the live API: paging, a missing table, the
//! schema of a LIMIT 0 probe, and a failing job.

use crate::support;

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

/// The one assumption in `bqjobs.rs` only a live server can confirm: a job
/// that fails is surfaced as an error by `jobs.query` or `getQueryResults`,
/// never as a complete page with no rows.
#[tokio::test]
async fn a_failing_query_is_an_error_not_an_empty_result() {
    let (project, _) = support::dataset();
    let jobs = support::jobs().await;
    let err = jobs
        .query(&project, "SELECT ERROR('freighter test failure') AS x")
        .await
        .expect_err("a failing query must not look like success");
    assert!(err.to_string().contains("freighter test failure"), "{err}");
    // And one that outlasts the long-poll window, so the getQueryResults
    // path (not just the first jobs.query page) is what carried the result.
    // 40M SHA-256s scan no table, so they cost nothing; 100M took 67 s, so
    // this runs well past 10 s. GENERATE_ARRAY caps its length, hence the
    // cross join; COUNT(DISTINCT) over it exceeds the query's memory, hence
    // the SUM. The nonce in the hashed value keeps a rerun out of the cache.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let r = jobs
        .query(
            &project,
            &format!(
                "SELECT SUM(LENGTH(SHA256(CONCAT('{nonce}/', CAST(a * 100000 + b AS STRING))))) AS n
FROM UNNEST(GENERATE_ARRAY(1, 4000)) AS a, UNNEST(GENERATE_ARRAY(1, 10000)) AS b"
            ),
        )
        .await
        .expect("slow query");
    // 40M digests of 32 bytes each.
    assert_eq!(r.rows[0].i64(0).unwrap(), 1_280_000_000);
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
