//! prepare -> plan -> move (emulator) -> verify, through the real binary,
//! against a synthetic 1.5M-row source. One sequential story.

use freighter::manifest::MoveManifest;
use freighter::source::SourceRecord;
use freighter::verify::VerifyReport;

use crate::support;

#[tokio::test]
async fn prepare_plan_move_verify() {
    support::require_emulator();
    let (project, _) = support::dataset();
    let jobs = support::jobs().await;
    let synthetic = support::unique_table("synthetic");
    let sharded = support::unique_table("sharded");
    eprintln!("synthetic table: {synthetic}\nsharded table: {sharded}");
    // The emulator outlives a run, so the Bigtable table is per-run too.
    let bt_table = format!("e2e_{}", std::process::id());
    let dir = std::env::temp_dir().join(format!("freighter-e2e-{}", std::process::id()));

    jobs.query(&project, &support::synthetic_ddl(&synthetic))
        .await
        .expect("synthetic table");
    let result = std::panic::AssertUnwindSafe(async {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = format!("SELECT key, json FROM {}", synthetic.sql());
        let source_path = dir.join("source.json");

        // ---- prepare ----
        let out = support::run_freighter(&[
            "prepare",
            "--query",
            &target,
            "--table",
            &sharded.to_string(),
            "--buckets",
            "16",
            "--expires-days",
            "1",
            "--out",
            source_path.to_str().unwrap(),
        ]);
        assert_eq!(out.status.code(), Some(0));
        let source =
            SourceRecord::from_json(&std::fs::read_to_string(&source_path).unwrap()).unwrap();
        assert_eq!(source.rows, 1_500_000);
        assert_eq!(source.buckets as usize, source.boundaries.len() + 1);
        // 1.5M rows clear the 50k-row floor at 16 buckets, so the cut is not
        // thinned; dedup dropped at least one of the 15 interior cuts.
        assert!((2..16).contains(&source.buckets), "{}", source.buckets);
        assert!(source.boundaries.iter().any(|b| b == "k/500000x"));
        assert!(source.logical_bytes_per_row > 200.0);
        // The synthetic json is 200 repeated characters, so it gzips well
        // below its logical size.
        assert!(
            0.0 < source.gzip_bytes_per_row
                && source.gzip_bytes_per_row < source.logical_bytes_per_row
        );
        assert!(source.raw_bytes_per_row > source.gzip_bytes_per_row);

        // A second prepare refuses the existing table.
        let again = support::run_freighter(&[
            "prepare",
            "--query",
            &target,
            "--table",
            &sharded.to_string(),
            "--buckets",
            "16",
        ]);
        assert_eq!(again.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&again.stderr).contains("already exists"));

        // The table as BigQuery describes it.
        let info = jobs
            .table(&sharded)
            .await
            .unwrap()
            .expect("sharded table exists");
        let rp = info.range_partitioning.expect("range partitioned");
        assert_eq!((rp.field.as_str(), rp.start, rp.interval), ("bucket", 0, 1));
        assert_eq!(rp.end, i64::from(source.buckets));
        assert_eq!(info.clustering, vec!["key"]);
        assert!(info.expiration_time_ms.is_some());

        // Buckets are dense, non-empty, contiguous, and start at the boundaries.
        let per_bucket = jobs
            .query(
                &project,
                &format!(
                    "SELECT bucket, COUNT(*) AS n, MIN(key) AS lo, MAX(key) AS hi FROM {} GROUP BY bucket ORDER BY bucket",
                    sharded.sql()
                ),
            )
            .await
            .unwrap();
        let buckets: Vec<i64> = per_bucket.rows.iter().map(|r| r.i64(0).unwrap()).collect();
        assert_eq!(buckets, (0..i64::from(source.buckets)).collect::<Vec<_>>());
        let counts: Vec<u64> = per_bucket
            .rows
            .iter()
            .map(|r| r.i64(1).unwrap() as u64)
            .collect();
        assert!(counts.iter().all(|&n| n > 0));
        let los: Vec<&str> = per_bucket
            .rows
            .iter()
            .skip(1)
            .map(|r| r.str(2).unwrap())
            .collect();
        assert_eq!(
            los,
            source
                .boundaries
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        for pair in per_bucket.rows.windows(2) {
            assert!(
                pair[0].str(3).unwrap() < pair[1].str(2).unwrap(),
                "buckets are contiguous and ordered"
            );
        }
        assert_eq!(source.max_rows_per_bucket, *counts.iter().max().unwrap());
        assert!(source.max_rows_per_bucket >= 5000);
        // The repeated key makes one bucket much bigger than the mean.
        assert!(source.rows.div_ceil(u64::from(source.buckets)) < source.max_rows_per_bucket);
        assert!(source.max_rows_per_bucket <= source.rows);

        // QUERY-ENGINE pruning: one bucket costs far less than the table.
        // An aggregate reads (and bills) both columns in full but returns one
        // row; a bare SELECT would page the whole table back over REST.
        let full = jobs
            .query(
                &project,
                &format!("SELECT MAX(key), MAX(LENGTH(json)) FROM {}", sharded.sql()),
            )
            .await
            .unwrap();
        let one = jobs
            .query(
                &project,
                &format!(
                    "SELECT MAX(key), MAX(LENGTH(json)) FROM {} WHERE bucket BETWEEN 0 AND 0",
                    sharded.sql()
                ),
            )
            .await
            .unwrap();
        assert!(full.total_bytes_processed > 0 && one.total_bytes_processed > 0);
        assert!(
            one.total_bytes_processed * 4 < full.total_bytes_processed,
            "one bucket {} vs full {}",
            one.total_bytes_processed,
            full.total_bytes_processed
        );

        // ---- plan ----
        let out_dir = dir.join("load");
        let out = support::run_freighter(&[
            "plan",
            "--source",
            source_path.to_str().unwrap(),
            "--lanes",
            "2",
            "--memory",
            "4Gi",
            "--bt-project",
            support::BT_PROJECT,
            "--bt-instance",
            support::BT_INSTANCE,
            "--destination-table",
            &bt_table,
            "--app-profile",
            "default",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);
        assert_eq!(out.status.code(), Some(0));
        let manifest_path = out_dir.join("manifest.json");
        let manifest =
            MoveManifest::from_json(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        assert!(manifest.shards.len() >= 2);

        // ---- move (into the emulator) ----
        support::create_bt_table(&bt_table);
        let uri = format!("file://{}", manifest_path.display());
        let out = support::run_freighter(&["move", &uri]);
        assert_eq!(out.status.code(), Some(0));

        // ---- verify ----
        let report_path = dir.join("report.json");
        let out = support::run_freighter(&["verify", &uri, "--out", report_path.to_str().unwrap()]);
        let report: VerifyReport =
            serde_json::from_str(&std::fs::read_to_string(&report_path).unwrap()).unwrap();
        assert!(report.ok, "{report:?}");
        assert_eq!(out.status.code(), Some(0));
        assert_eq!(report.shards, manifest.shards.len());

        // A stray verdict turns the report red and the exit code to 1. The
        // verdict's file name and its `shard_index` must agree, so both are
        // rewritten; the mover writes compact JSON.
        let verdict = std::fs::read_to_string(out_dir.join("shard_0000.json")).unwrap();
        assert!(verdict.contains("\"shard_index\":0"), "{verdict}");
        let stray = verdict.replace("\"shard_index\":0", "\"shard_index\":9999");
        std::fs::write(out_dir.join("shard_9999.json"), stray).unwrap();
        let out = support::run_freighter(&["verify", &uri]);
        assert_eq!(out.status.code(), Some(1));
        let report: VerifyReport = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(report.stray_verdicts, vec![9999]);
    });
    let outcome = futures::FutureExt::catch_unwind(result).await;
    support::drop_table(&jobs, &synthetic).await;
    support::drop_table(&jobs, &sharded).await;
    let _ = std::fs::remove_dir_all(&dir);
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
