//! `freighter verify`: the fence. For each shard in a manifest, compare the
//! cells and row deletes the mover says Bigtable ACKNOWLEDGED with one
//! grouped count of SOURCE rows over the shard's buckets. A BUCKET is one
//! value of the source table's range column; a SHARD is the manifest's
//! unit, a run of consecutive buckets. It never reads Bigtable: counting
//! the destination is a full scan against live reads.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};

use crate::bqjobs::BqJobs;
use crate::error::{Error, Result};
use crate::manifest::{MoveManifest, Outcome, ShardRange, ShardSpec, ShardVerdict};
use crate::source::TableRef;

pub const VERIFY_REPORT_VERSION: u32 = 1;

/// The published result of `verify`. `ok` is "no problems and no strays";
/// the exit code follows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyReport {
    pub version: u32,
    pub ok: bool,
    /// Shards in the manifest.
    pub shards: usize,
    pub problems: Vec<Problem>,
    /// Verdict files for shard indexes the manifest does not contain:
    /// another attempt's or another step's files in the same directory.
    pub stray_verdicts: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Problem {
    pub shard: u32,
    pub reason: String,
}

/// One source bucket's rows: those whose json the mover sets as one cell,
/// and those whose NULL json makes the mover delete the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BucketRows {
    pub sets: u64,
    pub deletes: u64,
}

/// `shard_<digits>.json` -> the index. Anything else beside the manifest
/// (the manifest itself, a step directory) is not a verdict.
pub fn verdict_index(file_name: &str) -> Option<u32> {
    let digits = file_name.strip_prefix("shard_")?.strip_suffix(".json")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The grouped count over the shards' bucket span. `range_column` is user
/// input from the manifest on its first trip into SQL: backtick-quoted, and
/// a name containing a backtick is refused.
pub fn count_sql(table: &TableRef, range_column: &str, lo: i64, hi: i64) -> Result<String> {
    if range_column.contains('`') || range_column.is_empty() {
        return Err(Error::Refused(format!(
            "range_column {range_column:?} is not a plain column name"
        )));
    }
    Ok(format!(
        "SELECT `{range_column}` AS bucket,\n       COUNTIF(json IS NOT NULL) AS set_count,\n       COUNTIF(json IS NULL) AS delete_count\nFROM {}\nWHERE `{range_column}` BETWEEN {lo} AND {hi}\nGROUP BY bucket\n",
        table.sql()
    ))
}

/// One problem per way the verdicts disagree with the source's rows per
/// bucket, in manifest order; stray verdict indexes sorted.
pub fn count_problems(
    shards: &[ShardSpec],
    per_bucket: &HashMap<i64, BucketRows>,
    verdicts: &[ShardVerdict],
) -> VerifyReport {
    let by_index: HashMap<u32, &ShardVerdict> =
        verdicts.iter().map(|v| (v.shard_index, v)).collect();
    let mut problems = Vec::new();
    for shard in shards {
        let ShardRange::Int { lo, hi } = shard.range;
        let (expected_sets, expected_deletes) = (lo..=hi)
            .map(|b| per_bucket.get(&b).copied().unwrap_or_default())
            .fold((0u64, 0u64), |(s, d), b| (s + b.sets, d + b.deletes));
        let mut problem = |reason: String| {
            problems.push(Problem {
                shard: shard.index,
                reason,
            })
        };
        match by_index.get(&shard.index) {
            None => problem("no verdict".to_owned()),
            Some(v)
                if !matches!(v.outcome, Outcome::Complete)
                    || v.cells_failed != 0
                    || v.row_deletes_failed != 0 =>
            {
                problem(format!(
                    "outcome {}, {} cells and {} row deletes failed",
                    v.outcome, v.cells_failed, v.row_deletes_failed
                ));
            }
            Some(v) => {
                if v.cells_acknowledged != expected_sets {
                    problem(format!(
                        "acknowledged {} cells, source has {expected_sets} rows to set",
                        v.cells_acknowledged
                    ));
                }
                if v.row_deletes_acknowledged != expected_deletes {
                    problem(format!(
                        "acknowledged {} row deletes, source has {expected_deletes} rows to delete",
                        v.row_deletes_acknowledged
                    ));
                }
            }
        }
    }
    let wanted: BTreeSet<u32> = shards.iter().map(|s| s.index).collect();
    let stray_verdicts: Vec<u32> = by_index
        .keys()
        .filter(|i| !wanted.contains(i))
        .copied()
        .collect::<BTreeSet<u32>>()
        .into_iter()
        .collect();
    VerifyReport {
        version: VERIFY_REPORT_VERSION,
        ok: problems.is_empty() && stray_verdicts.is_empty(),
        shards: shards.len(),
        problems,
        stray_verdicts,
    }
}

/// Reads the verdicts beside the manifest (the store is rooted at the
/// manifest's directory by `main`; the listing is non-recursive so a
/// `file://` manifest in `/tmp` never walks `/tmp` and a ramp's `step-N/`
/// directories are never swept in), runs one count, and judges.
pub async fn run(
    m: &MoveManifest,
    store: Arc<dyn ObjectStore>,
    jobs: &BqJobs,
    billing_project: &str,
) -> Result<VerifyReport> {
    let listing = store
        .list_with_delimiter(None)
        .await
        .map_err(|e| Error::Storage(format!("list verdicts beside the manifest: {e}")))?;
    let mut verdicts = Vec::new();
    for meta in listing.objects {
        let Some(name) = meta.location.filename() else {
            continue;
        };
        if verdict_index(name).is_none() {
            continue;
        }
        let bytes = store
            .get(&meta.location)
            .await
            .map_err(|e| Error::Storage(format!("read {}: {e}", meta.location)))?
            .bytes()
            .await
            .map_err(|e| Error::Storage(format!("read {}: {e}", meta.location)))?;
        let v: ShardVerdict = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Storage(format!("verdict {}: {e}", meta.location)))?;
        verdicts.push(v);
    }
    let table = TableRef::from_path(&m.source_table)
        .map_err(|e| Error::Manifest(format!("source_table: {e}")))?;
    let (lo, hi) = m.shards.iter().fold((i64::MAX, i64::MIN), |(lo, hi), s| {
        let ShardRange::Int { lo: l, hi: h } = s.range;
        (lo.min(l), hi.max(h))
    });
    let sql = count_sql(&table, &m.range_column, lo, hi)?;
    let counted = jobs.query(billing_project, &sql).await?;
    let mut per_bucket = HashMap::with_capacity(counted.rows.len());
    for row in &counted.rows {
        let bucket = row.i64(0)?;
        let sets = u64::try_from(row.i64(1)?)
            .map_err(|_| Error::BqJob("negative set_count".to_owned()))?;
        let deletes = u64::try_from(row.i64(2)?)
            .map_err(|_| Error::BqJob("negative delete_count".to_owned()))?;
        per_bucket.insert(bucket, BucketRows { sets, deletes });
    }
    log::info!(
        "verify: {} verdicts beside the manifest, {} buckets counted over [{lo}, {hi}]",
        verdicts.len(),
        per_bucket.len()
    );
    Ok(count_problems(&m.shards, &per_bucket, &verdicts))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::manifest::{Outcome, ShardRange};

    fn verdict(
        index: u32,
        cells: u64,
        deletes: u64,
        outcome: Outcome,
        failed: u64,
        deletes_failed: u64,
    ) -> ShardVerdict {
        ShardVerdict {
            manifest_version: 4,
            shard_index: index,
            rows_read: cells + deletes,
            cells_encoded: cells,
            cells_acknowledged: cells,
            cells_failed: failed,
            row_deletes_acknowledged: deletes,
            row_deletes_failed: deletes_failed,
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

    fn complete(index: u32, cells: u64, deletes: u64) -> ShardVerdict {
        verdict(index, cells, deletes, Outcome::Complete, 0, 0)
    }

    fn shards() -> Vec<ShardSpec> {
        vec![
            ShardSpec {
                index: 4,
                range: ShardRange::Int { lo: 10, hi: 11 },
                expect_empty: false,
                lane: 0,
            },
            ShardSpec {
                index: 5,
                range: ShardRange::Int { lo: 12, hi: 12 },
                expect_empty: false,
                lane: 1,
            },
        ]
    }

    fn per_bucket() -> HashMap<i64, BucketRows> {
        HashMap::from([
            (
                10,
                BucketRows {
                    sets: 100,
                    deletes: 0,
                },
            ),
            (
                11,
                BucketRows {
                    sets: 50,
                    deletes: 3,
                },
            ),
            (
                12,
                BucketRows {
                    sets: 70,
                    deletes: 0,
                },
            ),
        ])
    }

    fn problems(verdicts: &[ShardVerdict]) -> Vec<String> {
        count_problems(&shards(), &per_bucket(), verdicts)
            .problems
            .into_iter()
            .map(|p| format!("shard {}: {}", p.shard, p.reason))
            .collect()
    }

    #[test]
    fn matching_verdicts_are_no_problem() {
        let report = count_problems(
            &shards(),
            &per_bucket(),
            &[complete(4, 150, 3), complete(5, 70, 0)],
        );
        assert!(report.ok);
        assert_eq!(report.shards, 2);
        assert!(report.problems.is_empty() && report.stray_verdicts.is_empty());
    }

    #[test]
    fn a_shard_without_a_verdict_is_a_problem() {
        assert_eq!(problems(&[complete(4, 150, 3)]), ["shard 5: no verdict"]);
    }

    #[test]
    fn a_shard_that_did_not_complete_is_a_problem() {
        let failed = verdict(5, 70, 0, Outcome::Failed, 0, 0);
        assert_eq!(
            problems(&[complete(4, 150, 3), failed]),
            ["shard 5: outcome failed, 0 cells and 0 row deletes failed"]
        );
    }

    #[test]
    fn a_shard_with_failed_cells_is_a_problem() {
        let lossy = verdict(5, 69, 0, Outcome::Complete, 1, 0);
        assert_eq!(
            problems(&[complete(4, 150, 3), lossy]),
            ["shard 5: outcome complete, 1 cells and 0 row deletes failed"]
        );
    }

    #[test]
    fn a_shard_with_failed_row_deletes_is_a_problem() {
        let lossy = verdict(4, 150, 2, Outcome::Complete, 0, 1);
        assert_eq!(
            problems(&[lossy, complete(5, 70, 0)]),
            ["shard 4: outcome complete, 0 cells and 1 row deletes failed"]
        );
    }

    #[test]
    fn acknowledged_cells_that_differ_from_the_source_rows_are_a_problem() {
        assert_eq!(
            problems(&[complete(4, 149, 3), complete(5, 70, 0)]),
            ["shard 4: acknowledged 149 cells, source has 150 rows to set"]
        );
    }

    #[test]
    fn acknowledged_row_deletes_that_differ_from_the_source_rows_are_a_problem() {
        assert_eq!(
            problems(&[complete(4, 150, 2), complete(5, 70, 0)]),
            ["shard 4: acknowledged 2 row deletes, source has 3 rows to delete"]
        );
    }

    #[test]
    fn a_verdict_for_a_shard_not_in_the_manifest_is_a_problem() {
        let report = count_problems(
            &shards(),
            &per_bucket(),
            &[
                complete(4, 150, 3),
                complete(5, 70, 0),
                complete(9, 1, 0),
                complete(7, 1, 0),
            ],
        );
        assert!(!report.ok);
        assert!(report.problems.is_empty());
        assert_eq!(report.stray_verdicts, [7, 9]);
    }

    #[test]
    fn verdict_files_are_recognised_by_name() {
        assert_eq!(verdict_index("shard_0004.json"), Some(4));
        assert_eq!(verdict_index("shard_12345.json"), Some(12345));
        assert_eq!(verdict_index("manifest.json"), None);
        assert_eq!(verdict_index("shard_.json"), None);
        assert_eq!(verdict_index("shard_0004.json.bak"), None);
        assert_eq!(verdict_index("shard_x.json"), None);
    }

    #[test]
    fn the_count_query_quotes_the_range_column_and_refuses_a_backtick() {
        let t: TableRef = "example-project.d.t".parse().unwrap();
        let sql = count_sql(&t, "bucket", 0, 53).unwrap();
        assert!(sql.contains("SELECT `bucket` AS bucket"));
        assert!(sql.contains("COUNTIF(json IS NOT NULL) AS set_count"));
        assert!(sql.contains("COUNTIF(json IS NULL) AS delete_count"));
        assert!(sql.contains("FROM `example-project`.`d`.`t`"));
        assert!(sql.contains("WHERE `bucket` BETWEEN 0 AND 53"));
        assert!(sql.contains("GROUP BY bucket"));
        assert!(count_sql(&t, "b`ucket", 0, 1).is_err());
    }
}
