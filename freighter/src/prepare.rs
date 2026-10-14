//! `freighter prepare`: build the sharded source table from a `(key, json)`
//! query and measure what `plan` needs. The eight steps run in order here;
//! the cut is `boundaries`, the SQL is `queries`, both pure.

pub mod boundaries;
pub mod queries;

use crate::bqjobs::{BqJobs, QueryResult, TableInfo};
use crate::error::{Error, Result};
use crate::lookup::encode_value;
use crate::manifest::CellCompression;
use crate::source::{SourceRecord, TableRef, SOURCE_RECORD_VERSION};

use boundaries::{shard_boundaries, MAX_BUCKETS};
use queries::{BUCKET, JSON, KEY};

/// Buckets sampled for row sizes, and rows read from each. 2,000 rows from 10
/// buckets bound the sample's cost and are plenty for a mean.
const SAMPLE_BUCKETS: u32 = 10;
const SAMPLE_ROWS_PER_BUCKET: u32 = 200;

#[derive(Debug, Clone)]
pub struct PrepareConfig {
    /// A query yielding `key STRING, json STRING`.
    pub query: String,
    pub table: TableRef,
    pub billing_project: String,
    /// An upper bound on the cut; thinning may leave fewer.
    pub buckets: u32,
    /// Days until BigQuery drops the table; 0 = it never expires.
    pub expires_days: u32,
    pub null_json_deletes_row: bool,
}

/// Mean bytes one row adds to a write, by encoding.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RowSizes {
    pub gzip: f64,
    pub raw: f64,
}

/// `len(key) + len(value)` averaged over the sample, for both encodings,
/// through the mover's own encoder so the gzip figure is its output exactly.
/// A NULL json (a delete) carries only its key.
pub(crate) fn measure_row_sizes(sample: &QueryResult) -> Result<RowSizes> {
    if sample.rows.is_empty() {
        return Err(Error::BqJob(
            "the row-size sample returned no rows".to_owned(),
        ));
    }
    let (mut gzip, mut raw) = (0u64, 0u64);
    for row in &sample.rows {
        let key = row.str(0)?.len() as u64;
        match row.opt_str(1)? {
            None => {
                gzip += key;
                raw += key;
            }
            Some(json) => {
                gzip += key + encode_value(json.as_bytes(), CellCompression::Gzip)?.len() as u64;
                raw += key + json.len() as u64;
            }
        }
    }
    let n = sample.rows.len() as f64;
    Ok(RowSizes {
        gzip: gzip as f64 / n,
        raw: raw as f64 / n,
    })
}

/// `Error::BqJob` renders a status as "{code} {reason}: {message}". These
/// three on the schema probe are the user's query to fix, not a freighter
/// failure. (Carrying the reason structurally on `Error` is a follow-up.)
fn user_query_error(message: &str) -> bool {
    ["400 invalidQuery:", "404 notFound:", "403 accessDenied:"]
        .iter()
        .any(|prefix| message.starts_with(prefix))
}

/// Steps 6 to 8, on the table once it exists: its counts, a row-size sample,
/// and the largest bucket.
async fn measure(
    jobs: &BqJobs,
    cfg: &PrepareConfig,
    buckets: u32,
) -> Result<(TableInfo, RowSizes, u64)> {
    // 6. Counts.
    let info = jobs.table(&cfg.table).await?.ok_or_else(|| {
        Error::BqJob(format!("{} was created but cannot be read back", cfg.table))
    })?;
    if info.num_rows == 0 || info.num_bytes == 0 {
        return Err(Error::BqJob(format!(
            "{} was created but reports no rows or bytes",
            cfg.table
        )));
    }
    // 7. Row sizes.
    let sample = jobs
        .query(
            &cfg.billing_project,
            &queries::sample(
                &cfg.table,
                &queries::sample_buckets(buckets, SAMPLE_BUCKETS),
                SAMPLE_ROWS_PER_BUCKET,
            ),
        )
        .await?;
    let sizes = measure_row_sizes(&sample)?;
    // 8. Largest bucket.
    let largest = jobs
        .query(&cfg.billing_project, &queries::max_bucket_rows(&cfg.table))
        .await?;
    let max_rows_per_bucket = queries::decode_max_bucket_rows(&largest)?;
    Ok((info, sizes, max_rows_per_bucket))
}

/// Builds the source table and returns its record. Refuses (exit 2) before
/// any scan when the table exists or the query's columns are wrong, and
/// after the quantile scan when the query yields no rows.
pub async fn run(jobs: &BqJobs, cfg: &PrepareConfig) -> Result<SourceRecord> {
    // Before any scan: a count the CTAS would refuse must not cost a quantile pass.
    if !(1..=MAX_BUCKETS).contains(&cfg.buckets) {
        return Err(Error::Refused(format!(
            "buckets must be between 1 and {MAX_BUCKETS}; got {}",
            cfg.buckets
        )));
    }
    // 1. Checked before the quantile pass, which scans the whole target.
    if jobs.table(&cfg.table).await?.is_some() {
        return Err(Error::Refused(format!(
            "{} already exists; its boundaries also pre-split the Bigtable table, so drop it deliberately before rebuilding",
            cfg.table
        )));
    }
    // 2. Column types, for free. A query BigQuery rejects (400 invalidQuery:
    // a missing column, a syntax error) is the user's to fix: exit 2.
    let probe = match jobs
        .query(&cfg.billing_project, &queries::schema_probe(&cfg.query))
        .await
    {
        Ok(p) => p,
        Err(Error::BqJob(m)) if user_query_error(&m) => {
            return Err(Error::Refused(format!("the query is not valid: {m}")))
        }
        Err(e) => return Err(e),
    };
    queries::check_schema(&probe.schema)?;
    // 3. Quantiles.
    log::info!("prepare: scanning the query for {} quantiles", cfg.buckets);
    let quantile_result = jobs
        .query(
            &cfg.billing_project,
            &queries::quantiles(&cfg.query, cfg.buckets, cfg.null_json_deletes_row),
        )
        .await?;
    let Some(q) = queries::decode_quantiles(&quantile_result)? else {
        return Err(Error::Refused(
            "the query yields no rows to load".to_owned(),
        ));
    };
    // 4. Boundaries.
    let boundaries = shard_boundaries(&q.cuts, &q.lowest, q.rows);
    let buckets = u32::try_from(boundaries.len())
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    log::info!(
        "prepare: {} rows, {} buckets after thinning (asked for {}); {} bytes scanned",
        q.rows,
        buckets,
        cfg.buckets,
        quantile_result.total_bytes_processed
    );
    // 5. Build.
    let ctas = queries::ctas(
        &cfg.table,
        &cfg.query,
        &boundaries,
        cfg.expires_days,
        cfg.null_json_deletes_row,
    )?;
    log::info!(
        "prepare: building {} ({} buckets, expires {})",
        cfg.table,
        buckets,
        if cfg.expires_days == 0 {
            "never".to_owned()
        } else {
            format!("in {} days", cfg.expires_days)
        }
    );
    let ctas_result = jobs.query(&cfg.billing_project, &ctas).await?;
    log::info!(
        "prepare: built {} ({} bytes scanned)",
        cfg.table,
        ctas_result.total_bytes_processed
    );
    let (info, sizes, max_rows_per_bucket) =
        measure(jobs, cfg, buckets).await.map_err(|e| match e {
            Error::Refused(_) => e,
            e => Error::BqJob(format!(
                "{} was built, but measuring it failed: {e}; drop the table and rerun",
                cfg.table
            )),
        })?;
    let logical = info.num_bytes as f64 / info.num_rows as f64;
    log::info!(
        "prepare: {} rows; bytes per row logical {:.1}, gzip {:.1}, raw {:.1}; largest bucket {} rows",
        info.num_rows,
        logical,
        sizes.gzip,
        sizes.raw,
        max_rows_per_bucket
    );
    Ok(SourceRecord {
        version: SOURCE_RECORD_VERSION,
        table: cfg.table.path(),
        billing_project: cfg.billing_project.clone(),
        rowkey_column: KEY.to_owned(),
        json_column: JSON.to_owned(),
        range_column: BUCKET.to_owned(),
        boundaries,
        buckets,
        rows: info.num_rows,
        logical_bytes_per_row: logical,
        gzip_bytes_per_row: sizes.gzip,
        raw_bytes_per_row: sizes.raw,
        max_rows_per_bucket,
        null_json_deletes_row: cfg.null_json_deletes_row,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::bqjobs::{Cell, Field, Row};

    fn sample(rows: &[(&str, Option<&str>)]) -> QueryResult {
        QueryResult {
            schema: vec![
                Field {
                    name: "key".into(),
                    field_type: "STRING".into(),
                    mode: None,
                },
                Field {
                    name: "json".into(),
                    field_type: "STRING".into(),
                    mode: None,
                },
            ],
            rows: rows
                .iter()
                .map(|(k, j)| {
                    Row(vec![
                        Cell::Str((*k).into()),
                        j.map(|j| Cell::Str(j.into())).unwrap_or(Cell::Null),
                    ])
                })
                .collect(),
            total_bytes_processed: 0,
        }
    }

    #[test]
    fn row_sizes_are_the_key_plus_the_mover_s_own_encoding() {
        let padded = format!("{{\"pad\": \"{}\"}}", "x".repeat(1000));
        let sizes = measure_row_sizes(&sample(&[("", Some(&padded))])).unwrap();
        let gz = encode_value(padded.as_bytes(), CellCompression::Gzip)
            .unwrap()
            .len() as f64;
        assert_eq!(sizes.gzip, gz);
        assert!(sizes.gzip < 100.0);
        assert_eq!(sizes.raw, padded.len() as f64);
        // gzip's header and trailer make a tiny payload bigger, not smaller.
        assert!(
            measure_row_sizes(&sample(&[("", Some("{}"))]))
                .unwrap()
                .gzip
                > 18.0
        );
    }

    #[test]
    fn the_key_counts_in_utf8_bytes_and_a_delete_counts_only_its_key() {
        let padded = format!("{{\"pad\": \"{}\"}}", "x".repeat(1000));
        let with_key = measure_row_sizes(&sample(&[("p/t/é", Some(&padded))])).unwrap();
        let bare = measure_row_sizes(&sample(&[("", Some(&padded))])).unwrap();
        assert_eq!(with_key.gzip - bare.gzip, 6.0);
        assert_eq!(with_key.raw - bare.raw, 6.0);
        let delete = measure_row_sizes(&sample(&[("p/t/é", None)])).unwrap();
        assert_eq!((delete.gzip, delete.raw), (6.0, 6.0));
    }

    #[test]
    fn sizes_are_means_over_the_sample_and_an_empty_sample_is_an_error() {
        let two = measure_row_sizes(&sample(&[("ab", None), ("abcd", None)])).unwrap();
        assert_eq!((two.gzip, two.raw), (3.0, 3.0));
        assert!(measure_row_sizes(&sample(&[])).is_err());
        let mixed = measure_row_sizes(&sample(&[("ab", None), ("ab", Some("{}"))])).unwrap();
        assert_eq!(mixed.raw, 3.0);
        let encoded = encode_value(b"{}", CellCompression::Gzip).unwrap().len();
        assert_eq!(mixed.gzip, (2 + (2 + encoded)) as f64 / 2.0);
    }
}
