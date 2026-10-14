//! The SQL `prepare` runs, each builder beside the decoder of its result so
//! a query's column order and the code reading it stay together. The SQL
//! shapes here are the ones run in production at Faraday. Pure: no I/O.

use crate::bqjobs::{Field, QueryResult};
use crate::cells::JSON_COLUMN;
use crate::error::{Error, Result};
use crate::source::TableRef;

use super::boundaries::MAX_BUCKETS;

/// BigQuery's limit is 1,024K characters of unresolved query; 1,000,000 bytes
/// is deliberately under it.
const MAX_QUERY_BYTES: usize = 1_000_000;

pub const KEY: &str = "key";
pub const BUCKET: &str = "bucket";

/// `boundaries.len() + 1`: the buckets a cut makes.
pub(crate) fn bucket_count(boundaries: &[String]) -> u32 {
    u32::try_from(boundaries.len())
        .unwrap_or(u32::MAX)
        .saturating_add(1)
}

/// A GoogleSQL single-quoted string literal.
pub fn string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str(r"\\"),
            '\'' => out.push_str(r"\'"),
            '\n' => out.push_str(r"\n"),
            '\r' => out.push_str(r"\r"),
            '\t' => out.push_str(r"\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// With `null_json_deletes_row` a NULL json is a row delete and must be kept.
pub fn row_filter(null_json_deletes_row: bool) -> String {
    if null_json_deletes_row {
        format!("WHERE {KEY} IS NOT NULL")
    } else {
        format!("WHERE {KEY} IS NOT NULL AND {JSON_COLUMN} IS NOT NULL")
    }
}

/// `LIMIT 0` bills nothing and returns the column types.
pub fn schema_probe(target_sql: &str) -> String {
    format!("SELECT {KEY}, {JSON_COLUMN} FROM (\n{target_sql}\n) LIMIT 0")
}

/// Both columns present and `STRING`. A native `JSON` column or an `INT64`
/// key would pass `prepare` and `plan` and fail every shard at decode.
pub fn check_schema(schema: &[Field]) -> Result<()> {
    for name in [KEY, JSON_COLUMN] {
        match schema.iter().find(|f| f.name == name) {
            None => {
                return Err(Error::Refused(format!(
                    "the query has no {name} column; freighter needs `{KEY} STRING, {JSON_COLUMN} STRING`"
                )))
            }
            Some(f) if f.mode.as_deref() == Some("REPEATED") => {
                return Err(Error::Refused(format!(
                    "column {name} is an ARRAY<{}>; freighter needs a scalar STRING",
                    f.field_type
                )))
            }
            Some(f) if f.field_type != "STRING" => {
                return Err(Error::Refused(format!(
                "column {name} is {}, not STRING; cast it in the query (TO_JSON_STRING for JSON)",
                f.field_type
            )))
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// `APPROX_QUANTILES(key, n)` yields n+1 cuts including the min and max;
/// `MIN(key)` is exact and is NULL when the filtered target is empty, which
/// is how emptiness is detected.
pub fn quantiles(target_sql: &str, buckets: u32, null_json_deletes_row: bool) -> String {
    format!(
        "SELECT APPROX_QUANTILES({KEY}, {buckets}) AS cuts, MIN({KEY}) AS lowest,\n       COUNT(*) AS row_count\nFROM (\n{target_sql}\n)\n{}\n",
        row_filter(null_json_deletes_row)
    )
}

#[derive(Debug, PartialEq, Eq)]
pub struct Quantiles {
    pub cuts: Vec<String>,
    pub lowest: String,
    pub rows: u64,
}

/// `None` when the filtered target has no rows (`lowest` is NULL).
pub fn decode_quantiles(r: &QueryResult) -> Result<Option<Quantiles>> {
    let row = r
        .rows
        .first()
        .ok_or_else(|| Error::BqJob("quantile query returned no row".to_owned()))?;
    let Some(lowest) = row.opt_str(1)? else {
        return Ok(None);
    };
    Ok(Some(Quantiles {
        cuts: row.strs(0)?.into_iter().map(str::to_owned).collect(),
        lowest: lowest.to_owned(),
        rows: u64::try_from(row.i64(2)?)
            .map_err(|_| Error::BqJob("negative row_count".to_owned()))?,
    }))
}

/// The CREATE TABLE AS SELECT; there are `boundaries.len() + 1` buckets and
/// `expires_days == 0` means no expiration.
///
/// `CREATE TABLE`, never `OR REPLACE`: `prepare` refuses an existing table
/// before the scan, and a rebuild is the user's deliberate drop.
///
/// Three orderings have to agree for a bucket to be one contiguous run of
/// row keys: BigQuery compares STRINGs by Unicode code point, the mover
/// sorts rows by the key's UTF-8 bytes, and Bigtable orders row keys by
/// bytes. Code-point order and UTF-8 byte order are the same for all valid
/// Unicode, so they do. A collation, LOWER or NORMALIZE on `key` here would
/// break that silently: the load would still succeed, writing out of order.
pub fn ctas(
    table: &TableRef,
    target_sql: &str,
    boundaries: &[String],
    expires_days: u32,
    null_json_deletes_row: bool,
) -> Result<String> {
    let buckets = bucket_count(boundaries);
    if buckets > MAX_BUCKETS {
        return Err(Error::Refused(format!(
            "{buckets} buckets exceeds the {MAX_BUCKETS} partitions one CREATE TABLE AS job may write"
        )));
    }
    let literals = boundaries
        .iter()
        .map(|b| string_literal(b))
        .collect::<Vec<_>>()
        .join(", ");
    let options = if expires_days == 0 {
        String::new()
    } else {
        format!(
            "OPTIONS (\n  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {expires_days} DAY)\n)\n"
        )
    };
    let sql = format!(
        "CREATE TABLE {}\nPARTITION BY RANGE_BUCKET({BUCKET}, GENERATE_ARRAY(0, {buckets}, 1))\nCLUSTER BY {KEY}\n{options}AS\nSELECT {KEY}, {JSON_COLUMN}, RANGE_BUCKET({KEY}, ARRAY<STRING>[{literals}]) AS {BUCKET}\nFROM (\n{target_sql}\n)\n{}\n",
        table.sql(),
        row_filter(null_json_deletes_row)
    );
    if sql.len() > MAX_QUERY_BYTES {
        return Err(Error::Refused(format!(
            "the CREATE TABLE statement is {} bytes, over BigQuery's 1 MB query limit: the boundary keys are long; reduce --buckets",
            sql.len()
        )));
    }
    Ok(sql)
}

/// Up to `samples` bucket numbers spread evenly over `0..buckets`, always
/// including bucket 0.
pub fn sample_buckets(buckets: u32, samples: u32) -> Vec<u32> {
    let mut out: Vec<u32> = (0..samples)
        .map(|i| (u64::from(i) * u64::from(buckets) / u64::from(samples)) as u32)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// One `LIMIT` per bucket so each arm prunes to one partition; its rows are
/// measured in `prepare.rs`, beside the size arithmetic.
pub fn sample(table: &TableRef, buckets: &[u32], rows_per_bucket: u32) -> String {
    buckets
        .iter()
        .map(|b| {
            format!(
                "(SELECT {KEY}, {JSON_COLUMN} FROM {} WHERE {BUCKET} = {b} LIMIT {rows_per_bucket})",
                table.sql()
            )
        })
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n")
}

/// The largest bucket, not the mean, sizes a shard.
pub fn max_bucket_rows(table: &TableRef) -> String {
    format!(
        "SELECT MAX(n) AS max_rows FROM (SELECT COUNT(*) AS n FROM {} GROUP BY {BUCKET})",
        table.sql()
    )
}

pub fn decode_max_bucket_rows(r: &QueryResult) -> Result<u64> {
    let row = r
        .rows
        .first()
        .ok_or_else(|| Error::BqJob("max-bucket query returned no row".to_owned()))?;
    u64::try_from(row.i64(0)?).map_err(|_| Error::BqJob("negative max_rows".to_owned()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::bqjobs::{Cell, Field, QueryResult, Row};

    /// Two arms, so a builder that mangled the subquery would show.
    const TARGET: &str = "SELECT key, json FROM `example-project`.`d`.`t`\nUNION ALL\nSELECT key, json FROM `example-project`.`d`.`u`";

    fn table() -> TableRef {
        "example-project.scratch.lookup_src".parse().unwrap()
    }

    fn result(schema: &[(&str, &str)], rows: Vec<Row>) -> QueryResult {
        QueryResult {
            schema: schema
                .iter()
                .map(|(n, t)| Field {
                    name: (*n).into(),
                    field_type: (*t).into(),
                    mode: None,
                })
                .collect(),
            rows,
            total_bytes_processed: 0,
        }
    }

    #[test]
    fn string_literals_escape_quotes_backslashes_and_newlines() {
        assert_eq!(string_literal("p/t/it's"), r"'p/t/it\'s'");
        assert_eq!(string_literal(r"a\b"), r"'a\\b'");
        assert_eq!(string_literal("a\nb"), r"'a\nb'");
        assert_eq!(string_literal("a\rb"), r"'a\rb'");
        assert_eq!(string_literal("a\tb"), r"'a\tb'");
        assert_eq!(string_literal("a\u{0}b"), r"'a\u0000b'");
        assert_eq!(string_literal(""), "''");
    }

    #[test]
    fn the_schema_probe_bills_nothing_and_checks_both_columns() {
        assert_eq!(
            schema_probe(TARGET),
            format!("SELECT key, json FROM (\n{TARGET}\n) LIMIT 0")
        );
        let ok = result(&[("key", "STRING"), ("json", "STRING")], vec![]);
        check_schema(&ok.schema).unwrap();
        let json_typed = result(&[("key", "STRING"), ("json", "JSON")], vec![]);
        let err = check_schema(&json_typed.schema).unwrap_err();
        assert!(
            err.to_string().contains("json") && err.to_string().contains("JSON"),
            "{err}"
        );
        let int_key = result(&[("key", "INTEGER"), ("json", "STRING")], vec![]);
        assert!(check_schema(&int_key.schema).is_err());
        let mut array_key = result(&[("key", "STRING"), ("json", "STRING")], vec![]);
        array_key.schema[0].mode = Some("REPEATED".into());
        let err = check_schema(&array_key.schema).unwrap_err();
        assert!(err.to_string().contains("ARRAY"), "{err}");
        let missing = result(&[("key", "STRING")], vec![]);
        let err = check_schema(&missing.schema).unwrap_err();
        assert!(err.to_string().contains("json"), "{err}");
        assert!(matches!(err, Error::Refused(_)));
    }

    #[test]
    fn quantiles_cut_over_the_filtered_target() {
        assert_eq!(
            quantiles("SELECT key, json FROM `p`.`d`.`t`", 64, false),
            "SELECT APPROX_QUANTILES(key, 64) AS cuts, MIN(key) AS lowest,\n       COUNT(*) AS row_count\nFROM (\nSELECT key, json FROM `p`.`d`.`t`\n)\nWHERE key IS NOT NULL AND json IS NOT NULL\n"
        );
        let sql = quantiles(TARGET, 64, false);
        assert!(sql.contains("APPROX_QUANTILES(key, 64) AS cuts"));
        assert!(sql.contains("MIN(key) AS lowest"));
        assert!(sql.contains("COUNT(*) AS row_count"));
        assert!(sql.contains(&format!("FROM (\n{TARGET}\n)")));
        assert!(sql.contains("WHERE key IS NOT NULL AND json IS NOT NULL"));
        let deleting = quantiles(TARGET, 64, true);
        assert!(deleting.contains("WHERE key IS NOT NULL\n"));
        assert!(!deleting.contains("json IS NOT NULL"));
    }

    #[test]
    fn a_target_ending_in_a_comment_cannot_swallow_the_closing_paren() {
        let commented = "SELECT key, json FROM `p`.`d`.`t` -- note";
        for sql in [
            schema_probe(commented),
            quantiles(commented, 64, false),
            ctas(&table(), commented, &[], 7, false).unwrap(),
        ] {
            assert!(sql.contains("-- note\n)"), "{sql}");
        }
    }

    #[test]
    fn quantiles_decode_to_cuts_lowest_and_rows_or_none_when_empty() {
        let r = result(
            &[
                ("cuts", "STRING"),
                ("lowest", "STRING"),
                ("row_count", "INTEGER"),
            ],
            vec![Row(vec![
                Cell::Array(vec![Cell::Str("a".into()), Cell::Str("k".into())]),
                Cell::Str("a".into()),
                Cell::Str("1500000".into()),
            ])],
        );
        let q = decode_quantiles(&r).unwrap().unwrap();
        assert_eq!(
            (q.cuts, q.lowest, q.rows),
            (
                vec!["a".to_owned(), "k".to_owned()],
                "a".to_owned(),
                1_500_000
            )
        );
        let empty = result(
            &[
                ("cuts", "STRING"),
                ("lowest", "STRING"),
                ("row_count", "INTEGER"),
            ],
            vec![Row(vec![Cell::Null, Cell::Null, Cell::Str("0".into())])],
        );
        assert!(decode_quantiles(&empty).unwrap().is_none());
    }

    #[test]
    fn ctas_partitions_clusters_expires_and_buckets_the_key() {
        let sql = ctas(
            &table(),
            TARGET,
            &["p/t/3".to_owned(), "p/t/it's".to_owned()],
            7,
            false,
        )
        .unwrap();
        let golden = ctas(
            &table(),
            "SELECT key, json FROM `p`.`d`.`t`",
            &["b".to_owned(), "m".to_owned()],
            7,
            false,
        )
        .unwrap();
        assert_eq!(
            golden,
            "CREATE TABLE `example-project`.`scratch`.`lookup_src`\nPARTITION BY RANGE_BUCKET(bucket, GENERATE_ARRAY(0, 3, 1))\nCLUSTER BY key\nOPTIONS (\n  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)\n)\nAS\nSELECT key, json, RANGE_BUCKET(key, ARRAY<STRING>['b', 'm']) AS bucket\nFROM (\nSELECT key, json FROM `p`.`d`.`t`\n)\nWHERE key IS NOT NULL AND json IS NOT NULL\n"
        );
        assert!(sql.contains("CREATE TABLE `example-project`.`scratch`.`lookup_src`"));
        assert!(sql.contains("PARTITION BY RANGE_BUCKET(bucket, GENERATE_ARRAY(0, 3, 1))"));
        assert!(sql.contains("CLUSTER BY key"));
        assert!(sql
            .contains("expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"));
        assert!(sql.contains(r"RANGE_BUCKET(key, ARRAY<STRING>['p/t/3', 'p/t/it\'s']) AS bucket"));
        assert!(sql.contains(&format!("FROM (\n{TARGET}\n)")));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("WHERE key IS NOT NULL AND json IS NOT NULL"));
        let forever = ctas(&table(), TARGET, &[], 0, true).unwrap();
        assert!(!forever.contains("expiration_timestamp"));
        assert!(!forever.contains("OPTIONS"));
        assert!(forever.contains("ARRAY<STRING>[]"));
        assert!(forever.contains("WHERE key IS NOT NULL\n"));
        assert!(!forever.contains("json IS NOT NULL"));
    }

    #[test]
    fn ctas_refuses_the_partition_ceiling_plus_one_and_a_megabyte_statement() {
        let keys = |n: usize| -> Vec<String> { (0..n).map(|i| format!("{i:05}")).collect() };
        assert!(ctas(&table(), TARGET, &keys(3999), 7, false).is_ok());
        let err = ctas(&table(), TARGET, &keys(4000), 7, false).unwrap_err();
        assert!(err.to_string().contains("4001"), "{err}");
        let long_keys: Vec<String> = (0..3999).map(|i| format!("{i:0>300}")).collect();
        let err = ctas(&table(), TARGET, &long_keys, 7, false).unwrap_err();
        assert!(err.to_string().contains("--buckets"), "{err}");
        assert!(matches!(err, Error::Refused(_)));
    }

    #[test]
    fn sample_buckets_are_evenly_spread_and_start_at_zero() {
        assert_eq!(
            sample_buckets(4000, 10),
            vec![0, 400, 800, 1200, 1600, 2000, 2400, 2800, 3200, 3600]
        );
        assert_eq!(
            sample_buckets(16, 10),
            vec![0, 1, 3, 4, 6, 8, 9, 11, 12, 14]
        );
        assert_eq!(sample_buckets(3, 10), vec![0, 1, 2]);
        assert_eq!(sample_buckets(1, 10), vec![0]);
    }

    #[test]
    fn the_sample_reads_a_bounded_slice_of_each_sampled_partition() {
        let sql = sample(&table(), &[0, 7], 200);
        let t = "`example-project`.`scratch`.`lookup_src`";
        assert!(sql.contains(&format!(
            "(SELECT key, json FROM {t} WHERE bucket = 0 LIMIT 200)"
        )));
        assert!(sql.contains(&format!(
            "(SELECT key, json FROM {t} WHERE bucket = 7 LIMIT 200)"
        )));
        assert_eq!(sql.matches("UNION ALL").count(), 1);
    }

    #[test]
    fn the_largest_bucket_query_decodes_one_integer() {
        let sql = max_bucket_rows(&table());
        assert!(sql.contains("GROUP BY bucket"));
        let r = result(
            &[("max_rows", "INTEGER")],
            vec![Row(vec![Cell::Str("70000".into())])],
        );
        assert_eq!(decode_max_bucket_rows(&r).unwrap(), 70_000);
    }
}
