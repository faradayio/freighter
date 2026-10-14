//! The `prepare` -> `plan` contract: one JSON document describing the
//! prepared source table and the per-row sizes measured from it. Field
//! names ARE the contract, this module is its only validator, and
//! fixtures/freighter-source-record-example.json pins it.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::manifest::CellCompression;

pub const SOURCE_RECORD_VERSION: u32 = 1;

/// A BigQuery table address: `p.d.t` on the command line; the manifest and
/// the source record carry the REST path form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub project: String,
    pub dataset: String,
    pub table: String,
}

fn plain_identifier(part: &str, what: &str) -> Result<()> {
    if part.is_empty() || part.contains('`') || part.contains('/') {
        return Err(Error::Refused(format!(
            "{what} {part:?} is not a plain identifier"
        )));
    }
    Ok(())
}

impl TableRef {
    /// `projects/<p>/datasets/<d>/tables/<t>`.
    pub fn from_path(path: &str) -> Result<Self> {
        let parts: Vec<&str> = path.split('/').collect();
        match parts.as_slice() {
            ["projects", p, "datasets", d, "tables", t] => Self::new(p, d, t),
            _ => Err(Error::Refused(format!(
                "table path {path:?} is not projects/<p>/datasets/<d>/tables/<t>"
            ))),
        }
    }

    fn new(project: &str, dataset: &str, table: &str) -> Result<Self> {
        plain_identifier(project, "project")?;
        plain_identifier(dataset, "dataset")?;
        plain_identifier(table, "table")?;
        Ok(Self {
            project: project.to_owned(),
            dataset: dataset.to_owned(),
            table: table.to_owned(),
        })
    }

    pub fn path(&self) -> String {
        format!(
            "projects/{}/datasets/{}/tables/{}",
            self.project, self.dataset, self.table
        )
    }

    /// Backtick-quoted for GoogleSQL.
    pub fn sql(&self) -> String {
        format!("`{}`.`{}`.`{}`", self.project, self.dataset, self.table)
    }
}

impl FromStr for TableRef {
    type Err = Error;
    /// `project.dataset.table`.
    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        match parts.as_slice() {
            [p, d, t] => Self::new(p, d, t),
            _ => Err(Error::Refused(format!(
                "table {s:?} is not project.dataset.table"
            ))),
        }
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.project, self.dataset, self.table)
    }
}

/// Probed before the full parse, as manifest.rs does.
#[derive(Deserialize)]
struct VersionProbe {
    version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRecord {
    pub version: u32,
    /// `projects/<p>/datasets/<d>/tables/<t>` — the manifest's `source_table` form.
    pub table: String,
    pub billing_project: String,
    pub rowkey_column: String,
    /// Recorded for readers; the mover's decoder reads `json` by name.
    pub json_column: String,
    pub range_column: String,
    /// Interior quantile cuts, ascending; bucket i >= 1 starts at boundaries[i-1].
    pub boundaries: Vec<String>,
    /// `boundaries.len() + 1`.
    pub buckets: u32,
    pub rows: u64,
    /// BigQuery's `numBytes / numRows`. For the reader; never used for sizing.
    pub logical_bytes_per_row: f64,
    /// Sampled mean of `len(key) + len(gzip(json))`: what a gzip load writes per row.
    pub gzip_bytes_per_row: f64,
    /// Sampled mean of `len(key) + len(json)`: what an uncompressed load writes per row.
    pub raw_bytes_per_row: f64,
    /// Row count of the largest bucket. Deduplicated quantile cuts leave
    /// unequal buckets, and a load runs out of memory on the biggest shard.
    pub max_rows_per_bucket: u64,
    pub null_json_deletes_row: bool,
}

impl SourceRecord {
    pub fn from_json(doc: &str) -> Result<Self> {
        let probe: VersionProbe =
            serde_json::from_str(doc).map_err(|e| Error::Source(format!("parse: {e}")))?;
        if probe.version != SOURCE_RECORD_VERSION {
            return Err(Error::Source(format!(
                "version {} unsupported; this freighter understands {SOURCE_RECORD_VERSION}",
                probe.version
            )));
        }
        let s: SourceRecord =
            serde_json::from_str(doc).map_err(|e| Error::Source(format!("parse: {e}")))?;
        TableRef::from_path(&s.table).map_err(|e| Error::Source(e.to_string()))?;
        if s.buckets as usize != s.boundaries.len() + 1 {
            return Err(Error::Source(format!(
                "buckets {} must equal boundaries + 1 ({})",
                s.buckets,
                s.boundaries.len() + 1
            )));
        }
        if !s.boundaries.windows(2).all(|w| w[0] < w[1]) {
            return Err(Error::Source(
                "boundaries must be strictly ascending".to_owned(),
            ));
        }
        if s.rows == 0 {
            return Err(Error::Source("rows must be positive".to_owned()));
        }
        if s.max_rows_per_bucket == 0 {
            return Err(Error::Source(
                "max_rows_per_bucket must be positive".to_owned(),
            ));
        }
        for (name, v) in [
            ("gzip_bytes_per_row", s.gzip_bytes_per_row),
            ("raw_bytes_per_row", s.raw_bytes_per_row),
        ] {
            // `!(v > 0.0)`, not `v <= 0.0`: NaN must be refused too.
            #[allow(clippy::neg_cmp_op_on_partial_ord)]
            if !(v > 0.0) {
                return Err(Error::Source(format!(
                    "{name} {v} must be positive: the source's row size was not measured"
                )));
            }
        }
        if s.rowkey_column == s.range_column {
            return Err(Error::Source(
                "range_column must differ from rowkey_column".to_owned(),
            ));
        }
        Ok(s)
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(|e| Error::Source(format!("render: {e}")))
    }

    pub fn table_ref(&self) -> Result<TableRef> {
        TableRef::from_path(&self.table)
    }

    /// The one place the gzip-vs-raw sizing choice is made.
    pub fn row_bytes(&self, compression: CellCompression) -> f64 {
        match compression {
            CellCompression::Gzip => self.gzip_bytes_per_row,
            CellCompression::Uncompressed => self.raw_bytes_per_row,
        }
    }
}
