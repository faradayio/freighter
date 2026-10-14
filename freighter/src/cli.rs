//! The command line: clap types and their conversion into the library's
//! config objects. Bin-only, so clap never becomes part of the published
//! library's API. Domain values parse through the library's `FromStr`s.

use std::io::Read;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{ArgGroup, Args, Parser, Subcommand};
use freighter::error::{Error, Result};
use freighter::manifest::{CellCompression, MAX_CELL_TIMESTAMP_MICROS, MIN_CELL_TIMESTAMP_MICROS};
use freighter::plan::ramp::Ramp;
use freighter::plan::sizing::{parse_memory, SizingConfig, MAX_FLUSH_CELLS, POD_READ_STREAMS};
use freighter::plan::{Destination, PlanConfig};
use freighter::prepare::boundaries::MAX_BUCKETS;
use freighter::prepare::PrepareConfig;
use freighter::source::TableRef;

/// Bulk-load a BigQuery table into Bigtable in key order, with verdicts you
/// can verify.
#[derive(Parser)]
#[command(
    name = "freighter",
    version,
    arg_required_else_help = true,
    override_usage = "freighter <COMMAND>",
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    /// `freighter <manifest-uri>`: the v0.1 spelling of `move`.
    #[arg(hide = true, value_name = "MANIFEST_URI")]
    manifest_uri: Option<String>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Build the sharded source table from a (key, json) query and write
    /// its source record.
    Prepare(PrepareArgs),
    /// Size a load from a source record and write its manifest(s).
    Plan(PlanArgs),
    /// Run a manifest: read the shards, write Bigtable, write verdicts.
    Move {
        /// gs:// or file:// URI of the manifest.
        manifest_uri: String,
    },
    /// Compare a manifest's verdicts with the source table's counts.
    Verify(VerifyArgs),
    /// Check a manifest with the mover's validator. No network.
    Validate {
        /// file:// URI, or `-` for stdin.
        manifest: String,
    },
}

#[derive(Args)]
#[command(group(ArgGroup::new("query_source").required(true).args(["query", "query_file"])))]
pub struct PrepareArgs {
    /// SQL yielding `key STRING, json STRING`.
    #[arg(long)]
    query: Option<String>,
    /// File holding that SQL; `-` for stdin.
    #[arg(long)]
    query_file: Option<String>,
    /// project.dataset.table to create.
    #[arg(long, value_parser = parse_table)]
    table: TableRef,
    /// Upper bound on the number of buckets; thinned so each holds at least 50,000 rows.
    #[arg(long, default_value_t = MAX_BUCKETS, value_parser = clap::value_parser!(u32).range(1..=MAX_BUCKETS as i64))]
    buckets: u32,
    /// Table expiration in days; 0 = never.
    #[arg(long, default_value_t = 7)]
    expires_days: u32,
    /// Keep rows whose json is NULL as row deletes.
    #[arg(long)]
    null_json_deletes_row: bool,
    /// Project the queries are billed to (default: the table's).
    #[arg(long)]
    billing_project: Option<String>,
    /// Where to write the source record (default: stdout).
    #[arg(long)]
    pub out: Option<PathBuf>,
}

fn parse_table(s: &str) -> Result<TableRef> {
    s.parse()
}

fn parse_ramp(s: &str) -> Result<Ramp> {
    s.parse()
}

/// The manifest's rules for the one cell timestamp, applied at the command
/// line so a value in the wrong unit is the user's error (exit 2), not a
/// refusal of the manifest `plan` rendered.
fn parse_cell_timestamp(s: &str) -> Result<i64> {
    let ts: i64 = s
        .parse()
        .map_err(|_| Error::Refused(format!("cell timestamp {s:?} is not an integer")))?;
    if ts <= 0 {
        return Err(Error::Refused(format!(
            "cell timestamp {ts} must be positive, in microseconds since the epoch"
        )));
    }
    // The range comes before the whole-millisecond rule: seconds are rarely
    // a multiple of 1000, and "not microseconds" is the useful message.
    if !(MIN_CELL_TIMESTAMP_MICROS..=MAX_CELL_TIMESTAMP_MICROS).contains(&ts) {
        return Err(Error::Refused(format!(
            "cell timestamp {ts} is not microseconds since the epoch (accepted: 2001-09-09 to 2286-11-20); seconds or milliseconds would stamp cells in 1970"
        )));
    }
    if ts % 1000 != 0 {
        return Err(Error::Refused(format!(
            "cell timestamp {ts} must be a whole number of milliseconds, in microseconds since the epoch"
        )));
    }
    Ok(ts)
}

fn parse_compression(s: &str) -> Result<CellCompression> {
    s.parse()
}

impl PrepareArgs {
    pub fn into_config(self) -> Result<PrepareConfig> {
        let query = match (self.query, self.query_file) {
            (Some(q), _) => q,
            (None, Some(path)) if path == "-" => {
                let mut s = String::new();
                std::io::stdin()
                    .read_to_string(&mut s)
                    .map_err(|e| Error::Refused(format!("read query from stdin: {e}")))?;
                s
            }
            (None, Some(path)) => std::fs::read_to_string(&path)
                .map_err(|e| Error::Refused(format!("read {path}: {e}")))?,
            (None, None) => unreachable!("clap requires one of --query/--query-file"),
        };
        let billing_project = self
            .billing_project
            .unwrap_or_else(|| self.table.project.clone());
        Ok(PrepareConfig {
            query,
            table: self.table,
            billing_project,
            buckets: self.buckets,
            expires_days: self.expires_days,
            null_json_deletes_row: self.null_json_deletes_row,
        })
    }
}

#[derive(Args)]
pub struct PlanArgs {
    /// The source record `prepare` wrote.
    #[arg(long)]
    pub source: PathBuf,
    /// Concurrent lanes in the mover process.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    lanes: u32,
    /// Memory the mover process gets, as a Kubernetes quantity in Gi or Mi.
    #[arg(long, value_parser = parse_memory)]
    memory: u64,
    /// Bigtable project.
    #[arg(long)]
    bt_project: String,
    /// Bigtable instance.
    #[arg(long)]
    bt_instance: String,
    /// Bigtable table; must already exist with the column family.
    #[arg(long)]
    destination_table: String,
    /// Bigtable app profile to write through (a low-priority one lets serving reads win).
    #[arg(long)]
    app_profile: String,
    /// Column family of the one cell.
    #[arg(long, default_value = "data")]
    family: String,
    /// Column qualifier of the one cell.
    #[arg(long, default_value = "jsonz")]
    qualifier: String,
    /// gzip or none.
    #[arg(long, default_value = "gzip", value_parser = parse_compression)]
    compression: CellCompression,
    /// Target bytes per MutateRows request; sets the rows per request.
    #[arg(long, default_value_t = 4_000_000, value_parser = clap::value_parser!(u64).range(1..))]
    target_request_bytes: u64,
    // clap's `value_parser!(usize)` has no `.range`; the ranged parser is
    // spelled out.
    /// Ceiling on rows per MutateRows request; caps what --target-request-bytes derives.
    #[arg(long, default_value_t = MAX_FLUSH_CELLS, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=MAX_FLUSH_CELLS as u64))]
    max_flush_cells: usize,
    /// Storage Read streams for the whole process, shared out per lane.
    #[arg(long, default_value_t = POD_READ_STREAMS, value_parser = clap::builder::RangedI64ValueParser::<i32>::new().range(1..))]
    read_streams: i32,
    /// Fractions of the shards per step, e.g. 0.05,0.10,0.85.
    #[arg(long, default_value = "1.0", value_parser = parse_ramp)]
    ramp: Ramp,
    /// The one timestamp every cell carries. Default: now, in whole
    /// milliseconds. Pass the previous plan's value to re-run a load.
    #[arg(long, value_parser = parse_cell_timestamp)]
    cell_timestamp_micros: Option<i64>,
    /// A fresh directory per attempt: verdicts are written beside the manifest.
    #[arg(long, default_value = ".")]
    pub out_dir: PathBuf,
}

/// Now, truncated to whole milliseconds, in microseconds since the epoch.
fn load_timestamp_micros(now: SystemTime) -> i64 {
    let millis = now
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_millis() as i64;
    millis * 1000
}

impl PlanArgs {
    pub fn into_config(self, now: SystemTime) -> PlanConfig {
        PlanConfig {
            sizing: SizingConfig {
                lanes: self.lanes,
                memory_bytes: self.memory,
                compression: self.compression,
                target_request_bytes: self.target_request_bytes,
                max_flush_cells: self.max_flush_cells,
                read_streams: self.read_streams,
            },
            destination: Destination {
                bt_project: self.bt_project,
                bt_instance: self.bt_instance,
                table: self.destination_table,
                app_profile: self.app_profile,
                family: self.family,
                qualifier: self.qualifier,
            },
            ramp: self.ramp,
            cell_timestamp_micros: self
                .cell_timestamp_micros
                .unwrap_or_else(|| load_timestamp_micros(now)),
        }
    }
}

#[derive(Args)]
pub struct VerifyArgs {
    /// gs:// or file:// URI of the manifest; verdicts are read beside it.
    pub manifest_uri: String,
    /// Project the count query is billed to (default: the manifest's).
    #[arg(long)]
    pub billing_project: Option<String>,
    /// Where to write the report (default: stdout).
    #[arg(long)]
    pub out: Option<PathBuf>,
}

/// Parses the command line; the bare `freighter <uri>` form becomes `move`.
/// clap exits 2 on a usage error or no arguments, 0 on `--help`.
pub fn parse() -> Command {
    let cli = Cli::parse();
    match (cli.command, cli.manifest_uri) {
        (Some(c), _) => c,
        (None, Some(manifest_uri)) => Command::Move { manifest_uri },
        (None, None) => unreachable!("arg_required_else_help exits before this"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cell_timestamp_in_the_wrong_unit_is_refused() {
        let e = parse_cell_timestamp("1792003349")
            .expect_err("refused")
            .to_string();
        assert!(e.contains("not microseconds"), "{e}");
        let e = parse_cell_timestamp("1789776000123456")
            .expect_err("refused")
            .to_string();
        assert!(e.contains("milliseconds"), "{e}");
        assert_eq!(
            parse_cell_timestamp("1789776000123000").expect("accepted"),
            1_789_776_000_123_000
        );
    }

    #[test]
    fn the_default_cell_timestamp_is_whole_milliseconds() {
        let now = UNIX_EPOCH + std::time::Duration::from_micros(1_234_567_891);
        assert_eq!(load_timestamp_micros(now), 1_234_567_000);
    }
}
