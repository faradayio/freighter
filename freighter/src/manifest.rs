//! The caller <-> mover contract: versioned JSON in (manifest), JSON out
//! (per-shard verdicts). Field names ARE the contract, this module is its only
//! validator, and fixtures/freighter-lookup-manifest-example.json pins it.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// MutateRows allows 100k mutations per request; one row is one mutation, so
/// this stays under it with room.
pub const MAX_FLUSH_CELLS: usize = 90_000;
/// The version `plan` writes.
pub const MOVE_MANIFEST_VERSION: u32 = 4;
/// What this mover reads. A version-3 document is read as version 4 with
/// `qualifier` and `compression` at their defaults, so a caller that renders
/// v3 keeps working across the bump. The version is not cross-checked against
/// field presence: a v3 document carrying the v4 fields, or a v4 document
/// without them, both parse.
pub const SUPPORTED_MANIFEST_VERSIONS: [u32; 2] = [3, 4];

/// The smallest plausible `cell_timestamp_micros`: 2001-09-09 in microseconds.
/// A timestamp given in seconds or milliseconds sits far below it and would
/// stamp cells in 1970 — on a one-version table those lose to every existing
/// cell, so the load would succeed and change nothing.
pub const MIN_CELL_TIMESTAMP_MICROS: i64 = 1_000_000_000_000_000;

/// The largest plausible `cell_timestamp_micros`: 2286-11-20 in microseconds.
/// A timestamp given in nanoseconds sits far above it and would stamp cells
/// tens of thousands of years ahead — on a one-version table those beat every
/// later load, so the table could never be refreshed.
pub const MAX_CELL_TIMESTAMP_MICROS: i64 = 10_000_000_000_000_000;

/// Probed before the full parse so a future-versioned manifest with
/// changed fields gets the version-mismatch message, not a serde
/// missing-field error.
#[derive(Deserialize)]
struct VersionProbe {
    version: u32,
}

// deny_unknown_fields cannot apply to this internally-tagged enum (serde
// limitation), so an extra key beside `lo`/`hi` is silently dropped —
// accepted residual.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ShardRange {
    /// Contiguous signed-int64 range of the manifest's range column,
    /// inclusive both ends — exactly the bounds of the BigQuery BETWEEN.
    Int { lo: i64, hi: i64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardSpec {
    pub index: u32,
    pub range: ShardRange,
    pub expect_empty: bool,
    /// Band-spread lane: shards sharing a lane run sequentially
    /// in listed order; distinct lanes run concurrently.
    pub lane: u32,
}

impl ShardSpec {
    /// The `shard= lane=` label every log line for this shard carries.
    pub fn label(&self) -> ShardLabel {
        ShardLabel {
            shard: self.index,
            lane: self.lane,
        }
    }
}

/// What identifies one slice in a log line: its shard index and lane.
/// `Display` renders `shard={shard} lane={lane}` so a format string carries
/// both with one `{label}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardLabel {
    pub shard: u32,
    pub lane: u32,
}

impl std::fmt::Display for ShardLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard={} lane={}", self.shard, self.lane)
    }
}

/// How the cell value is encoded from the `json` string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CellCompression {
    /// gzip at flate2's default level — the framing Java's GZIPInputStream reads.
    #[default]
    Gzip,
    /// The `json` string's UTF-8 bytes as they are. Named so it never reads
    /// as `Option::None` in a match.
    #[serde(rename = "none")]
    Uncompressed,
}

impl std::str::FromStr for CellCompression {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "gzip" => Ok(Self::Gzip),
            "none" => Ok(Self::Uncompressed),
            other => Err(Error::Refused(format!(
                "compression {other:?}: expected gzip or none"
            ))),
        }
    }
}

impl std::fmt::Display for CellCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Gzip => "gzip",
            Self::Uncompressed => "none",
        })
    }
}

fn default_qualifier() -> String {
    "jsonz".to_owned()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveManifest {
    pub version: u32,
    pub source_table: String,
    pub billing_project: String,
    pub rowkey_column: String,
    /// The integer column shard ranges are cut on — never the rowkey column,
    /// which is a string.
    ///
    /// It must be ORDER-PRESERVING over the row key: rows with a lower value
    /// sort entirely before rows with a higher one (a quantile bucket of the
    /// key, never a hash of it). Ordered writes and the within-lane
    /// ascending-ranges rule both treat a shard's range as one contiguous
    /// run of row keys; a column that scatters keys across values passes
    /// every check here and quietly loses the append-order write path.
    pub range_column: String,
    pub family: String,
    /// Column qualifier of the one cell. v4; a v3 document gets `jsonz`.
    #[serde(default = "default_qualifier")]
    pub qualifier: String,
    /// Cell value encoding. v4; a v3 document gets gzip.
    #[serde(default)]
    pub compression: CellCompression,
    pub bt_project: String,
    pub bt_instance: String,
    pub destination_table: String,
    pub app_profile: String,
    /// Caller-ordered.
    pub shards: Vec<ShardSpec>,
    /// Always picked explicitly by the caller; 0/server-pick is not part
    /// of this contract.
    pub read_streams: i32,
    pub flush_cells: usize,
    /// Per-pod bound on MUTATIONS awaiting acknowledgement — what the
    /// Bigtable server queues — not requests. A request-count cap silently
    /// multiplies the server queue as flush_cells grows.
    pub inflight_mutations: usize,
    /// The one timestamp every cell of a load carries, picked once by the
    /// caller: a relaunched shard re-writes the same cells at the same
    /// timestamp, which is what makes the rewrite idempotent.
    pub cell_timestamp_micros: i64,
    /// A source row whose json is NULL deletes its Bigtable row. When false,
    /// a NULL json is upstream drift and fails the shard.
    pub null_json_deletes_row: bool,
    /// The rung the SOURCE TABLE was cut at — the total shard count, of which
    /// `shards` carries only this entry's subset. Without it a reader has no
    /// way to recover the cut from the manifest and `shards.len()` reads as
    /// the rung, which silently rescales every per-shard quantity derived
    /// from it (session volume, cells/shard, keyspace width). The mover
    /// never reads it: it annotates the manifest for whoever does.
    pub rung: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Complete,
    Failed,
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Complete => "complete",
            Self::Failed => "failed",
        })
    }
}

/// A mover's verdict on one shard: outcome, row/cell counts, latencies, and resilience counters.
///
/// Deliberately no `deny_unknown_fields`: a verdict from a newer mover may carry
/// counters this `verify` doesn't know, and it must still be readable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardVerdict {
    pub manifest_version: u32,
    pub shard_index: u32,
    pub rows_read: u64,
    pub cells_encoded: u64,
    /// Counts each entry at most once, on FINAL success (retry-proof).
    pub cells_acknowledged: u64,
    /// TERMINALLY failed after bounded retries. Completeness gate: == 0.
    pub cells_failed: u64,
    /// Row deletes acknowledged, each at most once, on FINAL success.
    pub row_deletes_acknowledged: u64,
    /// Row deletes TERMINALLY failed. Completeness gate: == 0.
    pub row_deletes_failed: u64,
    /// Transient history; never contaminates acknowledged or failed.
    pub entry_retries: u64,
    /// MutateRows entries sent (first attempts; retries re-send a subset,
    /// not new entries). entries/requests is the measured request shape —
    /// recorded so shape claims come from verdicts, not from arithmetic
    /// over assumed cells-per-row.
    pub entries: u64,
    /// MutateRows RPCs issued, including in-place entry retries and
    /// whole-batch RPC retries; entries/requests is still the measured
    /// request shape as-issued. `rpc_retries` is NOT a subset of this
    /// field to subtract out — it also sums read-side retries (see below)
    /// that never touch MutateRows, so the read/write split is not
    /// recoverable from the verdict.
    pub requests: u64,
    /// Read-stream re-opens (idle timeout or transport status) for this shard.
    pub read_reopens: u64,
    /// Unary-RPC retries, read-side and write-side summed: create_read_session
    /// and token minting (read) plus write_rows whole-batch re-sends (write)
    /// — two disjoint populations with no way to split them back out here.
    pub rpc_retries: u64,
    /// Session open → last decoder finished.
    pub read_seconds: f64,
    /// The shard-wide sort by row key.
    pub sort_seconds: f64,
    /// First flush issued → last MutateRows acknowledged.
    pub write_seconds: f64,
    /// Idle-timeout waits that ended in a re-open (`reopens_idle ×
    /// READ_IDLE_TIMEOUT`) — the stall cost this shard paid.
    pub stalled_seconds: f64,
    /// Read session opened → last MutateRows acknowledged. A lane reads its
    /// next shard while writing the current one, so this INCLUDES the time
    /// the read shard waited for the previous write: rate a shard from
    /// `write_seconds`, and a load from its manifest's wall, not from this.
    pub wall_seconds: f64,
    pub latency_ms_p50: f64,
    pub latency_ms_p99: f64,
    pub outcome: Outcome,
}

/// Band-spread lane rules, enforced ONLY for multi-lane manifests:
/// single-lane manifests (all shards lane 0) list shards in stride order,
/// which is deliberately non-ascending and must stay legal.
///
/// CROSS-lane disjointness is the launcher's guarantee (it partitions the
/// shard ranges), not validated here — this tripwire covers
/// within-lane order only.
pub(crate) fn validate_lanes(shards: &[ShardSpec]) -> Result<()> {
    let max_lane = shards.iter().map(|s| s.lane).max().unwrap_or(0);
    if max_lane == 0 {
        return Ok(());
    }
    let lanes: HashSet<u32> = shards.iter().map(|s| s.lane).collect();
    for lane in 0..=max_lane {
        if !lanes.contains(&lane) {
            return Err(Error::Manifest(format!(
                "lane values must be dense from 0: lane {lane} missing below max {max_lane}"
            )));
        }
    }
    // Within a lane, listed ranges strictly ascending and disjoint —
    // the mover-side tripwire for per-tablet append order.
    let mut last_hi: HashMap<u32, i64> = HashMap::new();
    for shard in shards {
        let ShardRange::Int { lo, hi } = shard.range;
        if let Some(&prev) = last_hi.get(&shard.lane) {
            if lo <= prev {
                return Err(Error::Manifest(format!(
                    "lane {} shard {} starts at {lo}, not after the lane's previous hi {prev} — within-lane ranges must be listed strictly ascending and disjoint (append order)",
                    shard.lane, shard.index
                )));
            }
        }
        last_hi.insert(shard.lane, hi);
    }
    Ok(())
}

/// The one rule for a cell timestamp, shared by the manifest validator and
/// `plan`'s flag. The range comes before the whole-millisecond check: a value
/// in seconds is rarely a multiple of 1000, and the unit is the useful message.
pub fn check_cell_timestamp(ts: i64) -> Result<()> {
    if ts <= 0 {
        return Err(Error::Manifest(format!(
            "cell_timestamp_micros {ts} must be positive"
        )));
    }
    if !(MIN_CELL_TIMESTAMP_MICROS..=MAX_CELL_TIMESTAMP_MICROS).contains(&ts) {
        return Err(Error::Manifest(format!(
            "cell_timestamp_micros {ts} is not microseconds since the epoch (accepted: 2001-09-09 to 2286-11-20) — seconds or milliseconds stamp cells in 1970, where they lose to every existing cell; nanoseconds stamp them millennia ahead, where they beat every later load"
        )));
    }
    if ts % 1000 != 0 {
        return Err(Error::Manifest(format!(
            "cell_timestamp_micros {ts} must be a whole number of milliseconds — Bigtable stores millisecond granularity"
        )));
    }
    Ok(())
}

impl MoveManifest {
    pub fn from_json(doc: &str) -> Result<Self> {
        let probe: VersionProbe =
            serde_json::from_str(doc).map_err(|e| Error::Manifest(format!("parse: {e}")))?;
        if !SUPPORTED_MANIFEST_VERSIONS.contains(&probe.version) {
            // Keep in step with SUPPORTED_MANIFEST_VERSIONS.
            return Err(Error::Manifest(format!(
                "version {} unsupported; this mover understands 3 or 4",
                probe.version
            )));
        }
        let m: MoveManifest =
            serde_json::from_str(doc).map_err(|e| Error::Manifest(format!("parse: {e}")))?;
        if m.shards.is_empty() {
            return Err(Error::Manifest("shards must be non-empty".to_owned()));
        }
        let mut seen = HashSet::new();
        for shard in &m.shards {
            if !seen.insert(shard.index) {
                return Err(Error::Manifest(format!(
                    "duplicate shard index {} — verdicts land at shard_{:04}.json and would overwrite",
                    shard.index, shard.index
                )));
            }
            let ShardRange::Int { lo, hi } = shard.range;
            if lo > hi {
                return Err(Error::Manifest(format!(
                    "shard {} has inverted range: lo {lo} > hi {hi}",
                    shard.index
                )));
            }
        }
        if m.range_column == m.rowkey_column {
            return Err(Error::Manifest(
                "range_column must name an integer column distinct from the rowkey column — int-banding a string keyspace is refused".to_owned(),
            ));
        }
        if m.qualifier.is_empty() {
            return Err(Error::Manifest(
                "qualifier must be non-empty — it names the cell's column".to_owned(),
            ));
        }
        check_cell_timestamp(m.cell_timestamp_micros)?;
        if m.read_streams <= 0 {
            return Err(Error::Manifest(format!(
                "read_streams {} must be positive — pick the stream count explicitly",
                m.read_streams
            )));
        }
        // MutateRows allows up to 100k entries and 100k mutations per
        // request. A row is one cell or one delete, so a request of
        // `flush_cells` rows is that many of each.
        if m.flush_cells == 0 || m.flush_cells > MAX_FLUSH_CELLS {
            return Err(Error::Manifest(format!(
                "flush_cells {} outside (0, 90000] — stay below the 100k mutation limit",
                m.flush_cells
            )));
        }
        if m.inflight_mutations < m.flush_cells {
            return Err(Error::Manifest(format!(
                "inflight_mutations {} < flush_cells {} — the budget can never admit one full flush; the write would wait on it forever",
                m.inflight_mutations, m.flush_cells
            )));
        }
        validate_lanes(&m.shards)?;
        Ok(m)
    }
}
