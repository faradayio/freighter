//! Memory and request sizing for one full-rate load, from the source's
//! measured bytes per row. Pure.

use crate::error::{Error, Result};
use crate::manifest::{CellCompression, MAX_FLUSH_CELLS};
use crate::source::SourceRecord;

/// What a resident row costs the mover beyond its key and value: the
/// RowWrite and CellWrite structs, five small heap allocations with
/// allocator overhead and rounding, and up to ~96 B while the shard's row
/// Vec grows. About 300 B, derived, not measured.
pub(crate) const RESIDENT_ROW_OVERHEAD_BYTES: f64 = 320.0;
/// Run-to-run spread of mover peak memory at identical resident size.
pub(crate) const RESIDENCY_SAFETY: f64 = 1.3;
/// A lane reads its next shard while it writes the current one, so two
/// whole shards are resident per lane at once.
pub(crate) const RESIDENT_SHARDS_PER_LANE: u32 = 2;
/// Runtime and gRPC channels.
pub(crate) const RESIDENCY_FIXED_BYTES: f64 = 1_000_000_000.0;
/// The in-flight budget counts twice: a slice's decoded rows and its
/// serialized request are both resident until the RPC returns.
pub(crate) const INFLIGHT_COPIES: f64 = 2.0;
/// Storage Read streams across the pod; each lane's session gets an equal
/// share so decode (gzip) parallelism does not grow with the lane count.
pub const POD_READ_STREAMS: i32 = 32;

#[derive(Debug, Clone, PartialEq)]
pub struct SizingConfig {
    pub lanes: u32,
    /// The mover process's memory, in bytes (from `--memory`).
    pub memory_bytes: u64,
    pub compression: CellCompression,
    /// Bytes per MutateRows request; sets `flush_cells`.
    pub target_request_bytes: u64,
    /// Caller cap on rows per request, at or under `MAX_FLUSH_CELLS`.
    pub max_flush_cells: usize,
    /// Pod total; shared out per lane.
    pub read_streams: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoadPlan {
    /// Inclusive (lo, hi) bucket ranges, ascending, covering 0..rung once.
    pub shard_ranges: Vec<(i64, i64)>,
    pub lanes: u32,
    pub read_streams: i32,
    pub flush_cells: usize,
    pub rung: u32,
}

/// A Kubernetes quantity in `Gi` or `Mi`, in bytes. Anything else is
/// refused rather than guessed at: a bare "24G" would size the pod 7% small
/// without a word.
pub fn parse_memory(s: &str) -> Result<u64> {
    let refuse = || Error::Refused(format!("memory {s:?}: give it in Gi or Mi, e.g. 24Gi"));
    let (number, shift) = if let Some(n) = s.strip_suffix("Gi") {
        (n, 30)
    } else if let Some(n) = s.strip_suffix("Mi") {
        (n, 20)
    } else {
        return Err(refuse());
    };
    let value: f64 = number.parse().map_err(|_| refuse())?;
    if !value.is_finite() || value <= 0.0 {
        return Err(refuse());
    }
    let bytes = (value * (1u64 << shift) as f64) as u64;
    if bytes == 0 {
        return Err(refuse());
    }
    Ok(bytes)
}

/// One MutateRows request in flight per lane: the mover admits requests
/// against ONE pod-wide semaphore of this many permits, so
/// `inflight_mutations / flush_cells` IS the pod's request concurrency.
pub(crate) fn inflight_mutations(flush_cells: usize, lanes: u32) -> usize {
    flush_cells * lanes as usize
}

/// Everything sized here is made of key + value: a MutateRows request, and
/// the decoded cells an ordered shard keeps resident. So every figure comes
/// from the sampled bytes per row for the chosen encoding, never from
/// BigQuery's logical size. A shard is sized as if every bucket in it were
/// the LARGEST bucket: the mover runs out of memory on the biggest shard,
/// not the average one.
pub(crate) fn plan_load(source: &SourceRecord, cfg: &SizingConfig) -> Result<LoadPlan> {
    if cfg.lanes < 1 {
        return Err(Error::Refused(format!(
            "lanes {} must be positive",
            cfg.lanes
        )));
    }
    source.validate()?;
    let row_bytes = source.row_bytes(cfg.compression);
    if cfg.read_streams <= 0 {
        return Err(Error::Refused(format!(
            "read_streams {} must be positive",
            cfg.read_streams
        )));
    }
    if cfg.max_flush_cells == 0 || cfg.max_flush_cells > MAX_FLUSH_CELLS {
        return Err(Error::Refused(format!(
            "max_flush_cells {} must be in (0, {MAX_FLUSH_CELLS}]",
            cfg.max_flush_cells
        )));
    }
    let flush_cells = ((cfg.target_request_bytes as f64 / row_bytes).floor() as usize)
        .max(1)
        .min(cfg.max_flush_cells);
    let inflight_bytes = inflight_mutations(flush_cells, cfg.lanes) as f64 * row_bytes;
    let resident_row_bytes = (row_bytes + RESIDENT_ROW_OVERHEAD_BYTES) * RESIDENCY_SAFETY;
    let rows_per_bucket = source.max_rows_per_bucket as f64;
    let overhead = RESIDENCY_FIXED_BYTES + INFLIGHT_COPIES * inflight_bytes;
    let gib = |b: f64| b / (1u64 << 30) as f64;
    if cfg.memory_bytes as f64 <= overhead {
        return Err(Error::Refused(format!(
            "memory {:.2}GiB leaves nothing for shards after {:.2}GiB fixed and {:.2}GiB of in-flight requests at {} lanes: raise memory or lower lanes",
            gib(cfg.memory_bytes as f64),
            gib(RESIDENCY_FIXED_BYTES),
            gib(INFLIGHT_COPIES * inflight_bytes),
            cfg.lanes
        )));
    }
    let shard_budget = (cfg.memory_bytes as f64 - overhead)
        / (f64::from(cfg.lanes) * f64::from(RESIDENT_SHARDS_PER_LANE));
    let buckets_per_shard = (shard_budget / (rows_per_bucket * resident_row_bytes)).floor();
    if buckets_per_shard < 1.0 {
        return Err(Error::Refused(format!(
            "one bucket ({} rows, {:.2}GiB resident) does not fit a resident shard's {:.2}GiB share of {:.2}GiB at {} lanes, two shards a lane: build the source with more buckets, or use fewer lanes",
            source.max_rows_per_bucket,
            gib(rows_per_bucket * resident_row_bytes),
            gib(shard_budget),
            gib(cfg.memory_bytes as f64),
            cfg.lanes
        )));
    }
    let buckets_per_shard = buckets_per_shard as i64;
    let buckets = i64::from(source.buckets);
    let shard_ranges = (0..buckets)
        .step_by(buckets_per_shard as usize)
        .map(|lo| (lo, (lo + buckets_per_shard).min(buckets) - 1))
        .collect();
    Ok(LoadPlan {
        shard_ranges,
        lanes: cfg.lanes,
        read_streams: (cfg.read_streams / cfg.lanes as i32).max(1),
        flush_cells,
        rung: source.buckets,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::source::SOURCE_RECORD_VERSION;

    fn source(
        rows: u64,
        gzip_bytes_per_row: f64,
        buckets: u32,
        max_rows_per_bucket: Option<u64>,
    ) -> SourceRecord {
        SourceRecord {
            version: SOURCE_RECORD_VERSION,
            table: "projects/example-project/datasets/d/tables/source".into(),
            billing_project: "example-project".into(),
            rowkey_column: "key".into(),
            range_column: "bucket".into(),
            boundaries: (1..buckets).map(|i| format!("k/{i:05}")).collect(),
            buckets,
            rows,
            logical_bytes_per_row: 2500.0,
            gzip_bytes_per_row,
            raw_bytes_per_row: 2000.0,
            max_rows_per_bucket: max_rows_per_bucket.unwrap_or(rows.div_ceil(buckets as u64)),
            null_json_deletes_row: false,
        }
    }

    // 62,250 rows per bucket on average; the largest bucket holds 70,000.
    fn large_source() -> SourceRecord {
        source(249_000_000, 500.0, 4000, Some(70_000))
    }

    fn cfg(lanes: u32, memory: &str) -> SizingConfig {
        SizingConfig {
            lanes,
            memory_bytes: parse_memory(memory).unwrap(),
            compression: CellCompression::Gzip,
            target_request_bytes: 4_000_000,
            max_flush_cells: MAX_FLUSH_CELLS,
            read_streams: POD_READ_STREAMS,
        }
    }

    #[test]
    fn sizes_requests_and_shards_from_gzip_bytes_per_row() {
        let plan = plan_load(&large_source(), &cfg(8, "24Gi")).unwrap();
        assert_eq!(plan.flush_cells, 8000);
        assert_eq!(plan.read_streams, 4);
        assert_eq!(plan.lanes, 8);
        assert_eq!(plan.rung, 4000);
        // The LARGEST bucket sizes a shard: 70,000 rows x (500 + 320) B x 1.3 =
        // 74.6 MB resident per bucket; (24 GiB - 1 GB fixed - 2 x 32 MB in
        // flight) / 8 lanes / 2 resident shards a lane = 1.54 GB per shard,
        // which holds 20 buckets.
        assert_eq!(plan.shard_ranges[0], (0, 19));
        assert_eq!(plan.shard_ranges[1], (20, 39));
        assert_eq!(plan.shard_ranges[plan.shard_ranges.len() - 1], (3980, 3999));
        assert_eq!(plan.shard_ranges.len(), 200);
    }

    #[test]
    fn sizes_shards_from_the_largest_bucket_not_the_mean() {
        let shards = |max_rows: u64| {
            plan_load(
                &source(249_000_000, 500.0, 4000, Some(max_rows)),
                &cfg(8, "24Gi"),
            )
            .unwrap()
            .shard_ranges
            .len()
        };
        assert_eq!(shards(62_250), 174);
        assert_eq!(shards(70_000), 200);
        assert_eq!(shards(124_500), 364);
    }

    #[test]
    fn refuses_a_lane_count_below_one() {
        let err = plan_load(&large_source(), &cfg(0, "24Gi")).unwrap_err();
        assert!(
            err.to_string().contains("lanes 0 must be positive"),
            "{err}"
        );
    }

    #[test]
    fn refuses_an_unmeasured_row_size() {
        for unmeasured in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let err =
                plan_load(&source(1_000_000, unmeasured, 100, None), &cfg(2, "24Gi")).unwrap_err();
            assert!(err.to_string().contains("must be positive"), "{err}");
        }
    }

    #[test]
    fn parse_memory_reads_gi_and_mi_and_refuses_the_rest() {
        assert_eq!(parse_memory("24Gi").unwrap(), 24 * (1u64 << 30));
        assert_eq!(parse_memory("1.5Gi").unwrap(), 3 * (1u64 << 29));
        assert_eq!(parse_memory("512Mi").unwrap(), 512 * (1u64 << 20));
        for other in [
            "24G",
            "24",
            "Gi",
            "-1Gi",
            "24gi",
            "infGi",
            "NaNGi",
            "0.0000001Mi",
        ] {
            let err = parse_memory(other).unwrap_err();
            assert!(err.to_string().contains("Gi or Mi"), "{other}: {err}");
        }
    }

    #[test]
    fn never_reads_the_logical_bytes_per_row() {
        let mut a = source(249_000_000, 500.0, 4000, None);
        let mut b = a.clone();
        a.logical_bytes_per_row = 400.0;
        b.logical_bytes_per_row = 5000.0;
        assert_eq!(
            plan_load(&a, &cfg(8, "24Gi")).unwrap(),
            plan_load(&b, &cfg(8, "24Gi")).unwrap()
        );
    }

    #[test]
    fn shards_cover_every_bucket_once() {
        let src = source(249_000_000, 500.0, 4010, Some(70_000));
        let plan = plan_load(&src, &cfg(8, "24Gi")).unwrap();
        assert_eq!(plan.shard_ranges.last(), Some(&(4000, 4009)));
        let covered: Vec<i64> = plan
            .shard_ranges
            .iter()
            .flat_map(|&(lo, hi)| lo..=hi)
            .collect();
        assert_eq!(covered, (0..4010).collect::<Vec<i64>>());
    }

    #[test]
    fn clamps_flush_cells_to_the_manifest_range() {
        let tiny_rows = source(1_000_000, 10.0, 100, None);
        assert_eq!(
            plan_load(&tiny_rows, &cfg(2, "24Gi")).unwrap().flush_cells,
            90_000
        );
        // 4 MB / 10 B would be 400,000 rows. A caller cap of 15,000 binds.
        let capped = SizingConfig {
            max_flush_cells: 15_000,
            ..cfg(2, "24Gi")
        };
        assert_eq!(plan_load(&tiny_rows, &capped).unwrap().flush_cells, 15_000);
        // 4 MB / 300 B is 13,333, under that cap, so the byte budget still binds.
        let set_rows = source(1_000_000, 300.0, 100, None);
        assert_eq!(plan_load(&set_rows, &capped).unwrap().flush_cells, 13_333);
        let huge_rows = source(1_000, 8_000_000.0, 100, None);
        assert_eq!(
            plan_load(&huge_rows, &cfg(2, "24Gi")).unwrap().flush_cells,
            1
        );
    }

    #[test]
    fn refuses_a_flush_cap_outside_the_manifest_range() {
        for max_flush_cells in [0, MAX_FLUSH_CELLS + 1] {
            let bad = SizingConfig {
                max_flush_cells,
                ..cfg(2, "24Gi")
            };
            let err = plan_load(&large_source(), &bad).unwrap_err();
            assert!(err.to_string().contains("max_flush_cells"), "{err}");
        }
    }

    #[test]
    fn every_lane_gets_at_least_one_read_stream() {
        assert_eq!(
            plan_load(&large_source(), &cfg(64, "64Gi"))
                .unwrap()
                .read_streams,
            1
        );
    }

    #[test]
    fn refuses_a_bucket_that_cannot_fit() {
        let coarse = source(249_000_000, 500.0, 16, None);
        let err = plan_load(&coarse, &cfg(8, "2Gi")).unwrap_err();
        assert!(err.to_string().contains("does not fit"), "{err}");
        assert!(matches!(err, Error::Refused(_)));
        let err = plan_load(&coarse, &cfg(8, "512Mi")).unwrap_err();
        assert!(
            err.to_string().contains("leaves nothing for shards"),
            "{err}"
        );
    }

    #[test]
    fn an_uncompressed_load_sizes_from_the_raw_mean() {
        let src = large_source(); // gzip 500 B, raw 2000 B
        let gz = plan_load(&src, &cfg(8, "24Gi")).unwrap();
        let raw = plan_load(
            &src,
            &SizingConfig {
                compression: CellCompression::Uncompressed,
                ..cfg(8, "24Gi")
            },
        )
        .unwrap();
        assert_eq!(gz.flush_cells, 8000);
        assert_eq!(raw.flush_cells, 2000);
        assert!(
            raw.shard_ranges.len() > gz.shard_ranges.len(),
            "raw rows are bigger, so shards hold fewer buckets"
        );
    }
}
