//! The quantile cut of the key space: which cuts survive, in what order.
//! Pure; the SQL that produces the cuts lives in `queries.rs`.

/// One BigQuery job may produce at most 4,000 partitions, and the source
/// table is built by one CREATE TABLE AS SELECT. Verified live: 4,001 range
/// partitions fail with "allowed 4000".
pub const MAX_BUCKETS: u32 = 4000;

/// A bucket is read through one BigQuery partition, so a cut finer than
/// this makes a small source cost a partition walk per handful of rows:
/// 1,000 rows over 1,000 partitions read in 27 s where one partition reads
/// in a second. `--buckets` is a ceiling; the cut is thinned to this floor.
pub(crate) const MIN_ROWS_PER_BUCKET: u64 = 50_000;

/// The interior quantile cut points, deduplicated and ascending, thinned so
/// that a bucket holds at least [`MIN_ROWS_PER_BUCKET`] of `rows`.
///
/// `RANGE_BUCKET(key, boundaries)` puts keys below `boundaries[0]` in
/// bucket 0 and keys in `[boundaries[i-1], boundaries[i])` in bucket i.
/// Every boundary is a key of the table, so bucket i >= 1 holds at least
/// `boundaries[i-1]`; dropping cuts equal to the lowest key keeps bucket 0
/// non-empty. Thinning keeps every k-th cut, so the buckets that remain are
/// unions of consecutive quantile buckets, within one quantile bucket of
/// equal size.
///
/// Rounding is half-to-even (`round_ties_even`): a pick that falls exactly
/// between two cuts takes the even position, so the same input always
/// yields the same cuts.
pub(crate) fn shard_boundaries(cuts: &[String], lowest: &str, rows: u64) -> Vec<String> {
    let interior: &[String] = cuts
        .get(1..cuts.len().saturating_sub(1))
        .unwrap_or_default();
    let keep = (rows / MIN_ROWS_PER_BUCKET).max(1) as usize;
    let thinned: Vec<&String> = if keep < interior.len() + 1 {
        let step = (interior.len() + 1) as f64 / keep as f64;
        // step > 1 and (keep-1)·step < interior.len(), so 0 <= pick < len.
        (1..keep)
            .map(|i| &interior[(i as f64 * step).round_ties_even() as usize - 1])
            .collect()
    } else {
        interior.iter().collect()
    };
    let mut out: Vec<String> = thinned
        .into_iter()
        .filter(|cut| cut.as_str() > lowest)
        .cloned()
        .collect();
    // Cuts arrive ascending from APPROX_QUANTILES and thinning keeps order;
    // sort anyway so the result is a valid RANGE_BUCKET array whatever came in.
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Enough rows that no cut is thinned.
    const UNTHINNED: u64 = u64::MAX;

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|x| (*x).to_owned()).collect()
    }

    #[test]
    fn boundaries_are_the_distinct_interior_cuts() {
        let cuts = s(&["a", "a", "c", "c", "f", "k", "k"]);
        assert_eq!(shard_boundaries(&cuts, "a", UNTHINNED), s(&["c", "f", "k"]));
    }

    #[test]
    fn a_single_bucket_has_no_boundaries() {
        assert_eq!(
            shard_boundaries(&s(&["a", "z"]), "a", UNTHINNED),
            Vec::<String>::new()
        );
    }

    #[test]
    fn boundaries_sort_by_code_point_like_bigquery() {
        let cuts = s(&["p/t/1", "p/t/9", "p/t/10", "p/t/Z", "p/t/a", "p/t/z"]);
        assert_eq!(
            shard_boundaries(&cuts, "p/t/1", UNTHINNED),
            s(&["p/t/10", "p/t/9", "p/t/Z", "p/t/a"])
        );
    }

    #[test]
    fn thinning_keeps_equal_buckets_down_to_the_row_floor() {
        // 8 quantile buckets of a 200k-row source: 4 buckets of 50k rows
        // survive, every second cut, so the survivors are still equal-sized.
        let cuts = s(&["a", "b", "c", "d", "e", "f", "g", "h", "z"]);
        assert_eq!(shard_boundaries(&cuts, "a", 200_000), s(&["c", "e", "g"]));
        // Fewer rows than the floor: one bucket, no boundaries at all.
        assert_eq!(shard_boundaries(&cuts, "a", 1_000), Vec::<String>::new());
        // A source big enough for every cut keeps every cut.
        assert_eq!(
            shard_boundaries(&cuts, "a", 8 * 50_000),
            s(&["b", "c", "d", "e", "f", "g", "h"])
        );
    }

    #[test]
    fn thinning_rounds_ties_to_even_like_python() {
        // 5 interior cuts, keep 4: step = 6/4 = 1.5, so the picks are
        // round(1.5)-1, round(3.0)-1, round(4.5)-1 = 1, 2, 3 under
        // half-to-even (Python). Half-away-from-zero would pick 1, 2, 4.
        let cuts = s(&["a", "b", "c", "d", "e", "f", "z"]);
        assert_eq!(
            shard_boundaries(&cuts, "a", 4 * MIN_ROWS_PER_BUCKET),
            s(&["c", "d", "e"])
        );
    }
}
