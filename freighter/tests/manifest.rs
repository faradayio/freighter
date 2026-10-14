#![allow(clippy::unwrap_used)] // tests; the workspace denies unwrap_used

use freighter::manifest::{MoveManifest, Outcome, ShardVerdict};

const LOOKUP_MANIFEST_FIXTURE: &str =
    include_str!("../fixtures/freighter-lookup-manifest-example.json");
const VERDICT_FIXTURE: &str = include_str!("../fixtures/freighter-verdict-example.json");

/// The lookup fixture with `from` swapped for `to`; panics when `from` is
/// absent, so a test can never run against the unmodified fixture.
fn lookup_fixture_with(from: &str, to: &str) -> String {
    assert!(
        LOOKUP_MANIFEST_FIXTURE.contains(from),
        "lookup fixture has no {from:?}"
    );
    LOOKUP_MANIFEST_FIXTURE.replace(from, to)
}

#[test]
fn verdict_serializes_to_the_fixture() {
    let verdict = ShardVerdict {
        manifest_version: 3,
        shard_index: 0,
        rows_read: 6_245_162,
        cells_encoded: 6_245_100,
        cells_acknowledged: 6_245_100,
        cells_failed: 0,
        row_deletes_acknowledged: 62,
        row_deletes_failed: 0,
        entry_retries: 3,
        entries: 6_245_162,
        requests: 2498,
        read_reopens: 1,
        rpc_retries: 2,
        read_seconds: 41.2,
        sort_seconds: 3.1,
        write_seconds: 760.0,
        stalled_seconds: 90.0,
        wall_seconds: 812.4,
        latency_ms_p50: 141.0,
        latency_ms_p99: 512.0,
        outcome: Outcome::Complete,
    };
    let fixture: serde_json::Value = serde_json::from_str(VERDICT_FIXTURE).unwrap();
    assert_eq!(serde_json::to_value(&verdict).unwrap(), fixture);
}

#[test]
fn refuses_wrong_version() {
    for wrong in [2, 4] {
        let doc = lookup_fixture_with("\"version\": 3", &format!("\"version\": {wrong}"));
        let err = MoveManifest::from_json(&doc).unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("version {wrong} unsupported")),
            "{err}"
        );
    }
}

#[test]
fn refuses_flush_cells_near_mutation_limit() {
    let doc = lookup_fixture_with("\"flush_cells\": 8000", "\"flush_cells\": 95000");
    let err = MoveManifest::from_json(&doc).unwrap_err();
    assert!(
        err.to_string().contains("flush_cells 95000 outside"),
        "{err}"
    );
}

#[test]
fn accepts_flush_cells_90k() {
    // Larger flushes amortize per-RPC overhead (fewer requests for the
    // same cells); the pipeline's overflow guard — not this validator — is
    // what keeps any single request under the 100k mutation limit.
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["flush_cells"] = serde_json::json!(90_000);
    doc["inflight_mutations"] = serde_json::json!(90_000);
    let m = MoveManifest::from_json(&doc.to_string()).expect("90k flush is legal");
    assert_eq!(m.flush_cells, 90_000);
}

#[test]
fn refuses_empty_shards() {
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"] = serde_json::json!([]);
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("shards"), "{err}");
}

#[test]
fn refuses_wrong_version_even_when_body_is_unparseable() {
    // A future v4 manifest with changed fields must get the version
    // message, not a serde missing-field error.
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["version"] = serde_json::json!(4);
    let obj = doc.as_object_mut().unwrap();
    obj.remove("shards");
    obj.remove("family");
    obj.remove("rowkey_column");
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("version"), "{err}");
    // And with ADDED fields: pins that VersionProbe itself carries no
    // deny_unknown_fields — a v4 doc with new knobs still gets the
    // version message, never an unknown-field error.
    let mut doc2: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc2["version"] = serde_json::json!(4);
    doc2["new_knob"] = serde_json::json!(true);
    let err2 = MoveManifest::from_json(&doc2.to_string()).unwrap_err();
    assert!(err2.to_string().contains("version"), "{err2}");
    assert!(!err2.to_string().contains("unknown field"), "{err2}");
    // A v1 manifest carries keys this version refuses; it gets the version
    // message too.
    let mut v1: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    v1["version"] = serde_json::json!(1);
    v1["rowkey_kind"] = serde_json::json!("str");
    let err3 = MoveManifest::from_json(&v1.to_string()).unwrap_err();
    assert!(err3.to_string().contains("version 1 unsupported"), "{err3}");
    assert!(!err3.to_string().contains("unknown field"), "{err3}");
}

#[test]
fn refuses_unknown_fields() {
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["surprise"] = serde_json::json!(true);
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("unknown field"), "{err}");
}

#[test]
fn refuses_zero_read_streams() {
    let doc = lookup_fixture_with("\"read_streams\": 4", "\"read_streams\": 0");
    let err = MoveManifest::from_json(&doc).unwrap_err();
    assert!(err.to_string().contains("read_streams"), "{err}");
}

#[test]
fn refuses_duplicate_shard_index() {
    // Verdicts land at shard_{index:04}.json — a duplicate index would
    // silently overwrite another shard's verdict.
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"][1]["index"] = serde_json::json!(0);
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("duplicate"), "{err}");
}

#[test]
fn refuses_inverted_int_range() {
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"][0]["range"] = serde_json::json!({ "kind": "int", "lo": 10, "hi": 5 });
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("inverted"), "{err}");
}

#[test]
fn refuses_inflight_mutations_below_flush_cells() {
    // A slice acquires one permit per row from the pod semaphore, which
    // holds inflight_mutations permits: a budget smaller than one full
    // slice could never satisfy that acquire. Refused loudly.
    let doc = lookup_fixture_with(
        "\"inflight_mutations\": 64000",
        "\"inflight_mutations\": 7999",
    );
    let err = MoveManifest::from_json(&doc).unwrap_err();
    assert!(err.to_string().contains("flush_cells"), "{err}");
}

#[test]
fn lane_and_rung_are_required() {
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"][0].as_object_mut().unwrap().remove("lane");
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("lane"), "{err}");

    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc.as_object_mut().unwrap().remove("rung");
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("rung"), "{err}");

    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc.as_object_mut().unwrap().remove("null_json_deletes_row");
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("null_json_deletes_row"), "{err}");
}

#[test]
fn single_lane_manifests_keep_stride_order_legal() {
    // Stride order lists shards NON-ascending; single-lane manifests
    // must stay legal — the ascending rule is a multi-lane rule only.
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"] = serde_json::json!([
        { "index": 1, "range": { "kind": "int", "lo": 100, "hi": 199 }, "expect_empty": false, "lane": 0 },
        { "index": 0, "range": { "kind": "int", "lo": 0,   "hi": 99  }, "expect_empty": false, "lane": 0 }
    ]);
    MoveManifest::from_json(&doc.to_string()).expect("stride order parses single-lane");
}

#[test]
fn accepts_multi_lane_manifest() {
    // Lane 0's slices are ADJACENT (hi 99, lo 100) — banded plans emit
    // contiguous slices, so strict-ascending must accept adjacency.
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"] = serde_json::json!([
        { "index": 0, "range": { "kind": "int", "lo": 0,   "hi": 99  }, "expect_empty": false, "lane": 0 },
        { "index": 1, "range": { "kind": "int", "lo": 200, "hi": 299 }, "expect_empty": false, "lane": 1 },
        { "index": 2, "range": { "kind": "int", "lo": 100, "hi": 199 }, "expect_empty": false, "lane": 0 }
    ]);
    let m = MoveManifest::from_json(&doc.to_string()).expect("multi-lane parses");
    assert_eq!(m.shards[1].lane, 1);
}

#[test]
fn refuses_sparse_lanes() {
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"][1]["lane"] = serde_json::json!(2); // lane 1 missing
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("dense"), "{err}");
}

#[test]
fn refuses_within_lane_overlapping_ranges() {
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["shards"] = serde_json::json!([
        { "index": 0, "range": { "kind": "int", "lo": 0,  "hi": 99  }, "expect_empty": false, "lane": 0 },
        { "index": 1, "range": { "kind": "int", "lo": 50, "hi": 149 }, "expect_empty": false, "lane": 0 },
        { "index": 2, "range": { "kind": "int", "lo": 200, "hi": 299 }, "expect_empty": false, "lane": 1 }
    ]);
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("ascending"), "{err}");
}

#[test]
fn parses_the_lookup_fixture() {
    let m = MoveManifest::from_json(LOOKUP_MANIFEST_FIXTURE).expect("lookup fixture parses");
    assert_eq!(m.version, 3);
    assert!(!m.null_json_deletes_row);
    assert_eq!(m.rowkey_column, "key");
    assert_eq!(m.range_column, "bucket");
    assert_eq!(m.family, "data");
    assert_eq!(m.bt_instance, "example-instance");
    assert_eq!(m.destination_table, "example_table");
    assert_eq!(m.app_profile, "bulk-write");
    assert_eq!(m.cell_timestamp_micros, 1_789_776_000_123_000);
    assert_eq!(m.shards.len(), 2);
    assert_eq!(m.shards[1].lane, 1);
}

#[test]
fn refuses_str_rowkey_with_int_shards_when_range_column_is_the_rowkey_column() {
    // Naming the rowkey column as the range column declares nothing: the
    // bands would still be cut on a string.
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["range_column"] = serde_json::json!("key");
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("range_column"), "{err}");
}

#[test]
fn lookup_refuses_int_shards_without_a_distinct_range_column() {
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc.as_object_mut().unwrap().remove("range_column");
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("range_column"), "{err}");
}

#[test]
fn lookup_refuses_a_non_positive_cell_timestamp() {
    for bad in [0i64, -1_000] {
        let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
        doc["cell_timestamp_micros"] = serde_json::json!(bad);
        let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
        assert!(err.to_string().contains("positive"), "{bad}: {err}");
    }
}

#[test]
fn lookup_refuses_a_sub_millisecond_cell_timestamp() {
    // Bigtable stores millisecond granularity and rejects a SetCell whose
    // micros are not a whole number of milliseconds.
    let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
    doc["cell_timestamp_micros"] = serde_json::json!(1_789_669_800_123_456i64);
    let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("milliseconds"), "{err}");
}

#[test]
fn lookup_refuses_a_cell_timestamp_in_the_wrong_unit() {
    // Whole milliseconds and positive, so only the magnitude rule can refuse
    // them: 2026-09-19 in seconds, in milliseconds, then in nanoseconds.
    for bad in [
        1_789_776_000i64,
        1_789_776_000_000,
        1_789_776_000_000_000_000,
    ] {
        let mut doc: serde_json::Value = serde_json::from_str(LOOKUP_MANIFEST_FIXTURE).unwrap();
        doc["cell_timestamp_micros"] = serde_json::json!(bad);
        let err = MoveManifest::from_json(&doc.to_string()).unwrap_err();
        assert!(
            err.to_string().contains("microseconds since the epoch"),
            "{bad}: {err}"
        );
    }
}
