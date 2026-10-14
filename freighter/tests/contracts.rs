//! Pins for the two JSON contracts `prepare` and `verify` publish, the way
//! tests/manifest.rs pins the manifest.
#![allow(clippy::unwrap_used)]

use freighter::manifest::CellCompression;
use freighter::source::{SourceRecord, TableRef};

const SOURCE_FIXTURE: &str = include_str!("../fixtures/freighter-source-record-example.json");

#[test]
fn source_record_parses_the_fixture_and_round_trips() {
    let s = SourceRecord::from_json(SOURCE_FIXTURE).expect("fixture parses");
    assert_eq!(s.version, 1);
    assert_eq!(s.buckets, 3);
    assert_eq!(s.boundaries, vec!["k/0500000", "k/1000000"]);
    assert_eq!(s.max_rows_per_bucket, 620_114);
    let fixture: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
    assert_eq!(serde_json::to_value(&s).unwrap(), fixture);
    let again = SourceRecord::from_json(&s.to_json().unwrap()).unwrap();
    assert_eq!(s, again);
}

#[test]
fn source_record_refuses_unknown_fields_and_wrong_versions() {
    let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
    doc["surprise"] = serde_json::json!(true);
    let err = SourceRecord::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("unknown field"), "{err}");

    let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
    doc["version"] = serde_json::json!(2);
    let err = SourceRecord::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("version 2 unsupported"), "{err}");
}

#[test]
fn source_record_refuses_inconsistent_cuts_and_unmeasured_sizes() {
    let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
    doc["buckets"] = serde_json::json!(4);
    let err = SourceRecord::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("buckets 4"), "{err}");

    let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
    doc["boundaries"] = serde_json::json!(["k/1000000", "k/0500000"]);
    let err = SourceRecord::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("ascending"), "{err}");

    for field in ["gzip_bytes_per_row", "raw_bytes_per_row"] {
        let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
        doc[field] = serde_json::json!(0.0);
        let err = SourceRecord::from_json(&doc.to_string()).unwrap_err();
        assert!(err.to_string().contains(field), "{field}: {err}");
    }

    let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
    doc["rows"] = serde_json::json!(0);
    let err = SourceRecord::from_json(&doc.to_string()).unwrap_err();
    assert!(err.to_string().contains("rows must be positive"), "{err}");

    let cases = [
        (
            "max_rows_per_bucket",
            serde_json::json!(0),
            "max_rows_per_bucket",
        ),
        (
            "boundaries",
            serde_json::json!(["k/0500000", "k/0500000"]),
            "ascending",
        ),
        ("range_column", serde_json::json!("key"), "range_column"),
        ("max_rows_per_bucket", serde_json::json!(100), "cannot hold"),
        (
            "max_rows_per_bucket",
            serde_json::json!(2_000_000),
            "exceeds rows",
        ),
    ];
    for (field, value, expect) in cases {
        let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
        doc[field] = value;
        let err = SourceRecord::from_json(&doc.to_string()).unwrap_err();
        assert!(err.to_string().contains(expect), "{field}: {err}");
    }
}

#[test]
fn row_bytes_follows_the_compression() {
    let s = SourceRecord::from_json(SOURCE_FIXTURE).unwrap();
    assert_eq!(s.row_bytes(CellCompression::Gzip), 97.3);
    assert_eq!(s.row_bytes(CellCompression::Uncompressed), 391.0);
}

#[test]
fn table_ref_parses_both_spellings() {
    let t: TableRef = "example-project.freighter.source".parse().unwrap();
    assert_eq!(
        (t.project.as_str(), t.dataset.as_str(), t.table.as_str()),
        ("example-project", "freighter", "source")
    );
    assert_eq!(
        t.path(),
        "projects/example-project/datasets/freighter/tables/source"
    );
    assert_eq!(t.sql(), "`example-project`.`freighter`.`source`");
    assert_eq!(TableRef::from_path(&t.path()).unwrap(), t);
    assert!("just-a-project".parse::<TableRef>().is_err());
    assert!("a.b.c.d".parse::<TableRef>().is_err());
    assert!(TableRef::from_path("projects/p/tables/t").is_err());
    assert!("p.d.we`ird".parse::<TableRef>().is_err());
    assert!("p..t".parse::<TableRef>().is_err());
    assert!("p.d/x.t".parse::<TableRef>().is_err());
    assert!(r"p.d.t\x".parse::<TableRef>().is_err());
    assert!(TableRef::from_path("projects/p/datasets/a.b/tables/t").is_err());
    assert!(TableRef::from_path("projects/example.com:proj/datasets/d/tables/t").is_ok());
}

#[test]
fn verify_report_serializes_to_its_fixture() {
    use freighter::verify::{Problem, VerifyReport, VERIFY_REPORT_VERSION};
    const REPORT_FIXTURE: &str = include_str!("../fixtures/freighter-verify-report-example.json");
    let report = VerifyReport {
        version: VERIFY_REPORT_VERSION,
        ok: false,
        shards: 200,
        problems: vec![
            Problem {
                shard: 4,
                reason: "no verdict".into(),
            },
            Problem {
                shard: 7,
                reason: "acknowledged 149 cells, source has 150 rows to set".into(),
            },
        ],
        stray_verdicts: vec![9],
    };
    let fixture: serde_json::Value = serde_json::from_str(REPORT_FIXTURE).unwrap();
    assert_eq!(serde_json::to_value(&report).unwrap(), fixture);
    let back: VerifyReport = serde_json::from_str(REPORT_FIXTURE).unwrap();
    assert_eq!(back, report);
}
