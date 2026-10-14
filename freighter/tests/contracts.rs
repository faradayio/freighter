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
    assert!(SourceRecord::from_json(&doc.to_string()).is_err());
    let mut doc: serde_json::Value = serde_json::from_str(SOURCE_FIXTURE).unwrap();
    doc["max_rows_per_bucket"] = serde_json::json!(0);
    assert!(SourceRecord::from_json(&doc.to_string()).is_err());
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
}
