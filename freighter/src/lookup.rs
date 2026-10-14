//! Pure decode: `(key, json)` rows -> one cell per row (family and qualifier
//! from the manifest, value = gzip(UTF-8 json) or the bytes as they are, row
//! key = the key string's bytes). No I/O and no clock: every cell carries the manifest's
//! `cell_timestamp_micros`.

use std::io::Write;

use crate::cells::{col, refuse_null_slots, TableConfig};
use crate::client::{CellWrite, RowChange, RowWrite};
use crate::error::{Error, Result};
use crate::manifest::CellCompression;
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use flate2::write::GzEncoder;
use flate2::Compression;

const JSON_COLUMN: &str = "json";

/// The Storage Read column list for a lookup source table. The shard
/// `range_column` is restricted on, never projected.
pub fn lookup_selected_fields(shape: &TableConfig) -> Vec<String> {
    vec![shape.rowkey_column.clone(), JSON_COLUMN.to_owned()]
}

/// The cell value for one `json` string: gzip at the default level (the
/// framing Java's GZIPOutputStream writes) or the bytes as they are. Shared
/// with `prepare`'s row-size sample, so the sizes it measures are exactly
/// what the mover writes.
pub(crate) fn encode_value(json: &[u8], compression: CellCompression) -> Result<Vec<u8>> {
    match compression {
        CellCompression::Uncompressed => Ok(json.to_vec()),
        CellCompression::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(json)
                .map_err(|e| Error::Decode(format!("gzip: {e}")))?;
            let mut value = encoder
                .finish()
                .map_err(|e| Error::Decode(format!("gzip: {e}")))?;
            // The encoder's buffer grows by doubling; the value stays resident
            // until its shard is written, so the spare half is given back here.
            value.shrink_to_fit();
            Ok(value)
        }
    }
}

/// Decodes one lookup batch into rows, one per BQ row: a single-cell set,
/// or a row delete where json is NULL and the config allows it. A NULL key
/// is always refused, and so is a NULL json the config does not allow: the
/// source query filters them, so one here is upstream drift.
pub fn decode_lookup(batch: &RecordBatch, shape: &TableConfig) -> Result<Vec<RowWrite>> {
    let timestamp_micros = shape.cell_timestamp_micros;
    let key = col::<StringArray>(batch, &shape.rowkey_column)?;
    let json = col::<StringArray>(batch, JSON_COLUMN)?;
    refuse_null_slots(&shape.rowkey_column, key)?;
    if !shape.null_json_deletes_row {
        refuse_null_slots(JSON_COLUMN, json)?;
    }

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let change = if json.is_null(i) {
            RowChange::Delete
        } else {
            RowChange::Set(vec![CellWrite {
                family: shape.family.clone(),
                qualifier: shape.qualifier.clone(),
                timestamp_micros,
                value: encode_value(json.value(i).as_bytes(), shape.compression)?,
            }])
        };
        out.push(RowWrite {
            rowkey: key.value(i).as_bytes().to_vec(),
            change,
        });
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests; the workspace denies unwrap_used
mod tests {
    use super::*;
    use arrow_array::builder::StringBuilder;

    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field, Schema};
    use flate2::read::GzDecoder;
    use std::io::Read;
    use std::sync::Arc;

    const TIMESTAMP_MICROS: i64 = 1_789_669_800_123_000;

    fn lookup_config() -> TableConfig {
        TableConfig {
            rowkey_column: "key".into(),
            family: "data".into(),
            qualifier: b"jsonz".to_vec(),
            compression: CellCompression::Gzip,
            cell_timestamp_micros: TIMESTAMP_MICROS,
            null_json_deletes_row: false,
        }
    }

    fn deleting_config() -> TableConfig {
        TableConfig {
            null_json_deletes_row: true,
            ..lookup_config()
        }
    }

    fn set_cells(row: &RowWrite) -> &[CellWrite] {
        match &row.change {
            RowChange::Set(cells) => cells,
            RowChange::Delete => panic!("expected a set, got a delete"),
        }
    }

    fn batch(rows: &[(Option<&str>, Option<&str>)]) -> RecordBatch {
        let mut key = StringBuilder::new();
        let mut json = StringBuilder::new();
        for (k, j) in rows {
            key.append_option(*k);
            json.append_option(*j);
        }
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("json", DataType::Utf8, true),
        ]);
        let arrays: Vec<ArrayRef> = vec![Arc::new(key.finish()), Arc::new(json.finish())];
        RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
    }

    fn gunzip(bytes: &[u8]) -> String {
        let mut out = String::new();
        GzDecoder::new(bytes).read_to_string(&mut out).unwrap();
        out
    }

    #[test]
    fn decodes_one_jsonz_cell_per_row_at_the_config_timestamp() {
        let source = [
            ("p/t/1001", r#"{"score":0.42}"#),
            ("p/t/05401", r#"{"count":17,"town":"Winooski — café"}"#),
        ];
        let rows: Vec<(Option<&str>, Option<&str>)> =
            source.iter().map(|(k, j)| (Some(*k), Some(*j))).collect();
        let decoded = decode_lookup(&batch(&rows), &lookup_config()).unwrap();
        assert_eq!(decoded.len(), 2, "one row per BQ row");
        for (row, (key, json)) in decoded.iter().zip(source) {
            assert_eq!(row.rowkey, key.as_bytes(), "row key is the key's bytes");
            let cells = set_cells(row);
            assert_eq!(cells.len(), 1, "one cell per row");
            let cell = &cells[0];
            assert_eq!(cell.family, "data");
            assert_eq!(cell.qualifier, b"jsonz".to_vec());
            assert_eq!(cell.timestamp_micros, TIMESTAMP_MICROS);
            assert_eq!(&cell.value[..2], &[0x1f, 0x8b], "gzip magic");
            assert_eq!(gunzip(&cell.value), json);
        }
    }

    #[test]
    fn refuses_a_null_key() {
        let rows = [(Some("p/t/1"), Some("{}")), (None, Some("{}"))];
        let err = decode_lookup(&batch(&rows), &lookup_config()).unwrap_err();
        assert!(
            err.to_string().contains("column key") && err.to_string().contains("NULL"),
            "{err}"
        );
    }

    #[test]
    fn refuses_a_null_json() {
        let rows = [(Some("p/t/1"), Some("{}")), (Some("p/t/2"), None)];
        let err = decode_lookup(&batch(&rows), &lookup_config()).unwrap_err();
        assert!(
            err.to_string().contains("column json") && err.to_string().contains("NULL"),
            "{err}"
        );
    }

    #[test]
    fn a_null_json_deletes_its_row_when_the_config_allows_it() {
        let rows = [(Some("p/t/1"), Some("{}")), (Some("p/t/2"), None)];
        let decoded = decode_lookup(&batch(&rows), &deleting_config()).unwrap();
        assert_eq!(gunzip(&set_cells(&decoded[0])[0].value), "{}");
        assert_eq!(decoded[1].rowkey, b"p/t/2".to_vec());
        assert_eq!(decoded[1].change, RowChange::Delete);
    }

    #[test]
    fn refuses_a_null_key_even_when_null_json_deletes() {
        let rows = [(None, None)];
        let err = decode_lookup(&batch(&rows), &deleting_config()).unwrap_err();
        assert!(err.to_string().contains("column key"), "{err}");
    }

    /// An ordered shard keeps every gzipped value resident until it is
    /// written, and the caller sizes shards from the value's LENGTH. Spare
    /// capacity left by the encoder's growing buffer is memory the plan never
    /// counted: at ~2 KB values it doubled the pod's residency and OOM-killed
    /// a load sized to fit.
    #[test]
    fn a_gzipped_value_holds_no_spare_capacity() {
        // ~6 KB of poorly compressible JSON, like a wide entity record.
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut json = String::from("[");
        while json.len() < 6_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            json.push_str(&format!("{},null,", state >> 40));
        }
        json.push_str("0]");
        let value = encode_value(json.as_bytes(), CellCompression::Gzip).unwrap();
        assert!(
            value.len() > 1_000,
            "test input compressed to {}",
            value.len()
        );
        assert_eq!(value.capacity(), value.len());
    }

    #[test]
    fn uncompressed_writes_the_json_bytes_under_the_configured_qualifier() {
        let config = TableConfig {
            qualifier: b"j".to_vec(),
            compression: CellCompression::Uncompressed,
            ..lookup_config()
        };
        let rows = [(Some("k"), Some(r#"{"a":1}"#))];
        let decoded = decode_lookup(&batch(&rows), &config).unwrap();
        let cell = &set_cells(&decoded[0])[0];
        assert_eq!(cell.qualifier, b"j".to_vec());
        assert_eq!(cell.value, br#"{"a":1}"#.to_vec());
    }

    #[test]
    fn encode_value_gzips_or_passes_through() {
        let raw = encode_value(b"{}", CellCompression::Uncompressed).unwrap();
        assert_eq!(raw, b"{}".to_vec());
        let gz = encode_value(b"{}", CellCompression::Gzip).unwrap();
        assert_eq!(&gz[..2], &[0x1f, 0x8b]);
        assert_eq!(gunzip(&gz), "{}");
    }
}
