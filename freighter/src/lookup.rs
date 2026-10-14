//! Pure decode: `(key, json)` rows -> one gzipped cell per row (family `data`,
//! qualifier `jsonz`, value = gzip(UTF-8 json), row key = the key string's
//! bytes). No I/O and no clock: every cell carries the manifest's
//! `cell_timestamp_micros`.

use std::io::Write;

use crate::cells::{col, refuse_null_slots, TableConfig};
use crate::client::{CellWrite, RowChange, RowWrite};
use crate::error::{Error, Result};
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use flate2::write::GzEncoder;
use flate2::Compression;

const JSON_COLUMN: &str = "json";
const JSONZ_QUALIFIER: &[u8] = b"jsonz";

/// The Storage Read column list for a lookup source table. The shard
/// `range_column` is restricted on, never projected.
pub fn lookup_selected_fields(shape: &TableConfig) -> Vec<String> {
    vec![shape.rowkey_column.clone(), JSON_COLUMN.to_owned()]
}

fn gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(bytes)
        .map_err(|e| Error::Decode(format!("gzip: {e}")))?;
    let mut value = encoder
        .finish()
        .map_err(|e| Error::Decode(format!("gzip: {e}")))?;
    // The encoder's buffer grows by doubling; the value stays resident until
    // its shard is written, so the spare half is given back here.
    value.shrink_to_fit();
    Ok(value)
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
                qualifier: JSONZ_QUALIFIER.to_vec(),
                timestamp_micros,
                value: gzip(json.value(i).as_bytes())?,
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
    /// written, and the launcher sizes shards from the value's LENGTH. Spare
    /// capacity left by the encoder's growing buffer is memory the plan never
    /// counted: at ~2 KB values it doubled the pod's residency and OOM-killed
    /// a load sized to fit.
    #[test]
    fn a_gzipped_value_holds_no_spare_capacity() {
        // ~6 KB of poorly compressible JSON, like a wide attribute-cache row.
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut json = String::from("[");
        while json.len() < 6_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            json.push_str(&format!("{},null,", state >> 40));
        }
        json.push_str("0]");
        let value = gzip(json.as_bytes()).unwrap();
        assert!(
            value.len() > 1_000,
            "test input compressed to {}",
            value.len()
        );
        assert_eq!(value.capacity(), value.len());
    }
}
