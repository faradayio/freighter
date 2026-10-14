//! What decode needs besides the batch: the per-table config and checked
//! Arrow column access. No I/O.

use arrow_array::Array;
use arrow_array::RecordBatch;

use crate::error::{Error, Result};
use crate::manifest::{CellCompression, MoveManifest};

/// The per-table config the manifest declares.
#[derive(Debug, Clone)]
pub struct TableConfig {
    pub rowkey_column: String,
    pub family: String,
    /// Column qualifier of the one cell, as bytes.
    pub qualifier: Vec<u8>,
    /// How the `json` value is encoded into the cell.
    pub compression: CellCompression,
    /// The one timestamp every cell carries.
    pub cell_timestamp_micros: i64,
    /// A NULL value deletes its row instead of being refused.
    pub null_json_deletes_row: bool,
}

impl TableConfig {
    pub fn from_manifest(m: &MoveManifest) -> Self {
        Self {
            rowkey_column: m.rowkey_column.clone(),
            family: m.family.clone(),
            qualifier: m.qualifier.as_bytes().to_vec(),
            compression: m.compression,
            cell_timestamp_micros: m.cell_timestamp_micros,
            null_json_deletes_row: m.null_json_deletes_row,
        }
    }
}

pub(crate) fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let idx = b
        .schema()
        .index_of(name)
        .map_err(|_| Error::Decode(format!("column {name} missing from batch")))?;
    b.column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::Decode(format!("column {name} has unexpected arrow type")))
}

/// Refuses any NULL slot in a column the source query promises non-NULL.
/// Arrow's `.value(i)` does not consult the validity bitmap, so a NULL
/// here would otherwise decode to garbage silently (null rowkey ->
/// phantom row).
pub(crate) fn refuse_null_slots(name: &str, a: &dyn Array) -> Result<()> {
    match a.null_count() {
        0 => Ok(()),
        n => Err(Error::Decode(format!(
            "column {name} has {n} NULL slots; contract requires it non-NULL"
        ))),
    }
}
