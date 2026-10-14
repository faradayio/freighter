//! `freighter plan`: size one load from a source record and render its
//! manifests, one per ramp step, each passed through the mover's own
//! validator before it is returned. `sizing` and `ramp` are the arithmetic
//! run in production at Faraday. All three are pure: the caller reads the
//! clock.

pub mod ramp;
pub mod sizing;

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::manifest::{MoveManifest, MOVE_MANIFEST_VERSION};
use crate::source::SourceRecord;

use ramp::{plan_ramp, Ramp};
use sizing::{plan_load, SizingConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    pub bt_project: String,
    pub bt_instance: String,
    pub table: String,
    pub app_profile: String,
    pub family: String,
    pub qualifier: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlanConfig {
    pub sizing: SizingConfig,
    pub destination: Destination,
    pub ramp: Ramp,
    /// One timestamp per LOAD, carried by every step's manifest. Picked by
    /// the caller (now, ms-truncated) or passed from a previous plan for a
    /// re-run.
    pub cell_timestamp_micros: i64,
}

/// One manifest per ramp step. Each is serialized and re-read through
/// `MoveManifest::from_json`, so what `plan` hands out is exactly what the
/// mover will accept; a refusal there is a bug in `plan` (`Error::Plan`).
pub fn plan(source: &SourceRecord, cfg: &PlanConfig) -> Result<Vec<MoveManifest>> {
    let load = plan_load(source, &cfg.sizing)?;
    let steps = plan_ramp(&load, &cfg.ramp)?;
    let d = &cfg.destination;
    steps
        .into_iter()
        .map(|step| {
            let (index, lanes) = (step.index, step.lanes);
            let m = MoveManifest {
                version: MOVE_MANIFEST_VERSION,
                source_table: source.table.clone(),
                billing_project: source.billing_project.clone(),
                rowkey_column: source.rowkey_column.clone(),
                range_column: source.range_column.clone(),
                family: d.family.clone(),
                qualifier: d.qualifier.clone(),
                compression: cfg.sizing.compression,
                bt_project: d.bt_project.clone(),
                bt_instance: d.bt_instance.clone(),
                destination_table: d.table.clone(),
                app_profile: d.app_profile.clone(),
                shards: step.shards,
                read_streams: load.read_streams,
                flush_cells: load.flush_cells,
                inflight_mutations: step.inflight_mutations,
                cell_timestamp_micros: cfg.cell_timestamp_micros,
                null_json_deletes_row: source.null_json_deletes_row,
                rung: load.rung,
            };
            let json =
                serde_json::to_string(&m).map_err(|e| Error::Plan(format!("render: {e}")))?;
            let checked = MoveManifest::from_json(&json)
                .map_err(|e| Error::Plan(format!("rendered a manifest the mover refuses: {e}")))?;
            log::info!(
                "plan: step {} — {} lanes, {} shards, {} cells in flight",
                index,
                lanes,
                checked.shards.len(),
                checked.inflight_mutations
            );
            Ok(checked)
        })
        .collect()
}

/// `<out_dir>/manifest.json` for one step; `<out_dir>/step-<i>/manifest.json`
/// for a ramp. The mover writes verdicts beside its manifest and shard
/// indexes are global across steps, so steps sharing a directory would read
/// each other's verdicts as strays.
///
/// Refuses to overwrite: a directory that already holds a manifest also holds
/// that attempt's verdicts.
pub fn write_manifests(out_dir: &Path, manifests: &[MoveManifest]) -> Result<Vec<PathBuf>> {
    // Local files are storage too; the variant is not GCS-specific.
    let io = |e: std::io::Error, p: &Path| Error::Storage(format!("{}: {e}", p.display()));
    let paths: Vec<PathBuf> = (0..manifests.len())
        .map(|i| {
            if manifests.len() == 1 {
                out_dir.join("manifest.json")
            } else {
                out_dir.join(format!("step-{i}")).join("manifest.json")
            }
        })
        .collect();
    if let Some(existing) = paths.iter().find(|p| p.exists()) {
        return Err(Error::Refused(format!(
            "{} already exists; each attempt needs a fresh --out-dir, because verdicts land beside the manifest",
            existing.display()
        )));
    }
    for (path, m) in paths.iter().zip(manifests) {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| io(e, dir))?;
        }
        let json =
            serde_json::to_string_pretty(m).map_err(|e| Error::Plan(format!("render: {e}")))?;
        // create_new: a writer that raced past the check above fails here
        // rather than overwriting.
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .and_then(|mut f| f.write_all(json.as_bytes()))
            .map_err(|e| io(e, path))?;
    }
    Ok(paths)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::manifest::CellCompression;

    const SOURCE_FIXTURE: &str = include_str!("../fixtures/freighter-source-record-example.json");

    fn config(lanes: u32, memory: &str, ramp: &str) -> PlanConfig {
        PlanConfig {
            sizing: sizing::SizingConfig {
                lanes,
                memory_bytes: sizing::parse_memory(memory).unwrap(),
                compression: CellCompression::Gzip,
                target_request_bytes: 4_000_000,
                max_flush_cells: crate::manifest::MAX_FLUSH_CELLS,
                read_streams: sizing::POD_READ_STREAMS,
            },
            destination: Destination {
                bt_project: "example-project".into(),
                bt_instance: "example-instance".into(),
                table: "example_table".into(),
                app_profile: "bulk-write".into(),
                family: "data".into(),
                qualifier: "jsonz".into(),
            },
            ramp: ramp.parse().unwrap(),
            cell_timestamp_micros: 1_789_776_000_123_000,
        }
    }

    #[test]
    fn a_single_step_plan_is_one_v4_manifest_the_mover_accepts() {
        let source = SourceRecord::from_json(SOURCE_FIXTURE).unwrap();
        let manifests = plan(&source, &config(1, "2Gi", "1.0")).unwrap();
        assert_eq!(manifests.len(), 1);
        let m = &manifests[0];
        assert_eq!(m.version, MOVE_MANIFEST_VERSION);
        assert_eq!(m.source_table, source.table);
        assert_eq!(m.billing_project, source.billing_project);
        MoveManifest::from_json(&serde_json::to_string(m).unwrap()).unwrap();
        assert_eq!(
            (m.rowkey_column.as_str(), m.range_column.as_str()),
            ("key", "bucket")
        );
        assert_eq!(m.qualifier, "jsonz");
        assert_eq!(m.compression, CellCompression::Gzip);
        assert_eq!(m.rung, 3);
        assert_eq!(m.cell_timestamp_micros, 1_789_776_000_123_000);
        assert_eq!(m.inflight_mutations, m.flush_cells);
        // 3 buckets of up to 620,114 rows at (97.3 + 320) x 1.3 B = 336 MB
        // each; (2 GiB - 1 GB - 2 x 4 MB) / 2 = 0.57 GB per shard holds one.
        assert_eq!(m.shards.len(), 3);
        assert!(m.shards.iter().all(|s| s.lane == 0 && !s.expect_empty));
    }

    #[test]
    fn a_ramp_is_one_manifest_per_step_sharing_the_timestamp() {
        let source = SourceRecord::from_json(SOURCE_FIXTURE).unwrap();
        let manifests = plan(&source, &config(1, "2Gi", "0.05,0.10,0.85")).unwrap();
        assert_eq!(manifests.len(), 3);
        assert!(manifests
            .iter()
            .all(|m| m.cell_timestamp_micros == 1_789_776_000_123_000));
        let indices: Vec<u32> = manifests
            .iter()
            .flat_map(|m| m.shards.iter().map(|s| s.index))
            .collect();
        assert_eq!(indices, [0, 1, 2]);
    }

    #[test]
    fn write_manifests_lays_out_one_file_or_step_directories() {
        let source = SourceRecord::from_json(SOURCE_FIXTURE).unwrap();
        let dir = std::env::temp_dir().join(format!("freighter-plan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let one = plan(&source, &config(1, "2Gi", "1.0")).unwrap();
        let one_dir = dir.join("one");
        let paths = write_manifests(&one_dir, &one).unwrap();
        assert_eq!(paths, vec![one_dir.join("manifest.json")]);
        let back = MoveManifest::from_json(&std::fs::read_to_string(&paths[0]).unwrap()).unwrap();
        assert_eq!(back, one[0]);
        let three = plan(&source, &config(1, "2Gi", "0.05,0.10,0.85")).unwrap();
        let ramp_dir = dir.join("ramp");
        let paths = write_manifests(&ramp_dir, &three).unwrap();
        assert_eq!(
            paths,
            vec![
                ramp_dir.join("step-0/manifest.json"),
                ramp_dir.join("step-1/manifest.json"),
                ramp_dir.join("step-2/manifest.json")
            ]
        );
        let err = write_manifests(&one_dir, &one).unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
