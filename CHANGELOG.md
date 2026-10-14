# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `freighter prepare`: builds the sharded source table from a `(key, json)`
  query and writes a source record (a new JSON contract).
- `freighter plan`: sizes a load from a source record and writes one
  manifest per ramp step.
- `freighter verify`: compares a manifest's verdicts with the source table
  and writes a report (a new JSON contract); exit 0 only when every shard
  checks out.
- `freighter validate`: the manifest validator alone, no network.
- Manifest version 4: `qualifier` and `compression` (`gzip` or `none`).
  Version 3 manifests are still accepted.
- Docs: a quickstart and an operating guide.

### Changed

- `freighter move <uri>` is the documented spelling; `freighter <uri>` still works.
- Library: `cells::TableConfig` gains `qualifier` and `compression`, so code
  that builds it by struct literal must add them.

## [0.1.0] - 2026-10-14

### Added

- The `freighter` loader: reads a sharded BigQuery table through the Storage
  Read API and writes one gzipped JSON cell per row to Bigtable in ascending
  key order, with one verdict per shard.
- Manifest format version 3, including row deletes for NULL `json`
  (`null_json_deletes_row`).
- Documentation: concepts, preparing a source table, manifest and verdict
  reference, sizing, verifying a load, design notes.
- Release binaries for Linux (x86_64, aarch64, static musl) and macOS, and a
  container image at `ghcr.io/faradayio/freighter`.
