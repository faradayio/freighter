# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
