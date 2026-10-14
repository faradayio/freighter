//! Real-BigQuery suite: `prepare -> plan -> move (emulator) -> verify` and
//! the jobs client against the live API. Never mocked. Run it with
//! `scripts/test-bigquery.sh`; without `FREIGHTER_TEST_DATASET` and
//! `BIGTABLE_EMULATOR_HOST` every test panics with instructions.
//!
//! `cargo clippy --all-targets` does NOT reach this target (`test = false`);
//! `scripts/check.sh` lints it by name.
#![allow(clippy::unwrap_used)]

mod end_to_end;
mod jobs;
mod support;
