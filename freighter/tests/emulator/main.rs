//! Emulator-backed suite: the Bigtable client and the mover's write half.
//!
//! These tests talk to a REAL Cloud Bigtable emulator — never a mock. Run
//! them with `scripts/test-emulator.sh`, which starts `cbtemulator`, exports
//! `BIGTABLE_EMULATOR_HOST` and serializes the suite. Without that env var
//! every test panics with instructions rather than skipping.
//!
//! NOTE: `cargo clippy --all-targets` does NOT reach this target — `--tests`
//! selects only targets with `test = true`, and this one is `test = false`
//! (see `Cargo.toml`). Lint it explicitly:
//! `cargo clippy -p bq-freighter --test emulator -- -D warnings`.
#![allow(clippy::unwrap_used)]

mod decode;
mod roundtrip;
mod support;
mod write_rows;
