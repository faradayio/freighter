# Contributing

## Building

```bash
cargo build --release   # target/release/freighter
```

Releases are built with the compiler pinned in `rust-toolchain.toml`. The
oldest supported compiler is 1.88 (`rust-version` in `freighter/Cargo.toml`,
the floor set by the Bigtable client's dependency tree; see
[docs/design.md](docs/design.md)). Clippy's `incompatible_msrv` lint refuses
newer standard-library APIs anywhere in the crate, tests included, and a CI job
builds with 1.88.

## Testing

```bash
./scripts/check.sh          # fmt, clippy (including the emulator target), unit tests
./scripts/test-emulator.sh  # the Bigtable emulator suite
```

The emulator suite is a separate target that a bare `cargo test` does not run
(`test = false` in `freighter/Cargo.toml`). `test-emulator.sh` starts
`cbtemulator` on `localhost:8086`, runs the suite single-threaded and stops the
emulator. It needs the `bigtable` and `cbt` gcloud components and says which
one to install if either is missing. Extra arguments go to the test harness:
`./scripts/test-emulator.sh write_rows`.

The suite **panics** rather than skipping when `BIGTABLE_EMULATOR_HOST` is
unset: a Bigtable test that silently does nothing is indistinguishable from a
passing one. There are no mocks. Tables are created with `cbt` (the client
crate has no admin API) and round trips run against the emulator.

`cargo clippy --all-targets` does **not** reach the emulator target. `check.sh`
lints it by name.

## Conventions

- One crate-level error enum (`freighter/src/error.rs`).
- Only `client.rs` names Bigtable client types, and only `bqread.rs` names
  BigQuery Storage Read types. See "Swap boundaries" in
  [docs/design.md](docs/design.md).
- Library code logs through `log`; no bare `println!`.
- The manifest and verdict formats are a contract. Changing a field means a
  new manifest `version`, updated fixtures, and a CHANGELOG entry.

## Releasing

1. Move the `Unreleased` notes in `CHANGELOG.md` under the new version,
   dated the day you tag.
2. Bump `version` in `freighter/Cargo.toml`.
3. Tag `vX.Y.Z` and push the tag. The release workflow builds binaries and the
   container image, creates the GitHub release, then publishes to crates.io.
4. First release only: set the `ghcr.io/faradayio/freighter` package's
   visibility to public (new organization packages start private).
