# Design notes

Why freighter is built the way it is. Measurements quoted here come from
production loads at Faraday.

## Whole-shard sort, append-order writes

Bigtable stores rows sorted by key in tablets, and writes that arrive in key
order within a tablet are its cheapest path. A BigQuery Storage Read session
does not preserve row order, even from a table clustered on the key, and its
parallel streams interleave. So freighter reads a whole shard, sorts it once
by row-key bytes, and writes it as consecutive, disjoint key slices. The cost
is memory (a shard is resident whole) and a short sort (seconds per shard);
the gain is that each tablet sees appends.

## Lanes, band-spread, and read-overlaps-write

One shard at a time would leave Bigtable idle during every read and BigQuery
idle during every write. A lane reads its next shard while it writes the
current one, which is why two shards are resident per lane. Several lanes run
at once; each takes a contiguous block of shards so their write cursors are
far apart in the keyspace and land on different tablets. Within a lane,
shards must be listed strictly ascending, which the manifest validator checks.

## One pod-wide in-flight budget

Concurrency is one semaphore of `inflight_mutations` permits shared by every
lane, counted in mutations, not requests. A per-request cap would multiply
what Bigtable has queued as `flush_cells` grows; a per-shard budget would
multiply it by the number of lanes. When the budget is full, decoding stops,
and Storage Read backs off behind it.

## Silent read streams

About 5% of Storage Read streams in production go silent for around 13
minutes and then resume. Freighter treats 90 seconds without a message as a
stall, re-opens the stream at the row offset it had reached, and records the
re-open and the time lost in the verdict. Nothing is lost or duplicated,
because Storage Read streams can be resumed at an offset.

## Channel rotation

Google closes long-lived gRPC connections at around 60 minutes. Every 30
minutes freighter builds fresh BigQuery and Bigtable connection pools and
swaps them in, so new requests never start on a connection near that wall. A
read stream opened just before a rotation can still outlive its connection;
when Google closes it, the stream is re-opened at its offset on the new pool.
A failed rotation keeps the old pools and tries again 5 minutes later.

## One timestamp per load

Every cell of a load carries the same, manifest-supplied timestamp. Writing a
cell again with the same value and timestamp leaves Bigtable as it was, so a
shard can be re-run any number of times. That turns "the pod was preempted
halfway" into "re-run the shards without a clean verdict". A fresh timestamp
per attempt would instead add a cell version per attempt.

## Report, don't judge

The mover writes counts and leaves the decision to the caller. It can't know
the source's true row counts without a second BigQuery query, and keeping that
outside the mover means the mover needs no BigQuery jobs permission and stays
small.

## Swap boundaries

The mover has two, one per vendor API, plus one shared policy module:

- **Bigtable: `freighter/src/client.rs`.** The mover writes through
  `BtClient` and never names `bigtable_rs` types (see below).
- **BigQuery Storage Read: `freighter/src/bqread.rs`.** The only module that
  names Storage Read or tonic client types. It hands out Arrow
  `RecordBatch`es; everything downstream is BigQuery-agnostic.
- **Retry policy: `freighter/src/retry.rs`** names `tonic::Code`, a gRPC
  status enum, so the read and write paths share one classification of what
  is transient.

## Choosing the Bigtable client

**Winner: `bigtable_rs` 0.4.0**, plus a direct dependency on
`googleapis-tonic-google-bigtable-v2` 0.39.0 for the raw protos.

Evaluated against a live `cbtemulator` (table + column family created with `cbt`,
one cell written at an explicit timestamp, read back through a
family → qualifier → timestamp-range → cells-per-column filter chain). Scratch
crates were built under `/tmp` and are not committed.

| Criterion | `bigtable_rs` 0.4.0 | google-cloud-rust (`google-cloud-bigtable-admin-v2` 1.13.0) | `gcloud-sdk` 0.30.2 (`google-bigtable-v2`) |
| --- | --- | --- | --- |
| (a) Raw `google.bigtable.v2` protos for `RowFilter`/`RowSet` — **REQUIRED** | **Pass.** Public API takes/returns the `googleapis-tonic-google-bigtable-v2` prost types. Built a 4-deep `Chain` by hand and it round-tripped. | **Fail — no data plane exists.** The repo generates *only* `google.bigtable.admin.v2`; there is no `google-cloud-bigtable-v2` crate on crates.io, and `src/generated/cloud` contains no bigtable directory at all. Clients are `BigtableInstanceAdmin` and `BigtableTableAdmin` only. | **Pass.** Ships `genproto/google.bigtable.v2.rs`; identical prost types. |
| (b) App-profile on ReadRows | **Pass.** `app_profile_id` field on `ReadRowsRequest`; the crate also propagates it into the `x-goog-request-params` routing header. | n/a — no ReadRows. | Pass (proto field), but nothing populates the routing header for you. |
| (c) `BIGTABLE_EMULATOR_HOST` | **Pass.** Read by `BigTableConnection::new`, which transparently swaps to a plaintext channel and skips auth. Verified end-to-end. | n/a | **Fail.** Zero occurrences of "emulator" in the crate. Its `GoogleApi` builder always attaches TLS + auth middleware, so the probe had to hand-roll a plaintext `Channel`, bypassing the SDK entirely. |
| (d) ADC / workload identity | **Pass.** Delegates to `gcp_auth` 0.12.7, which walks the full ADC chain: `GOOGLE_APPLICATION_CREDENTIALS`, gcloud user creds, and the GKE metadata server (workload identity). | Pass (`google-cloud-auth`). | Pass (own token-source layer). |
| (e) ReadRows chunk-merge maturity | **Pass.** Merges chunks itself and returns `RowCell { family_name, qualifier, value, timestamp_micros, labels }` — full cells, timestamps included. Correctness is covered by **Google's official `readrows.json` conformance suite**, vendored verbatim from `googleapis/conformance-tests` (the same fixtures `java-bigtable`'s `ReadRowsMergingAcceptanceTest` uses). | n/a | **Fail.** Hands back a raw `CellChunk` stream and nothing else. The probe printed unmerged chunks; `value_size` continuation, family/qualifier carry-forward, `commit_row` and `reset_row` would all be ours to implement. |
| (f) Maintenance signal | 0.4.0 released 2026-06-25; steady cadence (0.2.19 → 0.3.0 → 0.4.0 over 2026). 3 open issues, **none** about chunk merging. ~1.06M downloads. | 1.13.0 released 2026-07-13, actively maintained — but not for this use case. | 0.30.2 released 2026-07-24, actively maintained. ~5.5M downloads. |

### Why bigtable_rs

`bigtable_rs` is the only candidate that passes every criterion. The nominal
"google-cloud-rust Bigtable client" **does not exist** — Google's Rust SDK has
shipped Bigtable *admin* only, so it cannot serve a data plane at all; it was
never a real contender. That leaves `gcloud-sdk`, which is a fine proto
transport but stops at the wire: choosing it would mean writing and owning the
chunk-merge state machine when `bigtable_rs` already passes Google's own
conformance fixtures for exactly that. `bigtable_rs` also gives us emulator
support for free, which the entire test strategy depends on.

The costs are real but small and contained. `bigtable_rs` has no admin API, so
tests create tables via `cbt` rather than in-process. It does not re-export its
proto crate, so we depend on `googleapis-tonic-google-bigtable-v2` directly;
that version **must move in lockstep** with whatever `bigtable_rs` pulls, or we
get two incompatible copies of the same prost types. `cargo tree -i
googleapis-tonic-google-bigtable-v2` currently shows a single shared v0.39.0,
and should be re-checked after any bump.

### Toolchain floor

This dependency tree sets the toolchain floor at **1.88**.
`bigtable_rs` requires `tonic ^0.14.2`, and tonic 0.14.x declares
`rust-version = 1.88`. This is **not** a consequence of the choice: `gcloud-sdk`
resolves to the same tonic/hyper/icu tree and fails on 1.84 with an identical
`edition2024` manifest error. Any modern GCP client imposes this floor.

### The client swap boundary

**Only `client.rs` may name client-crate types.** `bigtable_rs` and
`googleapis-tonic-google-bigtable-v2` types (`BigTableConnection`, `BigTable`,
`ReadRowsRequest`, `RowCell`, `RowFilter`, `RowSet`, …) must not appear in any
other non-test module; everything else speaks our own domain types. The one
deliberate exception is the `RowFilter` proto accepted by
`BtClient::read_rows`: the emulator test harnesses build those protos for
read-back assertions, so the filter type crosses the boundary by design.
Anything beyond that means the boundary has leaked.
