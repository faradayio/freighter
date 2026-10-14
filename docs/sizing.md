# Sizing a load

Freighter holds each shard whole in memory so it can sort it, and a lane reads
its next shard while writing the current one. Sizing is about fitting that in
the process's memory, then picking request size and concurrency.
`freighter plan` does it from the [source record](preparing-a-source-table.md#the-source-record)
and writes the manifests.

## `freighter plan`

```bash
freighter plan --source source.json --lanes 8 --memory 24Gi \
  --bt-project my-project --bt-instance my-instance \
  --destination-table my_table --app-profile bulk-write --out-dir load
```

| Flag | Default | Meaning |
|---|---|---|
| `--source <path>` | required | the source record `prepare` wrote |
| `--lanes <n>` | required | concurrent lanes in the mover process |
| `--memory <quantity>` | required | memory the mover process gets, as a Kubernetes quantity in `Gi` or `Mi` (`24Gi`, `1536Mi`). Anything else is refused: `24G` would size the process 7% small |
| `--bt-project`, `--bt-instance` | required | the Bigtable instance |
| `--destination-table <t>` | required | the Bigtable table. It must already exist with the column family |
| `--app-profile <a>` | required | the app profile to write through. A low-priority one lets serving reads win |
| `--family <f>` | `data` | column family of the one cell |
| `--qualifier <q>` | `jsonz` | column qualifier of the one cell |
| `--compression <c>` | `gzip` | `gzip` or `none`: the cell value's encoding |
| `--target-request-bytes <n>` | `4000000` | target bytes per `MutateRows` request |
| `--max-flush-cells <n>` | `90000` | ceiling on rows per request, at most 90,000 |
| `--read-streams <n>` | `32` | Storage Read streams for the whole process, shared out per lane |
| `--ramp <fractions>` | `1.0` | fractions of the shards per step, e.g. `0.05,0.10,0.85`. See [Ramp](#ramp) |
| `--cell-timestamp-micros <t>` | now | the one timestamp every cell carries. See [Re-running a load](#re-running-a-load) |
| `--out-dir <dir>` | `.` | where the manifests go. Use a fresh one per attempt |

It writes `<out-dir>/manifest.json`, or one `step-N/manifest.json` per step
of a ramp, and logs each step's lanes, shard count and in-flight budget:

```
plan: step 0 — 8 lanes, 149 shards, 80000 cells in flight
```

Every manifest passes through the mover's own validator before it is written.
`plan` refuses to overwrite a manifest that is already there: a directory that
holds a manifest also holds that attempt's verdicts.

## What `plan` computes

### Row bytes

Everything below is sized from `row_bytes`, the mean bytes one row adds to a
write, which the source record holds for both encodings:

- `--compression gzip`: `gzip_bytes_per_row`, the mean `len(key) +
  len(gzip(json))`.
- `--compression none`: `raw_bytes_per_row`, the mean `len(key) + len(json)`.

Sizing a `none` load from gzipped bytes would under-size memory by the
compression ratio, often 4 to 10 times. BigQuery's logical row size is not used
at all: it is several times larger and says nothing about what is written.

The other input is `max_rows_per_bucket`. The process runs out of memory on
the biggest shard, not the average one, so every bucket is sized as if it
were the largest.

### Request size: `flush_cells`

```
flush_cells = min(max(floor(target_request_bytes / row_bytes), 1), max_flush_cells)
```

### Concurrency: `inflight_mutations`

One request in flight per lane:

```
inflight_mutations = flush_cells × lanes
```

### Shard size

Memory has to fit two resident shards per lane, the in-flight requests and a
fixed allowance:

```
lanes × 2 × shard_bytes  +  2 × inflight_mutations × row_bytes  +  fixed
```

The in-flight budget counts twice because a slice's decoded rows and its
serialized request are both resident until the request returns. Per resident
row, freighter spends `row_bytes` plus about 320 bytes of in-memory overhead,
and run-to-run variation is about 30%. `fixed` is 1 GB (10⁹ bytes) for the
runtime and gRPC channels:

```
resident_row_bytes = (row_bytes + 320) × 1.3
shard_budget       = (memory − 1 GB − 2 × inflight_mutations × row_bytes) / (lanes × 2)
buckets_per_shard  = floor(shard_budget / (max_rows_per_bucket × resident_row_bytes))
```

`plan` refuses, with exit 2, in two cases:

- **The memory is gone before any shard.** `memory` is no larger than the
  fixed allowance plus the in-flight requests. Raise `--memory` or lower
  `--lanes`.
- **One bucket does not fit.** `buckets_per_shard` comes out below 1. Rebuild
  the source with more buckets, or use fewer lanes.

Then it cuts buckets `0 .. buckets-1` into consecutive shards of
`buckets_per_shard` buckets, and deals them to lanes in contiguous blocks:
lane 0 takes the first run of shards, lane 1 the next, and so on.

### Read streams

```
read_streams = max(floor(32 / lanes), 1)
```

The 32 is `--read-streams`, a total for the process. Giving each lane an equal
share keeps decode parallelism from growing with the lane count. More streams
buy decode parallelism until CPU runs out.

## A worked example

200 M rows in 4,000 buckets, `row_bytes` = 400, largest bucket 60,000 rows, a
24 GiB process, 8 lanes, 4 MB requests:

- `flush_cells` = 4,000,000 / 400 = 10,000
- `inflight_mutations` = 10,000 × 8 = 80,000 (32 MB in flight)
- `resident_row_bytes` = (400 + 320) × 1.3 = 936
- `shard_budget` = (25.77 GB − 1 GB − 64 MB) / 16 ≈ 1.54 GB
- `buckets_per_shard` = 1.54 GB / (60,000 × 936) = 27, so 149 shards, 18 or
  19 to a lane
- `read_streams` = 32 / 8 = 4

`plan --lanes 8 --memory 24Gi` on a source record with those numbers writes
exactly that manifest.

## Ramp

A table that serves live reads is safer loaded in steps: a small part of the
load at one lane, then more at half the lanes, then the rest at full rate,
with a check between steps ([Operating a load](operating.md)). `--ramp` takes
one to three fractions that sum to 1:

```bash
freighter plan ... --lanes 8 --ramp 0.05,0.10,0.85 --out-dir load
```

```
plan: step 0 — 1 lanes, 8 shards, 10000 cells in flight
plan: step 1 — 4 lanes, 15 shards, 40000 cells in flight
plan: step 2 — 8 lanes, 126 shards, 80000 cells in flight
```

- **Lanes climb 1, then half, then all**, counted from the end: three
  fractions run 1 → `lanes/2` → `lanes`, two run `lanes/2` → `lanes`, and the
  default `1.0` is one full-rate step. A step never has more lanes than
  shards.
- **Shards are cut in order.** Step 0 takes the first shards, rounding up, and
  every step but the last gets at least one. Shard indexes are global across
  steps, so step 1 above starts at shard 8.
- **Every step gets its own directory**: `load/step-0/manifest.json`,
  `load/step-1/manifest.json`, `load/step-2/manifest.json`. The mover writes
  verdicts beside its manifest, and `verify` reads only that directory. Two
  steps in one directory would see each other's verdicts as strays.
- **All steps share one cell timestamp** and the same sizing. Only lanes,
  shards and `inflight_mutations` differ. Memory is sized for the full lane
  count, so earlier steps run with room to spare.

## Re-running a load

`plan` picks the cell timestamp once, from the clock, truncated to whole
milliseconds, and writes it into every step's manifest. One `plan` is one
load. To plan a re-run of the same load, pass the first plan's timestamp so
rewritten cells land on the same version instead of adding one:

```bash
jq .cell_timestamp_micros load/manifest.json
freighter plan ... --cell-timestamp-micros 1792003349318000 --out-dir load-rerun-1
```

The value is microseconds since the epoch and a whole number of
milliseconds, as in the manifest. A value in the wrong unit (seconds or
milliseconds) is refused at the flag with exit 2; the mover applies the same
rule to the manifest. Nothing is written:

```
error: invalid value '1792003349' for '--cell-timestamp-micros <CELL_TIMESTAMP_MICROS>': cell timestamp 1792003349 is not microseconds since the epoch (accepted: 2001-09-09 to 2286-11-20); seconds or milliseconds would stamp cells in 1970
```

See [Operating a load](operating.md#re-running-failed-shards).

## CPU

Give the process whole CPUs with requests equal to limits. The mover sizes its
thread pool to the CPUs it sees, and running more threads than a CPU quota
allows just buys throttling.
