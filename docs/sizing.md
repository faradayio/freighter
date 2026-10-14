# Sizing a load

Freighter holds each shard whole in memory so it can sort it, and a lane reads
its next shard while writing the current one. Sizing is about fitting that in
the process's memory, then picking request size and concurrency.

You need two measurements from the [source table](preparing-a-source-table.md):

- **row bytes**: mean `len(key) + len(gzip(json))`. Use this, not BigQuery's
  logical row size, which is several times larger and says nothing about what
  is written.
- **largest bucket**: the row count of the biggest bucket. Quantile cuts are
  not equal when keys repeat, and the process runs out of memory on the
  biggest shard, not the average one.

## Request size: `flush_cells`

Pick a target request size in bytes (a few MB is typical) and divide:

```
flush_cells = min(target_request_bytes / row_bytes, 90000)
```

## Concurrency: `inflight_mutations`

One request in flight per lane:

```
inflight_mutations = flush_cells × lanes
```

## Shard size

Memory has to fit:

```
lanes × 2 × shard_bytes  +  2 × inflight_mutations × row_bytes  +  fixed
```

where, per resident row, freighter spends `row_bytes` plus about 320 bytes of
in-memory overhead, and run-to-run variation is about 30%:

```
resident_row_bytes = (row_bytes + 320) × 1.3
shard_budget       = (memory − fixed − 2 × inflight_mutations × row_bytes) / (lanes × 2)
buckets_per_shard  = floor(shard_budget / (largest_bucket × resident_row_bytes))
```

Use about 1 GB for `fixed` (runtime, gRPC channels). If `buckets_per_shard`
comes out below 1, one bucket doesn't fit: cut the source into more buckets,
or use fewer lanes.

Then cut buckets `0 .. rung-1` into consecutive shards of `buckets_per_shard`
buckets, and deal them to lanes in contiguous blocks: lane 0 takes the first
run of shards, lane 1 the next, and so on.

## Read streams

Freighter at Faraday uses 32 Storage Read streams across the process, so
`read_streams = max(32 / lanes, 1)`. More streams buy decode parallelism
until CPU runs out.

## A worked example

200 M rows, `row_bytes` = 400, largest bucket 60,000 rows, 4,000 buckets, a
24 GiB pod, 8 lanes, 4 MB requests:

- `flush_cells` = 4,000,000 / 400 = 10,000
- `inflight_mutations` = 10,000 × 8 = 80,000 (32 MB in flight)
- `resident_row_bytes` = (400 + 320) × 1.3 = 936
- `shard_budget` = (25.8 GB − 1 GB − 64 MB) / 16 ≈ 1.54 GB
- `buckets_per_shard` = 1.54 GB / (60,000 × 936) = 27, so about 149 shards

## CPU

Give the process whole CPUs with requests equal to limits. The mover sizes its
thread pool to the CPUs it sees, and running more threads than a CPU quota
allows just buys throttling.
