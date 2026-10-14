# Concepts

Terms in the order you meet them, from the BigQuery side to the Bigtable side.

```
source table ──cut into──▶ buckets (one value of the bucket column each)
                              │
                     a run of buckets = one shard  ──▶ one verdict
                              │
                 shards are dealt into lanes (run concurrently)
                              │
               a shard is read, sorted, and written as slices
                              │
                     ascending key order ──▶ Bigtable tablets
```

**Source table.** A BigQuery table with one row per Bigtable row: a STRING
row-key column, a STRING `json` column, and an INT64 **bucket column**. It is
integer-range partitioned on the bucket column, so reading a bucket range
prunes to exactly those partitions, and clustered on the key. `freighter
prepare` builds it, along with a *source record* describing it. See
[Preparing a source table](preparing-a-source-table.md).

**Row key.** The key string's UTF-8 bytes become the Bigtable row key as they
are. Freighter never rewrites keys.

**Bucket.** One value of the bucket column. Bucket *i* holds one contiguous
run of the sorted keys (a quantile cut), so every key in bucket 3 sorts after
every key in bucket 2. The bucket column must be a quantile of the key,
**never a hash of it**: a hashed column passes every check and quietly turns
ordered writes into random ones.

**Rung.** How many buckets the source table was cut into. It is recorded in
the manifest so a reader of a manifest that carries only some of the shards
can still tell the full cut. The mover never reads it.

**Shard.** The mover's unit of work: an inclusive range `[lo, hi]` of buckets,
which is one contiguous key range. A shard is read in one BigQuery Storage
Read session, held in memory whole, sorted, written, and given one verdict.

**Lane.** A sequential track of shards inside one mover process. Shards in a
lane run in listed order, strictly ascending and disjoint; distinct lanes run
concurrently. A lane reads its *next* shard while writing the current one, so
two shards are resident per lane. Give each lane one contiguous *block* of
shards (band-spread): the lanes' write cursors then sit in distant parts of
the keyspace. Dealing shards round-robin would park every cursor on the same
few tablets.

**Read streams.** Parallel Storage Read streams per shard. This is also the
decode (gzip) parallelism.

**Re-open.** A read stream that delivers nothing for 90 seconds is re-opened
at the row offset it had reached, with no loss or duplication. In production
about 5% of streams go silent for around 13 minutes; without re-opens a load
waits them out.

**Slice.** A run of `flush_cells` consecutive rows of a sorted shard, sent as
one Bigtable `MutateRows` request. Slices go out in key order, so each tablet
sees append-order writes.

**In-flight mutation budget.** One pod-wide cap (`inflight_mutations`) on
cells sent but not yet acknowledged. Divided by `flush_cells`, it is the pod's
request concurrency. When it is full, decoding waits, and then reading waits:
this is the backpressure.

**Channel rotation.** Every 30 minutes the mover swaps in fresh BigQuery and
Bigtable connection pools, ahead of Google's roughly 60-minute connection
lifetime.

**Cell timestamp.** One timestamp, picked once per *load*, carried by every
cell. Re-running a shard rewrites identical cells at an identical timestamp,
which is what makes a relaunch safe. Picking a new timestamp on a retry would
create a second version of every cell, so persist it with the load. `freighter
plan` picks it once per invocation and writes it into every manifest it
produces; a re-run passes it back with `--cell-timestamp-micros`.

**Manifest.** The versioned JSON document handed to the mover: source,
destination, shards and their lanes, tuning knobs, and the cell timestamp.
`freighter plan` writes it. The mover is its only validator and refuses
anything it doesn't recognize; `freighter validate` runs that validator alone.
See the [Manifest reference](manifest.md).

**Verdict.** A per-shard JSON result (`shard_NNNN.json`, written beside the
manifest): rows read, cells acknowledged and failed, retries, timings, and
`complete` or `failed`. The mover *reports*; you *judge*. See the
[Verdict reference](verdicts.md).

**Verification.** For each shard, compare the cells it acknowledged with a
count of source rows over its buckets. A shard with a missing, failed or
mismatched verdict gets re-run. This is the load's only success criterion.
`freighter verify` does it and writes a *verify report*. See
[Verifying a load](verifying-a-load.md).

**`expect_empty`.** A per-shard flag saying the shard may read zero rows.
Without it an empty shard fails, because an empty read usually means the
source changed under the load.

**Circuit breaker.** Three consecutive failed shards stop *that lane*. Other
lanes finish, and the process exits non-zero naming every lane that stopped.

**Ramp.** Splitting one load into consecutive runs, e.g. 5% / 10% / 85% of
the shards at 1 lane, then half the lanes, then all of them, with a check
between runs. It's a rollout practice for writing into a table that serves
live traffic. Each step is its own manifest and its own mover run.
`freighter plan --ramp 0.05,0.10,0.85` produces the steps, each in its own
directory. See [Sizing a load](sizing.md#ramp) and
[Operating a load](operating.md).

**App profile.** The Bigtable app profile the mover writes through. A
low-priority profile lets serving reads win when the cluster is busy.
