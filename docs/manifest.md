# Manifest reference

A manifest is one JSON object. Unknown fields are refused (except inside a
shard's `range`), and the mover is its only validator. `freighter plan` writes
manifests; `freighter validate` runs the validator on its own, so you can check
one you wrote or edited by hand without credentials:

```bash
freighter validate file://$PWD/manifest.json   # silent, exit 0, when the mover would accept it
freighter validate - < manifest.json           # the same, from stdin
```

A complete example lives at [freighter/fixtures/freighter-lookup-manifest-example.json](../freighter/fixtures/freighter-lookup-manifest-example.json).

The current version is **4**, which `plan` writes. The mover reads versions 3
and 4: a version 3 manifest is read as version 4 with `qualifier` and
`compression` at their defaults, so a manifest written for an older mover
still runs. A manifest with any other `version` is refused before the rest is
parsed.

## Source

| Field | Type | Meaning |
|---|---|---|
| `version` | integer | `4`, or `3` |
| `source_table` | string | `projects/<p>/datasets/<d>/tables/<t>` |
| `billing_project` | string | project the Storage Read sessions are billed to |
| `rowkey_column` | string | the STRING key column |
| `range_column` | string | the INT64 bucket column. Must differ from `rowkey_column`, and must preserve key order ([why](concepts.md)) |
| `rung` | integer | how many buckets the source was cut into. Recorded for readers; the mover ignores it |
| `null_json_deletes_row` | bool | `true`: a NULL `json` deletes that Bigtable row. `false`: a NULL `json` fails the shard |

## Destination

| Field | Type | Meaning |
|---|---|---|
| `bt_project` | string | Bigtable project |
| `bt_instance` | string | Bigtable instance |
| `destination_table` | string | Bigtable table; must already exist with `family` |
| `family` | string | column family the cell is written under |
| `qualifier` | string | column qualifier of the cell. Optional, default `jsonz`; must be non-empty |
| `compression` | string | the cell value: `gzip` (default) for the gzipped `json` string, `none` for its bytes as they are |
| `app_profile` | string | Bigtable app profile to write through |
| `cell_timestamp_micros` | integer | the one timestamp every cell carries |

`cell_timestamp_micros` must be positive, a whole number of milliseconds
(Bigtable stores millisecond granularity), and between 2001-09-09 and
2286-11-20 in microseconds. The range catches unit mistakes: a timestamp in
seconds or milliseconds would stamp cells in 1970, where they lose to every
existing cell, so the load would succeed and change nothing. One in
nanoseconds would beat every later load.

## Shards

`shards` is a non-empty array of:

| Field | Type | Meaning |
|---|---|---|
| `index` | integer | unique; names the verdict file `shard_<index:04>.json` |
| `range` | object | `{"kind": "int", "lo": <i64>, "hi": <i64>}`, inclusive bucket range, `lo <= hi` |
| `expect_empty` | bool | the shard may legitimately read zero rows |
| `lane` | integer | which lane runs this shard |

Lane rules, checked only when some shard has `lane > 0`:

- Lanes are dense from 0: no gaps below the highest lane.
- Within a lane, shards are listed with strictly ascending, disjoint ranges.

Disjointness *across* lanes is not checked. Two lanes writing overlapping
ranges just write the same cells twice, and the counts still match.

When every shard is in lane 0, any order is allowed.

## Tuning

| Field | Type | Meaning |
|---|---|---|
| `read_streams` | integer > 0 | Storage Read streams per shard; also decode parallelism |
| `flush_cells` | integer in (0, 90000] | rows per `MutateRows` request. Bigtable's limit is 100,000 mutations |
| `inflight_mutations` | integer ≥ `flush_cells` | pod-wide cap on cells sent but not acknowledged |

`inflight_mutations / flush_cells` is the number of requests the process keeps
in flight. Setting `inflight_mutations = flush_cells × lanes` gives one request
per lane. See [Sizing a load](sizing.md).

## Refusal messages

Every refusal exits with code 2 and logs `manifest: <reason>`; `validate`
prints the same line on stderr. For example:

```
manifest: version 2 unsupported; this mover understands 3 or 4
manifest: duplicate shard index 4 — verdicts land at shard_0004.json and would overwrite
manifest: lane 1 shard 7 starts at 40, not after the lane's previous hi 52 — within-lane ranges must be listed strictly ascending and disjoint (append order)
manifest: inflight_mutations 4000 < flush_cells 8000 — the budget can never admit one full flush; the write would wait on it forever
```
