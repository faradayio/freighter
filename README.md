# freighter

Bulk-load a BigQuery table into Bigtable in key order, and get a per-shard
verdict you can check against the source.

Freighter reads a prepared BigQuery table one key range (a *shard*) at a time
through the Storage Read API, sorts each shard, and writes it to Bigtable in
ascending key order — the write pattern Bigtable handles most cheaply. For
every shard it writes a JSON *verdict* with rows read and cells acknowledged,
so "did everything land?" is a count comparison, not a guess.

Built for and run in production at [Faraday](https://faraday.ai), where it
loads tables of hundreds of millions of rows into a Bigtable instance that is
serving live reads at the same time.

## When to use it

Use freighter instead of Dataflow's BigQuery-to-Bigtable template when:

- you do large, periodic, full reloads of a table,
- into a Bigtable instance that is also serving live traffic,
- and you want verified counts and safe relaunch after a failure.

Don't use it for:

- **Streaming or incremental upserts.** Freighter loads whatever rows the
  source table holds. An incremental update works if you build a source table
  of just the changed rows yourself.
- **Multi-cell rows.** Each source row becomes exactly one cell (see below).
- **Anything outside GCP.** It talks to BigQuery, Bigtable and GCS only.

## What it does to your data

The source table has a `key` STRING, a `json` STRING, and an integer *bucket
column* that preserves key order. Each row becomes one Bigtable cell:

| | |
|---|---|
| Row key | the `key` string's UTF-8 bytes, unchanged |
| Column | `<family>:<qualifier>`, from the manifest (`data:jsonz` by default) |
| Value | gzip of the `json` string (readable by Java's `GZIPInputStream`), or the string's bytes as they are with `compression: none` |
| Timestamp | one timestamp for the whole load, from the manifest |

A NULL `json` deletes the row when the manifest sets `null_json_deletes_row`,
and fails the shard otherwise.

## Status

v0.2 is the loader plus the commands that plan and check a load. A load is
`prepare`, `plan`, `move`, `verify`: four commands, each one stage, with a
JSON file between each stage and the next. The
[quickstart](docs/quickstart.md) runs all four.

## Install

Download a binary from [Releases](https://github.com/faradayio/freighter/releases),
use the container image `ghcr.io/faradayio/freighter`, or build it:

```bash
cargo install bq-freighter   # installs the `freighter` binary
```

## Usage

```bash
freighter prepare --query-file q.sql --table my-project.my_dataset.freighter_source --out source.json
freighter plan    --source source.json --lanes 8 --memory 24Gi \
                  --bt-project my-project --bt-instance my-instance \
                  --destination-table my_table --app-profile bulk-write --out-dir load
freighter move    gs://my-bucket/loads/2026-10-14/manifest.json
freighter verify  gs://my-bucket/loads/2026-10-14/manifest.json --out report.json
```

- **`prepare`** builds the source table from any query that yields
  `key STRING, json STRING`, and writes a *source record*: the table, its
  bucket boundaries and the row sizes `plan` needs
  ([Preparing a source table](docs/preparing-a-source-table.md)).
- **`plan`** sizes the load for a given memory and lane count and writes the
  manifest, or one manifest per step of a ramp ([Sizing a load](docs/sizing.md)).
  Copy it to wherever the mover will run.
- **`move`** runs a manifest (`gs://` or `file://`). Verdicts land next to it as
  `shard_0000.json`, `shard_0001.json`, … `freighter <manifest-uri>` with no
  subcommand does the same.
- **`verify`** counts the source table per bucket, compares the counts with the
  verdicts, and writes a report ([Verifying a load](docs/verifying-a-load.md)).

`freighter validate <file://… | ->` checks a manifest with the mover's own
validator, without credentials or network: silent with exit 0 when the mover
would accept it, exit 2 with the reason on stderr when it would not.

Exit codes: `2` when you have something to fix (a manifest that could not be
read or was refused, a table that already exists, a bad flag); `1` when
something broke (the BigQuery or Bigtable connection could not be opened, a
lane stopped after three consecutive failed shards, or a verdict could not be
written); `0` otherwise. Running `freighter` with no arguments exits 2.
**Exit 0 from `move` does not mean every shard landed**: a lone failed shard
gets a `failed` verdict and its lane moves on. The verdicts, checked against
the source, are the only success criterion, and that check is `verify`. It
exits 0 only when every shard checks out, 1 when some do not, and 2 when the
check itself could not run.

Credentials come from Application Default Credentials:
`GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account key, the file
`gcloud auth application-default login` writes, or the GKE metadata server
(Workload Identity). Workload Identity Federation credential files
(`external_account`) are not supported for the manifest's GCS access yet.
The identity needs `roles/bigquery.readSessionUser` on the billing project,
`roles/bigquery.dataViewer` on the source table, `roles/bigtable.user` on the
instance, and `roles/storage.objectViewer` plus `roles/storage.objectCreator`
on the manifest's bucket. Verdicts are written beside the manifest; if you
re-run into a prefix that already holds verdicts, overwriting them also needs
delete, so use `roles/storage.objectUser` there.
`prepare` and `verify` also run BigQuery jobs, so the identity that runs them
needs `roles/bigquery.jobUser` on the billing project and
`roles/bigquery.dataEditor` on the dataset `prepare` writes to.

[examples/k8s/job.yaml](examples/k8s/job.yaml) runs it as a Kubernetes Job.

## Try it locally

The [quickstart](docs/quickstart.md) runs a whole load into the Bigtable
emulator. With `BIGTABLE_EMULATOR_HOST` set, writes go to the emulator, but
BigQuery is always real: you need a project where you can create a table.

## Documentation

- [Quickstart](docs/quickstart.md) — a whole load against the Bigtable emulator
- [Concepts](docs/concepts.md) — buckets, shards, lanes, verdicts and the rest
  of the vocabulary
- [Preparing a source table](docs/preparing-a-source-table.md)
- [Manifest reference](docs/manifest.md)
- [Verdict reference](docs/verdicts.md)
- [Sizing a load](docs/sizing.md)
- [Verifying a load](docs/verifying-a-load.md)
- [Operating a load](docs/operating.md) — ramps, re-runs and stopping, for a
  table that serves live reads
- [Design notes](docs/design.md) — why it is built the way it is
- [Contributing](CONTRIBUTING.md) — building and testing

## Support

Issues and pull requests are welcome. There is no support SLA.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
