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
| Column | `<family>:jsonz` (family from the manifest) |
| Value | gzip of the `json` string (readable by Java's `GZIPInputStream`) |
| Timestamp | one timestamp for the whole load, from the manifest |

A NULL `json` deletes the row when the manifest sets `null_json_deletes_row`,
and fails the shard otherwise.

## Status

v0.1 is the loader engine. Preparing the source table, cutting it into shards
and checking the verdicts against the source are done by you, with the SQL in
[docs/preparing-a-source-table.md](docs/preparing-a-source-table.md) and
[docs/verifying-a-load.md](docs/verifying-a-load.md). `freighter prepare`,
`plan` and `verify` subcommands that do this for you are planned.

## Install

Download a binary from [Releases](https://github.com/faradayio/freighter/releases),
use the container image `ghcr.io/faradayio/freighter`, or build it:

```bash
cargo install bq-freighter   # installs the `freighter` binary
```

## Usage

```bash
freighter gs://my-bucket/loads/2026-10-14/manifest.json
```

The manifest (`gs://` or `file://`) says what to read, where to write, and how
to cut the work. Verdicts land next to it as `shard_0000.json`,
`shard_0001.json`, …

Exit codes: `2` when the manifest could not be read or was refused; `1` when the
BigQuery or Bigtable connection could not be opened, a lane stopped (three
consecutive failed shards in one lane), or a verdict could not be written; `0`
otherwise. **Exit 0 does not mean every shard landed** — a
lone failed shard gets a `failed` verdict and its lane moves on. The verdicts,
checked against the source, are the only success criterion
([docs/verifying-a-load.md](docs/verifying-a-load.md)).

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

[examples/k8s/job.yaml](examples/k8s/job.yaml) runs it as a Kubernetes Job.

## Try it locally

Against the Bigtable emulator, with a source table you own:

```bash
gcloud components install beta bigtable cbt
gcloud beta emulators bigtable start --host-port=localhost:8086 &
export BIGTABLE_EMULATOR_HOST=localhost:8086
cbt -project example-project -instance example-instance createtable example_table
cbt -project example-project -instance example-instance createfamily example_table data

# Build a source table (docs/preparing-a-source-table.md), point a copy of
# freighter/fixtures/freighter-lookup-manifest-example.json at it, then:
freighter file://$PWD/manifest.json
cat shard_0000.json
cbt -project example-project -instance example-instance count example_table
```

With `BIGTABLE_EMULATOR_HOST` set, writes go to the emulator; reads still come
from real BigQuery.

## Documentation

- [Concepts](docs/concepts.md) — buckets, shards, lanes, verdicts and the rest
  of the vocabulary
- [Preparing a source table](docs/preparing-a-source-table.md)
- [Manifest reference](docs/manifest.md)
- [Verdict reference](docs/verdicts.md)
- [Sizing a load](docs/sizing.md)
- [Verifying a load](docs/verifying-a-load.md)
- [Design notes](docs/design.md) — why it is built the way it is
- [Contributing](CONTRIBUTING.md) — building and testing

## Support

Issues and pull requests are welcome. There is no support SLA.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
