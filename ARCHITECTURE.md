# Architecture

## Compatibility-engine design

The engine in `schema/Compatibility.scala` is a pure-Scala function from
`(oldSchema, newSchema, level) → Either[List[Violation], Unit]`. Three
design decisions are worth calling out.

### Sealed-trait violations

Each kind of compatibility break is a case of a sealed `Violation` trait.
This is the difference between an operator who sees `incompatible: 3
problems` (useless) and one who sees:

    - **MissingDefault** (total_amount_cents): field 'total_amount_cents' added without a default
    - **RenamedWithoutAlias** (total_cents): field 'total_cents' appears to have been renamed to 'total_amount_cents'

The schema-check CLI renders this as Markdown and exits 1; the same
violations roll up into the test assertions as typed pattern matches.

### Accumulating, not short-circuiting

Most schema-evolution checkers stop on the first failure. That's wrong for
operators reviewing a schema bump: they want the whole list, so they can
do all the fixes in one pass instead of chasing one violation at a time
through CI re-runs. The engine collects violations in a builder and
returns them all (deduplicated).

### Direction-asymmetric "RemovedRequiredField"

Most rules are direction-symmetric: they produce the same violation
whether you check `(old, new)` or `(new, old)`. The "removed required
field" rule is asymmetric — it only fires in the user's stated direction
(old → new). When checking forward compatibility (which internally swaps
arguments), we suppress this rule, because a "field added in new" isn't
"removed" from anywhere; the old reader simply doesn't know about it. See
`checkRemovedRequired` for the implementation.

## Validator/transformer split

`Validator.validate` returns a `Split(valid, bad)` of two DataFrames. The
`bad` DataFrame retains the original Kafka payload bytes plus a
structured failure reason. This lets ops:

1. Re-process bad records against an updated schema without re-reading
   from Kafka. The `bad` DataFrame is `(topic, partition, offset, ts,
   payload, reason)` — `payload` is the same bytes the producer sent.
2. Tail the bad-records topic for real-time alerting (`Validator` ships
   a `foreachBatch`-style hook in production wiring; the test version
   just inspects the DataFrame).
3. Track a per-stage error rate without conflating decode failures with
   transformation failures. The aggregator only sees clean records.

The split happens BEFORE any aggregation work. Aggregation is expensive;
running it on a stream that includes corrupt records and pruning later is
both slower and harder to reason about.

## Partitioning strategy

Output is partitioned by `(date_hour, customer_segment)`. The
choices:

- **`date_hour=YYYY-MM-DD-HH`**: Athena/Trino/DuckDB query patterns are
  almost always time-bounded (`WHERE date_hour BETWEEN ...`). Hourly
  partitions are coarse enough to keep the metadata-store entry count
  manageable but fine enough that a "last 24h" query touches 24 directories.
- **`customer_segment`**: low-cardinality (3–6 values) and is the most
  common second-axis filter ("show me last week's premium-segment
  totals"). Adding it as a partition prunes almost all the data on these
  queries.

`coalesce` is set per partition cardinality (`coalesceTo` in
`ParquetSink`). This is a hand-tuned default for moderate volume — at
scale, switching to `repartition($"date_hour", $"customer_segment")`
followed by `coalesce` per writer is the right move.

## Bad-record sink

Two destinations:

1. **`s3a://<bucket>/bad_records/dt=<date>/`** — durable archive,
   partitioned daily. The schema is `(topic, partition, offset,
   timestamp, payload, reason)`. The `payload` is BinaryType so the
   Parquet writer doesn't try to interpret the bytes as UTF-8.
2. **Kafka `<topic>-bad`** — side-channel for real-time alerting. The
   pipeline emits a single per-batch summary record (count, sample
   reason). The actual fanout is wired in the production deploy, not
   here.

A record landing in `bad` is a structural error (truncation, type
mismatch, missing required no-default field). Business-rule rejection
(`amount > 0`, `customer_id is not null`) is a separate concern handled
downstream in the aggregator's WHERE clause.

## Idempotency on re-runs

Re-running the same input against the same output path uses
`mode=append`. Two consequences:

- **Re-running is safe** in the sense that no data is overwritten or
  lost. It IS unsafe in the sense that you'll get duplicate output rows
  if you re-process the same Kafka offsets.
- **Downstream dedup** is the consumer's contract. Athena/Trino queries
  should `GROUP BY (customer_id, customer_segment, window_start)` and
  pick the latest write timestamp (or use Iceberg/Delta if you need a
  proper table format with time travel — explicitly out of scope here).

The alternative — `mode=overwrite` per partition with
`spark.sql.sources.partitionOverwriteMode=dynamic` — is also reasonable.
We picked append because it composes better with at-least-once Kafka
semantics: in a partial-failure scenario, two appends are recoverable;
two overwrites can lose work.

## Batch vs structured streaming

Two entry points share the same Validator + Aggregator topology:

- **`Main.run` (batch)**: `spark.read.format("kafka")` against a fixed
  start/end offset range. Use this for backfills, replays, one-shot
  processing of a captured offset range, and any pipeline that runs as a
  cron-triggered job. Latency is whatever the job's wall-clock is —
  typically minutes.
- **`Main.streaming` (structured streaming)**: `spark.readStream` against
  the live tail of the topic, plus a `withWatermark("event_time", "1
  hour")` and a `Trigger.ProcessingTime("30 seconds")`. Output mode is
  `append`, so each closed window emits exactly once after the watermark
  passes the window end. Use this for continuous low-latency ingest
  where downstream consumers want hourly aggregates within a couple of
  minutes of real time.

The validator and aggregator have streaming-safe variants
(`StreamingValidator`, `Aggregator.aggregateStreaming`) that avoid
batch-only operations: no `kafkaDf.rdd`, no `.count()`, no
`countDistinct` (which would require unbounded state). The trade-off
documented in `Aggregator.aggregateStreaming` is that streaming output
uses `order_count` as an upper-bound proxy for `order_id_distinct`
because Spark structured streaming doesn't support distinct count in
streaming aggregations without approximate-data-sketch state.

Pick batch for **backfills, replays, idempotent reprocessing,
deterministic test fixtures**. Pick streaming for **continuous ingest,
near-real-time dashboards, alerting**. Both write to the same
`date_hour` / `customer_segment` Parquet layout, so downstream queries
don't care which entry point produced a given row.

## Iceberg vs plain Parquet

Two output sinks are wired in parallel. Both write to the same partition
layout (`date_hour`, `customer_segment`) so a query can target either.

### Plain Parquet

`s3a://<bucket>/<output>/date_hour=YYYY-MM-DD-HH/customer_segment=…/part-*.parquet`

- One file tree, no metadata layer.
- Partial writes are visible (a Spark task that fails halfway leaves its
  output files in the directory; downstream readers see them until a
  cleanup job removes them).
- Schema evolution is producer-side only (this project's compatibility
  engine). Renaming a column requires rewriting historical data.
- Best for: single-writer pipelines, bounded blast radius, downstream
  consumers that already do their own dedup or use a separate catalog
  layer.

### Iceberg (HadoopCatalog)

`<warehouse>/<db>/<table>/data/...` plus
`<warehouse>/<db>/<table>/metadata/snap-*.avro`,
`<warehouse>/<db>/<table>/metadata/manifest-list-*.avro`,
`<warehouse>/<db>/<table>/metadata/version-hint.text`

The `IcebergSink` uses `org.apache.iceberg:iceberg-spark-runtime-3.5_2.13`
configured against a HadoopCatalog rooted at `SPARK_EVOLVE_ICEBERG_WAREHOUSE`.
HadoopCatalog is the simplest catalog: it stores all catalog state in
files under the warehouse path. There's no external metastore (no Hive,
no Glue, no REST), which makes it ideal for tests and for greenfield
deploys where you don't yet have a metastore.

What it buys you over plain Parquet:

- **ACID writes**: a commit produces a new snapshot file; readers always
  see a consistent view, never a half-written one.
- **Time travel**: every snapshot has an ID; query against any prior
  snapshot via `spark.read.option("snapshot-id", id).format("iceberg")`.
- **Schema evolution without data rewrite**: add/rename/drop columns in
  the table metadata. Existing Parquet files don't change. Reads
  reconcile field IDs across the metadata revision.
- **Concurrent writers**: optimistic-concurrency commit retries. Two
  pipelines writing the same partition concurrently produce two snapshots,
  not corrupted data.
- **Hidden partitioning**: producers can change partitioning without
  breaking consumer queries. (The pipeline declares
  `(date_hour, customer_segment)` for now; switching to
  `(bucket(customer_id, 16), date_hour)` later is a metadata-only
  operation.)

What it costs:

- **Metadata overhead**: every commit writes one snapshot + manifest-list
  + manifest file. For tiny tables this dominates. The Iceberg roundtrip
  test in `IcebergSinkSpec` exercises this by writing 50 rows and reading
  them back, which still runs in a few seconds — overhead is real but
  not blocking.
- **Catalog dependency**: HadoopCatalog requires the writer and all
  readers to share access to the same warehouse path. This is fine on
  S3/MinIO but constrains where the table can be queried from.
- **Maintenance jobs**: snapshot-expire, orphan-file cleanup, and rewrite
  manifests are not automatic; long-running tables accumulate metadata
  fast enough that operators need a periodic compaction job.

The pipeline's default is plain Parquet — the Iceberg dual-write turns
on only when `SPARK_EVOLVE_ICEBERG_WAREHOUSE` is set. This is deliberate:
Iceberg is the right answer for many production deploys, but adopting
it means committing to its operational surface area, and that's a
deliberate decision per deploy, not a default the framework should make.

## What's deliberately not here

- **Confluent wire format.** Records aren't prefixed with a 5-byte magic
  + schema ID. We use the registered subject's latest schema as the
  reader and rely on the on-disk registry. Adding the wire-format
  prefix is a per-record `getInt(payload[1..5])` lookup.
- **Schema migrations.** The registry only tracks versions; there's no
  migration runner that rewrites historical Parquet under a new schema.
- **Cross-record validation.** No referential checks (`customer_id must
  exist in customers table`). The validator is purely structural.
- **Lineage / data-catalog integration.** Glue, OpenLineage, Marquez,
  etc. all reasonable next steps, none implemented.
