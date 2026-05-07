# spark-evolve

Scala/Spark batch pipeline that consumes Avro-encoded events from Kafka,
validates each record against a registered Avro schema with codified
backward-compatibility evolution rules, computes per-key tumbling-window
aggregates, and writes partitioned Parquet to an S3-compatible store
(MinIO locally; AWS S3 in prod). Bad records are dead-lettered to a
separate sink with the original payload and a structured failure reason.

The load-bearing piece is the schema-evolution validator: a separately
testable library that decides whether a new schema can replace an old one
under a stated compatibility level.

## What this studies

- Codifying schema-evolution rules as a typed engine — sealed-trait
  violations, accumulating across all rules instead of short-circuiting on
  the first one.
- Splitting the validation step from the transformation step so ops can
  re-validate raw payloads against a new schema without re-reading from
  Kafka.
- Bad-record dead-lettering as a first-class sink, not a try/catch
  afterthought — every malformed record retains its original bytes plus the
  structured reason.

## Compatibility rules (Backward)

The rule engine is in `src/main/scala/com/say5/spark_evolve/schema/`.
Backward = a reader using the new schema can read data written under the
old schema. The change table:

| Change                                              | Verdict |
|-----------------------------------------------------|---------|
| Adding a field with a default value                 | OK      |
| Adding a union field that includes `null`           | OK      |
| Removing a field that had a default value           | OK      |
| Widening numeric types (`int → long`, `float → double`, …) | OK      |
| Adding a field without a default                    | rejected |
| Removing a field that had no default                | rejected |
| Renaming a field without `aliases`                  | rejected |
| Narrowing types                                     | rejected |
| Changing a non-nullable field to nullable WITHOUT a default | rejected |

Forward and Full are also implemented; see `Compatibility.scala`.

## Modules

| Path                                        | Contents                                                         |
|---------------------------------------------|------------------------------------------------------------------|
| `schema/Compatibility.scala`                | Rule engine, accumulates `List[Violation]`                       |
| `schema/SchemaRegistry.scala`               | File-backed `<root>/<subject>/v<n>.avsc` registry                |
| `schema/Violation.scala`                    | Sealed-trait of compatibility violations                         |
| `ingest/KafkaSource.scala`                  | Batch read from Kafka                                            |
| `transform/Validator.scala`                 | Per-record decode → split into valid / bad                       |
| `transform/Aggregator.scala`                | Group-by `(customer, segment, window)` + metrics                 |
| `transform/ParquetSink.scala`               | Partitioned Parquet writer                                       |
| `obs/Metrics.scala`                         | Spark accumulator counters                                       |
| `Main.scala`                                | Entry point + `schema check` CLI                                 |
| `BenchHarness.scala`                        | In-process bench harness backing `make bench`                    |

## Quickstart

Prereqs: JDK 17, sbt, Docker (for the live Kafka + MinIO stack).

```bash
# Unit tests, no Docker required.
make test

# Live demo: brings up Kafka + MinIO, runs the bench through the pipeline.
make up
make bench-smoke
make down

# Schema-compatibility CLI.
make schema-check-ok    # bundled v1 → v2 (compatible)
make schema-check-bad   # bundled v2 → v3 (incompatible — exits 1)
```

## Real bench numbers

Local-machine numbers from a single run on a developer workstation
(Apple M-series, JDK 17, `local[*]`, in-process — not cluster mode).
The committed JSON: [`bench/results/sample.json`](bench/results/sample.json).

| metric              | value         |
|---------------------|---------------|
| events              | 1,000,000     |
| events_per_sec      | ~107,000      |
| ms_total            | 9,345         |
| ms_validate         | 1,754         |
| ms_aggregate        | 1,113         |
| ms_write            | 2,494         |
| parquet_files       | 24            |
| mean_parquet_bytes  | 14,725        |

The 1M-event run finishes in under 10 wall-clock seconds on a developer
laptop. Throughput is bounded by the per-record Avro decode in the validator
plus Parquet write back-pressure, both of which scale with cluster size in
production. These numbers demonstrate end-to-end correctness at a meaningful
volume; cluster numbers are not projected from them — see `bench/README.md`
for the caveats and `make bench-regress` for the drift-gate the CI uses.

## Architecture

```
                        ┌─────────────────────┐
                        │   Schema Registry   │
                        │  (file-backed dir)  │
                        └──────────┬──────────┘
                                   │ (latest)
                                   ▼
  Kafka ───►  Validator  ──── valid ───►  Aggregator  ──►  Parquet on S3 (MinIO/AWS)
  topic       │                                              partitioned by
              │  bad                                         date_hour, customer_segment
              ▼
         bad_records sink
         (S3 + side-channel topic)
```

The schema registry is read once at startup and held in memory. The
validator's reader schema is the latest registered version of the subject;
records written under earlier compatible versions are read with default
filling.

## What this is NOT

- **Streaming.** Batch only. The structured-streaming swap is a one-line
  change to `readStream` in `KafkaSource`. Document, don't ship.
- **HTTP schema registry.** No Confluent registry HTTP server; the registry
  is the bundled file-backed dir. Confluent's wire format isn't implemented.
- **Iceberg/Delta/Hudi.** Plain Parquet partitioned by `date_hour` and
  `customer_segment`. Table format is the consumer's choice.
- **Exactly-once at the Kafka boundary.** This is at-least-once with
  idempotent writes; downstream dedup is the consumer's contract.
- **Provisioned AWS.** `infra/aws/` is a documented stub. No Terraform run.
- **Continuous deployment.** CI builds + tests + assembles the jar. There's
  no auto-deploy.
- **Schema-history queries.** The registry tracks versions but doesn't
  expose a "give me the schema at offset X" API.

## License

MIT. See [LICENSE](LICENSE).
