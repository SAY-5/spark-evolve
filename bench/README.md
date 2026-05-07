# Bench

This bench is local-machine only. It measures the in-process throughput of the
validate → aggregate → Parquet-write chain against synthetically generated
Avro events. It does NOT exercise the Kafka path; that's covered separately by
the integration test (`src/it/scala`).

## What it measures

- `events_per_sec`     — events processed end-to-end per wall-clock second
- `ms_validate`        — Avro decode + bad-record split (per-record cost)
- `ms_aggregate`       — Spark grouping + windowing
- `ms_write`           — Parquet partitioned write
- `mean_parquet_bytes` — average per-file Parquet size after coalesce

## How to run

    make bench-smoke   # 5k events, ~10s
    make bench         # 100k events, takes a few minutes on a laptop

Results JSON lands in `bench/results/<timestamp>.json`. The committed
`bench/results/sample.json` is the run that backs the README numbers and is
intended to be reproducible by checking out the same commit.

## What it doesn't measure

- Kafka producer throughput
- Network roundtrip to MinIO/S3
- Cluster shuffle behaviour at scale
- Garbage collection over a long-running streaming job

These are all real concerns for a production deploy. They are not what this
project is studying — see ARCHITECTURE.md for the deliberate scope.

## Caveats

- Numbers are from a developer workstation (Apple M-series, JDK 17,
  `local[*]`). They are not representative of cluster-mode execution.
- The synthetic data has uniform key distribution and a small key cardinality
  (1k customers); real workloads will skew differently.
- Bench file IO targets a local FS, not S3 — S3 latency is the dominant factor
  in real deploys.
