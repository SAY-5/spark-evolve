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

    make bench-smoke    # 10k events, ~5s (CI sanity)
    make bench-1m       # 1M events, ~10-30s on a laptop (the committed baseline)
    make bench          # alias for bench-1m
    make bench-regress  # compare latest run vs sample.json; fail on >=30% drop

Results JSON lands in `bench/results/<timestamp>.json`. The committed
`bench/results/sample.json` is the 1M-event reference run that backs the README
numbers. CI runs the 10k smoke on every push and asserts the result file has
the expected keys; the regress gate is also exercised in CI but at a loose
DRIFT setting (smoke vs 1M sample is not apples-to-apples — JIT warmup
dominates at small scales).

## bench-regress gate

`bench/regress.sh` reads the most recent `bench/results/*.json` (excluding
`sample.json`), pulls `events_per_sec`, and compares against the baseline. If
the candidate is below `(1 - DRIFT) * baseline_eps` the script exits non-zero.
Default DRIFT is 0.30 (30%). Override with `DRIFT=0.50` for a looser gate or
`DRIFT=0.10` for a tighter one.

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
