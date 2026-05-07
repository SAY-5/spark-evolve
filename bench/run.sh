#!/usr/bin/env bash
# Wrapper around the BenchHarness Scala main. Invoked by `make bench` and CI.
# Usage: ./bench/run.sh <event-count>
set -euo pipefail

EVENTS="${1:-100000}"
OUT_DIR="/tmp/spark-evolve-bench-out"
RESULTS_DIR="bench/results"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR" "$RESULTS_DIR"

echo "running bench with EVENTS=$EVENTS"

# Use sbt runMain so we don't need an assembly jar yet.
sbt -batch "runMain com.say5.spark_evolve.BenchHarness $EVENTS $OUT_DIR $RESULTS_DIR"
