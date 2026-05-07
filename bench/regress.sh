#!/usr/bin/env bash
# Bench regression gate.
#
# Compares the most recent run in bench/results/ against bench/results/sample.json.
# Fails if events_per_sec drops below (1 - DRIFT) * baseline. Default DRIFT=0.30
# (i.e., a 30% slowdown trips the gate). Override with DRIFT=0.50 etc.
#
# Usage:
#   ./bench/regress.sh                  # check latest non-sample json
#   ./bench/regress.sh path/to/run.json # check a specific file
#
# Requires: jq, bash 4+.
set -euo pipefail

BASELINE="${BASELINE:-bench/results/sample.json}"
DRIFT="${DRIFT:-0.30}"

if [ ! -f "$BASELINE" ]; then
  echo "regress: baseline $BASELINE not found" >&2
  exit 2
fi

CANDIDATE="${1:-}"
if [ -z "$CANDIDATE" ]; then
  CANDIDATE=$(ls -t bench/results/*.json 2>/dev/null | grep -v "sample.json" | head -1 || true)
  if [ -z "$CANDIDATE" ]; then
    echo "regress: no candidate run found in bench/results/ (excluding sample.json)" >&2
    exit 2
  fi
fi

if [ ! -f "$CANDIDATE" ]; then
  echo "regress: candidate $CANDIDATE not found" >&2
  exit 2
fi

baseline_eps=$(jq -r '.events_per_sec' "$BASELINE")
candidate_eps=$(jq -r '.events_per_sec' "$CANDIDATE")

if ! [[ "$baseline_eps" =~ ^[0-9]+$ ]] || ! [[ "$candidate_eps" =~ ^[0-9]+$ ]]; then
  echo "regress: events_per_sec missing or non-integer in one of the result files" >&2
  exit 2
fi

# Compute floor = baseline * (1 - DRIFT) using awk for portable float math.
floor=$(awk -v b="$baseline_eps" -v d="$DRIFT" 'BEGIN { printf "%.0f", b * (1 - d) }')

echo "baseline:   $baseline_eps events/sec ($BASELINE)"
echo "candidate:  $candidate_eps events/sec ($CANDIDATE)"
echo "drift cap:  ${DRIFT} (floor = $floor events/sec)"

if [ "$candidate_eps" -lt "$floor" ]; then
  echo "REGRESSION: candidate is below floor."
  exit 1
fi

echo "OK: within drift band."
