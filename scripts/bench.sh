#!/usr/bin/env bash
# Usage: scripts/bench.sh [matrix.toml] [out-name]
# Thin wrapper: builds release binaries, then lets matier-bench spawn/stop the server.
# Results: bench-results/<out-name>.json
set -euo pipefail
cd "$(dirname "$0")/.."
MATRIX="${1:-bench/matrix.toml}"
NAME="${2:-$(date +%Y%m%d-%H%M%S)}"
cargo build --release -p matier-bench -p matier-bench-server
mkdir -p bench-results
./target/release/matier-bench matrix --spawn-server --config "$MATRIX" --out "bench-results/${NAME}.json"
