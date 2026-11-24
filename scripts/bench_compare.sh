#!/usr/bin/env bash
set -euo pipefail

# Head-to-head CLI benchmark comparing juicefs-rs against Go juicefs (when installed).
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

echo "Building juicefs-rs in --release mode..."
cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml" -p juicefs-cli
RS_BIN="$ROOT_DIR/target/release/juicefs"

dd if=/dev/urandom of="$WORK_DIR/payload_16m.bin" bs=1048576 count=16 2>/dev/null

run_suite() {
  local label="$1"
  local bin="$2"
  local sub="$WORK_DIR/$label"
  mkdir -p "$sub/data"
  local meta="sqlite3://$sub/meta.db"

  echo ""
  echo "=== $label ==="
  "$bin" format "$meta" "benchvol" --storage file --bucket "$sub/data" --compress lz4 >/dev/null
  "$bin" mkdir -p "$meta" /bench

  local t0 t1
  t0=$(python3 -c 'import time; print(time.perf_counter())' 2>/dev/null || date +%s.%N)
  "$bin" put "$meta" "$WORK_DIR/payload_16m.bin" /bench/payload.bin >/dev/null
  t1=$(python3 -c 'import time; print(time.perf_counter())' 2>/dev/null || date +%s.%N)
  echo "put 16 MiB: $(awk "BEGIN {printf \"%.3f s\", $t1 - $t0}")"

  t0=$(python3 -c 'import time; print(time.perf_counter())' 2>/dev/null || date +%s.%N)
  "$bin" get "$meta" /bench/payload.bin "$sub/out.bin" >/dev/null
  t1=$(python3 -c 'import time; print(time.perf_counter())' 2>/dev/null || date +%s.%N)
  echo "get 16 MiB: $(awk "BEGIN {printf \"%.3f s\", $t1 - $t0}")"
}

run_suite "juicefs-rs" "$RS_BIN"

if command -v juicefs >/dev/null 2>&1; then
  run_suite "juicefs-go" "$(command -v juicefs)"
else
  echo ""
  echo "Note: Go 'juicefs' binary not found in PATH; install Go/JuiceFS to run side-by-side Go comparison."
fi
