#!/usr/bin/env bash
set -euo pipefail

# Head-to-head CLI benchmark comparing juicefs-rs against Go juicefs.
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

echo "Building juicefs-rs in --release mode..."
cargo build --release --manifest-path "$ROOT_DIR/Cargo.toml" -p juicefs-cli
RS_BIN="$ROOT_DIR/target/release/juicefs"

# Prepare 100 x 128 KiB files (12.5 MiB) for sync benchmark + 16 MiB file for VFS put/get
mkdir -p "$WORK_DIR/sync_src"
for i in $(seq 1 100); do
  dd if=/dev/urandom of="$WORK_DIR/sync_src/file_$(printf '%03d' "$i").bin" bs=131072 count=1 2>/dev/null
done
dd if=/dev/urandom of="$WORK_DIR/payload_16m.bin" bs=1048576 count=16 2>/dev/null

now_sec() {
  python3 -c 'import time; print(time.perf_counter())' 2>/dev/null || date +%s.%N
}

elapsed() {
  awk "BEGIN {printf \"%.3f s\", $2 - $1}"
}

run_common_bench() {
  local label="$1"
  local bin="$2"
  local sub="$WORK_DIR/$label"
  mkdir -p "$sub/data" "$sub/sync_dst"
  local meta="sqlite3://$sub/meta.db"

  echo ""
  echo "=== $label ==="
  local t0 t1
  t0=$(now_sec)
  "$bin" format "$meta" "benchvol" --storage file --bucket "$sub/data" --compress lz4 >/dev/null 2>&1
  t1=$(now_sec)
  echo "format (sqlite3+lz4): $(elapsed "$t0" "$t1")"

  t0=$(now_sec)
  "$bin" status "$meta" >/dev/null 2>&1
  t1=$(now_sec)
  echo "status:               $(elapsed "$t0" "$t1")"

  t0=$(now_sec)
  "$bin" fsck "$meta" >/dev/null 2>&1
  t1=$(now_sec)
  echo "fsck:                 $(elapsed "$t0" "$t1")"

  t0=$(now_sec)
  "$bin" sync --threads 8 "file://$WORK_DIR/sync_src/" "file://$sub/sync_dst/" >/dev/null 2>&1
  t1=$(now_sec)
  echo "sync 100x128KiB:      $(elapsed "$t0" "$t1")"
}

run_common_bench "juicefs-rs" "$RS_BIN"

# Also measure juicefs-rs VFS put/get 16 MiB
RS_META="sqlite3://$WORK_DIR/juicefs-rs/meta.db"
"$RS_BIN" mkdir -p "$RS_META" /bench
t0=$(now_sec)
"$RS_BIN" put "$RS_META" "$WORK_DIR/payload_16m.bin" /bench/payload.bin >/dev/null
t1=$(now_sec)
echo "vfs put 16 MiB:       $(elapsed "$t0" "$t1")"
t0=$(now_sec)
"$RS_BIN" get "$RS_META" /bench/payload.bin "$WORK_DIR/juicefs-rs/out.bin" >/dev/null
t1=$(now_sec)
echo "vfs get 16 MiB:       $(elapsed "$t0" "$t1")"

if command -v juicefs >/dev/null 2>&1; then
  run_common_bench "juicefs-go" "$(command -v juicefs)"
else
  echo ""
  echo "Note: Go 'juicefs' binary not found in PATH."
fi
