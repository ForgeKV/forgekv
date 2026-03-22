#!/usr/bin/env bash
# =============================================================================
# Redis vs Dragonfly vs ForgeKV — head-to-head benchmark
# Usage: bash run.sh [--skip-build] [--test-time N]
# =============================================================================
set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULTS_FILE="$RESULTS_DIR/bench_$TIMESTAMP.md"

PORT_SSDREDIS=6399
PORT_REDIS=6398
PORT_DRAGONFLY=6397

CONTAINER_SSDREDIS="bench-forgekv"
CONTAINER_REDIS="bench-redis"
CONTAINER_DRAGONFLY="bench-dragonfly"
NETWORK="bench-net"

IMAGE_SSDREDIS="forgekv:bench"

TEST_TIME=20
SKIP_BUILD=0
PIPELINE=16
VALUE_SIZE=64

# Benchmark matrix: "threads clients"
CONFIGS=(
  "1 10"
  "1 20"
  "2 10"
  "2 20"
  "4 10"
  "4 50"
)

# ── Arg parsing ───────────────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --skip-build)   SKIP_BUILD=1 ;;
    --test-time=*)  TEST_TIME="${arg#*=}" ;;
    --test-time)    shift; TEST_TIME="$1" ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

log()     { echo -e "${CYAN}[bench]${RESET} $*"; }
ok()      { echo -e "${GREEN}[  ok ]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[ warn]${RESET} $*"; }
die()     { echo -e "${RED}[fail ]${RESET} $*" >&2; exit 1; }
header()  { echo -e "\n${BOLD}━━━  $*  ━━━${RESET}"; }

cleanup() {
  log "Cleaning up containers..."
  docker rm -f "$CONTAINER_SSDREDIS" "$CONTAINER_REDIS" "$CONTAINER_DRAGONFLY" 2>/dev/null || true
  docker network rm "$NETWORK" 2>/dev/null || true
  rm -f "$TMPCONF_UNIX" 2>/dev/null || true
}
TMPCONF_UNIX=""  # will be set later; pre-declare for trap
trap cleanup EXIT

wait_ready() {
  local name=$1 port=$2
  local tries=0
  while ! docker run --rm --network host redis:7 redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; do
    tries=$((tries + 1))
    [[ $tries -gt 30 ]] && die "$name did not become ready on port $port"
    sleep 1
  done
  ok "$name ready on port $port"
}

# Extract SET ops/sec from memtier output line: "Sets  XXXXX.XX ..."
parse_ops() { echo "$1" | awk '{printf "%.0f", $2}'; }
parse_p99() { echo "$1" | awk '{printf "%.2f", $8}'; }
parse_avg() { echo "$1" | awk '{printf "%.3f", $5}'; }

run_memtier() {
  local port=$1 threads=$2 clients=$3
  docker run --rm --network host \
    redislabs/memtier_benchmark \
    -h 127.0.0.1 -p "$port" \
    --test-time="$TEST_TIME" \
    -c "$clients" -t "$threads" \
    --pipeline="$PIPELINE" \
    -d "$VALUE_SIZE" \
    2>&1
}

# ── Build ForgeKV image ─────────────────────────────────────────────────────
header "Step 1 — Build ForgeKV"
if [[ $SKIP_BUILD -eq 1 ]]; then
  warn "Skipping build (--skip-build)"
  docker image inspect "$IMAGE_SSDREDIS" &>/dev/null || die "Image $IMAGE_SSDREDIS not found. Run without --skip-build first."
else
  log "Building ForgeKV image from $REPO_ROOT/rust ..."
  docker build -t "$IMAGE_SSDREDIS" "$REPO_ROOT/rust"
  ok "Build complete"
fi

# ── Network ───────────────────────────────────────────────────────────────────
header "Step 2 — Create Docker network"
docker network create "$NETWORK" 2>/dev/null || true
ok "Network $NETWORK ready"

# ── Start servers ─────────────────────────────────────────────────────────────
header "Step 3 — Start servers"

log "Starting ForgeKV on port $PORT_SSDREDIS..."
# Use LOCALAPPDATA/Temp so Docker Desktop (Windows) can mount the file
WINTEMP="${LOCALAPPDATA:-$USERPROFILE/AppData/Local}/Temp"
TMPCONF_UNIX="$(cygpath -u "$WINTEMP" 2>/dev/null || echo "$WINTEMP")/forgekv-bench-$$.conf"
TMPCONF_WIN="$(cygpath -w "$TMPCONF_UNIX" 2>/dev/null || echo "$TMPCONF_UNIX")"
printf 'bind 0.0.0.0\nport %s\ndir /data\ndatabases 16\n' "$PORT_SSDREDIS" > "$TMPCONF_UNIX"
docker run -d --name "$CONTAINER_SSDREDIS" \
  --network host \
  -v "${TMPCONF_WIN}:/app/forgekv.conf:ro" \
  "$IMAGE_SSDREDIS" \
  > /dev/null

log "Starting Redis 7 on port $PORT_REDIS..."
docker run -d --name "$CONTAINER_REDIS" \
  --network host \
  redis:7 \
  redis-server --port "$PORT_REDIS" --save "" --appendonly no \
  > /dev/null

log "Starting Dragonfly on port $PORT_DRAGONFLY..."
docker run -d --name "$CONTAINER_DRAGONFLY" \
  --network host \
  docker.dragonflydb.io/dragonflydb/dragonfly \
  --port "$PORT_DRAGONFLY" \
  > /dev/null

wait_ready "ForgeKV"  "$PORT_SSDREDIS"
wait_ready "Redis 7"    "$PORT_REDIS"
wait_ready "Dragonfly"  "$PORT_DRAGONFLY"

# ── Run benchmarks ────────────────────────────────────────────────────────────
header "Step 4 — Benchmarking (test-time=${TEST_TIME}s per config)"

mkdir -p "$RESULTS_DIR"

declare -A SSD_OPS SSD_AVG SSD_P99
declare -A RDS_OPS RDS_AVG RDS_P99
declare -A DFY_OPS DFY_AVG DFY_P99

total=${#CONFIGS[@]}
idx=0
for cfg in "${CONFIGS[@]}"; do
  t=$(echo "$cfg" | cut -d' ' -f1)
  c=$(echo "$cfg" | cut -d' ' -f2)
  key="${t}x${c}"
  idx=$((idx + 1))
  echo ""
  log "[$idx/$total] t=$t c=$c  ($(( t * c )) total connections)"

  printf "  ForgeKV    ... "
  raw=$(run_memtier "$PORT_SSDREDIS" "$t" "$c")
  line=$(echo "$raw" | grep "^Sets")
  SSD_OPS[$key]=$(parse_ops "$line")
  SSD_AVG[$key]=$(parse_avg "$line")
  SSD_P99[$key]=$(parse_p99 "$line")
  echo "${SSD_OPS[$key]} ops/s  avg=${SSD_AVG[$key]}ms  p99=${SSD_P99[$key]}ms"

  printf "  Redis 7    ... "
  raw=$(run_memtier "$PORT_REDIS" "$t" "$c")
  line=$(echo "$raw" | grep "^Sets")
  RDS_OPS[$key]=$(parse_ops "$line")
  RDS_AVG[$key]=$(parse_avg "$line")
  RDS_P99[$key]=$(parse_p99 "$line")
  echo "${RDS_OPS[$key]} ops/s  avg=${RDS_AVG[$key]}ms  p99=${RDS_P99[$key]}ms"

  printf "  Dragonfly  ... "
  raw=$(run_memtier "$PORT_DRAGONFLY" "$t" "$c")
  line=$(echo "$raw" | grep "^Sets")
  DFY_OPS[$key]=$(parse_ops "$line")
  DFY_AVG[$key]=$(parse_avg "$line")
  DFY_P99[$key]=$(parse_p99 "$line")
  echo "${DFY_OPS[$key]} ops/s  avg=${DFY_AVG[$key]}ms  p99=${DFY_P99[$key]}ms"
done

# ── Print results table ───────────────────────────────────────────────────────
header "Step 5 — Results"

print_table() {
  local metric=$1   # "ops" "avg" "p99"
  local label=$2

  echo ""
  echo "### $label"
  echo ""
  printf "| %-12s | %10s | %10s | %10s | %8s | %8s |\n" \
    "Config" "ForgeKV" "Redis 7" "Dragonfly" "vs Redis" "vs Dfly"
  printf "|%s|%s|%s|%s|%s|%s|\n" \
    "$(printf '%0.s-' {1..14})" "$(printf '%0.s-' {1..12})" \
    "$(printf '%0.s-' {1..12})" "$(printf '%0.s-' {1..12})" \
    "$(printf '%0.s-' {1..10})" "$(printf '%0.s-' {1..10})"

  for cfg in "${CONFIGS[@]}"; do
    t=$(echo "$cfg" | cut -d' ' -f1)
    c=$(echo "$cfg" | cut -d' ' -f2)
    key="${t}x${c}"

    if [[ $metric == "ops" ]]; then
      ssd=${SSD_OPS[$key]}; rds=${RDS_OPS[$key]}; dfy=${DFY_OPS[$key]}
      fmt="%10.0f"
      vs_r=$(awk "BEGIN{printf \"%.0f%%\", ($ssd/$rds)*100}")
      vs_d=$(awk "BEGIN{printf \"%.0f%%\", ($ssd/$dfy)*100}")
    elif [[ $metric == "avg" ]]; then
      ssd=${SSD_AVG[$key]}; rds=${RDS_AVG[$key]}; dfy=${DFY_AVG[$key]}
      fmt="%10s"
      vs_r=$(awk "BEGIN{printf \"%.0f%%\", ($ssd/$rds)*100}")
      vs_d=$(awk "BEGIN{printf \"%.0f%%\", ($ssd/$dfy)*100}")
    else
      ssd=${SSD_P99[$key]}; rds=${RDS_P99[$key]}; dfy=${DFY_P99[$key]}
      fmt="%10s"
      vs_r=$(awk "BEGIN{printf \"%.0f%%\", ($ssd/$rds)*100}")
      vs_d=$(awk "BEGIN{printf \"%.0f%%\", ($ssd/$dfy)*100}")
    fi

    printf "| t=%-2s c=%-4s | $fmt | $fmt | $fmt | %8s | %8s |\n" \
      "$t" "$c" "$ssd" "$rds" "$dfy" "$vs_r" "$vs_d"
  done
}

print_table ops  "SET throughput (ops/sec, higher is better)"
print_table avg  "Average latency ms (lower is better)"
print_table p99  "p99 latency ms (lower is better)"

# ── Save markdown report ──────────────────────────────────────────────────────
{
  echo "# Benchmark: ForgeKV vs Redis 7 vs Dragonfly"
  echo ""
  echo "**Date:** $(date '+%Y-%m-%d %H:%M:%S')"
  echo "**Test time per config:** ${TEST_TIME}s"
  echo "**Pipeline:** $PIPELINE  |  **Value size:** ${VALUE_SIZE}B"
  echo "**Configs:** threads × clients/thread"
  echo ""
  print_table ops  "SET throughput (ops/sec, higher is better)"
  print_table avg  "Average latency ms (lower is better)"
  print_table p99  "p99 latency ms (lower is better)"
  echo ""
  echo "---"
  echo "*vs Redis / vs Dragonfly = ForgeKV ÷ competitor × 100%  (>100% = ForgeKV wins)*"
} > "$RESULTS_FILE"

echo ""
ok "Results saved to: $RESULTS_FILE"
echo ""
