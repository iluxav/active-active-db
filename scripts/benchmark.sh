#!/bin/bash
set -e

# Configuration
PORT=${REDIS_PORT:-16379}
RESULTS_DIR="./benchmark-results/$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "========================================"
echo "  Counter Store Performance Benchmark"
echo "========================================"
echo "Port: $PORT"
echo "Results: $RESULTS_DIR"
echo ""

# Wait for server to be ready
echo "Waiting for server..."
until redis-cli -p $PORT PING 2>/dev/null | grep -q PONG; do
  sleep 1
done
echo "Server ready!"
echo ""

# Helper function to run benchmark and save results
run_bench() {
  local name=$1
  local file=$2
  shift 2
  echo "----------------------------------------"
  echo "[$name]"
  redis-benchmark -p $PORT "$@" | tee "$RESULTS_DIR/$file"
  echo ""
}

# Test 1: Single Key Throughput (worst-case lock contention)
run_bench "1/6 Single Key Throughput" "01-single-key.txt" \
  -t incr -n 1000000 -c 50 -q

# Test 2: Multi-Key Throughput (100k random keys)
run_bench "2/6 Multi-Key Throughput (100k keys)" "02-multi-key.txt" \
  -t incr -n 1000000 -c 50 -r 100000 -q

# Test 3: Concurrency Scaling
echo "----------------------------------------"
echo "[3/6 Concurrency Scaling]"
{
  for c in 1 10 50 100 200 500; do
    echo "  Clients: $c"
    redis-benchmark -p $PORT -t incr -n 100000 -c $c -q
  done
} | tee "$RESULTS_DIR/03-concurrency.txt"
echo ""

# Test 4: Latency Distribution (CSV for analysis)
echo "----------------------------------------"
echo "[4/6 Latency Distribution]"
redis-benchmark -p $PORT -t incr -n 100000 -c 50 --csv > "$RESULTS_DIR/04-latency.csv"
echo "Saved to 04-latency.csv"
echo ""

# Test 5: Mixed Read/Write Workload
run_bench "5/6 Mixed Read/Write" "05-mixed.txt" \
  -t incr,get -n 100000 -c 50 -r 10000 -q

# Test 6: Memory Growth (1M unique keys)
echo "----------------------------------------"
echo "[6/6 Memory Growth (1M keys)]"
redis-cli -p $PORT FLUSHALL 2>/dev/null || true
redis-benchmark -p $PORT -t incr -n 1000000 -r 1000000 -c 10 -q | tee "$RESULTS_DIR/06-memory.txt"
echo ""
echo "Final state:"
redis-cli -p $PORT DBSIZE | tee -a "$RESULTS_DIR/06-memory.txt"
redis-cli -p $PORT INFO 2>/dev/null | grep -E "^(used_memory|keys)" | tee -a "$RESULTS_DIR/06-memory.txt" || true
echo ""

echo "========================================"
echo "  Benchmark Complete!"
echo "========================================"
echo "Results saved to: $RESULTS_DIR"
echo ""
echo "Summary files:"
ls -la "$RESULTS_DIR"
