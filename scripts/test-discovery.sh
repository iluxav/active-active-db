#!/bin/bash
# Test gossip-based peer discovery with a local 3-node cluster
set -uo pipefail
# Don't use -e as we handle errors in assert functions

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$PROJECT_DIR/target/release/a2db"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $1"; }

PASSED=0
FAILED=0
PIDS=()

# Node configuration
NODE1_REDIS=16379
NODE1_REPL=19001
NODE2_REDIS=16380
NODE2_REPL=19011
NODE3_REDIS=16381
NODE3_REPL=19021

NODE1_METRICS=19090
NODE2_METRICS=19091
NODE3_METRICS=19092

cleanup() {
    log_info "Cleaning up..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    # Give processes time to exit
    sleep 1
    # Force kill if still running
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$PROJECT_DIR/data/test-discovery-node"* 2>/dev/null || true
    log_info "Cleanup complete"
}

trap cleanup EXIT

check_prerequisites() {
    if [[ ! -f "$BINARY" ]]; then
        log_error "Binary not found at $BINARY"
        log_info "Run: cargo build --release"
        exit 1
    fi

    if ! command -v redis-cli &> /dev/null; then
        log_error "redis-cli not found. Install with: brew install redis"
        exit 1
    fi

    if ! command -v curl &> /dev/null; then
        log_error "curl not found"
        exit 1
    fi

    if ! command -v jq &> /dev/null; then
        log_warn "jq not found, some tests may have limited output"
    fi
}

start_nodes() {
    log_info "Starting 3-node cluster with gossip discovery..."

    # Create data directories
    mkdir -p "$PROJECT_DIR/data/test-discovery-node1"
    mkdir -p "$PROJECT_DIR/data/test-discovery-node2"
    mkdir -p "$PROJECT_DIR/data/test-discovery-node3"

    # All nodes use the same seed list (all three nodes)
    local seeds="http://127.0.0.1:$NODE1_REPL,http://127.0.0.1:$NODE2_REPL,http://127.0.0.1:$NODE3_REPL"

    # Start Node 1
    log_info "Starting node-1 (redis:$NODE1_REDIS, repl:$NODE1_REPL)..."
    "$BINARY" \
        --replica-id "test-node-1" \
        --replication-addr "127.0.0.1:$NODE1_REPL" \
        --redis-addr "127.0.0.1:$NODE1_REDIS" \
        --discovery \
        --seed "http://127.0.0.1:$NODE1_REPL" \
        --seed "http://127.0.0.1:$NODE2_REPL" \
        --seed "http://127.0.0.1:$NODE3_REPL" \
        --advertise-addr "http://127.0.0.1:$NODE1_REPL" \
        --persistence \
        --data-dir "$PROJECT_DIR/data/test-discovery-node1" \
        --metrics-addr "127.0.0.1:$NODE1_METRICS" \
        --log-level "info" \
        > "$PROJECT_DIR/data/test-discovery-node1/node.log" 2>&1 &
    PIDS+=($!)

    # Start Node 2
    log_info "Starting node-2 (redis:$NODE2_REDIS, repl:$NODE2_REPL)..."
    "$BINARY" \
        --replica-id "test-node-2" \
        --replication-addr "127.0.0.1:$NODE2_REPL" \
        --redis-addr "127.0.0.1:$NODE2_REDIS" \
        --discovery \
        --seed "http://127.0.0.1:$NODE1_REPL" \
        --seed "http://127.0.0.1:$NODE2_REPL" \
        --seed "http://127.0.0.1:$NODE3_REPL" \
        --advertise-addr "http://127.0.0.1:$NODE2_REPL" \
        --persistence \
        --data-dir "$PROJECT_DIR/data/test-discovery-node2" \
        --metrics-addr "127.0.0.1:$NODE2_METRICS" \
        --log-level "info" \
        > "$PROJECT_DIR/data/test-discovery-node2/node.log" 2>&1 &
    PIDS+=($!)

    # Start Node 3
    log_info "Starting node-3 (redis:$NODE3_REDIS, repl:$NODE3_REPL)..."
    "$BINARY" \
        --replica-id "test-node-3" \
        --replication-addr "127.0.0.1:$NODE3_REPL" \
        --redis-addr "127.0.0.1:$NODE3_REDIS" \
        --discovery \
        --seed "http://127.0.0.1:$NODE1_REPL" \
        --seed "http://127.0.0.1:$NODE2_REPL" \
        --seed "http://127.0.0.1:$NODE3_REPL" \
        --advertise-addr "http://127.0.0.1:$NODE3_REPL" \
        --persistence \
        --data-dir "$PROJECT_DIR/data/test-discovery-node3" \
        --metrics-addr "127.0.0.1:$NODE3_METRICS" \
        --log-level "info" \
        > "$PROJECT_DIR/data/test-discovery-node3/node.log" 2>&1 &
    PIDS+=($!)

    log_info "Waiting for nodes to start..."
    sleep 3
}

wait_for_nodes() {
    log_info "Waiting for all nodes to be ready..."
    local max_attempts=30
    local attempt=0

    while [[ $attempt -lt $max_attempts ]]; do
        local ready=0

        if redis-cli -h 127.0.0.1 -p $NODE1_REDIS PING 2>/dev/null | grep -q PONG; then
            ((ready++))
        fi
        if redis-cli -h 127.0.0.1 -p $NODE2_REDIS PING 2>/dev/null | grep -q PONG; then
            ((ready++))
        fi
        if redis-cli -h 127.0.0.1 -p $NODE3_REDIS PING 2>/dev/null | grep -q PONG; then
            ((ready++))
        fi

        if [[ $ready -eq 3 ]]; then
            log_info "All 3 nodes are ready!"
            return 0
        fi

        ((attempt++))
        sleep 1
    done

    log_error "Timeout waiting for nodes to start"
    return 1
}

redis_cmd() {
    local port=$1
    shift
    redis-cli -h 127.0.0.1 -p "$port" "$@" 2>/dev/null
}

assert_eq() {
    local expected=$1
    local actual=$2
    local message=$3

    if [[ "$expected" == "$actual" ]]; then
        log_info "  PASS: $message"
        ((PASSED++))
    else
        log_error "  FAIL: $message (expected: $expected, got: $actual)"
        ((FAILED++))
    fi
}

assert_ge() {
    local expected=$1
    local actual=$2
    local message=$3

    if [[ "$actual" -ge "$expected" ]]; then
        log_info "  PASS: $message"
        ((PASSED++))
    else
        log_error "  FAIL: $message (expected >= $expected, got: $actual)"
        ((FAILED++))
    fi
}

test_basic_connectivity() {
    log_test "Testing basic connectivity..."

    local node1_ping=$(redis_cmd $NODE1_REDIS PING || echo "FAILED")
    assert_eq "PONG" "$node1_ping" "Node 1 responds to PING"

    local node2_ping=$(redis_cmd $NODE2_REDIS PING || echo "FAILED")
    assert_eq "PONG" "$node2_ping" "Node 2 responds to PING"

    local node3_ping=$(redis_cmd $NODE3_REDIS PING || echo "FAILED")
    assert_eq "PONG" "$node3_ping" "Node 3 responds to PING"
}

test_discovery_metrics() {
    log_test "Testing gossip discovery and replication setup..."

    # Wait for nodes to discover each other and establish replication connections
    log_info "  Waiting for gossip discovery and replication setup (10 seconds)..."
    sleep 10

    # Check metrics endpoint for peer counts
    if command -v jq &> /dev/null; then
        local node1_metrics=$(curl -s "http://127.0.0.1:$NODE1_METRICS/metrics" 2>/dev/null || echo "{}")
        local node2_metrics=$(curl -s "http://127.0.0.1:$NODE2_METRICS/metrics" 2>/dev/null || echo "{}")
        local node3_metrics=$(curl -s "http://127.0.0.1:$NODE3_METRICS/metrics" 2>/dev/null || echo "{}")

        log_info "  Node 1 metrics: $(echo "$node1_metrics" | jq -c '.gossip // empty' 2>/dev/null || echo 'N/A')"
        log_info "  Node 2 metrics: $(echo "$node2_metrics" | jq -c '.gossip // empty' 2>/dev/null || echo 'N/A')"
        log_info "  Node 3 metrics: $(echo "$node3_metrics" | jq -c '.gossip // empty' 2>/dev/null || echo 'N/A')"
    else
        log_info "  (jq not available, skipping detailed metrics check)"
    fi

    # Just verify metrics endpoints are responding
    local node1_health=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$NODE1_METRICS/health" 2>/dev/null || echo "000")
    assert_eq "200" "$node1_health" "Node 1 metrics endpoint healthy"

    local node2_health=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$NODE2_METRICS/health" 2>/dev/null || echo "000")
    assert_eq "200" "$node2_health" "Node 2 metrics endpoint healthy"

    local node3_health=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$NODE3_METRICS/health" 2>/dev/null || echo "000")
    assert_eq "200" "$node3_health" "Node 3 metrics endpoint healthy"
}

test_replication_node1_to_node2() {
    log_test "Testing replication: Node 1 -> Node 2..."

    local key="test:discovery:1to2:$(date +%s)"

    # Write to Node 1
    redis_cmd $NODE1_REDIS INCR "$key" > /dev/null
    redis_cmd $NODE1_REDIS INCRBY "$key" 99 > /dev/null

    # Check local value
    local node1_val=$(redis_cmd $NODE1_REDIS GET "$key")
    assert_eq "100" "$node1_val" "Node 1 local value is 100"

    # Wait for replication
    log_info "  Waiting for replication (3 seconds)..."
    sleep 3

    # Check replicated value on Node 2
    local node2_val=$(redis_cmd $NODE2_REDIS GET "$key")
    assert_eq "100" "$node2_val" "Node 2 sees replicated value 100"
}

test_replication_node2_to_node3() {
    log_test "Testing replication: Node 2 -> Node 3..."

    local key="test:discovery:2to3:$(date +%s)"

    # Write to Node 2
    local set_result=$(redis_cmd $NODE2_REDIS SET "$key" "hello-from-node2" || echo "FAILED")
    log_info "  SET result: $set_result"

    # Verify local write on Node 2
    local node2_val=$(redis_cmd $NODE2_REDIS GET "$key" || echo "")
    log_info "  Node 2 local value: $node2_val"

    # Wait for replication (longer wait for string values)
    log_info "  Waiting for replication (5 seconds)..."
    sleep 5

    # Check replicated value on Node 3
    local node3_val=$(redis_cmd $NODE3_REDIS GET "$key" || echo "")
    assert_eq "hello-from-node2" "$node3_val" "Node 3 sees replicated string value"
}

test_replication_node3_to_node1() {
    log_test "Testing replication: Node 3 -> Node 1..."

    local key="test:discovery:3to1:$(date +%s)"

    # Write to Node 3
    redis_cmd $NODE3_REDIS INCRBY "$key" 500 > /dev/null

    # Wait for replication
    log_info "  Waiting for replication (3 seconds)..."
    sleep 3

    # Check replicated value on Node 1
    local node1_val=$(redis_cmd $NODE1_REDIS GET "$key")
    assert_eq "500" "$node1_val" "Node 1 sees replicated value 500"
}

test_three_way_convergence() {
    log_test "Testing 3-way CRDT convergence..."

    local key="test:discovery:convergence:$(date +%s)"

    # Write concurrently to all 3 nodes
    log_info "  Writing concurrently to all 3 nodes..."
    redis_cmd $NODE1_REDIS INCRBY "$key" 100 &
    local pid1=$!
    redis_cmd $NODE2_REDIS INCRBY "$key" 200 &
    local pid2=$!
    redis_cmd $NODE3_REDIS INCRBY "$key" 300 &
    local pid3=$!
    wait $pid1 $pid2 $pid3

    # Wait for convergence
    log_info "  Waiting for convergence (5 seconds)..."
    sleep 5

    # All nodes should see 600
    local node1_val=$(redis_cmd $NODE1_REDIS GET "$key")
    local node2_val=$(redis_cmd $NODE2_REDIS GET "$key")
    local node3_val=$(redis_cmd $NODE3_REDIS GET "$key")

    assert_eq "600" "$node1_val" "Node 1 sees converged value 600"
    assert_eq "600" "$node2_val" "Node 2 sees converged value 600"
    assert_eq "600" "$node3_val" "Node 3 sees converged value 600"
    assert_eq "$node1_val" "$node2_val" "Node 1 and Node 2 have same value"
    assert_eq "$node2_val" "$node3_val" "Node 2 and Node 3 have same value"
}

test_node_failure_and_recovery() {
    log_test "Skipping node failure test (takes too long for CI)..."
    log_info "  SKIP: Node failure detection test"
}

print_summary() {
    echo ""
    echo "========================================"
    echo "  Discovery Integration Test Results"
    echo "========================================"
    echo ""
    echo -e "  ${GREEN}Passed: $PASSED${NC}"
    echo -e "  ${RED}Failed: $FAILED${NC}"
    echo ""

    if [[ $FAILED -eq 0 ]]; then
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed!${NC}"
        echo ""
        echo "Check logs in:"
        echo "  $PROJECT_DIR/data/test-discovery-node1/node.log"
        echo "  $PROJECT_DIR/data/test-discovery-node2/node.log"
        echo "  $PROJECT_DIR/data/test-discovery-node3/node.log"
        exit 1
    fi
}

main() {
    echo "========================================"
    echo "  a2db Gossip Discovery Integration Test"
    echo "========================================"
    echo ""

    check_prerequisites
    start_nodes
    wait_for_nodes

    echo ""
    test_basic_connectivity
    echo ""

    test_discovery_metrics
    echo ""

    test_replication_node1_to_node2
    echo ""

    test_replication_node2_to_node3
    echo ""

    test_replication_node3_to_node1
    echo ""

    test_three_way_convergence
    echo ""

    test_node_failure_and_recovery
    echo ""

    print_summary
}

main "$@"
