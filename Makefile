.PHONY: build build-release run run-release test test-unit test-integration bench clean \
        docker-build docker-up docker-down docker-logs docker-bench \
        redis-up redis-down redis-bench compare fmt clippy check

# Default target
all: build

# =============================================================================
# Build
# =============================================================================

build:
	cargo build

build-release:
	cargo build --release

# =============================================================================
# Run
# =============================================================================

run: build
	cargo run --bin counter-server -- config/single-node.toml

run-release: build-release
	./target/release/counter-server config/single-node.toml

# =============================================================================
# Test
# =============================================================================

test:
	cargo test

test-unit:
	cargo test --lib

test-integration:
	cargo test --test '*'

# =============================================================================
# Code Quality
# =============================================================================

fmt:
	cargo fmt

clippy:
	cargo clippy -- -D warnings

check: fmt clippy test
	@echo "All checks passed!"

# =============================================================================
# Benchmarks (Local)
# =============================================================================

bench: build-release
	@echo "Starting server..."
	@./target/release/counter-server config/single-node.toml &
	@sleep 2
	@echo "Running benchmarks..."
	@redis-benchmark -p 16379 -t incr -n 100000 -c 50 -r 10000 -q || true
	@echo "Stopping server..."
	@pkill -f "counter-server" || true

bench-pipeline: build-release
	@echo "Starting server..."
	@./target/release/counter-server config/single-node.toml &
	@sleep 2
	@echo "Pipeline 1:"
	@redis-benchmark -p 16379 -t incr -n 200000 -c 100 -r 100000 -P 1 -q || true
	@echo "Pipeline 10:"
	@redis-benchmark -p 16379 -t incr -n 200000 -c 100 -r 100000 -P 10 -q || true
	@echo "Pipeline 100:"
	@redis-benchmark -p 16379 -t incr -n 200000 -c 100 -r 100000 -P 100 -q || true
	@echo "Stopping server..."
	@pkill -f "counter-server" || true

# =============================================================================
# Docker - Counter-DB
# =============================================================================

docker-build:
	docker compose build counter-db

docker-up: docker-build
	docker compose up -d counter-db
	@echo "Waiting for server..."
	@until redis-cli -p 16379 PING 2>/dev/null | grep -q PONG; do sleep 1; done
	@echo "Counter-DB is ready on port 16379"

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f counter-db

docker-bench: docker-up
	@echo "=== Counter-DB Docker Benchmark ==="
	@docker run --rm --network active-active-db_default redis:7-alpine \
		redis-benchmark -h counter-db -p 16379 -t incr -n 200000 -c 50 -r 100000 -q

docker-bench-full: docker-up
	@echo "=== Counter-DB Full Benchmark Suite ==="
	@echo ""
	@echo "--- Concurrency Test ---"
	@for c in 1 10 50 100 200 500; do \
		echo "Clients: $$c"; \
		docker run --rm --network active-active-db_default redis:7-alpine \
			redis-benchmark -h counter-db -p 16379 -t incr -n 100000 -c $$c -r 100000 -q 2>&1 | grep "requests per second"; \
	done
	@echo ""
	@echo "--- Pipeline Test ---"
	@for p in 1 10 50 100; do \
		echo "Pipeline: $$p"; \
		docker run --rm --network active-active-db_default redis:7-alpine \
			redis-benchmark -h counter-db -p 16379 -t incr -n 200000 -c 100 -r 100000 -P $$p -q 2>&1 | grep "requests per second"; \
	done

# =============================================================================
# Docker - Redis (for comparison)
# =============================================================================

redis-up:
	docker compose -f docker-compose.redis.yml up -d
	@echo "Waiting for Redis..."
	@until redis-cli -p 16379 PING 2>/dev/null | grep -q PONG; do sleep 1; done
	@echo "Redis is ready on port 16379"

redis-down:
	docker compose -f docker-compose.redis.yml down

redis-bench: redis-up
	@echo "=== Redis Docker Benchmark ==="
	@docker run --rm --network active-active-db_default redis:7-alpine \
		redis-benchmark -h redis-bench -p 6379 -t incr -n 200000 -c 50 -r 100000 -q

redis-bench-full: redis-up
	@echo "=== Redis Full Benchmark Suite ==="
	@echo ""
	@echo "--- Concurrency Test ---"
	@for c in 1 10 50 100 200 500; do \
		echo "Clients: $$c"; \
		docker run --rm --network active-active-db_default redis:7-alpine \
			redis-benchmark -h redis-bench -p 6379 -t incr -n 100000 -c $$c -r 100000 -q 2>&1 | grep "requests per second"; \
	done
	@echo ""
	@echo "--- Pipeline Test ---"
	@for p in 1 10 50 100; do \
		echo "Pipeline: $$p"; \
		docker run --rm --network active-active-db_default redis:7-alpine \
			redis-benchmark -h redis-bench -p 6379 -t incr -n 200000 -c 100 -r 100000 -P $$p -q 2>&1 | grep "requests per second"; \
	done

# =============================================================================
# Compare Counter-DB vs Redis
# =============================================================================

compare:
	@echo "=== Performance Comparison: Counter-DB vs Redis ==="
	@echo ""
	@echo "Building Counter-DB..."
	@docker compose build counter-db > /dev/null 2>&1
	@echo ""
	@echo "--- Counter-DB ---"
	@docker compose up -d counter-db > /dev/null 2>&1
	@sleep 3
	@docker run --rm --network active-active-db_default redis:7-alpine \
		redis-benchmark -h counter-db -p 16379 -t incr -n 200000 -c 100 -r 100000 -q 2>&1 | grep "requests per second"
	@docker run --rm --network active-active-db_default redis:7-alpine \
		redis-benchmark -h counter-db -p 16379 -t incr -n 200000 -c 100 -r 100000 -P 100 -q 2>&1 | grep "requests per second"
	@docker compose down > /dev/null 2>&1
	@echo ""
	@echo "--- Redis ---"
	@docker compose -f docker-compose.redis.yml up -d > /dev/null 2>&1
	@sleep 3
	@docker run --rm --network active-active-db_default redis:7-alpine \
		redis-benchmark -h redis-bench -p 6379 -t incr -n 200000 -c 100 -r 100000 -q 2>&1 | grep "requests per second"
	@docker run --rm --network active-active-db_default redis:7-alpine \
		redis-benchmark -h redis-bench -p 6379 -t incr -n 200000 -c 100 -r 100000 -P 100 -q 2>&1 | grep "requests per second"
	@docker compose -f docker-compose.redis.yml down > /dev/null 2>&1

# =============================================================================
# Clean
# =============================================================================

clean:
	cargo clean
	rm -rf data/ redis-data/
	docker compose down -v 2>/dev/null || true
	docker compose -f docker-compose.redis.yml down -v 2>/dev/null || true

# =============================================================================
# Help
# =============================================================================

help:
	@echo "Counter-DB Makefile"
	@echo ""
	@echo "Build:"
	@echo "  make build          - Build debug version"
	@echo "  make build-release  - Build release version"
	@echo ""
	@echo "Run:"
	@echo "  make run            - Run debug server locally"
	@echo "  make run-release    - Run release server locally"
	@echo ""
	@echo "Test:"
	@echo "  make test           - Run all tests"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make check          - Run fmt, clippy, and tests"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-up      - Start Counter-DB in Docker"
	@echo "  make docker-down    - Stop Counter-DB"
	@echo "  make docker-logs    - View Counter-DB logs"
	@echo "  make docker-bench   - Quick benchmark in Docker"
	@echo "  make docker-bench-full - Full benchmark suite"
	@echo ""
	@echo "Redis (comparison):"
	@echo "  make redis-up       - Start Redis in Docker"
	@echo "  make redis-down     - Stop Redis"
	@echo "  make redis-bench    - Quick Redis benchmark"
	@echo ""
	@echo "Compare:"
	@echo "  make compare        - Run Counter-DB vs Redis comparison"
	@echo ""
	@echo "Clean:"
	@echo "  make clean          - Remove build artifacts and data"
