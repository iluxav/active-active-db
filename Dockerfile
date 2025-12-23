# Multi-stage build for a2db
FROM rust:1.83-slim as builder

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy source
COPY . .

# Build release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary and config
COPY --from=builder /app/target/release/a2db /usr/local/bin/
COPY --from=builder /app/config/bench-persist.toml /etc/a2db/config.toml

# Create data directory
RUN mkdir -p /data

EXPOSE 16379 19000 19001

CMD ["a2db", "--config", "/etc/a2db/config.toml"]
