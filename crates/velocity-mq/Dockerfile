# ==============================================================================
# VelocityMQ Ultra-Lightweight Production Dockerfile - Phase 12
# Multi-stage build leveraging cargo-chef for aggressive dependency caching.
# Target: x86_64-unknown-linux-musl on Alpine runtime (<50MB image size).
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Cargo Chef Planner
# Analyzes Cargo.toml & Cargo.lock to create a minimal dependency recipe.json.
# ------------------------------------------------------------------------------
FROM lukemathwalker/cargo-chef:latest-rust-alpine AS planner
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ------------------------------------------------------------------------------
# Stage 2: Cargo Chef Cacher & Builder
# Compiles all third-party dependencies using static musl linking.
# This layer is cached across builds as long as dependencies remain unchanged!
# ------------------------------------------------------------------------------
FROM lukemathwalker/cargo-chef:latest-rust-alpine AS builder
WORKDIR /app

# Install native build tools required for C/assembly cryptographic crates (aws-lc-sys, ring)
RUN apk add --no-cache \
    musl-dev \
    build-base \
    cmake \
    perl \
    clang \
    llvm \
    make \
    linux-headers \
    binutils

# Copy dependency recipe from planner
COPY --from=planner /app/recipe.json recipe.json

# Build & cache dependencies (native musl on Alpine)
RUN cargo chef cook --release --recipe-path recipe.json

# ------------------------------------------------------------------------------
# Stage 3: Build VelocityMQ Application Binary
# ------------------------------------------------------------------------------
COPY . .
RUN cargo build --release --bin velocity_mq

# Strip debug symbols and dead code to guarantee binary is <20MB
RUN strip /app/target/release/velocity_mq

# ------------------------------------------------------------------------------
# Stage 4: Minimal Distroless/Alpine Runtime (<50MB total image footprint)
# ------------------------------------------------------------------------------
FROM alpine:3.20 AS runtime

# Install CA certificates for AWS S3 TLS/HTTPS connectivity and timezone data
RUN apk add --no-cache ca-certificates tzdata

# Create dedicated unprivileged system user/group for enterprise security
RUN addgroup -g 10001 -S velocitymq && \
    adduser -u 10001 -S velocitymq -G velocitymq -h /var/lib/velocitymq && \
    mkdir -p /var/lib/velocitymq/data && \
    chown -R velocitymq:velocitymq /var/lib/velocitymq

WORKDIR /var/lib/velocitymq

# Copy statically linked binary from builder
COPY --from=builder --chown=velocitymq:velocitymq /app/target/release/velocity_mq /usr/local/bin/velocity_mq

# Ensure execution permissions
RUN chmod +x /usr/local/bin/velocity_mq

# Default Environment Variables: binds to 0.0.0.0 for containerized access
ENV HOST="0.0.0.0" \
    PORT="9092" \
    CLUSTER_MODE="false" \
    REPLICATION_FACTOR="1" \
    LOG_RETENTION_HOURS="168" \
    DATA_DIR="/var/lib/velocitymq/data" \
    METRICS_ENABLED="true" \
    METRICS_HOST="0.0.0.0" \
    METRICS_PORT="9093"

# Drop root privileges
USER velocitymq:velocitymq

# Expose Kafka Wire Protocol (9092) and Prometheus Metrics HTTP (9093)
EXPOSE 9092 9093

# Persistent WAL storage mount point
VOLUME ["/var/lib/velocitymq/data"]

# Lightweight container health check using metrics server probe
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:9093/health || exit 1

ENTRYPOINT ["/usr/local/bin/velocity_mq"]
