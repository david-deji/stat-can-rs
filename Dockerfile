# Builder Stage
FROM rustlang/rust:nightly-slim as builder

WORKDIR /app

# Copy manifests to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies
RUN mkdir -p src/bin && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/bin/mcp_server.rs

# Build dependencies (release mode)
# This caches compiled dependencies in the layer
RUN cargo build --release --lib
RUN cargo build --release --bin mcp_server

# Remove dummy build artifacts so they don't interfere
RUN rm -f target/release/deps/statcan_rs*
RUN rm -f target/release/deps/mcp_server*

# Copy actual source code
COPY src src
COPY examples examples
COPY README.md .

# Build the binary (only compiles changes in src)
RUN cargo build --release --bin mcp_server

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies (ca-certificates for HTTPS)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/mcp_server /usr/local/bin/stat-can-mcp

# Expose port (default 3000)
EXPOSE 3000

# Set environment variables
ENV RUST_LOG=info

# Default command: Run in HTTP mode on port 3000
CMD ["stat-can-mcp", "--port", "3000"]
