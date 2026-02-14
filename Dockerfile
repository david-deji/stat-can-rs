# Builder Stage
FROM rust:1.75-slim-bookworm as builder

WORKDIR /app

# Copy manifests to cache dependencies
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies
RUN mkdir src && echo "fn main() {}" > src/lib.rs && echo "fn main() {}" > src/main.rs

# Build dependencies
# We need pkg-config and openssl-dev for reqwest/native-tls if not using rustls-tls (we are using rustls-tls in cargo.toml)
# But let's install them just in case specific features need them
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY src src
COPY examples examples
COPY README.md .

# Build the binary
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
