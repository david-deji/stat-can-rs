# Build Stage
FROM ubuntu:24.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY . .

# Build the binary
RUN cargo build --release --bin mcp_server

# Runtime Stage
FROM ubuntu:24.04

WORKDIR /app

# Install runtime dependencies (ca-certificates for HTTPS, libssl for networking)
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

# Copy binary from builder stage
COPY --from=builder /app/target/release/mcp_server /usr/local/bin/stat-can-mcp

# Set environment variables
ENV RUST_LOG=info

# Default command: Bind to the port provided by Render's environment
CMD ["stat-can-mcp", "--transport", "sse"]
