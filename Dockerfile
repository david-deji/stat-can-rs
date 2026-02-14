# Runtime Stage
FROM ubuntu:24.04

WORKDIR /app

# Install runtime dependencies (ca-certificates for HTTPS, libssl for networking)
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

# Copy binary from local build
COPY target/release/mcp_server /usr/local/bin/stat-can-mcp

# Expose port (default 3000)
EXPOSE 3000

# Set environment variables
ENV RUST_LOG=info

# Default command: Run in HTTP mode on port 3000
CMD ["stat-can-mcp", "--port", "3000"]
