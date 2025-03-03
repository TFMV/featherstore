# Use a multi-stage build for smaller final image
FROM golang:1.24-bookworm AS builder

# Install dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -a -o featherstore ./cmd/featherstore

# Use a smaller base image for the final stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m featherstore

# Create directories for data and config
RUN mkdir -p /app/data /app/config \
    && chown -R featherstore:featherstore /app

# Copy the built binary
COPY --from=builder /app/featherstore /app/featherstore

# Create a default config file
COPY config.yaml /app/config/config.yaml

# Set working directory
WORKDIR /app

# Switch to non-root user
USER featherstore

# Expose the HTTP API, Flight, and Prometheus ports
EXPOSE 8080 8081 9090

# Set environment variables
ENV FEATHERSTORE_STORAGE_DUCKDB_PATH="/app/data/features.db" \
    FEATHERSTORE_STORAGE_PARQUET_DIRECTORY="/app/data/features"

# Set the entrypoint
ENTRYPOINT ["/app/featherstore"]
CMD ["--config", "/app/config/config.yaml"] 