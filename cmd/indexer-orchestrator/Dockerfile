# Build stage
# Using golang:alpine (latest) for compatibility
# If you need a specific version, use golang:1.23-alpine or golang:1.22-alpine
FROM golang:alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build platform-server (required by orchestrator)
RUN mkdir -p bin && \
    go build -o bin/platform-server main.go

# Build indexer-orchestrator
RUN go build -o bin/platform-indexer-orchestrator cmd/indexer-orchestrator/*.go

# Runtime stage
FROM alpine:3.19

# Install ca-certificates and procps for healthcheck
RUN apk --no-cache add ca-certificates tzdata procps

# Create non-root user
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

# Set working directory
WORKDIR /app

# Create necessary directories
RUN mkdir -p logs bin && \
    chown -R appuser:appuser /app

# Copy binaries from builder
COPY --from=builder /build/bin/platform-server ./bin/
COPY --from=builder /build/bin/platform-indexer-orchestrator ./

# Copy config file if it exists (optional, can be mounted as volume)
# COPY .env .env

# Change ownership
RUN chown -R appuser:appuser /app && \
    chmod +x ./platform-indexer-orchestrator && \
    chmod +x ./bin/platform-server

# Switch to non-root user
USER appuser

# Expose port for HTTP server (SocketIO)
# Default port from config, adjust if needed
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD pgrep -f platform-indexer-orchestrator > /dev/null || exit 1

# Default command
# Can be overridden with docker run or docker-compose
CMD ["./platform-indexer-orchestrator", \
     "--workers", "2", \
     "--config", ".env", \
     "--server-path", "./bin/platform-server", \
     "--log-file", "logs/indexer-orchestrator.log", \
     "--check-interval", "10s", \
     "--servers", "wss://s1.ripple.com/,wss://s2.ripple.com/,wss://xrplcluster.com/"]

