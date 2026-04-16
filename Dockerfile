# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the gateway
RUN CGO_ENABLED=0 GOOS=linux go build -o gateway cmd/gateway/main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder
COPY --from=builder /app/gateway .
COPY --from=builder /app/config.yaml.example ./config.yaml

# Expose ports
# 9878: FIX Acceptor
# 9090: Prometheus Metrics
EXPOSE 9878 9090

ENTRYPOINT ["./gateway"]
