.PHONY: all build test clean docker help

# Project variables
BINARY_NAME=gateway
BUILD_DIR=bin
MAIN_PATH=cmd/gateway/main.go

all: build test

build:
	@echo "Building binary..."
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)

test-integration:
	@echo "Running integration tests..."
	@go test -v ./internal/integration/...

test-load:
	@echo "Running load test (1000 conns)..."
	@go run scripts/loadtest/main.go -c 1000 -d 30s

help:
	@echo "optimus Gateway Makefile commands:"
	@echo "  build            - Build the gateway binary"
	@echo "  test             - Run all unit tests"
	@echo "  test-integration - Run end-to-end integration tests"
	@echo "  test-load        - Run load test simulator"
	@echo "  clean            - Remove build artifacts"
	@echo "  docker-build     - Build the Docker image"
