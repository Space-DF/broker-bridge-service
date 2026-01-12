.PHONY: build run clean test lint

# Build the application
build:
	go build -o bin/bridge cmd/bridge/main.go

# Run the application
run:
	go run cmd/bridge/main.go serve

# Clean build artifacts
clean:
	rm -rf bin/

# Run tests
test:
	go test -v ./...

# Lint code
lint:
	golangci-lint run

# Install dependencies
deps:
	go mod download
	go mod tidy

# Development mode
dev:
	@echo "Running in development mode..."
	go run ./cmd/bridge serve

# Docker build
docker-build:
	docker build -t broker-bridge-service .

# Docker run
docker-run:
	docker run -p 8080:8080 --env-file .env broker-bridge-service

# Docker run with network (for connecting to existing EMQX)
docker-run-network:
	docker run -p 8080:8080 --network spacedf_default --env-file .env broker-bridge-service