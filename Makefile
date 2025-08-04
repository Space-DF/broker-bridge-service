.PHONY: build run clean test lint

# Build the application
build:
	go build -o bin/gateway cmd/gateway/main.go

# Run the application
run:
	go run cmd/gateway/main.go serve

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

# Development mode with hot reload
dev:
	air -c .air.toml

# Docker build
docker-build:
	docker build -t broker-bridge-service .

# Docker run
docker-run:
	docker run -p 8080:8080 --env-file .env broker-bridge-service

# Docker run with network (for connecting to existing EMQX)
docker-run-network:
	docker run -p 8080:8080 --network spacedf_default --env-file .env broker-bridge-service