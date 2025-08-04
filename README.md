# Broker Bridge Service

A Go service that acts as a message bridge between RabbitMQ and EMQX MQTT brokers.

## Features

- **Message Bridging**: Routes messages between RabbitMQ and EMQX brokers
- **MQTT Integration**: Subscribes to EMQX for device location data
- **Protocol Translation**: Handles message format conversion between brokers
- **Real-time Processing**: Processes device messages in real-time

## Architecture

```
RabbitMQ ←→ Broker Bridge Service ←→ EMQX MQTT
                     ↓
           Frontend Clients (MQTT over WebSocket directly to EMQX)
```

## Configuration

- `configs/config.yaml` - Main configuration file
- Environment variables override config file settings
- `.env` file for local development

## Usage

```bash
# Run the service
go run cmd/gateway/main.go serve

# Build
make build

# Run with Docker
make docker-build
make docker-run

# Run with Docker network (connect to existing EMQX)
make docker-run-network
```