# Broker Bridge Service

A Go service that acts as a message bridge between AMQP and EMQX MQTT brokers.

## Features

- **Message Bridging**: Routes messages between AMQP and EMQX brokers
- **MQTT Integration**: Subscribes to EMQX for device location data
- **Protocol Translation**: Handles message format conversion between brokers
- **Real-time Processing**: Processes device messages in real-time

## Architecture

```
AMQP ←→ Broker Bridge Service ←→ EMQX MQTT
                     ↓
           Frontend Clients (MQTT over WebSocket directly to EMQX)
```

## Configuration

### Configuration
Create a `.env` file with the following example settings:

```bash
# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8080
SERVER_LOG_LEVEL=info

# EMQX MQTT Configuration
MQTT_BROKER=localhost
MQTT_PORT=1883
MQTT_CLIENT_ID=frontend-gateway-service
MQTT_USERNAME=admin
MQTT_PASSWORD=public
MQTT_TOPICS=transformed/device/location,device/+/status
MQTT_QOS=1

# AMQP Configuration
AMQP_BROKER_URL=amqp://guest:guest@localhost:5672/
AMQP_EXCHANGE=
AMQP_QUEUE=transformed/device/location
AMQP_ROUTING_KEY=transformed/device/location
AMQP_CONSUMER_TAG=broker-bridge-consumer
AMQP_AUTO_ACK=false
AMQP_EXCLUSIVE=false
AMQP_NO_LOCAL=false
AMQP_NO_WAIT=false

# Rate Limiting
RATE_LIMIT_ENABLED=true
RATE_LIMIT_REQUESTS_PER_MINUTE=1000
RATE_LIMIT_BURST_SIZE=100

# CORS Configuration
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:3001
CORS_ALLOWED_METHODS=GET,POST,PUT,DELETE,OPTIONS
CORS_ALLOWED_HEADERS=Origin,Content-Type,Accept,Authorization,X-Requested-With
```

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

## Testing

### Unit Tests
```bash
make test
```

### Local Linting & Security Checks
- Tooling (once):
  ```bash
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0
  go install github.com/securego/gosec/v2/cmd/gosec@master
  ```
- Broker Bridge service:
  ```bash
  cd broker-bridge-service (optional)
  golangci-lint run
  gosec ./...
  ```
## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

[![SpaceDF - A project from Digital Fortress](https://df.technology/images/SpaceDF.png)](https://df.technology/)