package bridge

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/Space-DF/broker-bridge-service/internal/amqp"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
	"github.com/Space-DF/broker-bridge-service/internal/mqtt"
	"github.com/Space-DF/broker-bridge-service/internal/telemetry"
	otellog "go.opentelemetry.io/otel/log"
)

// Bridge connects AMQP (RabbitMQ) and MQTT (EMQX) brokers
type Bridge struct {
	config     config.Config
	mqttClient *mqtt.Client
	amqpClient *amqp.Client
	done       chan bool
	stopOnce   sync.Once
}

// NewBridge creates a new bridge instance
func NewBridge(cfg config.Config) *Bridge {
	return &Bridge{
		config:     cfg,
		mqttClient: mqtt.NewClient(cfg.MQTT),
		amqpClient: amqp.NewClient(cfg.AMQP, cfg.OrgEvents),
		done:       make(chan bool),
	}
}

// Start initializes and starts the bridge service
func (b *Bridge) Start(ctx context.Context) error {
	log.Println("Starting Broker Bridge Service")

	// Connect to EMQX
	if err := b.mqttClient.Connect(); err != nil {
		return err
	}

	// Connect to AMQP
	if err := b.amqpClient.Connect(); err != nil {
		return err
	}

	// Create error channel to capture startup errors
	errorChan := make(chan error, 2)

	// Start MQTT client
	go func() {
		if err := b.mqttClient.Start(ctx); err != nil {
			log.Printf("MQTT client error: %v", err)
			errorChan <- err
			b.stopOnce.Do(func() { close(b.done) })
			return
		}
	}()

	// Start AMQP client
	go func() {
		if err := b.amqpClient.Start(ctx); err != nil {
			log.Printf("AMQP client error: %v", err)
			errorChan <- err
			b.stopOnce.Do(func() { close(b.done) })
			return
		}
	}()

	// Start message processing from AMQP to MQTT
	go b.processAMQPMessages(ctx)

	// Start message processing from MQTT (if needed)
	go b.processMQTTMessages(ctx)

	log.Printf("Broker bridge service started successfully")
	log.Printf("- MQTT connected to: %s:%d", b.config.MQTT.Broker, b.config.MQTT.Port)
	log.Printf("- AMQP connected to: %s", b.config.AMQP.URL)

	// Wait for context cancellation, done signal, or startup error
	select {
	case <-ctx.Done():
		log.Println("Context cancelled, stopping bridge")
		return ctx.Err()
	case <-b.done:
		log.Println("Bridge stopped")
		return nil
	case err := <-errorChan:
		log.Printf("Bridge startup failed: %v", err)
		return err
	}
}

// Stop gracefully stops the bridge service
func (b *Bridge) Stop() error {
	log.Println("Stopping Broker Bridge Service")

	b.stopOnce.Do(func() { close(b.done) })

	if b.mqttClient != nil {
		if err := b.mqttClient.Stop(); err != nil {
			log.Printf("Error stopping MQTT client: %v", err)
		}
	}

	if b.amqpClient != nil {
		if err := b.amqpClient.Stop(); err != nil {
			log.Printf("Error stopping AMQP client: %v", err)
		}
	}

	log.Println("Broker bridge service stopped")
	return nil
}

// processAMQPMessages handles processing messages from AMQP and publishes to MQTT
func (b *Bridge) processAMQPMessages(ctx context.Context) {
	log.Println("Starting AMQP message processing")

	messagesChan := b.amqpClient.GetMessagesChan()

	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping AMQP message processing")
			return
		case <-b.done:
			log.Println("AMQP message processing stopped")
			return
		case messageWithDelivery, ok := <-messagesChan:
			if !ok {
				log.Println("AMQP messages channel closed")
				return
			}

			delivery := messageWithDelivery.Delivery

			switch messageWithDelivery.Kind {
			case models.KindLocationUpdate:
				locationUpdate := messageWithDelivery.LocationUpdate

				ctx := context.Background()
				topic, err := b.mqttClient.PublishDeviceTelemetry(locationUpdate)
				if err != nil {
					deviceId := locationUpdate.DeviceID
					if deviceId == "" {
						deviceId = "unknown"
					}

					log.Printf("Failed to publish device telemetry for %s: %v", deviceId, err)
					telemetry.LogError(ctx, fmt.Sprintf("Failed to publish device %s telemetry to MQTT", locationUpdate.DeviceEUI),
						otellog.String("device_id", deviceId),
						otellog.String("device_eui", locationUpdate.DeviceEUI),
						otellog.String("error", err.Error()),
					)
					// NACK the message to requeue it
					_ = b.amqpClient.NackMessage(delivery, true)
				} else {
					log.Printf("Successfully published telemetry to MQTT topic %s", topic)
					telemetry.LogInfo(ctx, fmt.Sprintf("Successfully published device %s telemetry to MQTT topic %s", locationUpdate.DeviceEUI, topic),
						otellog.String("device_id", locationUpdate.DeviceID),
						otellog.String("device_eui", locationUpdate.DeviceEUI),
						otellog.String("mqtt_topic", topic),
					)
					// ACK the message only after successful MQTT publish
					_ = b.amqpClient.AckMessage(delivery)
				}

			case models.KindEntityTelemetry:
				entityUpdate := messageWithDelivery.EntityUpdate

				ctx := context.Background()
				topic, err := b.mqttClient.PublishEntityTelemetry(entityUpdate)
				if err != nil {
					entityId := entityUpdate.Entity.UniqueID
					if entityId == "" {
						entityId = "unknown"
					}
					log.Printf("Failed to publish entity telemetry for %s: %v", entityId, err)
					telemetry.LogError(ctx, fmt.Sprintf("Failed to publish entity %s telemetry to MQTT", entityId),
						otellog.String("entity_id", entityId),
						otellog.String("error", err.Error()),
					)
					_ = b.amqpClient.NackMessage(delivery, true)
				} else {
					telemetry.LogInfo(ctx, fmt.Sprintf("Successfully published entity %s telemetry to MQTT topic %s", entityUpdate.Entity.UniqueID, topic),
						otellog.String("entity_id", entityUpdate.Entity.UniqueID),
						otellog.String("mqtt_topic", topic),
					)
					_ = b.amqpClient.AckMessage(delivery)
				}

			default:
				log.Printf("Unknown message kind; NACKing")
				_ = b.amqpClient.NackMessage(delivery, false)
			}
		}
	}
}

// processMQTTMessages handles processing messages from MQTT (if needed for bidirectional communication)
func (b *Bridge) processMQTTMessages(ctx context.Context) {
	log.Println("Starting MQTT message processing")

	messagesChan := b.mqttClient.GetMessagesChan()

	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping MQTT message processing")
			return
		case <-b.done:
			log.Println("MQTT message processing stopped")
			return
		case deviceMsg, ok := <-messagesChan:
			if !ok {
				log.Println("MQTT messages channel closed")
				return
			}

			// Process MQTT messages if needed for bidirectional communication
			log.Printf("Received MQTT message from device: %s on topic: %s", deviceMsg.DevEUI, deviceMsg.Topic)
		}
	}
}
