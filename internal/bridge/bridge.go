package bridge

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/amqp"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/mqtt"
)

// Bridge connects AMQP (RabbitMQ) and MQTT (EMQX) brokers
type Bridge struct {
	config        config.Config
	mqttClient    *mqtt.Client
	amqpClient    *amqp.Client
	done          chan bool
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
			close(b.done)
			return
		}
	}()

	// Start AMQP client
	go func() {
		if err := b.amqpClient.Start(ctx); err != nil {
			log.Printf("AMQP client error: %v", err)
			errorChan <- err
			close(b.done)
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
	
	close(b.done)

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
			
			locationUpdate := messageWithDelivery.LocationUpdate
			delivery := messageWithDelivery.Delivery
			
			// Publish to MQTT device telemetry topic using device_id
			deviceID := locationUpdate.DeviceID
			if deviceID == "" {
				deviceID = "unknown"
			}
			
			if err := b.mqttClient.PublishDeviceTelemetry(deviceID, locationUpdate); err != nil {
				log.Printf("Failed to publish device telemetry for %s: %v", deviceID, err)
				// NACK the message to requeue it
				_ = b.amqpClient.NackMessage(delivery, true)
			} else {
				log.Printf("Successfully published telemetry for device %s to MQTT topic device/%s/telemetry", deviceID, deviceID)
				// ACK the message only after successful MQTT publish
				_ = b.amqpClient.AckMessage(delivery)
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