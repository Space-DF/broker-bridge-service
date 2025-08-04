package bridge

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/mqtt"
	"github.com/Space-DF/broker-bridge-service/internal/amqp"
)

// Bridge connects AMQP (RabbitMQ) and MQTT (EMQX) brokers
type Bridge struct {
	config        config.Config
	mqttClient    *mqtt.Client
	amqpClient *amqp.Client
	done          chan bool
}

// NewBridge creates a new bridge instance
func NewBridge(cfg config.Config) *Bridge {
	return &Bridge{
		config:         cfg,
		mqttClient:     mqtt.NewClient(cfg.MQTT),
		amqpClient: amqp.NewClient(cfg.AMQP),
		done:           make(chan bool),
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

	// Start MQTT client
	go b.mqttClient.Start(ctx)

	// Start AMQP client
	go b.amqpClient.Start(ctx)

	// Start message processing from AMQP to MQTT
	go b.processAMQPMessages(ctx)

	// Start message processing from MQTT (if needed)
	go b.processMQTTMessages(ctx)

	log.Printf("Broker bridge service started successfully")
	log.Printf("- MQTT connected to: %s:%d", b.config.MQTT.Broker, b.config.MQTT.Port)
	log.Printf("- AMQP connected to: %s", b.config.AMQP.URL)

	// Wait for context cancellation or done signal
	select {
	case <-ctx.Done():
		log.Println("Context cancelled, stopping bridge")
	case <-b.done:
		log.Println("Bridge stopped")
	}

	return nil
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
			
			// Publish to MQTT device telemetry topic
			if err := b.mqttClient.PublishDeviceTelemetry(locationUpdate.DevEUI, locationUpdate); err != nil {
				log.Printf("Failed to publish device telemetry for %s: %v", locationUpdate.DevEUI, err)
				// NACK the message to requeue it
				b.amqpClient.NackMessage(delivery, true)
			} else {
				log.Printf("Successfully published telemetry for device %s to MQTT", locationUpdate.DevEUI)
				// ACK the message only after successful MQTT publish
				b.amqpClient.AckMessage(delivery)
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