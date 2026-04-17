package bridge

import (
	"context"
	"log"
	"sync"

	"github.com/Space-DF/broker-bridge-service/internal/amqp"
	"github.com/Space-DF/broker-bridge-service/internal/celery"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
	"github.com/Space-DF/broker-bridge-service/internal/mqtt"
)

// Bridge connects AMQP (RabbitMQ) and MQTT (EMQX) brokers
type Bridge struct {
	config          config.Config
	mqttClient      *mqtt.Client
	amqpClient      *amqp.Client
	celeryPublisher *celery.Publisher
	done            chan bool
	stopOnce        sync.Once
}

// NewBridge creates a new bridge instance
func NewBridge(cfg config.Config) *Bridge {
	// Create Celery publisher for dispatching location updates to device-service.
	// Uses the same default-vhost AMQP URL.
	var cp *celery.Publisher
	cp, err := celery.NewPublisher(cfg.AMQP.URL)

	if err != nil {
		log.Printf("Warning: failed to create Celery publisher: %v (location updates to device-service disabled)", err)
	}

	return &Bridge{
		config:          cfg,
		mqttClient:      mqtt.NewClient(cfg.MQTT),
		amqpClient:      amqp.NewClient(cfg.AMQP, cfg.OrgEvents),
		celeryPublisher: cp,
		done:            make(chan bool),
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
			select {
			case errorChan <- err:
			case <-b.done:
			}
			b.stopOnce.Do(func() { close(b.done) })
			return
		}
	}()

	// Start AMQP client
	go func() {
		if err := b.amqpClient.Start(ctx); err != nil {
			log.Printf("AMQP client error: %v", err)
			select {
			case errorChan <- err:
			case <-b.done:
			}
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

	if b.celeryPublisher != nil {
		b.celeryPublisher.Close()
	}

	log.Println("Broker bridge service stopped")
	return nil
}

// processAMQPMessages handles processing messages from AMQP and publishes to MQTT.
func (b *Bridge) processAMQPMessages(ctx context.Context) {
	log.Println("Starting AMQP message processing")

	messagesChan := b.amqpClient.GetMessagesChan()
	registry := &ProcessorRegistry{bridge: b}

	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping AMQP message processing")
			return
		case <-b.done:
			log.Println("AMQP message processing stopped")
			return
		case msg, ok := <-messagesChan:
			if !ok {
				log.Println("AMQP messages channel closed, draining remaining messages")
				for remainingMsg := range messagesChan {
					b.processMessage(ctx, registry, remainingMsg)
				}
				return
			}

			b.processMessage(ctx, registry, msg)
		}
	}
}

// processMessage handles a single message using the appropriate processor.
func (b *Bridge) processMessage(ctx context.Context, registry *ProcessorRegistry, msg *models.AMQPMessageWithDelivery) {
	processor := registry.GetProcessor(msg)

	// Try to publish to MQTT
	topic, err := processor.Publish(ctx)
	if err != nil {
		log.Printf("Failed to publish %s for %s: %v", msg.Kind, processor.GetIdentifier(), err)
		processor.LogFailure(ctx, err)
		if err := b.amqpClient.NackMessage(msg.Delivery, true); err != nil {
			log.Printf("Error occurred while negatively acknowledging message: %v", err)
		}
		return
	}

	log.Printf("Successfully published %s to MQTT topic %s", msg.Kind, topic)
	processor.LogSuccess(ctx, topic)

	// Run post-processing (e.g., Celery tasks)
	if postErr := processor.PostProcess(ctx); postErr != nil {
		log.Printf("Post-processing failed for %s: %v", processor.GetIdentifier(), postErr)
	}

	// ACK after successful processing
	if err := b.amqpClient.AckMessage(msg.Delivery); err != nil {
		log.Printf("Error occurred while acknowledging message: %v", err)
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
