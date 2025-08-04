package gateway

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/mqtt"
)

// Gateway bridges messages between RabbitMQ and EMQX MQTT brokers
type Gateway struct {
	config     config.Config
	mqttClient *mqtt.Client
	done       chan bool
}

// NewGateway creates a new gateway instance
func NewGateway(cfg config.Config) *Gateway {
	return &Gateway{
		config:     cfg,
		mqttClient: mqtt.NewClient(cfg.MQTT),
		done:       make(chan bool),
	}
}

// Start initializes and starts the gateway service
func (g *Gateway) Start(ctx context.Context) error {
	log.Println("Starting Broker Bridge Service")

	// Connect to EMQX
	if err := g.mqttClient.Connect(); err != nil {
		return err
	}

	// Start MQTT client
	go g.mqttClient.Start(ctx)

	// Start message processing
	go g.processMessages(ctx)

	log.Printf("Broker bridge service started successfully")
	log.Printf("- MQTT connected to: %s", g.config.MQTT.BrokerURL)

	// Wait for context cancellation or done signal
	select {
	case <-ctx.Done():
		log.Println("Context cancelled, stopping gateway")
	case <-g.done:
		log.Println("Gateway stopped")
	}

	return nil
}

// Stop gracefully stops the gateway service
func (g *Gateway) Stop() error {
	log.Println("Stopping Broker Bridge Service")
	
	close(g.done)

	if g.mqttClient != nil {
		if err := g.mqttClient.Stop(); err != nil {
			log.Printf("Error stopping MQTT client: %v", err)
		}
	}

	log.Println("Broker bridge service stopped")
	return nil
}

// processMessages handles processing messages from MQTT
func (g *Gateway) processMessages(ctx context.Context) {
	log.Println("Starting message processing")
	
	messagesChan := g.mqttClient.GetMessagesChan()
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping message processing")
			return
		case <-g.done:
			log.Println("Message processing stopped")
			return
		case deviceMsg, ok := <-messagesChan:
			if !ok {
				log.Println("MQTT messages channel closed")
				return
			}
			
			// Process the message (placeholder for RabbitMQ integration)
			log.Printf("Processing message from device: %s on topic: %s", deviceMsg.DevEUI, deviceMsg.Topic)
			// TODO: Add RabbitMQ publishing logic here
		}
	}
}