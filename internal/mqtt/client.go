package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
)

// Client handles MQTT connection to EMQX
type Client struct {
	config       config.MQTTConfig
	client       mqtt.Client
	messagesChan chan *models.DeviceMessage
	done         chan bool
}

// NewClient creates a new MQTT client
func NewClient(cfg config.MQTTConfig) *Client {
	return &Client{
		config:       cfg,
		messagesChan: make(chan *models.DeviceMessage, 100),
		done:         make(chan bool),
	}
}

// Connect establishes connection to EMQX
func (c *Client) Connect() error {
	// Configure MQTT client options
	opts := mqtt.NewClientOptions()
	opts.AddBroker(c.config.GetBrokerURL())
	opts.SetClientID(c.config.ClientID)
	opts.SetUsername(c.config.Username)
	opts.SetPassword(c.config.Password)
	opts.SetCleanSession(c.config.CleanSession)
	opts.SetKeepAlive(time.Duration(c.config.KeepAlive) * time.Second)
	opts.SetConnectTimeout(c.config.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(c.config.ReconnectDelay)

	// Set connection handlers
	opts.SetConnectionLostHandler(c.onConnectionLost)
	opts.SetReconnectingHandler(c.onReconnecting)
	opts.SetOnConnectHandler(c.onConnect)

	// Create and connect client
	c.client = mqtt.NewClient(opts)
	if token := c.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to EMQX: %w", token.Error())
	}

	log.Printf("Successfully connected to EMQX at %s", c.config.GetBrokerURL())
	return nil
}

// Start begins consuming messages from EMQX
func (c *Client) Start(ctx context.Context) error {
	// Subscribe to configured topics
	for _, topic := range c.config.Topics {
		if token := c.client.Subscribe(topic, c.config.QoS, c.messageHandler); token.Wait() && token.Error() != nil {
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, token.Error())
		}
		log.Printf("Subscribed to topic: %s", topic)
	}

	// Wait for context cancellation or done signal
	select {
	case <-ctx.Done():
		log.Println("Context cancelled, stopping MQTT client")
	case <-c.done:
		log.Println("MQTT client stopped")
	}

	return nil
}

// GetMessagesChan returns the channel for receiving messages
func (c *Client) GetMessagesChan() <-chan *models.DeviceMessage {
	return c.messagesChan
}

// Stop gracefully stops the MQTT client
func (c *Client) Stop() error {
	close(c.done)

	if c.client != nil && c.client.IsConnected() {
		// Unsubscribe from all topics
		for _, topic := range c.config.Topics {
			if token := c.client.Unsubscribe(topic); token.Wait() && token.Error() != nil {
				log.Printf("Error unsubscribing from topic %s: %v", topic, token.Error())
			}
		}

		// Disconnect
		c.client.Disconnect(250)
		log.Println("Disconnected from EMQX")
	}

	close(c.messagesChan)
	return nil
}

// messageHandler processes incoming MQTT messages
func (c *Client) messageHandler(client mqtt.Client, msg mqtt.Message) {
	log.Printf("Received message from topic: %s", msg.Topic())

	var deviceMessage models.DeviceMessage
	if err := json.Unmarshal(msg.Payload(), &deviceMessage); err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		return
	}

	// Set topic and timestamp
	deviceMessage.Topic = msg.Topic()
	deviceMessage.ReceivedAt = time.Now()

	// Send to message channel (non-blocking)
	select {
	case c.messagesChan <- &deviceMessage:
		log.Printf("Message queued for distribution: device %s", deviceMessage.DevEUI)
	default:
		log.Printf("Message channel full, dropping message from device %s", deviceMessage.DevEUI)
	}
}

// Connection event handlers
func (c *Client) onConnect(client mqtt.Client) {
	log.Println("Connected to EMQX broker")
}

func (c *Client) onConnectionLost(client mqtt.Client, err error) {
	log.Printf("Connection to EMQX lost: %v", err)
}

func (c *Client) onReconnecting(client mqtt.Client, opts *mqtt.ClientOptions) {
	log.Println("Attempting to reconnect to EMQX...")
}

// Publish sends a message to EMQX (optional feature for bidirectional communication)
func (c *Client) Publish(topic string, payload interface{}) error {
	if !c.client.IsConnected() {
		return fmt.Errorf("MQTT client not connected")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	token := c.client.Publish(topic, c.config.QoS, false, data)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish message: %w", token.Error())
	}

	log.Printf("Published message to topic: %s", topic)
	return nil
}

// PublishDeviceTelemetry publishes device location data to device/{device_id}/telemetry topic
func (c *Client) PublishDeviceTelemetry(deviceID string, locationUpdate *models.DeviceLocationUpdate) error {
	topic := fmt.Sprintf("device/%s/telemetry", deviceID)
	return c.Publish(topic, locationUpdate)
}