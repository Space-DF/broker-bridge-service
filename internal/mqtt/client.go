package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Client handles MQTT connection to EMQX
type Client struct {
	config       config.MQTTConfig
	client       mqtt.Client
	messagesChan chan *models.DeviceMessage
	done         chan bool
	subscribed   bool
	subscribeMu  sync.Mutex
	stopOnce     sync.Once
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
	if err := c.subscribeTopics(); err != nil {
		return err
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

// subscribeTopics subscribes to all configured topics
func (c *Client) subscribeTopics() error {
	c.subscribeMu.Lock()
	defer c.subscribeMu.Unlock()

	for _, topic := range c.config.Topics {
		if token := c.client.Subscribe(topic, c.config.QoS, c.messageHandler); token.Wait() && token.Error() != nil {
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, token.Error())
		}
		log.Printf("Subscribed to topic: %s", topic)
	}

	c.subscribed = true
	return nil
}

// GetMessagesChan returns the channel for receiving messages
func (c *Client) GetMessagesChan() <-chan *models.DeviceMessage {
	return c.messagesChan
}

// Stop gracefully stops the MQTT client
func (c *Client) Stop() error {
	c.stopOnce.Do(func() {
		close(c.done)

		c.subscribeMu.Lock()
		c.subscribed = false
		c.subscribeMu.Unlock()

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
	})
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

	// Restore subscriptions after reconnection
	c.subscribeMu.Lock()
	wasSubscribed := c.subscribed
	c.subscribeMu.Unlock()

	if wasSubscribed {
		log.Println("Restoring MQTT subscriptions after reconnection...")
		if err := c.subscribeTopics(); err != nil {
			log.Printf("Failed to restore subscriptions: %v (will retry on next reconnect)", err)
			// Keep subscribed=true so next onConnect retry will attempt again
		} else {
			log.Println("Successfully restored MQTT subscriptions")
		}
	}
}

func (c *Client) onConnectionLost(client mqtt.Client, err error) {
	log.Printf("Connection to EMQX lost: %v", err)

	// Mark as unsubscribed so onConnect will restore subscriptions
	c.subscribeMu.Lock()
	c.subscribed = false
	c.subscribeMu.Unlock()
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

	return nil
}

// PublishDeviceTelemetry publishes device location data to a tenant-scoped topic.
func (c *Client) PublishDeviceTelemetry(locationUpdate *models.DeviceLocationUpdate) (string, error) {
	if locationUpdate == nil {
		return "", fmt.Errorf("location update is nil")
	}

	topic := buildTelemetryTopic(locationUpdate)
	if err := c.Publish(topic, locationUpdate); err != nil {
		return "", err
	}

	return topic, nil
}

// PublishEntityTelemetry publishes per-entity telemetry to a tenant-scoped topic.
func (c *Client) PublishEntityTelemetry(entityUpdate *models.EntityTelemetryPayload) (string, error) {
	if entityUpdate == nil {
		return "", fmt.Errorf("entity update is nil")
	}

	topic := buildEntityTopic(entityUpdate)
	if err := c.Publish(topic, entityUpdate); err != nil {
		return "", err
	}

	return topic, nil
}

// PublishEvent publishes an event to a tenant-scoped topic.
func (c *Client) PublishEvent(event *models.Event) (string, error) {
	if event == nil {
		return "", fmt.Errorf("event is nil")
	}

	topic := buildEventTopic(event)
	if err := c.Publish(topic, event); err != nil {
		return "", err
	}

	return topic, nil
}
