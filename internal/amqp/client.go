package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TenantConsumer represents a consumer for a specific tenant
type TenantConsumer struct {
	OrgSlug   string
	QueueName string
	Channel   *amqp.Channel
	Cancel    context.CancelFunc
	wg        sync.WaitGroup
}

type Client struct {
	config            config.AMQPConfig
	orgEventsConfig   config.OrgEventsConfig
	connection        *amqp.Connection
	channel           *amqp.Channel
	orgEventsChannel  *amqp.Channel
	messagesChan      chan *models.AMQPMessageWithDelivery
	done              chan bool
	tenantConsumers   map[string]*TenantConsumer
	tenantConsumersMu sync.RWMutex
}

func NewClient(cfg config.AMQPConfig, orgEventsCfg config.OrgEventsConfig) *Client {
	return &Client{
		config:          cfg,
		orgEventsConfig: orgEventsCfg,
		messagesChan:    make(chan *models.AMQPMessageWithDelivery, 100),
		done:            make(chan bool),
		tenantConsumers: make(map[string]*TenantConsumer),
	}
}

func (c *Client) Connect() error {
	var err error

	c.connection, err = amqp.Dial(c.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to AMQP: %w", err)
	}

	c.channel, err = c.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Create separate channel for org events
	c.orgEventsChannel, err = c.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open org events channel: %w", err)
	}

	log.Printf("Successfully connected to AMQP at %s", c.config.URL)

	return nil
}

func (c *Client) Start(ctx context.Context) error {
	log.Println("Starting AMQP client with org event discovery...")

	// Start listening for org events
	go func() {
		if err := c.listenToOrgEvents(ctx); err != nil {
			log.Printf("Error listening to org events: %v", err)
		}
	}()

	// Send discovery request to get all active organizations
	if err := c.sendDiscoveryRequest(ctx); err != nil {
		log.Printf("Warning: Failed to send discovery request: %v", err)
	}

	log.Println("AMQP client started successfully")

	select {
	case <-ctx.Done():
		log.Println("Context cancelled, stopping AMQP client")
	case <-c.done:
		log.Println("AMQP client stopped")
	}

	return nil
}

func (c *Client) handleMessage(msg amqp.Delivery) {
	log.Printf("Received message from AMQP: routing key %s", msg.RoutingKey)
	log.Printf("AMQP message body: %s", string(msg.Body))

	var locationUpdate models.DeviceLocationUpdate
	if err := json.Unmarshal(msg.Body, &locationUpdate); err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		if !c.config.AutoAck {
			_ = msg.Nack(false, false)
		}
		return
	}

	locationUpdate.UpdatedAt = time.Now()

	// Create message with delivery for reliable processing
	messageWithDelivery := &models.AMQPMessageWithDelivery{
		LocationUpdate: &locationUpdate,
		Delivery:       &msg,
	}

	select {
	case c.messagesChan <- messageWithDelivery:
		log.Printf("Location update queued for device: %s (channel length after: %d/%d)",
			locationUpdate.DeviceEUI, len(c.messagesChan), cap(c.messagesChan))
		// Do not ACK here - ACK will be handled after successful MQTT publish
	default:
		log.Printf("DEBUG: Message channel full! Current length: %d, capacity: %d",
			len(c.messagesChan), cap(c.messagesChan))
		log.Printf("DEBUG: Channel is at 100%% capacity - consumer may be too slow or blocked")
		log.Printf("Message channel full, dropping location update for device: %s", locationUpdate.DeviceEUI)
		if !c.config.AutoAck {
			_ = msg.Nack(false, true)
		}
	}
}

func (c *Client) GetMessagesChan() <-chan *models.AMQPMessageWithDelivery {
	return c.messagesChan
}

// AckMessage acknowledges an AMQP message
func (c *Client) AckMessage(delivery *amqp.Delivery) error {
	if !c.config.AutoAck && delivery != nil {
		if err := delivery.Ack(false); err != nil {
			log.Printf("Failed to ACK message: %v", err)
			return err
		}
	}
	return nil
}

// NackMessage negatively acknowledges an AMQP message
func (c *Client) NackMessage(delivery *amqp.Delivery, requeue bool) error {
	if !c.config.AutoAck && delivery != nil {
		if err := delivery.Nack(false, requeue); err != nil {
			log.Printf("Failed to NACK message: %v", err)
			return err
		}
	}
	return nil
}

// sendDiscoveryRequest sends a discovery request to get all active organizations
func (c *Client) sendDiscoveryRequest(ctx context.Context) error {
	log.Println("Sending discovery request for active organizations...")

	request := models.OrgDiscoveryRequest{
		EventType:   models.OrgDiscoveryReq,
		EventID:     fmt.Sprintf("discovery-%d", time.Now().Unix()),
		Timestamp:   time.Now(),
		ServiceName: "broker-bridge-service",
		ReplyTo:     c.orgEventsConfig.Queue, // We'll receive response on our org events queue
	}

	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal discovery request: %w", err)
	}

	err = c.orgEventsChannel.PublishWithContext(
		ctx,
		c.orgEventsConfig.Exchange,     // "org.events"
		string(models.OrgDiscoveryReq), // "org.discovery.request"
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Timestamp:   time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish discovery request: %w", err)
	}

	log.Printf("Discovery request sent to exchange: %s", c.orgEventsConfig.Exchange)
	return nil
}

func (c *Client) ensureOrgEventsTopology() error {
	if err := c.orgEventsChannel.ExchangeDeclare(
		c.orgEventsConfig.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare failed: %w", err)
	}

	if _, err := c.orgEventsChannel.QueueDeclare(
		c.orgEventsConfig.Queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("queue declare failed: %w", err)
	}

	if err := c.orgEventsChannel.QueueBind(
		c.orgEventsConfig.Queue,
		c.orgEventsConfig.RoutingKey,
		c.orgEventsConfig.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("queue bind failed: %w", err)
	}

	return nil
}

// listenToOrgEvents listens for organization lifecycle events
func (c *Client) listenToOrgEvents(ctx context.Context) error {
	log.Println("Setting up organization events listener...")

	var (
		messages <-chan amqp.Delivery
		err      error
		attempt  = 1
	)

	for {
		if err = c.ensureOrgEventsTopology(); err == nil {
			messages, err = c.orgEventsChannel.Consume(
				c.orgEventsConfig.Queue,
				c.orgEventsConfig.ConsumerTag,
				false, // manual ack for reliability
				false,
				false,
				false,
				nil,
			)
			if err == nil {
				break
			}
		}

		backoff := time.Duration(attempt)
		if backoff > 10 {
			backoff = 10
		}

		log.Printf("Broker-bridge org events setup retry (attempt %d, next in %ds): %v", attempt, int(backoff), err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff * time.Second):
		}

		if attempt < 10 {
			attempt++
		}
	}

	// Process org events
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-messages:
			if !ok {
				return nil
			}

			log.Printf("Received org event: %s", msg.RoutingKey)

			if err := c.handleOrgEvent(ctx, msg); err != nil {
				log.Printf("Error handling org event: %v", err)
				_ = msg.Nack(false, true) // Requeue
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}

// handleOrgEvent processes organization lifecycle events
func (c *Client) handleOrgEvent(ctx context.Context, msg amqp.Delivery) error {
	log.Printf("Received org event with routing key: %s", msg.RoutingKey)

	// Handle discovery response (contains all active orgs)
	// if msg.RoutingKey == "org.discovery.response" {
	// 	var response models.OrgDiscoveryResponse
	// 	if err := json.Unmarshal(msg.Body, &response); err != nil {
	// 		return fmt.Errorf("failed to unmarshal discovery response: %w", err)
	// 	}

	// 	log.Printf("Received discovery response with %d active organizations", response.TotalCount)

	// 	// Subscribe to each active organization
	// 	for _, org := range response.Organizations {
	// 		if org.IsActive {
	// 			log.Printf("Bootstrapping subscription for org: %s", org.Slug)
	// 			if err := c.subscribeToOrganization(ctx, org.Slug); err != nil {
	// 				log.Printf("Failed to subscribe to org '%s': %v", org.Slug, err)
	// 				continue
	// 			}
	// 		}
	// 	}

	// 	log.Printf("Bootstrap complete: subscribed to %d organizations", response.TotalCount)
	// 	return nil
	// }

	// Handle regular org lifecycle events
	var event models.OrgEvent

	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal org event: %w", err)
	}

	log.Printf("Processing org event: %s for org: %s", event.EventType, event.Payload.Slug)

	switch event.EventType {

	case models.OrgCreated:
		// New org created - subscribe to its queue
		return c.subscribeToOrganization(ctx, event.Payload.Slug)

	case models.OrgDeactivated, models.OrgDeleted:
		// Org deleted/deactivated - unsubscribe
		c.unsubscribeFromOrganization(event.Payload.Slug)
		return nil

	default:
		log.Printf("Unknown event type: %s", event.EventType)
		return nil
	}
}

// subscribeToOrganization subscribes to a specific organization's queue
func (c *Client) subscribeToOrganization(ctx context.Context, orgSlug string) error {
	c.tenantConsumersMu.Lock()
	defer c.tenantConsumersMu.Unlock()

	// Check if already subscribed
	if _, exists := c.tenantConsumers[orgSlug]; exists {
		log.Printf("Already subscribed to org: %s", orgSlug)
		return nil
	}

	log.Printf("Subscribing to organization: %s", orgSlug)

	// Create dedicated channel for this tenant
	tenantChannel, err := c.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel for org %s: %w", orgSlug, err)
	}

	// Declare consumer tag for this tenant
	consumerTag := fmt.Sprintf("%s-broker-bridge-%d", orgSlug, time.Now().Unix())
	queueName := fmt.Sprintf("%s.transformed.data.queue", orgSlug)

	// Start consuming messages
	messages, err := tenantChannel.Consume(
		queueName,
		consumerTag, // consumer tag
		false,       // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = tenantChannel.Close()
		return fmt.Errorf("failed to start consuming for org %s: %w", orgSlug, err)
	}

	// Create cancellable context for this tenant
	tenantCtx, cancel := context.WithCancel(context.Background())

	// Store tenant consumer info
	tenantConsumer := &TenantConsumer{
		OrgSlug:   orgSlug,
		QueueName: queueName,
		Channel:   tenantChannel,
		Cancel:    cancel,
	}
	c.tenantConsumers[orgSlug] = tenantConsumer

	// Start processing messages for this tenant
	tenantConsumer.wg.Add(1)
	go func() {
		defer tenantConsumer.wg.Done()
		c.processTenantMessages(tenantCtx, orgSlug, messages)
	}()

	log.Printf("Successfully subscribed to org: %s (queue: %s)", orgSlug, queueName)
	return nil
}

// unsubscribeFromOrganization unsubscribes from a specific organization
func (c *Client) unsubscribeFromOrganization(orgSlug string) {
	c.tenantConsumersMu.Lock()
	defer c.tenantConsumersMu.Unlock()

	consumer, exists := c.tenantConsumers[orgSlug]
	if !exists {
		log.Printf("Not subscribed to org: %s", orgSlug)
		return
	}

	log.Printf("Unsubscribing from organization: %s", orgSlug)

	// Cancel the context to stop message processing
	consumer.Cancel()

	// Wait for goroutine to finish
	consumer.wg.Wait()

	// Close the channel
	if consumer.Channel != nil {
		_ = consumer.Channel.Close()
	}

	// Remove from active consumers
	delete(c.tenantConsumers, orgSlug)

	log.Printf("Successfully unsubscribed from org: %s", orgSlug)
}

// processTenantMessages processes messages for a specific tenant
func (c *Client) processTenantMessages(ctx context.Context, orgSlug string, messages <-chan amqp.Delivery) {
	log.Printf("Processing messages for org: %s", orgSlug)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping message processing for org: %s", orgSlug)
			return

		case msg, ok := <-messages:
			if !ok {
				log.Printf("Message channel closed for org: %s", orgSlug)
				return
			}

			log.Printf("Received message for org %s: routing key %s", orgSlug, msg.RoutingKey)
			c.handleMessage(msg)
		}
	}
}

func (c *Client) Stop() error {
	log.Println("Stopping AMQP client")

	close(c.done)

	if c.channel != nil {
		if err := c.channel.Cancel(c.config.ConsumerTag, true); err != nil {
			log.Printf("Error cancelling consumer: %v", err)
		}
		_ = c.channel.Close()
	}

	if c.connection != nil {
		_ = c.connection.Close()
	}

	close(c.messagesChan)
	log.Println("AMQP client stopped")
	return nil
}
