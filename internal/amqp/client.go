package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/amqp/helpers"
	"github.com/Space-DF/broker-bridge-service/internal/amqp/logging"
	"github.com/Space-DF/broker-bridge-service/internal/amqp/rabbitmq"
	pool "github.com/Space-DF/broker-bridge-service/internal/amqp/vhostpool"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TenantConsumer represents a consumer for a specific tenant
type TenantConsumer struct {
	OrgSlug     string
	Vhost       string
	QueueName   string
	ConsumerTag string
	Channel     *amqp.Channel
	Cancel      context.CancelFunc
	wg          sync.WaitGroup
}

type Client struct {
	config            config.AMQPConfig
	orgEventsConfig   config.OrgEventsConfig
	orgEventsConn     *amqp.Connection
	orgEventsChannel  *amqp.Channel
	messagesChan      chan *models.AMQPMessageWithDelivery
	done              chan bool
	tenantConsumers   map[string]*TenantConsumer
	tenantConsumersMu sync.RWMutex
	vhostPool         *pool.Pool
	eventManager      *rabbitmq.EventManager
}

func NewClient(cfg config.AMQPConfig, orgEventsCfg config.OrgEventsConfig) *Client {
	return &Client{
		config:          cfg,
		orgEventsConfig: orgEventsCfg,
		messagesChan:    make(chan *models.AMQPMessageWithDelivery, 100),
		done:            make(chan bool),
		tenantConsumers: make(map[string]*TenantConsumer),
		vhostPool:       pool.New(cfg.URL),
	}
}

func (c *Client) Connect() error {
	var err error

	c.orgEventsConn, err = amqp.Dial(c.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to AMQP: %w", err)
	}

	// Create separate channel for org events
	c.orgEventsChannel, err = c.orgEventsConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open org events channel: %w", err)
	}

	c.eventManager = rabbitmq.NewEventManager(
		c.orgEventsChannel,
		c.orgEventsConfig,
		c.subscribeToOrganization,
		c.unsubscribeFromOrganization,
	)

	log.Printf("Successfully connected to AMQP at %s", c.config.URL)

	return nil
}

func (c *Client) Start(ctx context.Context) error {
	log.Println("Starting AMQP client with org event discovery...")

	// Start listening for org events
	go func() {
		if err := c.eventManager.ListenToOrgEvents(ctx, c.handleOrgEvent); err != nil {
			log.Printf("Error listening to org events: %v", err)
		}
	}()

	// Send discovery request to get all active organizations
	if err := c.eventManager.SendDiscoveryRequest(ctx); err != nil {
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
	isEntityTelemetry := strings.Contains(msg.RoutingKey, ".entity.") && strings.HasSuffix(msg.RoutingKey, ".telemetry")
	
	if isEntityTelemetry {
		var entityPayload models.EntityTelemetryPayload
		if err := json.Unmarshal(msg.Body, &entityPayload); err == nil && entityPayload.Entity.UniqueID != "" {
			messageWithDelivery := &models.AMQPMessageWithDelivery{
				Kind:         models.KindEntityTelemetry,
				EntityUpdate: &entityPayload,
				Delivery:     &msg,
			}

			select {
			case c.messagesChan <- messageWithDelivery:
				// Successfully queued
			default:
				log.Printf("Message channel full, dropping entity telemetry for entity: %s", entityPayload.Entity.UniqueID)
				if !c.config.AutoAck {
					_ = msg.Nack(false, true)
				}
			}
			return
		}
	}

	// Try to parse as device location update
	var locationUpdate models.DeviceLocationUpdate
	if err := json.Unmarshal(msg.Body, &locationUpdate); err == nil && locationUpdate.DeviceEUI != "" {
		locationUpdate.UpdatedAt = time.Now()

		messageWithDelivery := &models.AMQPMessageWithDelivery{
			Kind:           models.KindLocationUpdate,
			LocationUpdate: &locationUpdate,
			Delivery:       &msg,
		}

		select {
		case c.messagesChan <- messageWithDelivery:
			// Successfully queued
		default:
			log.Printf("Message channel full, dropping location update for device: %s", locationUpdate.DeviceEUI)
			if !c.config.AutoAck {
				_ = msg.Nack(false, true)
			}
		}
		return
	}

	log.Printf("Error unmarshaling message into known types; dropping")
	if !c.config.AutoAck {
		_ = msg.Nack(false, false)
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

// handleOrgEvent processes organization lifecycle events
func (c *Client) handleOrgEvent(ctx context.Context, msg amqp.Delivery) error {
	log.Printf("Received org event with routing key: %s", msg.RoutingKey)

	// Handle regular org lifecycle events
	var event models.OrgEvent

	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("failed to unmarshal org event: %w", err)
	}

	log.Printf("Processing org event: %s for org: %s", event.EventType, event.Payload.Slug)

	orgSlug := event.Payload.Slug
	vhost := event.Payload.Vhost
	queueName := event.Payload.TransformedQueue

	if queueName == "" && orgSlug != "" {
		queueName = fmt.Sprintf("%s.transformed.data.queue", orgSlug)
	}

	switch event.EventType {

	case models.OrgCreated:
		// New org created - subscribe to its queue
		if vhost == "" {
			log.Printf("Org created event missing vhost for org: %s", orgSlug)
			return nil
		}
		return c.subscribeToOrganization(ctx, vhost, orgSlug, queueName)

	case models.OrgDeactivated, models.OrgDeleted:
		// Org deleted/deactivated - unsubscribe
		c.unsubscribeFromOrganization(orgSlug)
		return nil

	default:
		log.Printf("Unknown event type: %s", event.EventType)
		return nil
	}
}

// subscribeToOrganization subscribes to a specific organization's queue
func (c *Client) subscribeToOrganization(ctx context.Context, vhost, orgSlug, queueName string) error {
	if queueName == "" {
		queueName = fmt.Sprintf("%s.transformed.data.queue", orgSlug)
	}

	if !helpers.ShouldHandleVhost(vhost, c.config.AllowedVhosts) {
		log.Printf("Skipping subscription for org %s in vhost %s (not assigned to this instance)", orgSlug, vhost)
		return nil
	}

	c.tenantConsumersMu.Lock()
	if _, exists := c.tenantConsumers[orgSlug]; exists {
		c.tenantConsumersMu.Unlock()
		log.Printf("Already subscribed to org: %s (vhost: %s)", orgSlug, vhost)
		return nil
	}
	c.tenantConsumersMu.Unlock()

	log.Printf("Subscribing to organization: %s (vhost: %s)", orgSlug, vhost)

	conn, err := c.vhostPool.Acquire(vhost)
	if err != nil {
		return err
	}

	tenantChannel, err := conn.Channel()
	if err != nil {
		c.vhostPool.Release(vhost)
		return fmt.Errorf("failed to create channel for org %s in vhost %s: %w", orgSlug, vhost, err)
	}

	if err := tenantChannel.Qos(10, 0, false); err != nil {
		_ = tenantChannel.Close()
		c.vhostPool.Release(vhost)
		return fmt.Errorf("failed to set QoS for org %s in vhost %s: %w", orgSlug, vhost, err)
	}

	consumerTag := helpers.MakeConsumerTag(orgSlug, vhost)

	messages, err := tenantChannel.Consume(
		queueName,
		consumerTag,
		c.config.AutoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = tenantChannel.Close()
		c.vhostPool.Release(vhost)
		return fmt.Errorf("failed to start consuming for org %s in vhost %s: %w", orgSlug, vhost, err)
	}

	tenantCtx, cancel := context.WithCancel(ctx)

	tenantConsumer := &TenantConsumer{
		OrgSlug:     orgSlug,
		Vhost:       vhost,
		QueueName:   queueName,
		ConsumerTag: consumerTag,
		Channel:     tenantChannel,
		Cancel:      cancel,
	}

	c.tenantConsumersMu.Lock()
	c.tenantConsumers[orgSlug] = tenantConsumer
	c.tenantConsumersMu.Unlock()

	tenantConsumer.wg.Add(1)
	go func() {
		defer tenantConsumer.wg.Done()
		c.processTenantMessages(tenantCtx, tenantConsumer, messages)
	}()

	log.Printf("Successfully subscribed to org: %s (vhost: %s, queue: %s)", orgSlug, vhost, queueName)
	return nil
}

// unsubscribeFromOrganization unsubscribes from a specific organization
func (c *Client) unsubscribeFromOrganization(orgSlug string) {
	c.tenantConsumersMu.Lock()
	consumer, exists := c.tenantConsumers[orgSlug]
	if !exists {
		c.tenantConsumersMu.Unlock()
		log.Printf("Not subscribed to org: %s", orgSlug)
		return
	}

	delete(c.tenantConsumers, orgSlug)
	c.tenantConsumersMu.Unlock()

	log.Printf("Unsubscribing from organization: %s", orgSlug)

	// Cancel the context to stop message processing
	consumer.Cancel()

	// Wait for goroutine to finish
	consumer.wg.Wait()

	// Close the channel
	if consumer.Channel != nil {
		_ = consumer.Channel.Cancel(consumer.ConsumerTag, false)
		_ = consumer.Channel.Close()
	}

	c.vhostPool.Release(consumer.Vhost)

	log.Printf("Successfully unsubscribed from org: %s (vhost: %s)", orgSlug, consumer.Vhost)
}

// processTenantMessages processes messages for a specific tenant
func (c *Client) processTenantMessages(ctx context.Context, tenant *TenantConsumer, messages <-chan amqp.Delivery) {
	log.Printf("Processing messages for org: %s (vhost: %s)", tenant.OrgSlug, tenant.Vhost)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping message processing for org: %s (vhost: %s)", tenant.OrgSlug, tenant.Vhost)
			return

		case msg, ok := <-messages:
			if !ok {
				log.Printf("Message channel closed for org: %s (vhost: %s)", tenant.OrgSlug, tenant.Vhost)
				return
			}

			log.Printf("Received message for org %s (vhost: %s): routing key %s", tenant.OrgSlug, tenant.Vhost, msg.RoutingKey)
			c.handleMessage(msg)
		}
	}
}

// stopAllConsumers stops all active tenant consumers
func (c *Client) stopAllConsumers() {
	log.Println("Stopping all tenant consumers...")
	c.tenantConsumersMu.Lock()
	for slug, consumer := range c.tenantConsumers {
		if consumer != nil {
			consumer.Cancel()
			consumer.wg.Wait()
			if consumer.Channel != nil {
				_ = consumer.Channel.Cancel(consumer.ConsumerTag, false)
				_ = consumer.Channel.Close()
			}
			c.vhostPool.Release(consumer.Vhost)
			logging.Tenant(consumer.OrgSlug, consumer.Vhost, "", "Stopped consuming from %s", consumer.QueueName)
		}
		delete(c.tenantConsumers, slug)
	}
	c.tenantConsumersMu.Unlock()

	c.vhostPool.CloseAll()
	log.Println("All tenant consumers stopped")
}

func (c *Client) Stop() error {
	log.Println("Stopping AMQP client")

	close(c.done)

	c.stopAllConsumers()

	if c.orgEventsChannel != nil {
		_ = c.orgEventsChannel.Close()
	}

	if c.orgEventsConn != nil {
		_ = c.orgEventsConn.Close()
	}

	close(c.messagesChan)
	log.Println("AMQP client stopped")
	return nil
}
