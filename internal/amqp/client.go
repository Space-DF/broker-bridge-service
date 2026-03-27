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
	"github.com/Space-DF/broker-bridge-service/internal/circuitbreaker"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
	"github.com/Space-DF/broker-bridge-service/internal/telemetry"
	amqp "github.com/rabbitmq/amqp091-go"
	otellog "go.opentelemetry.io/otel/log"
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

	// Connection monitoring
	circuitBreaker       *circuitbreaker.CircuitBreaker
	reconnectChan        chan struct{}
	connCloseNotifier    chan *amqp.Error
	channelCloseNotifier chan *amqp.Error
	reconnecting         bool // Flag to prevent concurrent reconnections
}

func NewClient(cfg config.AMQPConfig, orgEventsCfg config.OrgEventsConfig) *Client {
	// Create circuit breaker
	cbConfig := circuitbreaker.Config{
		MaxFailures:      5,
		ResetTimeout:     30 * time.Second,
		SuccessThreshold: 2,
	}
	cb := circuitbreaker.New(cbConfig)

	return &Client{
		config:               cfg,
		orgEventsConfig:      orgEventsCfg,
		messagesChan:         make(chan *models.AMQPMessageWithDelivery, 100),
		done:                 make(chan bool),
		tenantConsumers:      make(map[string]*TenantConsumer),
		vhostPool:            pool.New(cfg.URL),
		circuitBreaker:       cb,
		reconnectChan:        make(chan struct{}, 1),
		connCloseNotifier:    make(chan *amqp.Error, 1),
		channelCloseNotifier: make(chan *amqp.Error, 1),
	}
}

func (c *Client) Connect() error {
	var err error

	// Use circuit breaker for connection attempts
	err = c.circuitBreaker.Execute(func() error {
		// Connect to AMQP broker
		c.orgEventsConn, err = amqp.Dial(c.config.URL)
		if err != nil {
			return fmt.Errorf("failed to connect to AMQP broker: %w", err)
		}

		// Create separate channel for org events
		c.orgEventsChannel, err = c.orgEventsConn.Channel()
		if err != nil {
			defer func() {
				if err = c.orgEventsConn.Close(); err != nil {
					log.Printf("Failed to close AMQP connection: %v", err)
				}
			}()
			return fmt.Errorf("failed to open org events channel: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	c.eventManager = rabbitmq.NewEventManager(
		c.orgEventsChannel,
		c.orgEventsConfig,
		c.subscribeToOrganizationForEvent,
		c.unsubscribeFromOrganizationForEvent,
	)

	// Register close notifiers for connection monitoring
	c.setupConnectionMonitoring()

	log.Printf("Successfully connected to AMQP at %s", c.config.URL)
	return nil
}

// setupConnectionMonitoring sets up close notifiers for the connection and channel
func (c *Client) setupConnectionMonitoring() {
	c.connCloseNotifier = make(chan *amqp.Error, 1)
	c.channelCloseNotifier = make(chan *amqp.Error, 1)
	c.orgEventsConn.NotifyClose(c.connCloseNotifier)
	c.orgEventsChannel.NotifyClose(c.channelCloseNotifier)
}

// monitorConnection monitors the connection and triggers reconnection if needed
func (c *Client) monitorConnection(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case err, ok := <-c.connCloseNotifier:
			if ok && err != nil {
				c.handleConnectionClosed(err)
			}

		case err, ok := <-c.channelCloseNotifier:
			if ok && err != nil {
				c.handleChannelClosed(err)
			}

		case <-c.reconnectChan:
			if err := c.reconnectConnection(ctx); err != nil {
				log.Printf("Reconnection failed: %v", err)
			}
		}
	}
}

// handleConnectionClosed handles unexpected connection closure
func (c *Client) handleConnectionClosed(err *amqp.Error) {
	log.Printf("AMQP connection closed unexpectedly: %v (code: %d)", err, err.Code)

	// Invalidate all pooled vhost connections since they're also closed
	c.vhostPool.InvalidateAll()

	// Record failure in circuit breaker
	c.circuitBreaker.RecordFailure()

	// Notify reconnection goroutine
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

// handleChannelClosed handles unexpected channel closure
func (c *Client) handleChannelClosed(err *amqp.Error) {
	log.Printf("AMQP channel closed unexpectedly: %v (code: %d)", err, err.Code)

	// Just trigger full reconnection - it's safer and simpler
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

// reconnectConnection attempts to reconnect to the AMQP broker
func (c *Client) reconnectConnection(ctx context.Context) error {
	log.Println("Attempting to reconnect to AMQP broker")

	// Set reconnecting flag to prevent individual tenant subscriptions
	c.reconnecting = true
	defer func() {
		// Will be set to false by caller on success, or here on failure
		if c.reconnecting {
			c.reconnecting = false
		}
	}()

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 30

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check circuit breaker
		if c.circuitBreaker.State() == circuitbreaker.StateOpen {
			log.Printf("Circuit breaker is open, waiting %v", 30*time.Second)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
			}
		}

		// Close existing connection if any
		if c.orgEventsConn != nil && !c.orgEventsConn.IsClosed() {
			_ = c.orgEventsConn.Close()
		}

		// Attempt to reconnect
		err := func() error {
			conn, err := amqp.Dial(c.config.URL)
			if err != nil {
				return err
			}

			ch, err := conn.Channel()
			if err != nil {
				if err := conn.Close(); err != nil {
					log.Printf("Failed to close AMQP connection: %v", err)
				}

				return err
			}

			c.orgEventsConn = conn
			c.orgEventsChannel = ch

			// Re-register close notifiers
			c.connCloseNotifier = make(chan *amqp.Error, 1)
			c.channelCloseNotifier = make(chan *amqp.Error, 1)
			c.orgEventsConn.NotifyClose(c.connCloseNotifier)
			c.orgEventsChannel.NotifyClose(c.channelCloseNotifier)

			// Recreate event manager
			c.eventManager = rabbitmq.NewEventManager(
				c.orgEventsChannel,
				c.orgEventsConfig,
				c.subscribeToOrganizationForEvent,
				c.unsubscribeFromOrganizationForEvent,
			)

			return nil
		}()

		if err == nil {
			log.Printf("Successfully reconnected to AMQP broker (attempt %d)", attempt)

			// Record success in circuit breaker
			c.circuitBreaker.RecordSuccess()

			// Wait a moment for connection to stabilize before re-establishing tenants
			log.Println("Waiting for connection to stabilize before re-establishing tenants...")
			time.Sleep(500 * time.Millisecond)

			// Re-establish all tenant connections (with reconnecting flag still set)
			c.reestablishTenantConnections(ctx)

			log.Printf("Re-established %d tenant connections", len(c.tenantConsumers))
			return nil
		}

		log.Printf("Reconnection attempt %d failed: %v", attempt, err)

		// Record failure in circuit breaker
		c.circuitBreaker.RecordFailure()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Exponential backoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return fmt.Errorf("failed to reconnect after %d attempts", maxAttempts)
}

// reestablishTenantConnections re-establishes connections for all active tenants
func (c *Client) reestablishTenantConnections(ctx context.Context) {
	c.tenantConsumersMu.Lock()
	tenants := make([]*TenantConsumer, 0, len(c.tenantConsumers))
	for _, consumer := range c.tenantConsumers {
		tenants = append(tenants, consumer)
	}
	c.tenantConsumersMu.Unlock()

	successCount := 0
	for _, tenant := range tenants {
		// Remove from map so subscribeToOrganization can add it back
		c.tenantConsumersMu.Lock()
		delete(c.tenantConsumers, tenant.OrgSlug)
		c.tenantConsumersMu.Unlock()

		// Bypass reconnecting check since we're already in reconnection flow
		err := c.subscribeToOrganization(ctx, tenant.Vhost, tenant.OrgSlug, tenant.QueueName, true)

		if err == nil {
			successCount++
		} else {
			log.Printf("Failed to resubscribe to %s: %v", tenant.OrgSlug, err)
		}
	}

	log.Printf("Re-established %d/%d tenant connections", successCount, len(tenants))
}

func (c *Client) Start(ctx context.Context) error {
	log.Println("Starting AMQP client with org event discovery...")

	// Start reconnection monitor goroutine
	go c.monitorConnection(ctx)

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
	ctx := context.Background()
	isEntityTelemetry := strings.Contains(msg.RoutingKey, ".entity.") && strings.HasSuffix(msg.RoutingKey, ".telemetry")

	if isEntityTelemetry {
		var entityPayload models.EntityTelemetryPayload
		if err := json.Unmarshal(msg.Body, &entityPayload); err == nil && entityPayload.Entity.UniqueID != "" {
			messageWithDelivery := &models.AMQPMessageWithDelivery{
				Kind:         models.KindEntityTelemetry,
				EntityUpdate: &entityPayload,
				Delivery:     &msg,
			}

			telemetry.LogInfo(ctx, fmt.Sprintf("Received entity telemetry for entity %s from routing key %s", entityPayload.Entity.UniqueID, msg.RoutingKey),
				otellog.String("entity_id", entityPayload.Entity.UniqueID),
				otellog.String("routing_key", msg.RoutingKey),
			)

			select {
			case c.messagesChan <- messageWithDelivery:
				// Successfully queued
			default:
				log.Printf("Message channel full, dropping entity telemetry for entity: %s", entityPayload.Entity.UniqueID)
				telemetry.LogWarn(ctx, fmt.Sprintf("Message channel full, dropping entity telemetry for entity %s", entityPayload.Entity.UniqueID),
					otellog.String("entity_id", entityPayload.Entity.UniqueID),
				)
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

		telemetry.LogInfo(ctx, fmt.Sprintf("Received device location update for device %s from routing key %s", locationUpdate.DeviceEUI, msg.RoutingKey),
			otellog.String("device_eui", locationUpdate.DeviceEUI),
			otellog.String("device_id", locationUpdate.DeviceID),
			otellog.String("routing_key", msg.RoutingKey),
		)

		select {
		case c.messagesChan <- messageWithDelivery:
			// Successfully queued
		default:
			log.Printf("Message channel full, dropping location update for device: %s", locationUpdate.DeviceEUI)
			telemetry.LogWarn(ctx, fmt.Sprintf("Message channel full, dropping location update for device %s", locationUpdate.DeviceEUI),
				otellog.String("device_eui", locationUpdate.DeviceEUI),
			)
			if !c.config.AutoAck {
				_ = msg.Nack(false, true)
			}
		}
		return
	}

	log.Printf("Error unmarshaling message into known types; dropping")
	telemetry.LogError(ctx, fmt.Sprintf("Failed to unmarshal message from routing key %s into known types", msg.RoutingKey),
		otellog.String("routing_key", msg.RoutingKey),
	)
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
		// Silently ignore ACK errors from closed channels (testing)
		// The message will be requeued by RabbitMQ automatically
		_ = delivery.Ack(false)
	}
	return nil
}

// NackMessage negatively acknowledges an AMQP message
func (c *Client) NackMessage(delivery *amqp.Delivery, requeue bool) error {
	if !c.config.AutoAck && delivery != nil {
		// Silently ignore NACK errors from closed channels (testing)
		_ = delivery.Nack(false, requeue)
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
		return c.subscribeToOrganization(ctx, vhost, orgSlug, queueName, false)

	case models.OrgDeactivated, models.OrgDeleted:
		// Org deleted/deactivated - unsubscribe
		c.unsubscribeFromOrganization(orgSlug)
		return nil

	default:
		log.Printf("Unknown event type: %s", event.EventType)
		return nil
	}
}

// subscribeToOrganizationForEvent is a wrapper for EventManager callback (doesn't bypass reconnecting check)
func (c *Client) subscribeToOrganizationForEvent(ctx context.Context, vhost, orgSlug, queueName string) error {
	return c.subscribeToOrganization(ctx, vhost, orgSlug, queueName, false)
}

// unsubscribeFromOrganizationForEvent is a wrapper for EventManager callback
func (c *Client) unsubscribeFromOrganizationForEvent(orgSlug string) {
	c.unsubscribeFromOrganization(orgSlug)
}

// subscribeToOrganization subscribes to a specific organization's queue
func (c *Client) subscribeToOrganization(ctx context.Context, vhost, orgSlug, queueName string, bypassReconnectingCheck bool) error {
	if queueName == "" {
		queueName = fmt.Sprintf("%s.transformed.data.queue", orgSlug)
	}

	if !helpers.ShouldHandleVhost(vhost, c.config.AllowedVhosts) {
		log.Printf("Skipping subscription for org %s in vhost %s (not assigned to this instance)", orgSlug, vhost)
		return nil
	}

	// Block subscription if main connection is reconnecting (unless bypassed)
	if !bypassReconnectingCheck && c.reconnecting {
		return fmt.Errorf("main connection is reconnecting, tenant subscription deferred for %s", orgSlug)
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

	tenantCtx, cancel := context.WithCancel(ctx) // #nosec G118

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
				log.Printf("Message channel closed for org: %s (vhost: %s), draining remaining messages and triggering resubscription", tenant.OrgSlug, tenant.Vhost)

				// Drain any remaining messages in the channel (they will be requeued by RabbitMQ)
				for range messages {
					// Just drain, don't process or ACK
				}

				// Trigger resubscription for this tenant
				go c.resubscribeTenant(ctx, tenant)
				return
			}

			// Check if the delivery channel is still valid before processing
			// When the AMQP connection closes, the delivery becomes invalid
			if tenant.Channel == nil || tenant.Channel.IsClosed() {
				log.Printf("Tenant channel closed, skipping message processing for %s", tenant.OrgSlug)
				go c.resubscribeTenant(ctx, tenant)
				return
			}

			log.Printf("Received message for org %s (vhost: %s): routing key %s", tenant.OrgSlug, tenant.Vhost, msg.RoutingKey)

			// Process the message (puts it in messagesChan for bridge to handle)
			c.handleMessage(msg)

			// NOTE: ACK is handled by the bridge layer via AckMessage()
			// We don't ACK here to avoid double-ACKing
		}
	}
}

// resubscribeTenant attempts to resubscribe to a single tenant
func (c *Client) resubscribeTenant(ctx context.Context, oldTenant *TenantConsumer) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 5

	// Check if main connection is down - if so, let centralized reconnection handle it
	if c.orgEventsConn == nil || c.orgEventsConn.IsClosed() {
		log.Printf("Main connection down, waiting for centralized reconnection for %s", oldTenant.OrgSlug)
		return
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Remove old tenant from map first so subscribeToOrganization can create a new subscription
		c.tenantConsumersMu.Lock()
		delete(c.tenantConsumers, oldTenant.OrgSlug)
		c.tenantConsumersMu.Unlock()

		err := c.subscribeToOrganization(ctx, oldTenant.Vhost, oldTenant.OrgSlug, oldTenant.QueueName, false)
		if err == nil {
			log.Printf("Successfully resubscribed to %s", oldTenant.OrgSlug)
			return
		}

		log.Printf("Failed to resubscribe to %s (attempt %d): %v", oldTenant.OrgSlug, attempt+1, err)

		// Exponential backoff
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	log.Printf("Failed to resubscribe to %s after %d attempts", oldTenant.OrgSlug, maxAttempts)
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
