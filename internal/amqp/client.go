package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/amqp/helpers"
	"github.com/Space-DF/broker-bridge-service/internal/amqp/logging"
	"github.com/Space-DF/broker-bridge-service/internal/amqp/rabbitmq"
	pool "github.com/Space-DF/broker-bridge-service/internal/amqp/vhostpool"
	"github.com/Space-DF/broker-bridge-service/internal/circuitbreaker"
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
	ParentCtx   context.Context
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
	handlerRegistry   *HandlerRegistry

	stopOnce sync.Once

	// Connection monitoring
	circuitBreaker       *circuitbreaker.CircuitBreaker
	reconnectChan        chan struct{}
	connCloseNotifier    chan *amqp.Error
	channelCloseNotifier chan *amqp.Error
	reconnecting         atomic.Bool
	orgEventsCancel      context.CancelFunc
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
		handlerRegistry:      NewHandlerRegistry(),
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
			// Check if already reconnecting to prevent concurrent reconnections
			if c.reconnecting.Load() {
				log.Println("Already reconnecting, skipping duplicate request")
				continue
			}

			// Set flag before calling reconnectConnection
			c.reconnecting.Store(true)

			if err := c.reconnectConnection(ctx); err != nil {
				c.reconnecting.Store(false)
				log.Printf("Reconnection failed: %v", err)
				// Schedule another reconnection attempt
				go func() {
					select {
					case <-time.After(10 * time.Second):
						select {
						case c.reconnectChan <- struct{}{}:
						default:
						}
					case <-ctx.Done():
					}
				}()
			} else {
				// Clear flag immediately — reestablishTenantConnections already
				// resubscribed all tenants, so org events can be processed normally.
				c.reconnecting.Store(false)
				// Drain stale reconnection requests
				for {
					select {
					case _, ok := <-c.reconnectChan:
						if !ok {
							goto done
						}
					default:
						goto done
					}
				}
			done:
				log.Println("Successfully reconnected and re-established all tenants")
			}
		}
	}
}

// handleConnectionClosed handles unexpected connection closure
func (c *Client) handleConnectionClosed(err *amqp.Error) {
	log.Printf("AMQP connection closed unexpectedly: %v (code: %d)", err, err.Code)

	// Invalidate all pooled vhost connections since they're also closed
	c.vhostPool.InvalidateAll()

	// Notify reconnection goroutine
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

// handleChannelClosed handles unexpected channel closure
func (c *Client) handleChannelClosed(err *amqp.Error) {
	log.Printf("AMQP channel closed unexpectedly: %v (code: %d)", err, err.Code)

	// Record failure — channel error means something went wrong
	c.circuitBreaker.RecordFailure()

	// Just trigger full reconnection - it's safer and simpler
	select {
	case c.reconnectChan <- struct{}{}:
	default:
	}
}

// reconnectConnection attempts to reconnect to the AMQP broker
func (c *Client) reconnectConnection(ctx context.Context) error {
	log.Println("Attempting to reconnect to AMQP broker")

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second
	maxAttempts := 30

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check circuit breaker
		if c.circuitBreaker.State() == circuitbreaker.StateOpen {
			cbTimeout := c.circuitBreaker.ResetTimeout()
			log.Printf("Circuit breaker is open, waiting %v", cbTimeout)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cbTimeout):
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

			// Reset circuit breaker - connection is working again
			c.circuitBreaker.Reset()

			// Wait a moment for connection to stabilize before re-establishing tenants
			log.Println("Waiting for connection to stabilize before re-establishing tenants...")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}

			// Re-establish all tenant connections (with reconnecting flag still set)
			c.reestablishTenantConnections(ctx)

			log.Printf("Re-established %d tenant connections", len(c.tenantConsumers))
			// Cancel old org events listener
			if c.orgEventsCancel != nil {
				c.orgEventsCancel()
			}

			// Restart org events listener on the new EventManager
			orgEventsCtx, orgEventsCancel := context.WithCancel(ctx)
			c.orgEventsCancel = orgEventsCancel
			go func() {
				if err := c.eventManager.ListenToOrgEvents(orgEventsCtx, c.handleOrgEvent); err != nil {
					log.Printf("Error listening to org events after reconnection: %v", err)
				}
			}()

			// Send discovery on a separate channel to avoid racing
			go func() {
				ch, err := c.orgEventsConn.Channel()
				if err != nil {
					log.Printf("Warning: failed to open channel for discovery request: %v", err)
					return
				}
				defer ch.Close()
				if err := c.eventManager.SendDiscoveryRequestOnChannel(ctx, ch); err != nil {
					log.Printf("Warning: Failed to send discovery request after reconnection: %v", err)
				}
			}()

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
	orgEventsCtx, orgEventsCancel := context.WithCancel(ctx)
	c.orgEventsCancel = orgEventsCancel
	go func() {
		if err := c.eventManager.ListenToOrgEvents(orgEventsCtx, c.handleOrgEvent); err != nil {
			log.Printf("Error listening to org events: %v", err)
		}
	}()

	// Send discovery request on a separate channel to avoid concurrent access
	go func() {
		ch, err := c.orgEventsConn.Channel()
		if err != nil {
			log.Printf("Warning: failed to open channel for discovery request: %v", err)
			return
		}
		defer ch.Close()
		if err := c.eventManager.SendDiscoveryRequestOnChannel(ctx, ch); err != nil {
			log.Printf("Warning: Failed to send discovery request: %v", err)
		}
	}()

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

	result, err := c.handlerRegistry.Route(ctx, msg)
	if err != nil {
		c.handleParseError(msg, err)
		return
	}

	if !queueMessage(c.messagesChan, result) {
		identifier := c.getMessageIdentifier(result)
		logDroppedMessage(string(result.Kind), identifier, msg.RoutingKey)
		c.nackMessage(msg, true)
	}
}

// handleParseError handles messages that couldn't be parsed by any handler.
func (c *Client) handleParseError(msg amqp.Delivery, err error) {
	log.Printf("Error unmarshaling message from routing key %s: %v", msg.RoutingKey, err)
	c.nackMessage(msg, false)
}

// nackMessage negatively acknowledges an AMQP message.
func (c *Client) nackMessage(msg amqp.Delivery, requeue bool) {
	if !c.config.AutoAck {
		_ = msg.Nack(false, requeue)
	}
}

// getMessageIdentifier extracts a meaningful identifier from a message for logging.
func (c *Client) getMessageIdentifier(msg *models.AMQPMessageWithDelivery) string {
	switch msg.Kind {
	case models.KindEvent:
		if msg.Event != nil {
			return msg.Event.DeviceID
		}
	case models.KindEntityTelemetry:
		if msg.EntityUpdate != nil {
			return msg.EntityUpdate.Entity.UniqueID
		}
	case models.KindLocationUpdate:
		if msg.LocationUpdate != nil {
			return msg.LocationUpdate.DeviceEUI
		}
	}
	return "unknown"
}

func (c *Client) GetMessagesChan() <-chan *models.AMQPMessageWithDelivery {
	return c.messagesChan
}

// AckMessage acknowledges an AMQP message
func (c *Client) AckMessage(delivery *amqp.Delivery) error {
	if !c.config.AutoAck && delivery != nil {
		// Silently ignore ACK errors from closed channels (testing)
		// The message will be requeued by RabbitMQ automatically
		if err := delivery.Ack(false); err != nil {
			return err
		}
	}
	return nil
}

// NackMessage negatively acknowledges an AMQP message
func (c *Client) NackMessage(delivery *amqp.Delivery, requeue bool) error {
	if !c.config.AutoAck && delivery != nil {
		// Silently ignore NACK errors from closed channels
		if err := delivery.Nack(false, requeue); err != nil {
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
		// Skip if already subscribed (e.g. from reestablishTenantConnections)
		c.tenantConsumersMu.RLock()
		_, already := c.tenantConsumers[orgSlug]
		c.tenantConsumersMu.RUnlock()
		if already {
			log.Printf("Skipping org.created for %s: already subscribed", orgSlug)
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
	if !bypassReconnectingCheck && c.reconnecting.Load() {
		return fmt.Errorf("main connection is reconnecting, tenant subscription deferred for %s", orgSlug)
	}

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
		ParentCtx:   ctx,
	}

	// Check for existing consumer
	c.tenantConsumersMu.Lock()
	if existing, exists := c.tenantConsumers[orgSlug]; exists {
		log.Printf("WARNING: Replacing existing subscription for org: %s (vhost: %s). Old tag: %s, New tag: %s",
			orgSlug, vhost, existing.ConsumerTag, consumerTag)
		// Cancel and clean up the old consumer
		existing.Cancel()
		if existing.Channel != nil {
			_ = existing.Channel.Cancel(existing.ConsumerTag, false)
			_ = existing.Channel.Close()
		}
		c.vhostPool.Release(existing.Vhost)
	}
	c.tenantConsumers[orgSlug] = tenantConsumer
	c.tenantConsumersMu.Unlock()

	log.Printf("Creating consumer for org: %s (vhost: %s) with tag: %s", orgSlug, vhost, consumerTag)

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

				// Trigger resubscription for this tenant using parent context
				go c.resubscribeTenant(tenant.ParentCtx, tenant)
				return
			}

			// Check if the delivery channel is still valid before processing
			// When the AMQP connection closes, the delivery becomes invalid
			if tenant.Channel == nil || tenant.Channel.IsClosed() {
				log.Printf("Tenant channel closed, skipping message processing for %s", tenant.OrgSlug)
				go c.resubscribeTenant(tenant.ParentCtx, tenant)
				return
			}

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
		// Release the old tenant's vhost connection since we can't resubscribe
		if oldTenant.Channel != nil {
			_ = oldTenant.Channel.Cancel(oldTenant.ConsumerTag, false)
			_ = oldTenant.Channel.Close()
		}
		c.vhostPool.Release(oldTenant.Vhost)
		return
	}

	// Clean up the old tenant's resources
	if oldTenant.Channel != nil {
		_ = oldTenant.Channel.Cancel(oldTenant.ConsumerTag, false)
		_ = oldTenant.Channel.Close()
	}
	c.vhostPool.Release(oldTenant.Vhost)

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Remove old tenant from map first so subscribeToOrganization can create a new subscription
		c.tenantConsumersMu.Lock()
		delete(c.tenantConsumers, oldTenant.OrgSlug)
		c.tenantConsumersMu.Unlock()

		// Bypass reconnecting check since this is a tenant-level resubscription after channel closure
		err := c.subscribeToOrganization(ctx, oldTenant.Vhost, oldTenant.OrgSlug, oldTenant.QueueName, true)
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

	c.stopOnce.Do(func() {
		close(c.done)

		c.stopAllConsumers()

		if c.orgEventsChannel != nil {
			_ = c.orgEventsChannel.Close()
		}

		if c.orgEventsConn != nil {
			_ = c.orgEventsConn.Close()
		}

		close(c.messagesChan)
	})
	log.Println("AMQP client stopped")
	return nil
}
