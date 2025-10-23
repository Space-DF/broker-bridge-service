package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

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

type pooledConnection struct {
	conn     *amqp.Connection
	refCount int
}

type Client struct {
	config             config.AMQPConfig
	orgEventsConfig    config.OrgEventsConfig
	orgEventsConn      *amqp.Connection
	orgEventsChannel   *amqp.Channel
	messagesChan       chan *models.AMQPMessageWithDelivery
	done               chan bool
	tenantConsumers    map[string]*TenantConsumer
	tenantConsumersMu  sync.RWMutex
	vhostConnections   map[string]*pooledConnection
	vhostConnectionsMu sync.Mutex
}

func NewClient(cfg config.AMQPConfig, orgEventsCfg config.OrgEventsConfig) *Client {
	return &Client{
		config:           cfg,
		orgEventsConfig:  orgEventsCfg,
		messagesChan:     make(chan *models.AMQPMessageWithDelivery, 100),
		done:             make(chan bool),
		tenantConsumers:  make(map[string]*TenantConsumer),
		vhostConnections: make(map[string]*pooledConnection),
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

func (c *Client) shouldHandleVhost(vhost string) bool {
	if len(c.config.AllowedVhosts) == 0 {
		return true
	}

	for _, allowed := range c.config.AllowedVhosts {
		if allowed == vhost {
			return true
		}
	}
	return false
}

func (c *Client) buildVhostURL(vhost string) (string, error) {
	baseURL := c.config.URL
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse broker url: %w", err)
	}

	if vhost == "" {
		parsed.Path = "/"
		parsed.RawPath = ""
	} else {
		encoded := "/" + url.PathEscape(vhost)
		parsed.Path = encoded
		parsed.RawPath = encoded
	}

	return parsed.String(), nil
}

func (c *Client) getOrCreateVhostConnection(vhost string) (*amqp.Connection, error) {
	c.vhostConnectionsMu.Lock()
	defer c.vhostConnectionsMu.Unlock()

	if pooled, exists := c.vhostConnections[vhost]; exists {
		pooled.refCount++
		return pooled.conn, nil
	}

	vhostURL, err := c.buildVhostURL(vhost)
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(vhostURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to AMQP vhost %s: %w", vhost, err)
	}

	c.vhostConnections[vhost] = &pooledConnection{
		conn:     conn,
		refCount: 1,
	}

	return conn, nil
}

func (c *Client) releaseVhostConnection(vhost string) {
	c.vhostConnectionsMu.Lock()
	defer c.vhostConnectionsMu.Unlock()

	pooled, exists := c.vhostConnections[vhost]
	if !exists {
		return
	}

	pooled.refCount--
	if pooled.refCount <= 0 {
		_ = pooled.conn.Close()
		delete(c.vhostConnections, vhost)
	}
}

func (c *Client) makeConsumerTag(orgSlug, vhost string) string {
	safeVhost := strings.NewReplacer("/", "_", ".", "_", ":", "_").Replace(vhost)
	return fmt.Sprintf("%s-broker-bridge-%s-%d", orgSlug, safeVhost, time.Now().UnixNano())
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

	if !c.shouldHandleVhost(vhost) {
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

	conn, err := c.getOrCreateVhostConnection(vhost)
	if err != nil {
		return err
	}

	tenantChannel, err := conn.Channel()
	if err != nil {
		c.releaseVhostConnection(vhost)
		return fmt.Errorf("failed to create channel for org %s in vhost %s: %w", orgSlug, vhost, err)
	}

	if err := tenantChannel.Qos(10, 0, false); err != nil {
		_ = tenantChannel.Close()
		c.releaseVhostConnection(vhost)
		return fmt.Errorf("failed to set QoS for org %s in vhost %s: %w", orgSlug, vhost, err)
	}

	consumerTag := c.makeConsumerTag(orgSlug, vhost)

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
		c.releaseVhostConnection(vhost)
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

	c.releaseVhostConnection(consumer.Vhost)

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

func (c *Client) stopAllTenantConsumers() {
	c.tenantConsumersMu.Lock()
	for slug, consumer := range c.tenantConsumers {
		log.Printf("Stopping tenant consumer for org: %s (vhost: %s)", consumer.OrgSlug, consumer.Vhost)
		consumer.Cancel()
		consumer.wg.Wait()
		if consumer.Channel != nil {
			_ = consumer.Channel.Cancel(consumer.ConsumerTag, false)
			_ = consumer.Channel.Close()
		}
		c.releaseVhostConnection(consumer.Vhost)
		delete(c.tenantConsumers, slug)
	}
	c.tenantConsumersMu.Unlock()

	c.vhostConnectionsMu.Lock()
	for vhost, pooled := range c.vhostConnections {
		if pooled != nil && pooled.conn != nil {
			_ = pooled.conn.Close()
		}
		delete(c.vhostConnections, vhost)
	}
	c.vhostConnectionsMu.Unlock()
}

func (c *Client) Stop() error {
	log.Println("Stopping AMQP client")

	close(c.done)

	c.stopAllTenantConsumers()

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
