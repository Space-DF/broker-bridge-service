package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

// messageHandler defines the contract for handling different AMQP message types.
type messageHandler interface {
	Handle(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error)
}

// ErrUnhandled indicates that a handler cannot process this message.
var ErrUnhandled = fmt.Errorf("message not handled")

// Singleton handler instances - handlers are stateless, so reuse single instances.
var (
	singletonEventHandler           = &eventHandler{}
	singletonEntityTelemetryHandler = &entityTelemetryHandler{}
	singletonLocationUpdateHandler  = &locationUpdateHandler{}
	singletonUnknownHandler         = &unknownHandler{}
)

// messageKindFromRoutingKey determines the message type from the routing key.
// Pure function with no side effects - easy to test and reason about.
func messageKindFromRoutingKey(routingKey string) models.MessageKind {
	// Order matters: check more specific patterns first
	switch {
	case strings.HasSuffix(routingKey, ".event"):
		// Matches: tenant.{org}.space.{space}.device.{device_id}.event
		return models.KindEvent
	case strings.Contains(routingKey, ".entity.") && strings.HasSuffix(routingKey, ".telemetry"):
		// Matches: tenant.{org}.space.{space}.entity.{entity_id}_{type}.telemetry
		return models.KindEntityTelemetry
	case strings.HasSuffix(routingKey, ".location") || strings.Contains(routingKey, ".transformed.device."):
		// Matches: tenant.{org}.space.{space}.device.{device_id}.location
		return models.KindLocationUpdate
	default:
		// Unknown routing key pattern - log warning but default to location update for backward compatibility
		log.Printf("WARNING: Unknown routing key pattern: %s, defaulting to location_update", routingKey)
		return models.KindLocationUpdate
	}
}

// HandlerRegistry manages message handlers with O(1) map-based routing.
type HandlerRegistry struct {
	handlers map[models.MessageKind]messageHandler
}

// NewHandlerRegistry creates a new registry with all registered handlers.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: map[models.MessageKind]messageHandler{
			models.KindEvent:           singletonEventHandler,
			models.KindEntityTelemetry: singletonEntityTelemetryHandler,
			models.KindLocationUpdate:  singletonLocationUpdateHandler,
		},
	}
}

// Route determines the message kind and routes to the appropriate handler.
func (r *HandlerRegistry) Route(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error) {
	kind := messageKindFromRoutingKey(msg.RoutingKey)

	handler, exists := r.handlers[kind]
	if !exists {
		log.Printf("WARNING: No handler registered for message kind: %s, using unknown handler", kind)
		return singletonUnknownHandler.Handle(ctx, msg)
	}

	return handler.Handle(ctx, msg)
}

// eventHandler handles event messages with routing key ending in ".event" or ".events".
type eventHandler struct{}

func (h *eventHandler) Handle(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error) {
	var event models.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal event: %v", ErrUnhandled, err)
	}

	if event.DeviceID == "" {
		log.Printf("WARNING: Event message rejected: DeviceID is empty. Routing key: %s", msg.RoutingKey)
		return nil, ErrUnhandled
	}

	// Extract organization from routing key pattern: tenant.{org}.space.{space}.device.{device}.events
	event.Organization = extractOrgFromRoutingKey(msg.RoutingKey)

	messageWithDelivery := &models.AMQPMessageWithDelivery{
		Kind:     models.KindEvent,
		Event:    &event,
		Delivery: &msg,
	}

	return messageWithDelivery, nil
}

// entityTelemetryHandler handles entity telemetry messages.
type entityTelemetryHandler struct{}

func (h *entityTelemetryHandler) Handle(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error) {
	var entityPayload models.EntityTelemetryPayload
	if err := json.Unmarshal(msg.Body, &entityPayload); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal entity telemetry: %v", ErrUnhandled, err)
	}

	if entityPayload.Entity.UniqueID == "" {
		log.Printf("WARNING: Entity telemetry rejected: UniqueID is empty. Routing key: %s", msg.RoutingKey)
		return nil, ErrUnhandled
	}

	log.Printf("Received entity telemetry for entity %s from routing key %s", entityPayload.Entity.UniqueID, msg.RoutingKey)

	messageWithDelivery := &models.AMQPMessageWithDelivery{
		Kind:         models.KindEntityTelemetry,
		EntityUpdate: &entityPayload,
		Delivery:     &msg,
	}

	return messageWithDelivery, nil
}

// locationUpdateHandler handles device location update messages (fallback/default).
type locationUpdateHandler struct{}

func (h *locationUpdateHandler) Handle(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error) {
	var locationUpdate models.DeviceLocationUpdate
	if err := json.Unmarshal(msg.Body, &locationUpdate); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal location update: %v", ErrUnhandled, err)
	}

	if locationUpdate.DeviceEUI == "" {
		log.Printf("WARNING: Location update rejected: DeviceEUI is empty. Routing key: %s", msg.RoutingKey)
		return nil, ErrUnhandled
	}

	locationUpdate.UpdatedAt = time.Now()

	messageWithDelivery := &models.AMQPMessageWithDelivery{
		Kind:           models.KindLocationUpdate,
		LocationUpdate: &locationUpdate,
		Delivery:       &msg,
	}

	return messageWithDelivery, nil
}

// unknownHandler handles unknown message types as a fallback.
type unknownHandler struct{}

func (h *unknownHandler) Handle(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error) {
	log.Printf("WARNING: Received message with unknown routing key pattern: %s. Message will be dropped.", msg.RoutingKey)
	return nil, ErrUnhandled
}

// extractOrgFromRoutingKey extracts the organization slug from a routing key.
// Expected format: tenant.{org}.space.{space}.device.{device}.event(s)
func extractOrgFromRoutingKey(routingKey string) string {
	parts := strings.Split(routingKey, ".")
	if len(parts) >= 2 && parts[0] == "tenant" {
		return parts[1]
	}
	return "unknown"
}

// queueMessage attempts to send a message to the channel, with backpressure handling.
func queueMessage(ch chan<- *models.AMQPMessageWithDelivery, msg *models.AMQPMessageWithDelivery) bool {
	select {
	case ch <- msg:
		return true
	default:
		return false
	}
}

// logDroppedMessage logs when a message is dropped due to a full channel.
func logDroppedMessage(ctx context.Context, msgType, identifier, routingKey string) {
	log.Printf("WARNING: Dropping %s message for %s due to full channel. Routing key: %s", msgType, identifier, routingKey)
}
