package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/models"
	"github.com/google/uuid"
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
	singletonAlertHandler           = &alertHandler{}
	singletonEntityTelemetryHandler = &entityTelemetryHandler{}
	singletonLocationUpdateHandler  = &locationUpdateHandler{}
	singletonActivityLogHandler     = &activityLogHandler{}
	singletonUnknownHandler         = &unknownHandler{}
)

// messageKindFromRoutingKey determines the message type from the routing key.
func messageKindFromRoutingKey(routingKey string) models.MessageKind {
	switch {
	case strings.HasSuffix(routingKey, ".event"):
		// Matches: tenant.{org}.space.{space}.device.{device_id}.event
		return models.KindEvent
	case strings.HasSuffix(routingKey, ".alert"):
		// Matches: tenant.{org}.space.{space}.device.{device_id}.alert
		return models.KindAlert
	case strings.HasSuffix(routingKey, ".telemetry"):
		// Matches: tenant.{org}.space.{space}.entity.{entity_id}_{type}.telemetry
		return models.KindEntityTelemetry
	case strings.HasSuffix(routingKey, ".location"):
		// Matches: tenant.{org}.space.{space}.device.{device_id}.location
		return models.KindLocationUpdate
	case strings.HasSuffix(routingKey, ".activity_log"):
		// Matches: tenant.{org}.device.{device_eui}.activity_log
		return models.KindActivityLog
	default:
		// Unknown routing key pattern - log warning but default to location update for backward compatibility
		log.Printf("WARNING: Unknown routing key pattern: %s, defaulting to location_update", routingKey)
		return models.KindLocationUpdate
	}
}

// HandlerRegistry manages message handlers routing.
type HandlerRegistry struct {
	handlers map[models.MessageKind]messageHandler
}

// NewHandlerRegistry creates a new registry with all registered handlers.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: map[models.MessageKind]messageHandler{
			models.KindEvent:           singletonEventHandler,
			models.KindAlert:           singletonAlertHandler,
			models.KindEntityTelemetry: singletonEntityTelemetryHandler,
			models.KindLocationUpdate:  singletonLocationUpdateHandler,
			models.KindActivityLog:     singletonActivityLogHandler,
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

// alertHandler handles alert messages with routing key ending in ".alert".
type alertHandler struct{}

func (h *alertHandler) Handle(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error) {
	var alert models.Alert
	if err := json.Unmarshal(msg.Body, &alert); err == nil && (alert.DeviceID != "" || alert.Message != "" || alert.Title != "" || alert.Level != nil) {
		alert.Organization = extractOrgFromRoutingKey(msg.RoutingKey)
		messageWithDelivery := &models.AMQPMessageWithDelivery{
			Kind:     models.KindAlert,
			Alert:    &alert,
			Delivery: &msg,
		}
		return messageWithDelivery, nil
	}

	var event models.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal alert/event payload: %v", ErrUnhandled, err)
	}

	if event.DeviceID == "" {
		log.Printf("WARNING: Alert message rejected: DeviceID is empty. Routing key: %s", msg.RoutingKey)
		return nil, ErrUnhandled
	}

	mappedAlert := mapEventToAlert(&event)
	mappedAlert.Organization = extractOrgFromRoutingKey(msg.RoutingKey)
	messageWithDelivery := &models.AMQPMessageWithDelivery{
		Kind:     models.KindAlert,
		Alert:    mappedAlert,
		Delivery: &msg,
	}

	return messageWithDelivery, nil
}

func mapEventToAlert(event *models.Event) *models.Alert {
	if event == nil {
		return &models.Alert{}
	}

	var level *string
	if event.EventLevel != nil {
		level = event.EventLevel
	} else if event.LNSAlert != nil {
		levelValue := event.LNSAlert.Level
		level = &levelValue
	}

	message := ""
	title := event.Title
	if event.LNSAlert != nil && event.LNSAlert.Message != "" {
		message = event.LNSAlert.Message
	}
	if title == "" {
		title = message
	}
	if message == "" {
		message = title
	}
	if title == "" && event.EventType != "" {
		title = event.EventType
	}
	if message == "" && title != "" {
		message = title
	}

	var reportedAt time.Time
	if event.TimeFiredTs > 0 {
		reportedAt = time.UnixMilli(event.TimeFiredTs).UTC()
	}

	return &models.Alert{
		Type:         "alert",
		Title:        title,
		Level:        level,
		Organization: event.Organization,
		SpaceSlug:    event.SpaceSlug,
		DeviceID:     event.DeviceID,
		EntityID:     event.EntityID,
		Message:      message,
		ReportedAt:   reportedAt,
	}
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

type activityLogHandler struct{}

func (h *activityLogHandler) Handle(ctx context.Context, msg amqp.Delivery) (*models.AMQPMessageWithDelivery, error) {
	var activityLog models.ActivityLog
	if err := json.Unmarshal(msg.Body, &activityLog); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal activity log: %v", ErrUnhandled, err)
	}

	if activityLog.DeviceEUI == "" {
		log.Printf("WARNING: Activity log rejected: DeviceEUI is empty. Routing key: %s", msg.RoutingKey)
		return nil, ErrUnhandled
	}

	activityLog.Organization = extractOrgFromRoutingKey(msg.RoutingKey)

	if activityLog.Timestamp.IsZero() {
		activityLog.Timestamp = time.Now()
	}

	if activityLog.ID == "" {
		activityLog.ID = uuid.New().String()
	}

	messageWithDelivery := &models.AMQPMessageWithDelivery{
		Kind:        models.KindActivityLog,
		ActivityLog: &activityLog,
		Delivery:    &msg,
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
func logDroppedMessage(msgType, identifier, routingKey string) {
	log.Printf("WARNING: Dropping %s message for %s due to full channel. Routing key: %s", msgType, identifier, routingKey)
}
