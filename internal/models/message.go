package models

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DeviceMessage represents a message received from EMQX
type DeviceMessage struct {
	DevEUI       string                 `json:"dev_eui"`
	DeviceName   string                 `json:"device_name,omitempty"`
	Organization string                 `json:"organization,omitempty"`
	SpaceSlug    string                 `json:"space_slug,omitempty"`
	IsPublished  bool                   `json:"is_published"`
	Latitude     float64                `json:"latitude"`
	Longitude    float64                `json:"longitude"`
	Accuracy     float64                `json:"accuracy,omitempty"`
	Timestamp    time.Time              `json:"timestamp"`
	Topic        string                 `json:"topic,omitempty"`
	ReceivedAt   time.Time              `json:"received_at,omitempty"`
	RawData      map[string]interface{} `json:"raw_data,omitempty"`
}

// WebSocketMessage represents a message sent to WebSocket clients
type WebSocketMessage struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

// WebSocketClient represents a connected WebSocket client
type WebSocketClient struct {
	ID           string                 `json:"id"`
	Organization string                 `json:"organization,omitempty"`
	UserID       string                 `json:"user_id,omitempty"`
	Filters      map[string]interface{} `json:"filters,omitempty"`
	ConnectedAt  time.Time              `json:"connected_at"`
	LastPing     time.Time              `json:"last_ping"`
}

// ConnectionInfo represents client connection information
type ConnectionInfo struct {
	UserID       string `json:"user_id"`
	Organization string `json:"organization"`
	ClientType   string `json:"client_type,omitempty"`
	ConnectedAt  int64  `json:"connected_at"`
}

// Location represents location data
type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Accuracy  float64 `json:"accuracy,omitempty"`
}

// DeviceLocationUpdate represents real-time location updates
type DeviceLocationUpdate struct {
	DeviceEUI    string                 `json:"device_eui"`
	DeviceID     string                 `json:"device_id"`
	SpaceSlug    string                 `json:"space_slug,omitempty"`
	IsPublished  bool                   `json:"is_published"`
	Location     Location               `json:"location"`
	Timestamp    time.Time              `json:"timestamp"`
	Organization string                 `json:"organization,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	Source       string                 `json:"source,omitempty"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

// AMQPMessageWithDelivery combines location update with AMQP delivery info for reliable processing
type AMQPMessageWithDelivery struct {
	Kind           MessageKind
	LocationUpdate *DeviceLocationUpdate
	EntityUpdate   *EntityTelemetryPayload
	Event          *Event
	Delivery       *amqp.Delivery
}

// DeviceStatusUpdate represents device status changes
type DeviceStatusUpdate struct {
	DevEUI     string    `json:"dev_eui"`
	DeviceName string    `json:"device_name,omitempty"`
	Status     string    `json:"status"`
	LastSeen   time.Time `json:"last_seen"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ErrorMessage represents error responses
type ErrorMessage struct {
	Error   string `json:"error"`
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// MessageKind identifies what type of AMQP payload was received.
type MessageKind string

const (
	KindLocationUpdate  MessageKind = "location_update"
	KindEntityTelemetry MessageKind = "entity_telemetry"
	KindEvent           MessageKind = "event"
)

// EntityTelemetryPayload mirrors transformer per-entity telemetry.
type EntityTelemetryPayload struct {
	Organization string          `json:"organization"`
	DeviceEUI    string          `json:"device_eui"`
	DeviceID     string          `json:"device_id,omitempty"`
	SpaceSlug    string          `json:"space_slug,omitempty"`
	Entity       TelemetryEntity `json:"entity"`
	Timestamp    string          `json:"timestamp"`
	Source       string          `json:"source"`
	Metadata     map[string]any  `json:"metadata,omitempty"`
}

// TelemetryEntity represents a single entity state.
type TelemetryEntity struct {
	UniqueID    string         `json:"unique_id"`
	EntityID    string         `json:"entity_id"`
	EntityType  string         `json:"entity_type"`
	DeviceClass string         `json:"device_class,omitempty"`
	Name        string         `json:"name"`
	State       any            `json:"state"`
	Attributes  map[string]any `json:"attributes,omitempty"`
	DisplayType []string       `json:"display_type,omitempty"`
	UnitOfMeas  string         `json:"unit_of_measurement,omitempty"`
	Icon        string         `json:"icon,omitempty"`
	Timestamp   string         `json:"timestamp"`
}

// Event represents an event occurrence from the telemetry service.
type Event struct {
	EventID         int64     `json:"event_id"`
	EventTypeID     int       `json:"event_type_id"`
	EventLevel      *string   `json:"event_level,omitempty"`       // manufacturer, system, automation
	EventRuleID     *string   `json:"event_rule_id,omitempty"`     // Rule that triggered this event
	AutomationID    *string   `json:"automation_id,omitempty"`     // Automation that triggered this event
	AutomationName  *string   `json:"automation_name,omitempty"`   // Name of the automation
	GeofenceID      *string   `json:"geofence_id,omitempty"`       // Geofence that triggered this event
	GeofenceName    *string   `json:"geofence_name,omitempty"`     // Name of the geofence
	Organization    string    `json:"organization,omitempty"`      // Extracted from routing key
	SpaceSlug       string    `json:"space_slug,omitempty"`
	DeviceID        string    `json:"device_id,omitempty"`
	EntityID        *string   `json:"entity_id,omitempty"`
	StateID         *int64    `json:"state_id,omitempty"`
	Title           string    `json:"title,omitempty"`
	TimeFiredTs     int64     `json:"time_fired_ts"`
	EventType       string    `json:"event_type,omitempty"`
	Location        *Location `json:"location,omitempty"`
}
