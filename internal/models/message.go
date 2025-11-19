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
	SpaceSlug      string               `json:"space_slug,omitempty"`
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
	Location     Location               `json:"location"`
	Timestamp    time.Time              `json:"timestamp"`
	Organization string                 `json:"organization,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	Source       string                 `json:"source,omitempty"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

// AMQPMessageWithDelivery combines location update with AMQP delivery info for reliable processing
type AMQPMessageWithDelivery struct {
	LocationUpdate *DeviceLocationUpdate
	Delivery       *amqp.Delivery
}

// DeviceStatusUpdate represents device status changes
type DeviceStatusUpdate struct {
	DevEUI      string    `json:"dev_eui"`
	DeviceName  string    `json:"device_name,omitempty"`
	Status      string    `json:"status"`
	LastSeen    time.Time `json:"last_seen"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ErrorMessage represents error responses
type ErrorMessage struct {
	Error   string `json:"error"`
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}