package models

import "time"

// OrgEventType represents the type of organization event
type OrgEventType string

const (
	OrgCreated     OrgEventType = "org.created"
	OrgUpdated     OrgEventType = "org.updated"
	OrgDeactivated OrgEventType = "org.deactivated"
	OrgDeleted     OrgEventType = "org.deleted"
	
	// Bootstrap/Discovery events (Request-Response pattern)
	OrgDiscoveryReq  OrgEventType = "org.discovery.request"  // Bridge → Console
	OrgDiscoveryResp OrgEventType = "org.discovery.response" // Console → Bridge
)

// OrgEvent represents an organization lifecycle event
type OrgEvent struct {
	EventType    OrgEventType `json:"event_type"`
	EventID      string       `json:"event_id"`
	Timestamp    time.Time    `json:"timestamp"`
	Organization Organization `json:"organization"`
}

// Organization represents organization data in events
type Organization struct {
	ID        string    `json:"id"`
	Slug      string    `json:"slug"`
	Name      string    `json:"name"`
	IsActive  bool      `json:"is_active"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// OrgDiscoveryRequest is sent by broker-bridge to request all active orgs
type OrgDiscoveryRequest struct {
	EventType   OrgEventType `json:"event_type"` // "org.discovery.request"
	EventID     string       `json:"event_id"`
	Timestamp   time.Time    `json:"timestamp"`
	ServiceName string       `json:"service_name"` // "broker-bridge-service"
	ReplyTo     string       `json:"reply_to"`     // Queue name for response
}

// OrgDiscoveryResponse contains all active organizations
type OrgDiscoveryResponse struct {
	EventType     OrgEventType   `json:"event_type"` // "org.discovery.response"
	EventID       string         `json:"event_id"`
	Timestamp     time.Time      `json:"timestamp"`
	Organizations []Organization `json:"organizations"`
	TotalCount    int            `json:"total_count"`
}
