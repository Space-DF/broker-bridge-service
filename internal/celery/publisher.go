package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	locationQueue    = "update_device_location"
	locationExchange = "update_device_location"
	locationTask     = "spacedf.tasks.update_device_location"
)

// celeryMessage matches Celery protocol v2
type celeryMessage struct {
	ID      string         `json:"id"`
	Task    string         `json:"task"`
	Kwargs  map[string]any `json:"kwargs"`
	Retries int            `json:"retries"`
	Eta     *string        `json:"eta"`
}

// Publisher publishes Celery-compatible messages to the default RabbitMQ vhost
// so the device-service Celery worker can process location updates.
type Publisher struct {
	brokerURL string
	conn      *amqp.Connection
	channel   *amqp.Channel
	mu        sync.Mutex
}

// NewPublisher creates a connected Celery publisher.
func NewPublisher(brokerURL string) (*Publisher, error) {
	p := &Publisher{brokerURL: brokerURL}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) connect() error {
	var err error
	p.conn, err = amqp.Dial(p.brokerURL)
	if err != nil {
		return fmt.Errorf("celery: dial failed: %w", err)
	}

	p.channel, err = p.conn.Channel()
	if err != nil {
		return fmt.Errorf("celery: channel failed: %w", err)
	}

	// Declare exchange + queue matching device-service celery.py config
	if err := p.channel.ExchangeDeclare(locationExchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("celery: exchange declare failed: %w", err)
	}

	if _, err := p.channel.QueueDeclare(locationQueue, true, false, false, false, amqp.Table{"x-single-active-consumer": true}); err != nil {
		return fmt.Errorf("celery: queue declare failed: %w", err)
	}

	if err := p.channel.QueueBind(locationQueue, locationQueue, locationExchange, false, nil); err != nil {
		return fmt.Errorf("celery: queue bind failed: %w", err)
	}

	log.Printf("Celery publisher: connected, queue %s ready", locationQueue)
	return nil
}

// PublishLocationUpdate sends a Celery task to update a device's location.
func (p *Publisher) PublishLocationUpdate(orgSlug, deviceID string, latitude, longitude float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel == nil {
		if err := p.connect(); err != nil {
			return err
		}
	}

	msg := celeryMessage{
		ID:   uuid.New().String(),
		Task: locationTask,
		Kwargs: map[string]any{
			"organization_slug_name": orgSlug,
			"device_id":              deviceID,
			"latitude":               latitude,
			"longitude":              longitude,
		},
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("celery: marshal failed: %w", err)
	}

	pub := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}

	err = p.channel.PublishWithContext(context.Background(), locationExchange, locationQueue, false, false, pub)
	if err != nil {
		// Reconnect once on failure
		log.Printf("Celery publisher: publish failed, reconnecting: %v", err)
		if reconnErr := p.connect(); reconnErr != nil {
			return fmt.Errorf("celery: reconnect failed: %w", reconnErr)
		}
		err = p.channel.PublishWithContext(context.Background(), locationExchange, locationQueue, false, false, pub)
		if err != nil {
			return fmt.Errorf("celery: publish retry failed: %w", err)
		}
	}

	log.Printf("Celery publisher: dispatched location update for device %s (org: %s)", deviceID, orgSlug)
	return nil
}

// Close cleans up the AMQP connection.
func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
}
