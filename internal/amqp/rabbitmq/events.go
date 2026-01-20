package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
	amqp "github.com/rabbitmq/amqp091-go"
)

type EventManager struct {
	channel       *amqp.Channel
	cfg           config.OrgEventsConfig
	subscribeFn   func(context.Context, string, string, string) error
	unsubscribeFn func(string)
}

func NewEventManager(ch *amqp.Channel, cfg config.OrgEventsConfig,
	subscribeToOrganization func(context.Context, string, string, string) error,
	unsubscribeFromOrganization func(string)) *EventManager {

	return &EventManager{channel: ch, cfg: cfg, subscribeFn: subscribeToOrganization, unsubscribeFn: unsubscribeFromOrganization}
}

type OrgEventProcessor func(context.Context, amqp.Delivery) error

// sendDiscoveryRequest sends a discovery request to get all active organizations
func (m *EventManager) SendDiscoveryRequest(ctx context.Context) error {
	log.Println("Sending discovery request for active organizations...")

	request := models.OrgDiscoveryRequest{
		EventType:   models.OrgDiscoveryReq,
		EventID:     fmt.Sprintf("discovery-%d", time.Now().Unix()),
		Timestamp:   time.Now(),
		ServiceName: "broker-bridge-service",
		ReplyTo:     m.cfg.Queue,
	}

	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal discovery request: %w", err)
	}

	err = m.channel.PublishWithContext(
		ctx,
		m.cfg.Exchange,
		string(models.OrgDiscoveryReq),
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

	log.Printf("Discovery request sent to exchange: %s", m.cfg.Exchange)
	return nil
}

func (m *EventManager) ensureOrgEventsTopology() error {
	if err := m.channel.ExchangeDeclare(
		m.cfg.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare failed: %w", err)
	}

	if _, err := m.channel.QueueDeclare(
		m.cfg.Queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("queue declare failed: %w", err)
	}

	if err := m.channel.QueueBind(
		m.cfg.Queue,
		m.cfg.RoutingKey,
		m.cfg.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("queue bind failed: %w", err)
	}

	return nil
}

// listenToOrgEvents listens for organization lifecycle events
func (m *EventManager) ListenToOrgEvents(ctx context.Context, process OrgEventProcessor) error {
	log.Println("Setting up organization events listener...")

	var (
		messages <-chan amqp.Delivery
		err      error
		attempt  = 1
	)

	for {
		if err = m.ensureOrgEventsTopology(); err == nil {
			messages, err = m.channel.Consume(
				m.cfg.Queue,
				m.cfg.ConsumerTag,
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

			if err := process(ctx, msg); err != nil {
				log.Printf("Error handling org event: %v", err)
				_ = msg.Nack(false, true) // Requeue
			} else {
				_ = msg.Ack(false)
			}
		}
	}
}
