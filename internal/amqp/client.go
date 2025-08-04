package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/models"
)

type Client struct {
	config       config.AMQPConfig
	connection   *amqp.Connection
	channel      *amqp.Channel
	messagesChan chan *models.AMQPMessageWithDelivery
	done         chan bool
}

type Consumer struct {
	config       config.AMQPConfig
	conn         *amqp.Connection
	channel      *amqp.Channel
	messagesChan chan *models.DeviceLocationUpdate
	done         chan bool
}

func NewClient(cfg config.AMQPConfig) *Client {
	return &Client{
		config:       cfg,
		messagesChan: make(chan *models.AMQPMessageWithDelivery, 100),
		done:         make(chan bool),
	}
}

func (c *Client) Connect() error {
	var err error
	
	c.connection, err = amqp.Dial(c.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to AMQP: %w", err)
	}

	c.channel, err = c.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Only declare exchange if not using default exchange
	if c.config.Exchange != "" {
		err = c.channel.ExchangeDeclare(
			c.config.Exchange,
			"topic",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to declare exchange: %w", err)
		}
	}

	queue, err := c.channel.QueueDeclare(
		c.config.Queue,
		true,
		false,
		c.config.Exclusive,
		c.config.NoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Only bind queue if using a named exchange (not default exchange)
	if c.config.Exchange != "" {
		err = c.channel.QueueBind(
			queue.Name,
			c.config.RoutingKey,
			c.config.Exchange,
			c.config.NoWait,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to bind queue: %w", err)
		}
		log.Printf("Queue %s bound to exchange %s with routing key %s", queue.Name, c.config.Exchange, c.config.RoutingKey)
	} else {
		log.Printf("Using default exchange, queue %s will receive messages with routing key %s", queue.Name, c.config.RoutingKey)
	}

	log.Printf("Successfully connected to AMQP at %s", c.config.URL)
	
	return nil
}

func (c *Client) Start(ctx context.Context) error {
	msgs, err := c.channel.Consume(
		c.config.Queue,
		c.config.ConsumerTag,
		c.config.AutoAck,
		c.config.Exclusive,
		c.config.NoLocal,
		c.config.NoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf("Started consuming messages from queue: %s", c.config.Queue)

	go func() {
		for msg := range msgs {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, stopping message consumption")
				return
			case <-c.done:
				log.Println("AMQP client stopped")
				return
			default:
				c.handleMessage(msg)
			}
		}
	}()

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

	var locationUpdate models.DeviceLocationUpdate
	if err := json.Unmarshal(msg.Body, &locationUpdate); err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		if !c.config.AutoAck {
			msg.Nack(false, false)
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
			locationUpdate.DevEUI, len(c.messagesChan), cap(c.messagesChan))
		// Do not ACK here - ACK will be handled after successful MQTT publish
	default:
		log.Printf("DEBUG: Message channel full! Current length: %d, capacity: %d", 
			len(c.messagesChan), cap(c.messagesChan))
		log.Printf("DEBUG: Channel is at 100%% capacity - consumer may be too slow or blocked")
		log.Printf("Message channel full, dropping location update for device: %s", locationUpdate.DevEUI)
		if !c.config.AutoAck {
			msg.Nack(false, true)
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

func (c *Client) Stop() error {
	log.Println("Stopping AMQP client")
	
	close(c.done)

	if c.channel != nil {
		if err := c.channel.Cancel(c.config.ConsumerTag, true); err != nil {
			log.Printf("Error cancelling consumer: %v", err)
		}
		c.channel.Close()
	}

	if c.connection != nil {
		c.connection.Close()
	}

	close(c.messagesChan)
	log.Println("AMQP client stopped")
	return nil
}