package bridge

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/models"
)

// eventProcessor handles event messages.
type eventProcessor struct {
	bridge   *Bridge
	event    *models.Event
	delivery interface{}
}

func (p *eventProcessor) Publish(ctx context.Context) (string, error) {
	return p.bridge.mqttClient.PublishEvent(p.event)
}

func (p *eventProcessor) LogSuccess(ctx context.Context, topic string) {
	log.Printf("Successfully published event for device %s to MQTT topic %s\n", p.event.DeviceID, topic)
}

func (p *eventProcessor) LogFailure(ctx context.Context, err error) {
	deviceId := p.event.DeviceID
	if deviceId == "" {
		deviceId = "unknown"
	}
	log.Printf("Failed to publish event for device %s: %v\n", deviceId, err)
}

func (p *eventProcessor) PostProcess(ctx context.Context) error {
	return nil // No post-processing needed
}

func (p *eventProcessor) GetIdentifier() string {
	return p.event.DeviceID
}
