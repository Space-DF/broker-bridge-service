package bridge

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/models"
)

// entityTelemetryProcessor handles entity telemetry messages.
type entityTelemetryProcessor struct {
	bridge   *Bridge
	update   *models.EntityTelemetryPayload
	delivery interface{}
}

func (p *entityTelemetryProcessor) Publish(ctx context.Context) (string, error) {
	return p.bridge.mqttClient.PublishEntityTelemetry(p.update)
}

func (p *entityTelemetryProcessor) LogSuccess(ctx context.Context, topic string) {
	log.Printf("Successfully published telemetry for entity %s to MQTT topic %s\n", p.update.Entity.UniqueID, topic)
}

func (p *entityTelemetryProcessor) LogFailure(ctx context.Context, err error) {
	entityId := p.update.Entity.UniqueID
	if entityId == "" {
		entityId = "unknown"
	}
	log.Printf("Failed to publish entity %s telemetry: %v\n", entityId, err)
}

func (p *entityTelemetryProcessor) PostProcess(ctx context.Context) error {
	return nil // No post-processing needed
}

func (p *entityTelemetryProcessor) GetIdentifier() string {
	return p.update.Entity.UniqueID
}
