package bridge

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/models"
)

// locationUpdateProcessor handles device location updates.
type locationUpdateProcessor struct {
	bridge   *Bridge
	update   *models.DeviceLocationUpdate
	delivery interface{}
}

func (p *locationUpdateProcessor) Publish(ctx context.Context) (string, error) {
	return p.bridge.mqttClient.PublishDeviceTelemetry(p.update)
}

func (p *locationUpdateProcessor) LogSuccess(ctx context.Context, topic string) {
	log.Printf("Successfully published device %s telemetry to MQTT topic %s\n", p.update.DeviceEUI, topic)
}

func (p *locationUpdateProcessor) LogFailure(ctx context.Context, err error) {
	log.Printf("Failed to publish device %s telemetry to MQTT: %v\n", p.update.DeviceEUI, err)
}

func (p *locationUpdateProcessor) PostProcess(ctx context.Context) error {
	// Dispatch Celery task to device-service to update device location in DB
	if p.bridge.celeryPublisher == nil || p.update.DeviceID == "" || p.update.DeviceID == "unknown" {
		return nil
	}

	org := p.update.Organization
	if org == "" {
		org = "unknown"
	}

	return p.bridge.celeryPublisher.PublishLocationUpdate(
		org,
		p.update.DeviceID,
		p.update.Location.Latitude,
		p.update.Location.Longitude,
	)
}

func (p *locationUpdateProcessor) GetIdentifier() string {
	if p.update.DeviceID != "" {
		return p.update.DeviceID
	}
	return p.update.DeviceEUI
}
