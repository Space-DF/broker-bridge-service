package bridge

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/models"
)

type alertProcessor struct {
	bridge *Bridge
	alert  *models.Alert
}

func (p *alertProcessor) Publish(ctx context.Context) (string, error) {
	if p.alert == nil {
		log.Printf("[ALERT] alert payload is nil")
		return "", nil
	}

	return p.bridge.mqttClient.PublishAlert(p.alert)
}

func (p *alertProcessor) LogSuccess(ctx context.Context, topic string) {
	log.Printf("Successfully published alert for device %s to MQTT topic %s\n", p.alert.DeviceID, topic)
}

func (p *alertProcessor) LogFailure(ctx context.Context, err error) {
	deviceId := p.alert.DeviceID
	if deviceId == "" {
		deviceId = "unknown"
	}
	log.Printf("Failed to publish alert for device %s: %v\n", deviceId, err)
}

func (p *alertProcessor) PostProcess(ctx context.Context) error {
	return nil // No post-processing needed
}

func (p *alertProcessor) GetIdentifier() string {
	return p.alert.DeviceID
}
