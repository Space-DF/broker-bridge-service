package bridge

import (
	"context"
	"log"

	"github.com/Space-DF/broker-bridge-service/internal/models"
)

// activityLogProcessor handles activity log messages.
type activityLogProcessor struct {
	bridge *Bridge
	log    *models.ActivityLog
}

func (p *activityLogProcessor) Publish(ctx context.Context) (string, error) {
	return p.bridge.mqttClient.PublishActivityLog(p.log)
}
func (p *activityLogProcessor) LogSuccess(ctx context.Context, topic string) {
	deviceEUI := "unknown"
	if p.log != nil {
		deviceEUI = p.log.DeviceEUI
	}
	log.Printf("Successfully published activity log for device %s to MQTT topic %s\n", deviceEUI, topic)
}

func (p *activityLogProcessor) LogFailure(ctx context.Context, err error) {
	deviceEUI := "unknown"
	if p.log != nil && p.log.DeviceEUI != "" {
		deviceEUI = p.log.DeviceEUI
	}
	log.Printf("Failed to publish activity log for device %s: %v\n", deviceEUI, err)
}

func (p *activityLogProcessor) PostProcess(ctx context.Context) error {
	return nil
}

func (p *activityLogProcessor) GetIdentifier() string {
	if p.log == nil {
		return "unknown"
	}
	return p.log.DeviceEUI
}
