package bridge

import (
	"context"

	"github.com/Space-DF/broker-bridge-service/internal/models"
)

// messageProcessor defines the contract for processing different message types.
type messageProcessor interface {
	// Publish sends the message to MQTT and returns the topic or error.
	Publish(ctx context.Context) (string, error)
	// LogSuccess logs successful processing.
	LogSuccess(ctx context.Context, topic string)
	// LogFailure logs failed processing.
	LogFailure(ctx context.Context, err error)
	// PostProcess runs after successful publish (e.g., Celery tasks).
	PostProcess(ctx context.Context) error
	// GetIdentifier returns a unique identifier for logging.
	GetIdentifier() string
}

// ProcessorRegistry maps message kinds to their processors.
type ProcessorRegistry struct {
	bridge *Bridge
}

func (r *ProcessorRegistry) GetProcessor(msg *models.AMQPMessageWithDelivery) messageProcessor {
	switch msg.Kind {
	case models.KindLocationUpdate:
		return &locationUpdateProcessor{
			bridge: r.bridge,
			update: msg.LocationUpdate,
		}
	case models.KindEntityTelemetry:
		return &entityTelemetryProcessor{
			bridge: r.bridge,
			update: msg.EntityUpdate,
		}
	case models.KindAlert:
		return &alertProcessor{
			bridge: r.bridge,
			alert:  msg.Alert,
		}
	case models.KindEvent:
		return &eventProcessor{
			bridge: r.bridge,
			event:  msg.Event,
		}
	case models.KindActivityLog:
		return &activityLogProcessor{
			bridge: r.bridge,
			log:    msg.ActivityLog,
		}
	default:
		return &unknownProcessor{}
	}
}
