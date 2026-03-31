package mqtt

import (
	"fmt"
	"strings"

	"github.com/Space-DF/broker-bridge-service/internal/models"
)

func buildEntityTopic(update *models.EntityTelemetryPayload) string {
	if update == nil {
		return "tenant/unknown/entity/unknown/telemetry"
	}

	org := strings.TrimSpace(update.Organization)
	if org == "" {
		org = "unknown"
	}

	entity := strings.TrimSpace(update.Entity.UniqueID)
	if entity == "" {
		entity = strings.TrimSpace(update.Entity.EntityID)
	}
	if entity == "" {
		entity = "unknown"
	}

	space := strings.TrimSpace(update.SpaceSlug)

	if space != "" {
		return fmt.Sprintf("tenant/%s/space/%s/entity/%s/telemetry", org, space, entity)
	}

	return fmt.Sprintf("tenant/%s/entity/%s/telemetry", org, entity)
}

func buildTelemetryTopic(update *models.DeviceLocationUpdate) string {
	if update == nil {
		return "tenant/unknown/device/unknown/telemetry"
	}

	org := strings.TrimSpace(update.Organization)
	if org == "" {
		org = "unknown"
	}

	device := strings.TrimSpace(update.DeviceID)
	if device == "" {
		device = "unknown"
	}

	space := strings.TrimSpace(update.SpaceSlug)
	if update.IsPublished {
		return fmt.Sprintf("tenant/%s/device/%s/telemetry", org, device)
	}

	return fmt.Sprintf("tenant/%s/space/%s/device/%s/telemetry", org, space, device)
}

func buildEventTopic(event *models.Event) string {
	if event == nil {
		return "tenant/unknown/device/unknown/events"
	}

	org := strings.TrimSpace(event.Organization)
	if org == "" {
		org = "unknown"
	}

	device := strings.TrimSpace(event.DeviceID)
	if device == "" {
		device = "unknown"
	}

	space := strings.TrimSpace(event.SpaceSlug)

	if space != "" {
		return fmt.Sprintf("tenant/%s/space/%s/device/%s/events", org, space, device)
	}

	return fmt.Sprintf("tenant/%s/device/%s/events", org, device)
}
