package bridge

import (
	"context"
	"log"
)

// unknownProcessor handles unknown message types.
type unknownProcessor struct {
	delivery interface{}
}

func (p *unknownProcessor) Publish(ctx context.Context) (string, error) {
	return "", nil // Just return empty topic
}

func (p *unknownProcessor) LogSuccess(ctx context.Context, topic string) {
	// No success logging for unknown messages
}

func (p *unknownProcessor) LogFailure(ctx context.Context, err error) {
	log.Printf("Failed to process unknown message: %v\n", err)
}

func (p *unknownProcessor) PostProcess(ctx context.Context) error {
	return nil
}

func (p *unknownProcessor) GetIdentifier() string {
	return "unknown"
}
