package telemetry

import (
	"context"
	"log"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var logger otellog.Logger

// InitTracing initializes OpenTelemetry tracing for the service
func InitTracing(serviceName string, cfg config.OpenTelemetryConfig) func() {
	ctx := context.Background()

	otlpEndpoint := cfg.Endpoint
	if otlpEndpoint == "" {
		otlpEndpoint = "signoz-otel-collector:4317"
	}

	environment := cfg.Environment
	if environment == "" {
		environment = "development"
	}

	// Validate and clamp sampling ratio
	samplingRatio := cfg.SamplingRatio
	if samplingRatio < 0.0 {
		samplingRatio = 0.0
	} else if samplingRatio > 1.0 {
		samplingRatio = 1.0
	}

	// Create resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("1.0.0"),
			semconv.DeploymentEnvironmentKey.String(environment),
		),
	)
	if err != nil {
		log.Printf("Failed to create resource: %v", err)
		return func() {}
	}

	// Initialize Tracing
	expCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	traceExporter, err := otlptracegrpc.New(expCtx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		log.Printf("Failed to create OTLP trace exporter: %v", err)
		return func() {}
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(res),
		trace.WithSampler(trace.TraceIDRatioBased(samplingRatio)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Initialize Logging
	logExpCtx, logCancel := context.WithTimeout(ctx, 5*time.Second)
	defer logCancel()
	logExporter, err := otlploggrpc.New(logExpCtx,
		otlploggrpc.WithEndpoint(otlpEndpoint),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		log.Printf("Failed to create OTLP log exporter: %v", err)
	} else {
		lp := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
			sdklog.WithResource(res),
		)
		global.SetLoggerProvider(lp)
		logger = lp.Logger(serviceName)
		log.Printf("OpenTelemetry logging initialized for service: %s", serviceName)
	}

	log.Printf("OpenTelemetry tracing initialized for service: %s", serviceName)

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
		if lp := global.GetLoggerProvider(); lp != nil {
			if logProvider, ok := lp.(*sdklog.LoggerProvider); ok {
				if err := logProvider.Shutdown(shutdownCtx); err != nil {
					log.Printf("Error shutting down logger provider: %v", err)
				}
			}
		}
	}
}

// GetTracer returns a tracer for the given name
func GetTracer(name string) oteltrace.Tracer {
	return otel.Tracer(name)
}

// LogInfo sends an info log to OpenTelemetry with structured attributes
func LogInfo(ctx context.Context, message string, attrs ...otellog.KeyValue) {
	if logger == nil {
		return
	}
	record := otellog.Record{}
	record.SetTimestamp(time.Now())
	record.SetBody(otellog.StringValue(message))
	record.SetSeverity(otellog.SeverityInfo)
	record.AddAttributes(attrs...)
	logger.Emit(ctx, record)
}

// LogError sends an error log to OpenTelemetry with structured attributes
func LogError(ctx context.Context, message string, attrs ...otellog.KeyValue) {
	if logger == nil {
		return
	}
	record := otellog.Record{}
	record.SetTimestamp(time.Now())
	record.SetBody(otellog.StringValue(message))
	record.SetSeverity(otellog.SeverityError)
	record.AddAttributes(attrs...)
	logger.Emit(ctx, record)
}

// LogWarn sends a warning log to OpenTelemetry with structured attributes
func LogWarn(ctx context.Context, message string, attrs ...otellog.KeyValue) {
	if logger == nil {
		return
	}
	record := otellog.Record{}
	record.SetTimestamp(time.Now())
	record.SetBody(otellog.StringValue(message))
	record.SetSeverity(otellog.SeverityWarn)
	record.AddAttributes(attrs...)
	logger.Emit(ctx, record)
}
