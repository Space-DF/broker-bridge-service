package telemetry

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

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
func InitTracing(serviceName string) func() {
	ctx := context.Background()

	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "signoz-otel-collector:4317"
	}

	// Create resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String("1.0.0"),
			semconv.DeploymentEnvironmentKey.String(getEnv("OTEL_ENVIRONMENT", "development")),
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
		trace.WithSampler(trace.TraceIDRatioBased(getSamplingRatio())),
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

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getSamplingRatio gets the sampling ratio from environment or returns 1.0 (100%)
// Supports any value between 0.0 and 1.0
func getSamplingRatio() float64 {
	ratio := getEnv("OTEL_TRACES_SAMPLER_ARG", "1.0")
	
	// Parse as float
	value, err := strconv.ParseFloat(ratio, 64)
	if err != nil {
		log.Printf("Invalid OTEL_TRACES_SAMPLER_ARG '%s', using default 1.0: %v", ratio, err)
		return 1.0
	}
	
	// Validate range [0.0, 1.0]
	if value < 0.0 || value > 1.0 {
		log.Printf("OTEL_TRACES_SAMPLER_ARG '%s' out of range [0.0, 1.0], using 1.0", ratio)
		return 1.0
	}
	
	return value
}
