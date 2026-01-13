/*
Copyright 2026 Digital Fortress.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Space-DF/broker-bridge-service/internal/bridge"
	"github.com/Space-DF/broker-bridge-service/internal/config"
	"github.com/Space-DF/broker-bridge-service/internal/telemetry"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var (
	configFile string
	rootCmd    = &cobra.Command{
		Use:   "bridge",
		Short: "Broker Bridge Service for SpaceDF",
		Long:  "A message bridge service that routes messages between AMQP and EMQX MQTT brokers",
	}

	serveCmd = &cobra.Command{
		Use:   "serve",
		Short: "Start the bridge server",
		Run:   runServer,
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file (default is configs/config.yaml)")
	rootCmd.AddCommand(serveCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func runServer(cmd *cobra.Command, args []string) {
	// Load configuration
	cfg, err := config.New()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize OpenTelemetry tracing
	cleanup := telemetry.InitTracing("broker-bridge-service", cfg.OpenTelemetry)
	defer cleanup()

	// Create bridge instance
	br := bridge.NewBridge(cfg)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start bridge in goroutine
	bridgeErr := make(chan error, 1)
	go func() {
		if err := br.Start(ctx); err != nil {
			bridgeErr <- err
		}
	}()

	// Setup HTTP server for health check endpoint
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", healthCheckHandler)

	// Wrap the handler with OpenTelemetry middleware
	handler := otelhttp.NewHandler(mux, "broker-bridge-service")

	// Create HTTP server
	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      handler,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// Start HTTP server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		log.Printf("HTTP server starting on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	// Wait for shutdown signal or error
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v", sig)
	case err := <-bridgeErr:
		log.Printf("Bridge error: %v", err)
	case err := <-serverErr:
		log.Printf("Server error: %v", err)
	}

	// Graceful shutdown
	log.Println("Shutting down...")

	// Cancel context to stop bridge
	cancel()

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	// Stop bridge
	if err := br.Stop(); err != nil {
		log.Printf("Bridge stop error: %v", err)
	}

	log.Println("Shutdown complete")
}

// healthCheckHandler provides a simple health check endpoint
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := `{
		"status": "healthy",
		"service": "broker-bridge-service",
		"timestamp": "` + time.Now().UTC().Format(time.RFC3339) + `"
	}`

	_, _ = w.Write([]byte(response))
}
