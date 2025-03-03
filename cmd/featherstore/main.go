package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/featherstore/pkg/api"
	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/flight"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/TFMV/featherstore/pkg/storage"
	"go.uber.org/zap"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Create necessary directories
	if err := cfg.EnsureDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directories: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	log, err := logger.Initialize(&cfg.Logging)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()

	// Initialize metrics
	m := metrics.NewMetrics(&cfg.Metrics, log)
	if err := m.Start(&cfg.Metrics); err != nil {
		log.Error("Failed to start metrics server", zap.Error(err))
	}

	// Create application context with cancellation
	// We store this in a variable but currently don't use it directly in main
	// It could be used for passing to components if needed in the future
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage backend based on configuration
	log.Info("Initializing storage backend", zap.String("backend", cfg.Storage.Backend))
	store, err := storage.NewStorage(&cfg.Storage, log, m)
	if err != nil {
		log.Error("Failed to initialize storage", zap.Error(err))
		os.Exit(1)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("Error closing storage", zap.Error(err))
		}
	}()

	// Initialize Arrow Flight server
	log.Info("Initializing Arrow Flight server")
	flightServer, err := flight.NewServer(&cfg.Server, log, m, store)
	if err != nil {
		log.Error("Failed to create Arrow Flight server", zap.Error(err))
		os.Exit(1)
	}

	// Start Arrow Flight server
	if err := flightServer.Start(); err != nil {
		log.Error("Failed to start Arrow Flight server", zap.Error(err))
		os.Exit(1)
	}

	// Initialize HTTP API server
	log.Info("Initializing HTTP API server")
	apiServer, err := api.NewServer(&cfg.Server, log, m, store)
	if err != nil {
		log.Error("Failed to create API server", zap.Error(err))
		os.Exit(1)
	}

	// Start HTTP API server
	if err := apiServer.Start(); err != nil {
		log.Error("Failed to start API server", zap.Error(err))
		os.Exit(1)
	}

	log.Info("FeatherStore started successfully")

	// Set up graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	<-shutdown
	log.Info("Received shutdown signal, gracefully shutting down...")

	// Create a context with timeout for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Stop all services gracefully
	if err := apiServer.Stop(shutdownCtx); err != nil {
		log.Error("Error stopping API server", zap.Error(err))
	}

	if err := flightServer.Stop(); err != nil {
		log.Error("Error stopping Arrow Flight server", zap.Error(err))
	}

	if err := m.Stop(shutdownCtx); err != nil {
		log.Error("Error stopping metrics server", zap.Error(err))
	}

	log.Info("FeatherStore shutdown complete")
}
