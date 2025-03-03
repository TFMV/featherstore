package api

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/core"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.uber.org/zap"
)

// Server represents the HTTP API server
type Server struct {
	config  *config.ServerConfig
	log     *logger.Logger
	metrics *metrics.Metrics
	store   core.FeatureStore
	app     *fiber.App
}

// NewServer creates a new HTTP API server
func NewServer(
	cfg *config.ServerConfig,
	log *logger.Logger,
	metrics *metrics.Metrics,
	store core.FeatureStore,
) (*Server, error) {
	server := &Server{
		config:  cfg,
		log:     log,
		metrics: metrics,
		store:   store,
	}

	// Initialize Fiber app with configuration
	server.app = fiber.New(fiber.Config{
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  2 * time.Minute,
		ErrorHandler: server.customErrorHandler,
	})

	// Add middlewares
	server.app.Use(recover.New())
	server.app.Use(compress.New())
	server.app.Use(server.loggingMiddleware)

	// Register API endpoints
	server.app.Get("/health", server.handleHealth)

	// Group API endpoints under /api/v1
	api := server.app.Group("/api/v1")
	api.Get("/feature_sets", server.handleListFeatureSets)
	api.Get("/feature_sets/:name", server.handleGetFeatureSet)
	api.Post("/feature_sets", server.handleCreateFeatureSet)
	api.Delete("/feature_sets/:name", server.handleDeleteFeatureSet)
	api.Get("/features/:feature_set", server.handleGetFeatures)
	api.Get("/features/:feature_set/history", server.handleGetFeatureHistory)

	return server, nil
}

// Start starts the HTTP API server
func (s *Server) Start() error {
	// Start server in a goroutine
	addr := fmt.Sprintf(":%d", s.config.HTTPPort)

	go func() {
		s.log.Info("Starting HTTP API server", zap.String("address", addr))
		if err := s.app.Listen(addr); err != nil {
			s.log.Error("HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP API server
func (s *Server) Stop(ctx context.Context) error {
	s.log.Info("Stopping HTTP API server")
	return s.app.Shutdown()
}

// loggingMiddleware adds request logging
func (s *Server) loggingMiddleware(c *fiber.Ctx) error {
	start := time.Now()

	// Process request
	err := c.Next()

	// Log after request is processed
	duration := time.Since(start)
	status := c.Response().StatusCode()

	s.metrics.ObserveHTTPRequest(c.Method(), c.Path(), status, duration)

	return err
}

// customErrorHandler provides structured error responses
func (s *Server) customErrorHandler(c *fiber.Ctx, err error) error {
	// Default status code and message
	code := fiber.StatusInternalServerError
	message := "Internal Server Error"

	// Check for specific Fiber error
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
		message = e.Message
	}

	s.log.Error("Request failed",
		zap.String("method", c.Method()),
		zap.String("path", c.Path()),
		zap.Int("status", code),
		zap.Error(err),
	)

	return c.Status(code).JSON(fiber.Map{
		"error":   true,
		"message": message,
	})
}

// handleHealth handles health check requests
func (s *Server) handleHealth(c *fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// handleListFeatureSets handles requests to list all feature sets
func (s *Server) handleListFeatureSets(c *fiber.Ctx) error {
	ctx := c.Context()

	featureSets, err := s.store.ListFeatureSets(ctx)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to list feature sets")
	}

	return c.JSON(featureSets)
}

// handleGetFeatureSet handles requests to get a specific feature set
func (s *Server) handleGetFeatureSet(c *fiber.Ctx) error {
	ctx := c.Context()
	name := c.Params("name")

	featureSet, err := s.store.GetFeatureSet(ctx, name)
	if err != nil {
		if err == core.ErrFeatureSetNotFound {
			return fiber.NewError(fiber.StatusNotFound, "Feature set not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to get feature set")
	}

	return c.JSON(featureSet)
}

// handleCreateFeatureSet handles requests to create a new feature set
func (s *Server) handleCreateFeatureSet(c *fiber.Ctx) error {
	ctx := c.Context()

	var featureSet core.FeatureSet
	if err := c.BodyParser(&featureSet); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "Invalid feature set data")
	}

	// Set creation timestamp
	featureSet.CreatedAt = time.Now()
	featureSet.UpdatedAt = featureSet.CreatedAt

	if err := s.store.CreateFeatureSet(ctx, &featureSet); err != nil {
		if err == core.ErrFeatureSetAlreadyExists {
			return fiber.NewError(fiber.StatusConflict, "Feature set already exists")
		} else if err == core.ErrInvalidSchema {
			return fiber.NewError(fiber.StatusBadRequest, "Invalid schema")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to create feature set")
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"status":  "success",
		"message": fmt.Sprintf("Feature set '%s' created successfully", featureSet.Name),
	})
}

// handleDeleteFeatureSet handles requests to delete a feature set
func (s *Server) handleDeleteFeatureSet(c *fiber.Ctx) error {
	ctx := c.Context()
	name := c.Params("name")

	if err := s.store.DeleteFeatureSet(ctx, name); err != nil {
		if err == core.ErrFeatureSetNotFound {
			return fiber.NewError(fiber.StatusNotFound, "Feature set not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to delete feature set")
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": fmt.Sprintf("Feature set '%s' deleted successfully", name),
	})
}

// handleGetFeatures handles requests to get features for specific entities
func (s *Server) handleGetFeatures(c *fiber.Ctx) error {
	ctx := c.Context()
	featureSetName := c.Params("feature_set")

	// Get entity IDs from query parameters
	entityIDs := c.Queries()["entity_id"]
	if len(entityIDs) == 0 {
		return fiber.NewError(fiber.StatusBadRequest, "Missing entity_id parameter")
	}

	// Retrieve features
	batch, err := s.store.GetFeatures(ctx, featureSetName, []string{entityIDs})
	if err != nil {
		if err == core.ErrFeatureSetNotFound {
			return fiber.NewError(fiber.StatusNotFound, "Feature set not found")
		} else if err == core.ErrEntityNotFound {
			return fiber.NewError(fiber.StatusNotFound, "Entity not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to get features")
	}

	// Convert Arrow batch to feature vectors for JSON response
	vectors, err := core.RecordBatchToFeatureVectors(batch)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to convert features")
	}

	return c.JSON(vectors)
}

// handleGetFeatureHistory handles requests to get feature history for a specific entity
func (s *Server) handleGetFeatureHistory(c *fiber.Ctx) error {
	ctx := c.Context()
	featureSetName := c.Params("feature_set")

	// Get entity ID and time range from query parameters
	entityID := c.Query("entity_id")
	if entityID == "" {
		return fiber.NewError(fiber.StatusBadRequest, "Missing entity_id parameter")
	}

	startTimeStr := c.Query("start_time")
	if startTimeStr == "" {
		return fiber.NewError(fiber.StatusBadRequest, "Missing start_time parameter")
	}

	endTimeStr := c.Query("end_time")
	if endTimeStr == "" {
		return fiber.NewError(fiber.StatusBadRequest, "Missing end_time parameter")
	}

	// Parse time range
	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "Invalid start_time format")
	}

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "Invalid end_time format")
	}

	// Retrieve feature history
	batch, err := s.store.GetFeatureHistory(ctx, featureSetName, entityID, startTime, endTime)
	if err != nil {
		if err == core.ErrFeatureSetNotFound {
			return fiber.NewError(fiber.StatusNotFound, "Feature set not found")
		} else if err == core.ErrEntityNotFound {
			return fiber.NewError(fiber.StatusNotFound, "Entity not found")
		}
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to get feature history")
	}

	// Convert Arrow batch to feature vectors for JSON response
	vectors, err := core.RecordBatchToFeatureVectors(batch)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "Failed to convert features")
	}

	return c.JSON(vectors)
}

// GetApp returns the underlying Fiber app instance for testing
func (s *Server) GetApp() *fiber.App {
	return s.app
}
