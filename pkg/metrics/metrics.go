package metrics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Metrics represents the instrumentation for the application
type Metrics struct {
	registry *prometheus.Registry
	log      *logger.Logger

	// Feature store metrics
	featureIngestionCount   *prometheus.CounterVec
	featureRetrievalCount   *prometheus.CounterVec
	ingestedFeatureBytes    *prometheus.CounterVec
	retrievedFeatureBytes   *prometheus.CounterVec
	featureIngestionLatency *prometheus.HistogramVec
	featureRetrievalLatency *prometheus.HistogramVec

	// Storage metrics
	duckDBConnectionsActive *prometheus.GaugeVec
	duckDBQueryLatency      *prometheus.HistogramVec
	parquetFileCount        *prometheus.GaugeVec
	parquetStorageBytes     *prometheus.GaugeVec
	duckDBStorageBytes      *prometheus.GaugeVec

	// HTTP Server metrics
	httpRequestsTotal  *prometheus.CounterVec
	httpRequestLatency *prometheus.HistogramVec

	// Flight Server metrics
	flightRequestsTotal  *prometheus.CounterVec
	flightRequestLatency *prometheus.HistogramVec

	server *http.Server
}

// NewMetrics creates a new metrics instance with all required metrics registered
func NewMetrics(cfg *config.MetricsConfig, log *logger.Logger) *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		registry: registry,
		log:      log,

		// Feature store metrics
		featureIngestionCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "featherstore_feature_ingestion_count",
				Help: "Number of feature vectors ingested",
			},
			[]string{"feature_set"},
		),
		featureRetrievalCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "featherstore_feature_retrieval_count",
				Help: "Number of feature vectors retrieved",
			},
			[]string{"feature_set"},
		),
		ingestedFeatureBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "featherstore_ingested_feature_bytes",
				Help: "Total bytes of feature data ingested",
			},
			[]string{"feature_set"},
		),
		retrievedFeatureBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "featherstore_retrieved_feature_bytes",
				Help: "Total bytes of feature data retrieved",
			},
			[]string{"feature_set"},
		),
		featureIngestionLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "featherstore_feature_ingestion_latency_seconds",
				Help:    "Latency of feature ingestion operations",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
			},
			[]string{"feature_set"},
		),
		featureRetrievalLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "featherstore_feature_retrieval_latency_seconds",
				Help:    "Latency of feature retrieval operations",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
			},
			[]string{"feature_set"},
		),

		// Storage metrics
		duckDBConnectionsActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "featherstore_duckdb_connections_active",
				Help: "Number of active DuckDB connections",
			},
			[]string{},
		),
		duckDBQueryLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "featherstore_duckdb_query_latency_seconds",
				Help:    "Latency of DuckDB queries",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
			},
			[]string{"query_type"},
		),
		parquetFileCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "featherstore_parquet_file_count",
				Help: "Number of Parquet files in storage",
			},
			[]string{"feature_set"},
		),
		parquetStorageBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "featherstore_parquet_storage_bytes",
				Help: "Total bytes used by Parquet storage",
			},
			[]string{"feature_set"},
		),
		duckDBStorageBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "featherstore_duckdb_storage_bytes",
				Help: "Total bytes used by DuckDB storage",
			},
			[]string{"feature_set"},
		),

		// HTTP Server metrics
		httpRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "featherstore_http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "path", "status"},
		),
		httpRequestLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "featherstore_http_request_latency_seconds",
				Help:    "Latency of HTTP requests",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
			},
			[]string{"method", "path"},
		),

		// Flight Server metrics
		flightRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "featherstore_flight_requests_total",
				Help: "Total number of Arrow Flight requests",
			},
			[]string{"action", "status"},
		),
		flightRequestLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "featherstore_flight_request_latency_seconds",
				Help:    "Latency of Arrow Flight requests",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // from 1ms to ~1s
			},
			[]string{"action"},
		),
	}

	// Register all metrics
	registry.MustRegister(
		m.featureIngestionCount,
		m.featureRetrievalCount,
		m.ingestedFeatureBytes,
		m.retrievedFeatureBytes,
		m.featureIngestionLatency,
		m.featureRetrievalLatency,
		m.duckDBConnectionsActive,
		m.duckDBQueryLatency,
		m.parquetFileCount,
		m.parquetStorageBytes,
		m.duckDBStorageBytes,
		m.httpRequestsTotal,
		m.httpRequestLatency,
		m.flightRequestsTotal,
		m.flightRequestLatency,
	)

	return m
}

// Start begins serving Prometheus metrics on the configured port
func (m *Metrics) Start(cfg *config.MetricsConfig) error {
	if !cfg.Enabled {
		m.log.Info("Metrics server disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))

	addr := ":" + strconv.Itoa(cfg.PrometheusPort)
	m.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		m.log.Info("Starting metrics server", zap.String("address", addr))
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.log.Error("Failed to start metrics server", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the metrics server
func (m *Metrics) Stop(ctx context.Context) error {
	if m.server == nil {
		return nil
	}

	m.log.Info("Stopping metrics server")
	return m.server.Shutdown(ctx)
}

// ObserveFeatureIngestion records metrics for feature ingestion
func (m *Metrics) ObserveFeatureIngestion(featureSet string, count int, bytes int64, duration time.Duration) {
	m.featureIngestionCount.WithLabelValues(featureSet).Add(float64(count))
	m.ingestedFeatureBytes.WithLabelValues(featureSet).Add(float64(bytes))
	m.featureIngestionLatency.WithLabelValues(featureSet).Observe(duration.Seconds())
}

// ObserveFeatureRetrieval records metrics for feature retrieval
func (m *Metrics) ObserveFeatureRetrieval(featureSet string, count int, bytes int64, duration time.Duration) {
	m.featureRetrievalCount.WithLabelValues(featureSet).Add(float64(count))
	m.retrievedFeatureBytes.WithLabelValues(featureSet).Add(float64(bytes))
	m.featureRetrievalLatency.WithLabelValues(featureSet).Observe(duration.Seconds())
}

// ObserveDuckDBQuery records metrics for DuckDB queries
func (m *Metrics) ObserveDuckDBQuery(queryType string, duration time.Duration) {
	m.duckDBQueryLatency.WithLabelValues(queryType).Observe(duration.Seconds())
}

// SetDuckDBConnectionsActive sets the current count of active DuckDB connections
func (m *Metrics) SetDuckDBConnectionsActive(count int) {
	m.duckDBConnectionsActive.WithLabelValues().Set(float64(count))
}

// SetParquetFileCount sets the current count of Parquet files
func (m *Metrics) SetParquetFileCount(featureSet string, count int) {
	m.parquetFileCount.WithLabelValues(featureSet).Set(float64(count))
}

// SetParquetStorageBytes sets the current storage size for Parquet files
func (m *Metrics) SetParquetStorageBytes(featureSet string, bytes int64) {
	m.parquetStorageBytes.WithLabelValues(featureSet).Set(float64(bytes))
}

// SetDuckDBStorageBytes sets the current storage size for DuckDB
func (m *Metrics) SetDuckDBStorageBytes(featureSet string, bytes int64) {
	m.duckDBStorageBytes.WithLabelValues(featureSet).Set(float64(bytes))
}

// ObserveHTTPRequest records metrics for HTTP requests
func (m *Metrics) ObserveHTTPRequest(method, path string, status int, duration time.Duration) {
	statusStr := strconv.Itoa(status)
	m.httpRequestsTotal.WithLabelValues(method, path, statusStr).Inc()
	m.httpRequestLatency.WithLabelValues(method, path).Observe(duration.Seconds())
}

// ObserveFlightRequest records metrics for Arrow Flight requests
func (m *Metrics) ObserveFlightRequest(action string, status string, duration time.Duration) {
	m.flightRequestsTotal.WithLabelValues(action, status).Inc()
	m.flightRequestLatency.WithLabelValues(action).Observe(duration.Seconds())
}

// HTTPMiddleware returns a middleware that records HTTP request metrics
func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Create a response wrapper to capture the status code
		wrapper := newResponseWriter(w)

		// Call the next handler
		next.ServeHTTP(wrapper, r)

		// Record metrics
		duration := time.Since(start)
		m.ObserveHTTPRequest(r.Method, r.URL.Path, wrapper.statusCode, duration)
	})
}

// responseWriter is a wrapper for http.ResponseWriter that captures the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// newResponseWriter creates a new responseWriter
func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

// WriteHeader captures the status code and calls the underlying WriteHeader
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
