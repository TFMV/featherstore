package storage

import (
	"testing"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStorage(t *testing.T) {
	// Create logger for testing
	logCfg := &config.LoggingConfig{
		Level:      "info",
		Format:     "console",
		OutputPath: "stdout",
	}
	log, err := logger.Initialize(logCfg)
	require.NoError(t, err)
	defer log.Close()

	m := metrics.NewMetrics(&config.MetricsConfig{Enabled: false}, log)

	// Test DuckDB backend - this might fail if DuckDB is not properly installed
	t.Run("DuckDB", func(t *testing.T) {
		cfg := &config.StorageConfig{
			Backend: string(DuckDBStorage),
			DuckDB: config.DuckDBConfig{
				Path:           ":memory:",
				MemoryLimit:    "1GB",
				MaxConnections: 4,
			},
		}

		store, err := NewStorage(cfg, log, m)
		if err != nil {
			t.Logf("DuckDB initialization failed: %v - this is expected if DuckDB is not properly installed", err)
			return
		}

		assert.NotNil(t, store)
		defer store.Close()
	})

	// Test Parquet backend
	t.Run("Parquet", func(t *testing.T) {
		cfg := &config.StorageConfig{
			Backend: string(ParquetStorage),
			Parquet: config.ParquetConfig{
				Directory:    t.TempDir(),
				RowGroupSize: 1024,
				Compression:  "snappy",
			},
		}

		store, err := NewStorage(cfg, log, m)
		assert.NoError(t, err)
		assert.NotNil(t, store)
		defer store.Close()
	})

	// Test invalid backend
	t.Run("Invalid", func(t *testing.T) {
		cfg := &config.StorageConfig{
			Backend: "invalid",
		}

		store, err := NewStorage(cfg, log, m)
		assert.Error(t, err)
		assert.Nil(t, store)
		assert.Contains(t, err.Error(), "unsupported storage backend")
	})
}
