package storage

import (
	"fmt"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/core"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/TFMV/featherstore/pkg/storage/duckdb"
	"github.com/TFMV/featherstore/pkg/storage/parquet"
)

// StorageType defines the type of storage backend
type StorageType string

const (
	// DuckDBStorage uses DuckDB as the storage backend
	DuckDBStorage StorageType = "duckdb"
	// ParquetStorage uses Parquet files as the storage backend
	ParquetStorage StorageType = "parquet"
)

// NewStorage creates a new storage backend based on the configuration
func NewStorage(cfg *config.StorageConfig, log *logger.Logger, m *metrics.Metrics) (core.FeatureStore, error) {
	storageType := StorageType(cfg.Backend)

	switch storageType {
	case DuckDBStorage:
		return duckdb.NewStorage(&cfg.DuckDB, log, m)
	case ParquetStorage:
		return parquet.NewStorage(&cfg.Parquet, log, m)
	default:
		return nil, fmt.Errorf("unsupported storage backend: %s", storageType)
	}
}
