package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the application
type Config struct {
	Storage StorageConfig `mapstructure:"storage"`
	Server  ServerConfig  `mapstructure:"server"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logging LoggingConfig `mapstructure:"logging"`
}

// StorageConfig holds storage-related configuration
type StorageConfig struct {
	// Backend specifies which storage backend to use (duckdb or parquet)
	Backend string        `mapstructure:"backend"`
	DuckDB  DuckDBConfig  `mapstructure:"duckdb"`
	Parquet ParquetConfig `mapstructure:"parquet"`
	// Time after which features are moved from DuckDB to Parquet
	ArchiveAfter time.Duration `mapstructure:"archive_after"`
}

// DuckDBConfig holds DuckDB-specific configuration
type DuckDBConfig struct {
	Path        string `mapstructure:"path"`
	MemoryLimit string `mapstructure:"memory_limit"`
	// Number of concurrent connections to DuckDB
	MaxConnections int `mapstructure:"max_connections"`
}

// ParquetConfig holds Parquet-specific configuration
type ParquetConfig struct {
	Directory string `mapstructure:"directory"`
	// Row group size for Parquet files
	RowGroupSize int64 `mapstructure:"row_group_size"`
	// Compression codec to use (snappy, gzip, etc.)
	Compression string `mapstructure:"compression"`
}

// ServerConfig holds HTTP and gRPC server configuration
type ServerConfig struct {
	HTTPPort     int           `mapstructure:"http_port"`
	FlightPort   int           `mapstructure:"flight_port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	// Maximum size of ingestion requests
	MaxRequestSize int64 `mapstructure:"max_request_size"`
}

// MetricsConfig holds metrics and observability configuration
type MetricsConfig struct {
	PrometheusPort int  `mapstructure:"prometheus_port"`
	Enabled        bool `mapstructure:"enabled"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"`
	OutputPath string `mapstructure:"output_path"`
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	// Set default values
	setDefaultConfig(v)

	// Read from config file if it exists
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	// Override with environment variables
	v.SetEnvPrefix("FEATHERSTORE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	return &config, nil
}

// setDefaultConfig sets sensible default values
func setDefaultConfig(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.http_port", 8080)
	v.SetDefault("server.flight_port", 8081)
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.write_timeout", "30s")
	v.SetDefault("server.max_request_size", 50*1024*1024) // 50MB

	// Storage defaults
	v.SetDefault("storage.backend", "duckdb")
	v.SetDefault("storage.archive_after", "168h") // 7 days
	v.SetDefault("storage.duckdb.path", "./data/features.db")
	v.SetDefault("storage.duckdb.memory_limit", "4GB")
	v.SetDefault("storage.duckdb.max_connections", 10)
	v.SetDefault("storage.parquet.directory", "./data/features")
	v.SetDefault("storage.parquet.row_group_size", 8192)
	v.SetDefault("storage.parquet.compression", "snappy")

	// Metrics defaults
	v.SetDefault("metrics.prometheus_port", 9090)
	v.SetDefault("metrics.enabled", true)

	// Logging defaults
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.output_path", "stdout")
}

// EnsureDirs creates necessary directories defined in configuration
func (c *Config) EnsureDirs() error {
	dirs := []string{
		c.Storage.Parquet.Directory,
		// Extract directory from the DuckDB path
		strings.TrimSuffix(c.Storage.DuckDB.Path, "/features.db"),
	}

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}
