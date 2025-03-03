package parquet

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/core"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParquetStorage(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "parquet-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create test configuration
	cfg := &config.ParquetConfig{
		Directory:    tempDir,
		RowGroupSize: 1024,
		Compression:  "snappy",
	}

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

	// Create storage
	storage, err := NewStorage(cfg, log, m)
	require.NoError(t, err)
	defer storage.Close()

	// Test context
	ctx := context.Background()

	// Define timestamp type consistently
	timestampType := &arrow.TimestampType{Unit: arrow.Millisecond}

	// Create a test feature set
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "entity_id", Type: arrow.BinaryTypes.String},
			{Name: "timestamp", Type: timestampType},
			{Name: "feature1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "feature2", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	featureSet := &core.FeatureSet{
		Name:        "test_features",
		Description: "Test feature set",
		Schema:      schema,
		Features: []core.Feature{
			{Name: "feature1", Description: "Test feature 1"},
			{Name: "feature2", Description: "Test feature 2"},
		},
		Tags:      map[string]string{"env": "test"},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Test CreateFeatureSet
	t.Run("CreateFeatureSet", func(t *testing.T) {
		err := storage.CreateFeatureSet(ctx, featureSet)
		assert.NoError(t, err)

		// Check if metadata file was created
		metadataPath := filepath.Join(tempDir, "test_features", "metadata.json")
		_, err = os.Stat(metadataPath)
		assert.NoError(t, err)
	})

	// Test GetFeatureSet
	t.Run("GetFeatureSet", func(t *testing.T) {
		fs, err := storage.GetFeatureSet(ctx, "test_features")
		assert.NoError(t, err)
		assert.Equal(t, "test_features", fs.Name)
		assert.Equal(t, "Test feature set", fs.Description)
		assert.Equal(t, 2, len(fs.Features))
		assert.Equal(t, 4, len(fs.Schema.Fields()))
	})

	// Test IngestBatch and GetFeatures
	t.Run("IngestAndRetrieve", func(t *testing.T) {
		// Create a test batch
		allocator := memory.NewGoAllocator()

		// Create builders
		entityBuilder := array.NewStringBuilder(allocator)
		defer entityBuilder.Release()

		timestampBuilder := array.NewTimestampBuilder(allocator, timestampType)
		defer timestampBuilder.Release()

		feature1Builder := array.NewFloat64Builder(allocator)
		defer feature1Builder.Release()

		feature2Builder := array.NewInt64Builder(allocator)
		defer feature2Builder.Release()

		// Add data
		entityBuilder.AppendValues([]string{"entity1", "entity2", "entity3"}, nil)

		now := time.Now()
		timestampBuilder.AppendValues(
			[]arrow.Timestamp{
				arrow.Timestamp(now.UnixNano() / int64(time.Millisecond)),
				arrow.Timestamp(now.UnixNano() / int64(time.Millisecond)),
				arrow.Timestamp(now.UnixNano() / int64(time.Millisecond)),
			},
			nil,
		)

		feature1Builder.AppendValues([]float64{1.1, 2.2, 3.3}, nil)
		feature2Builder.AppendValues([]int64{10, 20, 30}, nil)

		// Create arrays
		entityArray := entityBuilder.NewArray()
		defer entityArray.Release()

		timestampArray := timestampBuilder.NewArray()
		defer timestampArray.Release()

		feature1Array := feature1Builder.NewArray()
		defer feature1Array.Release()

		feature2Array := feature2Builder.NewArray()
		defer feature2Array.Release()

		// Create record batch
		batch := array.NewRecord(
			schema,
			[]arrow.Array{entityArray, timestampArray, feature1Array, feature2Array},
			3,
		)
		defer batch.Release()

		// Ingest batch
		err := storage.IngestBatch(ctx, "test_features", batch)
		assert.NoError(t, err)

		// Check if a Parquet file was created
		files, err := os.ReadDir(filepath.Join(tempDir, "test_features"))
		assert.NoError(t, err)

		var parquetFiles []string
		for _, file := range files {
			if filepath.Ext(file.Name()) == ".parquet" {
				parquetFiles = append(parquetFiles, file.Name())
			}
		}
		assert.Equal(t, 1, len(parquetFiles))

		// Retrieve features
		result, err := storage.GetFeatures(ctx, "test_features", []string{"entity1", "entity2"})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(2), result.NumRows())

		// Check the schema and number of columns
		assert.Equal(t, 4, len(result.Schema().Fields()))

		// Check values - safely access columns
		if result.NumCols() > 0 {
			entityCol, ok := result.Column(0).(*array.String)
			if assert.True(t, ok, "First column should be a string column") {
				assert.Equal(t, "entity1", entityCol.Value(0))
				assert.Equal(t, "entity2", entityCol.Value(1))
			}
		}

		if result.NumCols() > 2 {
			feature1Col, ok := result.Column(2).(*array.Float64)
			if assert.True(t, ok, "Third column should be a float64 column") {
				assert.Equal(t, 1.1, feature1Col.Value(0))
				assert.Equal(t, 2.2, feature1Col.Value(1))
			}
		}
	})

	// Test ListFeatureSets
	t.Run("ListFeatureSets", func(t *testing.T) {
		featureSets, err := storage.ListFeatureSets(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(featureSets))
		assert.Equal(t, "test_features", featureSets[0].Name)
	})

	// Test DeleteFeatureSet
	t.Run("DeleteFeatureSet", func(t *testing.T) {
		err := storage.DeleteFeatureSet(ctx, "test_features")
		assert.NoError(t, err)

		// Verify it's gone
		_, err = storage.GetFeatureSet(ctx, "test_features")
		assert.Error(t, err)
		assert.Equal(t, core.ErrFeatureSetNotFound, err)

		// Directory should be gone
		_, err = os.Stat(filepath.Join(tempDir, "test_features"))
		assert.True(t, os.IsNotExist(err))
	})
}
