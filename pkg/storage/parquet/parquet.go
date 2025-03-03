package parquet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/core"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.uber.org/zap"
)

// metadataFile is the name of the file containing feature set metadata
const metadataFile = "metadata.json"

// Storage implements core.FeatureStore interface using Parquet files
type Storage struct {
	config      *config.ParquetConfig
	log         *logger.Logger
	metrics     *metrics.Metrics
	rootDir     string
	schemaCache map[string]*arrow.Schema
	cacheMutex  sync.RWMutex
	allocator   memory.Allocator
}

// FeatureSetMetadata holds metadata about a feature set
type FeatureSetMetadata struct {
	Name        string            `json:"name"`
	Features    []core.Feature    `json:"features"`
	Description string            `json:"description"`
	Tags        map[string]string `json:"tags"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	SchemaJSON  string            `json:"schema"`
}

// NewStorage creates a new Parquet storage instance
func NewStorage(cfg *config.ParquetConfig, log *logger.Logger, m *metrics.Metrics) (*Storage, error) {
	// Ensure the root directory exists
	if err := os.MkdirAll(cfg.Directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &Storage{
		config:      cfg,
		log:         log,
		metrics:     m,
		rootDir:     cfg.Directory,
		schemaCache: make(map[string]*arrow.Schema),
		allocator:   memory.NewGoAllocator(),
	}, nil
}

// featureSetDir returns the directory for a feature set
func (s *Storage) featureSetDir(name string) string {
	return filepath.Join(s.rootDir, name)
}

// metadataPath returns the path to the metadata file for a feature set
func (s *Storage) metadataPath(name string) string {
	return filepath.Join(s.featureSetDir(name), metadataFile)
}

// getDataFilePath returns a path for a new data file in the feature set
// Uses timestamp to ensure uniqueness
func (s *Storage) getDataFilePath(featureSetName string, ts time.Time) string {
	timestamp := ts.Format("20060102-150405.000")
	return filepath.Join(s.featureSetDir(featureSetName), fmt.Sprintf("%s.parquet", timestamp))
}

// CreateFeatureSet creates a new feature set
func (s *Storage) CreateFeatureSet(ctx context.Context, featureSet *core.FeatureSet) error {
	// Check if feature set already exists
	if existing, _ := s.GetFeatureSet(ctx, featureSet.Name); existing != nil {
		return core.ErrFeatureSetAlreadyExists
	}

	// Validate schema
	if featureSet.Schema == nil || len(featureSet.Schema.Fields()) == 0 {
		return core.ErrInvalidSchema
	}

	// Ensure the schema contains entity_id
	hasEntityID := false
	for _, field := range featureSet.Schema.Fields() {
		if field.Name == "entity_id" {
			hasEntityID = true
			break
		}
	}

	if !hasEntityID {
		return fmt.Errorf("schema must contain 'entity_id' field")
	}

	// Create directory for the feature set
	featureSetDir := s.featureSetDir(featureSet.Name)
	if err := os.MkdirAll(featureSetDir, 0755); err != nil {
		return fmt.Errorf("failed to create feature set directory: %w", err)
	}

	// Serialize schema to JSON for storage
	schemaJSON, err := serializeSchema(featureSet.Schema)
	if err != nil {
		return fmt.Errorf("failed to serialize schema: %w", err)
	}

	// Create metadata
	metadata := FeatureSetMetadata{
		Name:        featureSet.Name,
		Features:    featureSet.Features,
		Description: featureSet.Description,
		Tags:        featureSet.Tags,
		CreatedAt:   featureSet.CreatedAt,
		UpdatedAt:   featureSet.UpdatedAt,
		SchemaJSON:  schemaJSON,
	}

	// Write metadata to file
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := ioutil.WriteFile(s.metadataPath(featureSet.Name), metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	// Cache the schema
	s.cacheMutex.Lock()
	s.schemaCache[featureSet.Name] = featureSet.Schema
	s.cacheMutex.Unlock()

	s.log.Info("Created feature set",
		zap.String("feature_set", featureSet.Name),
		zap.Int("num_features", len(featureSet.Features)),
	)

	return nil
}

// GetFeatureSet retrieves a feature set by name
func (s *Storage) GetFeatureSet(ctx context.Context, name string) (*core.FeatureSet, error) {
	// Check if metadata file exists
	metadataPath := s.metadataPath(name)
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		return nil, core.ErrFeatureSetNotFound
	}

	// Read metadata file
	metadataBytes, err := ioutil.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var metadata FeatureSetMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Check schema cache first
	s.cacheMutex.RLock()
	schema, ok := s.schemaCache[name]
	s.cacheMutex.RUnlock()

	// If not in cache, deserialize from JSON
	if !ok {
		schema, err = deserializeSchema(metadata.SchemaJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize schema: %w", err)
		}

		// Add to cache
		s.cacheMutex.Lock()
		s.schemaCache[name] = schema
		s.cacheMutex.Unlock()
	}

	// Create feature set
	return &core.FeatureSet{
		Name:        metadata.Name,
		Features:    metadata.Features,
		Schema:      schema,
		Description: metadata.Description,
		Tags:        metadata.Tags,
		CreatedAt:   metadata.CreatedAt,
		UpdatedAt:   metadata.UpdatedAt,
	}, nil
}

// ListFeatureSets returns all feature sets
func (s *Storage) ListFeatureSets(ctx context.Context) ([]*core.FeatureSet, error) {
	// Get all directories in the root directory
	entries, err := ioutil.ReadDir(s.rootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage directory: %w", err)
	}

	var featureSets []*core.FeatureSet
	for _, entry := range entries {
		if entry.IsDir() {
			// Check if metadata file exists in this directory
			metadataPath := filepath.Join(s.rootDir, entry.Name(), metadataFile)
			if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
				continue
			}

			// Get feature set
			featureSet, err := s.GetFeatureSet(ctx, entry.Name())
			if err != nil {
				s.log.Warn("Failed to get feature set",
					zap.String("feature_set", entry.Name()),
					zap.Error(err),
				)
				continue
			}

			featureSets = append(featureSets, featureSet)
		}
	}

	return featureSets, nil
}

// DeleteFeatureSet removes a feature set and all its data
func (s *Storage) DeleteFeatureSet(ctx context.Context, name string) error {
	// Check if feature set exists
	if _, err := s.GetFeatureSet(ctx, name); err != nil {
		return err
	}

	// Remove from cache
	s.cacheMutex.Lock()
	delete(s.schemaCache, name)
	s.cacheMutex.Unlock()

	// Remove the directory and all its contents
	if err := os.RemoveAll(s.featureSetDir(name)); err != nil {
		return fmt.Errorf("failed to delete feature set: %w", err)
	}

	s.log.Info("Deleted feature set", zap.String("feature_set", name))
	return nil
}

// IngestBatch ingests a batch of features into Parquet files
func (s *Storage) IngestBatch(ctx context.Context, featureSetName string, batch arrow.Record) error {
	// Check if feature set exists
	featureSet, err := s.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		return err
	}

	// Validate batch schema against feature set schema
	if !schemasCompatible(featureSet.Schema, batch.Schema()) {
		return fmt.Errorf("batch schema is not compatible with feature set schema")
	}

	start := time.Now()

	// Create a new file for this batch
	filePath := s.getDataFilePath(featureSetName, time.Now())
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file: %w", err)
	}
	defer file.Close()

	// Create Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(getParquetCompression(s.config.Compression)),
		parquet.WithDataPageSize(int64(s.config.RowGroupSize)),
	)

	// Create Arrow writer
	arrowWriter, err := pqarrow.NewFileWriter(
		batch.Schema(),
		file,
		writerProps,
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		return fmt.Errorf("failed to create Arrow writer: %w", err)
	}

	// Write the batch
	if err := arrowWriter.Write(batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	// Close the writer
	if err := arrowWriter.Close(); err != nil {
		return fmt.Errorf("failed to close Arrow writer: %w", err)
	}

	duration := time.Since(start)

	// Update metrics
	if s.metrics != nil {
		s.metrics.ObserveFeatureIngestion(
			featureSetName,
			int(batch.NumRows()),
			estimateBatchSize(batch),
			duration,
		)
	}

	s.log.Info("Ingested feature batch to Parquet",
		zap.String("feature_set", featureSetName),
		zap.Int64("num_rows", batch.NumRows()),
		zap.String("file", filepath.Base(filePath)),
		zap.Duration("duration", duration),
	)

	return nil
}

// GetFeatures retrieves features for specific entities
func (s *Storage) GetFeatures(ctx context.Context, featureSetName string, entityIDs []string) (arrow.Record, error) {
	if len(entityIDs) == 0 {
		return nil, fmt.Errorf("no entity IDs provided")
	}

	// Check if feature set exists
	featureSet, err := s.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		return nil, err
	}

	start := time.Now()

	// Get list of Parquet files for this feature set
	featureSetDir := s.featureSetDir(featureSetName)
	files, err := listParquetFiles(featureSetDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list Parquet files: %w", err)
	}

	if len(files) == 0 {
		// No data files yet, return empty batch
		return emptyBatch(featureSet.Schema), nil
	}

	// Create a set of entity IDs for faster lookup
	entityIDSet := make(map[string]bool)
	for _, id := range entityIDs {
		entityIDSet[id] = true
	}

	// Find the latest features for each entity
	// First, collect all features
	var allRecords []arrow.Record
	entityIndex := make(map[string]int) // Maps entity_id to the record index

	// Convert from newest to oldest
	for i := len(files) - 1; i >= 0; i-- {
		filePath := files[i]

		// Read the file
		record, err := readParquetFile(ctx, filePath, s.allocator)
		if err != nil {
			s.log.Warn("Failed to read Parquet file",
				zap.String("file", filePath),
				zap.Error(err),
			)
			continue
		}

		// Extract relevant records
		filteredRecord, err := filterByEntityIDs(record, entityIDSet, entityIndex)
		if err != nil {
			s.log.Warn("Failed to filter records",
				zap.String("file", filePath),
				zap.Error(err),
			)
			record.Release()
			continue
		}

		if filteredRecord.NumRows() > 0 {
			allRecords = append(allRecords, filteredRecord)
		} else {
			filteredRecord.Release()
		}

		record.Release()

		// If we've found all entities, we can stop
		if len(entityIndex) == len(entityIDs) {
			break
		}
	}

	// If we didn't find any records, return an empty batch
	if len(allRecords) == 0 {
		return emptyBatch(featureSet.Schema), nil
	}

	// Combine all records into a single batch
	result, err := combineRecords(allRecords, featureSet.Schema)
	if err != nil {
		for _, record := range allRecords {
			record.Release()
		}
		return nil, fmt.Errorf("failed to combine records: %w", err)
	}

	// Release the intermediate records
	for _, record := range allRecords {
		record.Release()
	}

	duration := time.Since(start)

	// Update metrics
	if s.metrics != nil {
		s.metrics.ObserveFeatureRetrieval(
			featureSetName,
			int(result.NumRows()),
			estimateBatchSize(result),
			duration,
		)
	}

	s.log.Info("Retrieved features from Parquet",
		zap.String("feature_set", featureSetName),
		zap.Int("num_entities", len(entityIDs)),
		zap.Int64("num_rows", result.NumRows()),
		zap.Duration("duration", duration),
	)

	return result, nil
}

// GetFeatureHistory retrieves historical features for a specific entity
func (s *Storage) GetFeatureHistory(
	ctx context.Context,
	featureSetName string,
	entityID string,
	startTime, endTime time.Time,
) (arrow.Record, error) {
	// Check if feature set exists
	featureSet, err := s.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		return nil, err
	}

	start := time.Now()

	// Get list of Parquet files for this feature set
	featureSetDir := s.featureSetDir(featureSetName)
	files, err := listParquetFiles(featureSetDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list Parquet files: %w", err)
	}

	if len(files) == 0 {
		// No data files yet, return empty batch
		return emptyBatch(featureSet.Schema), nil
	}

	// Collect records from all files within the time range
	var allRecords []arrow.Record

	for _, filePath := range files {
		// Check if file timestamp is within range (from filename)
		fileTime, err := parseFileTimestamp(filepath.Base(filePath))
		if err != nil {
			s.log.Warn("Failed to parse file timestamp",
				zap.String("file", filePath),
				zap.Error(err),
			)
			continue
		}

		// Skip files created before start time or after end time
		if fileTime.Before(startTime) || fileTime.After(endTime) {
			continue
		}

		// Read the file
		record, err := readParquetFile(ctx, filePath, s.allocator)
		if err != nil {
			s.log.Warn("Failed to read Parquet file",
				zap.String("file", filePath),
				zap.Error(err),
			)
			continue
		}

		// Filter by entity ID and time range
		filteredRecord, err := filterByEntityAndTimeRange(record, entityID, startTime, endTime)
		if err != nil {
			s.log.Warn("Failed to filter records",
				zap.String("file", filePath),
				zap.Error(err),
			)
			record.Release()
			continue
		}

		if filteredRecord.NumRows() > 0 {
			allRecords = append(allRecords, filteredRecord)
		} else {
			filteredRecord.Release()
		}

		record.Release()
	}

	// If we didn't find any records, return an empty batch
	if len(allRecords) == 0 {
		return emptyBatch(featureSet.Schema), nil
	}

	// Combine all records into a single batch
	result, err := combineRecords(allRecords, featureSet.Schema)
	if err != nil {
		for _, record := range allRecords {
			record.Release()
		}
		return nil, fmt.Errorf("failed to combine records: %w", err)
	}

	// Release the intermediate records
	for _, record := range allRecords {
		record.Release()
	}

	duration := time.Since(start)

	// Update metrics
	if s.metrics != nil {
		s.metrics.ObserveFeatureRetrieval(
			featureSetName,
			int(result.NumRows()),
			estimateBatchSize(result),
			duration,
		)
	}

	s.log.Info("Retrieved feature history from Parquet",
		zap.String("feature_set", featureSetName),
		zap.String("entity_id", entityID),
		zap.Time("start_time", startTime),
		zap.Time("end_time", endTime),
		zap.Int64("num_rows", result.NumRows()),
		zap.Duration("duration", duration),
	)

	return result, nil
}

// Close closes the storage
func (s *Storage) Close() error {
	// Nothing to close for Parquet storage
	return nil
}

// Helper function to list Parquet files in a directory sorted by creation time
func listParquetFiles(dir string) ([]string, error) {
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".parquet") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}

	// Sort by name (which contains timestamp)
	sort.Strings(files)
	return files, nil
}

// Helper function to parse timestamp from filename (format: 20060102-150405.000.parquet)
func parseFileTimestamp(filename string) (time.Time, error) {
	// Remove the .parquet extension
	base := strings.TrimSuffix(filename, ".parquet")
	// Parse the timestamp
	return time.Parse("20060102-150405.000", base)
}

// Helper to read a Parquet file into an Arrow record batch
func readParquetFile(ctx context.Context, path string, allocator memory.Allocator) (arrow.Record, error) {
	// Open the file
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Create a Parquet file reader
	reader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Create Arrow reader
	props := pqarrow.ArrowReadProperties{}
	fileReader, err := pqarrow.NewFileReader(reader, props, allocator)
	if err != nil {
		return nil, err
	}

	// Read all record batches to a table
	table, err := fileReader.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer table.Release()

	// Convert table to record batch
	return tableToRecord(table)
}

// Helper to filter records by entity IDs and update entity index
func filterByEntityIDs(record arrow.Record, entityIDs map[string]bool, entityIndex map[string]int) (arrow.Record, error) {
	// Find the entity_id column
	entityIDIdx := -1
	for i, field := range record.Schema().Fields() {
		if field.Name == "entity_id" {
			entityIDIdx = i
			break
		}
	}

	if entityIDIdx == -1 {
		return nil, fmt.Errorf("entity_id column not found")
	}

	// Get the entity_id column
	entityIDArray := record.Column(entityIDIdx).(*array.String)

	// Find rows matching entity IDs that we haven't seen yet
	matchingRows := make([]int, 0, record.NumRows())
	for i := 0; i < int(record.NumRows()); i++ {
		id := entityIDArray.Value(i)
		if entityIDs[id] && entityIndex[id] == 0 {
			matchingRows = append(matchingRows, i)
			entityIndex[id] = len(entityIndex) + 1 // Mark as found
		}
	}

	// If no matching rows, return an empty batch
	if len(matchingRows) == 0 {
		return emptyBatch(record.Schema()), nil
	}

	// Create a new record with only the matching rows
	return sliceRecord(record, matchingRows)
}

// Helper to filter records by entity ID and time range
func filterByEntityAndTimeRange(record arrow.Record, entityID string, startTime, endTime time.Time) (arrow.Record, error) {
	// Find the entity_id and timestamp columns
	entityIDIdx := -1
	timestampIdx := -1
	for i, field := range record.Schema().Fields() {
		if field.Name == "entity_id" {
			entityIDIdx = i
		} else if field.Name == "timestamp" {
			timestampIdx = i
		}
	}

	if entityIDIdx == -1 {
		return nil, fmt.Errorf("entity_id column not found")
	}
	if timestampIdx == -1 {
		return nil, fmt.Errorf("timestamp column not found")
	}

	// Get the entity_id and timestamp columns
	entityIDArray := record.Column(entityIDIdx).(*array.String)
	timestampArray := record.Column(timestampIdx).(*array.Timestamp)
	timestampType := record.Schema().Field(timestampIdx).Type.(*arrow.TimestampType)

	// Find rows matching entity ID and time range
	matchingRows := make([]int, 0, record.NumRows())
	for i := 0; i < int(record.NumRows()); i++ {
		id := entityIDArray.Value(i)
		if id != entityID {
			continue
		}

		// Convert timestamp to time.Time
		ts := timestampArray.Value(i)
		var t time.Time
		switch timestampType.Unit {
		case arrow.Second:
			t = time.Unix(int64(ts), 0)
		case arrow.Millisecond:
			t = time.Unix(0, int64(ts)*int64(time.Millisecond))
		case arrow.Microsecond:
			t = time.Unix(0, int64(ts)*int64(time.Microsecond))
		case arrow.Nanosecond:
			t = time.Unix(0, int64(ts))
		default:
			return nil, fmt.Errorf("unsupported timestamp unit: %v", timestampType.Unit)
		}

		// Check if timestamp is in range
		if !t.Before(startTime) && !t.After(endTime) {
			matchingRows = append(matchingRows, i)
		}
	}

	// If no matching rows, return an empty batch
	if len(matchingRows) == 0 {
		return emptyBatch(record.Schema()), nil
	}

	// Create a new record with only the matching rows
	return sliceRecord(record, matchingRows)
}

// Helper to slice a record to include only the specified rows
func sliceRecord(record arrow.Record, indices []int) (arrow.Record, error) {
	if len(indices) == 0 {
		return emptyBatch(record.Schema()), nil
	}

	// Create builders for each column
	builders := make([]array.Builder, record.NumCols())
	for i, field := range record.Schema().Fields() {
		builders[i] = array.NewBuilder(memory.DefaultAllocator, field.Type)
		defer builders[i].Release()
	}

	// Append values to builders
	for _, idx := range indices {
		for i := 0; i < int(record.NumCols()); i++ {
			col := record.Column(i)
			appendValue(builders[i], col, idx)
		}
	}

	// Build arrays
	columns := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		columns[i] = builder.NewArray()
		defer columns[i].Release()
	}

	// Create a new record
	return array.NewRecord(record.Schema(), columns, int64(len(indices))), nil
}

// Helper to append a value from an array to a builder
func appendValue(builder array.Builder, arr arrow.Array, idx int) {
	if arr.IsNull(idx) {
		builder.AppendNull()
		return
	}

	switch arr := arr.(type) {
	case *array.Boolean:
		builder.(*array.BooleanBuilder).Append(arr.Value(idx))
	case *array.Int8:
		builder.(*array.Int8Builder).Append(arr.Value(idx))
	case *array.Int16:
		builder.(*array.Int16Builder).Append(arr.Value(idx))
	case *array.Int32:
		builder.(*array.Int32Builder).Append(arr.Value(idx))
	case *array.Int64:
		builder.(*array.Int64Builder).Append(arr.Value(idx))
	case *array.Uint8:
		builder.(*array.Uint8Builder).Append(arr.Value(idx))
	case *array.Uint16:
		builder.(*array.Uint16Builder).Append(arr.Value(idx))
	case *array.Uint32:
		builder.(*array.Uint32Builder).Append(arr.Value(idx))
	case *array.Uint64:
		builder.(*array.Uint64Builder).Append(arr.Value(idx))
	case *array.Float32:
		builder.(*array.Float32Builder).Append(arr.Value(idx))
	case *array.Float64:
		builder.(*array.Float64Builder).Append(arr.Value(idx))
	case *array.String:
		builder.(*array.StringBuilder).Append(arr.Value(idx))
	case *array.Binary:
		builder.(*array.BinaryBuilder).Append(arr.Value(idx))
	case *array.Timestamp:
		builder.(*array.TimestampBuilder).Append(arr.Value(idx))
	case *array.Date32:
		builder.(*array.Date32Builder).Append(arr.Value(idx))
	case *array.Date64:
		builder.(*array.Date64Builder).Append(arr.Value(idx))
	default:
		builder.AppendNull()
	}
}

// Helper to convert an Arrow table to a record batch
func tableToRecord(table arrow.Table) (arrow.Record, error) {
	// If the table is empty, return an empty record
	if table.NumRows() == 0 {
		return emptyBatch(table.Schema()), nil
	}

	// Create a record batch reader from the table
	reader := array.NewTableReader(table, table.NumRows())
	defer reader.Release()

	// Read the batch
	if !reader.Next() {
		if reader.Err() != nil {
			return nil, reader.Err()
		}
		return emptyBatch(table.Schema()), nil
	}

	// Get the record and make a copy (since reader will be released)
	record := reader.Record()
	newRecord := array.NewRecord(record.Schema(), record.Columns(), record.NumRows())
	return newRecord, nil
}

// Helper to combine multiple record batches into a single batch
func combineRecords(records []arrow.Record, schema *arrow.Schema) (arrow.Record, error) {
	if len(records) == 0 {
		return emptyBatch(schema), nil
	}

	if len(records) == 1 {
		return records[0], nil
	}

	// Create a table from the records
	table := array.NewTableFromRecords(schema, records)
	if table == nil {
		return nil, fmt.Errorf("failed to create table from records")
	}
	defer table.Release()

	return tableToRecord(table)
}

// Helper to create an empty batch with the given schema
func emptyBatch(schema *arrow.Schema) arrow.Record {
	columns := make([]arrow.Array, len(schema.Fields()))
	for i, field := range schema.Fields() {
		columns[i] = array.MakeArrayOfNull(memory.DefaultAllocator, field.Type, 0)
		defer columns[i].Release()
	}
	return array.NewRecord(schema, columns, 0)
}

// Helper to estimate the size of a record batch in bytes
func estimateBatchSize(batch arrow.Record) int64 {
	if batch == nil {
		return 0
	}

	var size int64 = 0

	// Calculate the size of each column
	for i := 0; i < int(batch.NumCols()); i++ {
		col := batch.Column(i)
		// Estimate size by summing up buffer lengths
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
	}

	return size
}

// Helper to check if two schemas are compatible
func schemasCompatible(schema1, schema2 *arrow.Schema) bool {
	// Check if all required fields are present
	schema1Fields := make(map[string]arrow.DataType)
	for _, field := range schema1.Fields() {
		schema1Fields[field.Name] = field.Type
	}

	for _, field := range schema2.Fields() {
		// For required fields (entity_id, timestamp), check type compatibility
		if field.Name == "entity_id" || field.Name == "timestamp" {
			if dataType, ok := schema1Fields[field.Name]; ok {
				if !typesCompatible(dataType, field.Type) {
					return false
				}
			} else {
				return false
			}
		}
	}

	return true
}

// Helper to check if two data types are compatible
func typesCompatible(type1, type2 arrow.DataType) bool {
	// For now, just check if the types are the same
	// Could be extended to handle type conversions
	return type1.ID() == type2.ID()
}

// Helper to serialize an Arrow schema to JSON
func serializeSchema(schema *arrow.Schema) (string, error) {
	// Use the IPC package to serialize the schema
	payload := ipc.GetSchemaPayload(schema, memory.NewGoAllocator())
	// Convert to base64 for storage as a string
	encoded := base64.StdEncoding.EncodeToString(payload.Meta().Bytes())
	return encoded, nil
}

// Helper to deserialize an Arrow schema from JSON
func deserializeSchema(schemaJSON string) (*arrow.Schema, error) {
	// Decode from base64
	schemaBytes, err := base64.StdEncoding.DecodeString(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to decode schema: %w", err)
	}

	// Use the IPC reader to deserialize the schema
	reader, err := ipc.NewReader(bytes.NewReader(schemaBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Release()

	return reader.Schema(), nil
}

// Helper to get Parquet compression from string
func getParquetCompression(compressionStr string) compress.Compression {
	switch strings.ToLower(compressionStr) {
	case "snappy":
		return compress.Codecs.Snappy
	case "gzip":
		return compress.Codecs.Gzip
	case "brotli":
		return compress.Codecs.Brotli
	case "zstd":
		return compress.Codecs.Zstd
	case "lz4":
		return compress.Codecs.Lz4
	default:
		return compress.Codecs.Snappy // Default to Snappy
	}
}
