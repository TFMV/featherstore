package core

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Feature represents a single feature with metadata
type Feature struct {
	// Name of the feature
	Name string
	// DataType is the Arrow data type of this feature
	DataType arrow.DataType
	// Description is human-readable information about this feature
	Description string
	// Tags are arbitrary key-value pairs for feature metadata
	Tags map[string]string
}

// FeatureVector represents a collection of feature values for a specific entity
type FeatureVector struct {
	// EntityID is the unique identifier for the entity (user, item, etc.)
	EntityID string
	// Values maps feature names to their values
	Values map[string]interface{}
	// Timestamp is when this feature vector was recorded
	Timestamp time.Time
}

// FeatureSet defines a collection of related features
type FeatureSet struct {
	// Name is a unique identifier for this feature set
	Name string
	// Features is a list of features in this set
	Features []Feature
	// Schema is the Arrow schema for this feature set
	Schema *arrow.Schema
	// Description provides information about this feature set
	Description string
	// Tags are arbitrary key-value pairs for feature set metadata
	Tags map[string]string
	// CreatedAt is when this feature set was created
	CreatedAt time.Time
	// UpdatedAt is when this feature set was last updated
	UpdatedAt time.Time
}

// FeatureStore defines the interface for feature storage and retrieval
type FeatureStore interface {
	// CreateFeatureSet creates a new feature set with the given schema
	CreateFeatureSet(ctx context.Context, featureSet *FeatureSet) error

	// GetFeatureSet retrieves a feature set by name
	GetFeatureSet(ctx context.Context, name string) (*FeatureSet, error)

	// ListFeatureSets returns all feature sets
	ListFeatureSets(ctx context.Context) ([]*FeatureSet, error)

	// DeleteFeatureSet removes a feature set and all its data
	DeleteFeatureSet(ctx context.Context, name string) error

	// IngestBatch ingests a batch of features into the store
	IngestBatch(ctx context.Context, featureSetName string, batch arrow.Record) error

	// GetFeatures retrieves features for specific entities
	GetFeatures(ctx context.Context, featureSetName string, entityIDs []string) (arrow.Record, error)

	// GetFeatureHistory retrieves historical features for a specific entity
	GetFeatureHistory(
		ctx context.Context,
		featureSetName string,
		entityID string,
		startTime, endTime time.Time,
	) (arrow.Record, error)

	// Close cleans up resources used by the feature store
	Close() error
}

// ErrFeatureSetNotFound is returned when a requested feature set doesn't exist
var ErrFeatureSetNotFound = fmt.Errorf("feature set not found")

// ErrFeatureSetAlreadyExists is returned when creating a feature set that already exists
var ErrFeatureSetAlreadyExists = fmt.Errorf("feature set already exists")

// ErrInvalidSchema is returned when a schema is invalid for a feature set
var ErrInvalidSchema = fmt.Errorf("invalid schema for feature set")

// ErrEntityNotFound is returned when a requested entity doesn't exist
var ErrEntityNotFound = fmt.Errorf("entity not found")

// RecordBatchToFeatureVectors converts an Arrow record batch to FeatureVector objects
func RecordBatchToFeatureVectors(batch arrow.Record) ([]*FeatureVector, error) {
	if batch.NumRows() == 0 {
		return []*FeatureVector{}, nil
	}

	// Get field indices
	schema := batch.Schema()
	entityIDIdx := -1
	timestampIdx := -1

	for i, field := range schema.Fields() {
		switch field.Name {
		case "entity_id":
			entityIDIdx = i
		case "timestamp":
			timestampIdx = i
		}
	}

	if entityIDIdx == -1 {
		return nil, fmt.Errorf("record batch must contain 'entity_id' column")
	}

	// Extract columns
	columns := make([]arrow.Array, int(batch.NumCols()))
	for i := 0; i < int(batch.NumCols()); i++ {
		columns[i] = batch.Column(i)
	}

	// Convert to feature vectors
	vectors := make([]*FeatureVector, batch.NumRows())

	for i := 0; i < int(batch.NumRows()); i++ {
		// Get entity ID
		entityID, err := getStringValue(columns[entityIDIdx], i)
		if err != nil {
			return nil, fmt.Errorf("error getting entity_id: %w", err)
		}

		// Get timestamp if available
		var timestamp time.Time
		if timestampIdx >= 0 {
			ts, err := getTimestampValue(columns[timestampIdx], i)
			if err != nil {
				return nil, fmt.Errorf("error getting timestamp: %w", err)
			}
			timestamp = ts
		} else {
			timestamp = time.Now()
		}

		// Create feature vector with values
		vector := &FeatureVector{
			EntityID:  entityID,
			Timestamp: timestamp,
			Values:    make(map[string]interface{}),
		}

		// Extract feature values
		for j, field := range schema.Fields() {
			if j == entityIDIdx || j == timestampIdx {
				continue
			}

			val, err := getColumnValue(columns[j], i)
			if err != nil {
				return nil, fmt.Errorf("error getting value for field %s: %w", field.Name, err)
			}

			vector.Values[field.Name] = val
		}

		vectors[i] = vector
	}

	return vectors, nil
}

// Helper function to get a string value from an Arrow array
func getStringValue(arr arrow.Array, idx int) (string, error) {
	if arr.IsNull(idx) {
		return "", nil
	}

	switch a := arr.(type) {
	case *array.String:
		return a.Value(idx), nil
	default:
		return "", fmt.Errorf("expected string array, got %T", arr)
	}
}

// Helper function to get a timestamp value from an Arrow array
func getTimestampValue(arr arrow.Array, idx int) (time.Time, error) {
	if arr.IsNull(idx) {
		return time.Time{}, nil
	}

	switch a := arr.(type) {
	case *array.Timestamp:
		ts := a.Value(idx)
		unit := a.DataType().(*arrow.TimestampType).Unit
		var nsec int64

		switch unit {
		case arrow.Second:
			nsec = int64(ts) * int64(time.Second)
		case arrow.Millisecond:
			nsec = int64(ts) * int64(time.Millisecond)
		case arrow.Microsecond:
			nsec = int64(ts) * int64(time.Microsecond)
		case arrow.Nanosecond:
			nsec = int64(ts)
		default:
			return time.Time{}, fmt.Errorf("unsupported timestamp unit: %v", unit)
		}

		return time.Unix(0, nsec), nil
	default:
		return time.Time{}, fmt.Errorf("expected timestamp array, got %T", arr)
	}
}

// Helper function to get a generic value from an Arrow array
func getColumnValue(arr arrow.Array, idx int) (interface{}, error) {
	if arr.IsNull(idx) {
		return nil, nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx), nil
	case *array.Int8:
		return a.Value(idx), nil
	case *array.Int16:
		return a.Value(idx), nil
	case *array.Int32:
		return a.Value(idx), nil
	case *array.Int64:
		return a.Value(idx), nil
	case *array.Uint8:
		return a.Value(idx), nil
	case *array.Uint16:
		return a.Value(idx), nil
	case *array.Uint32:
		return a.Value(idx), nil
	case *array.Uint64:
		return a.Value(idx), nil
	case *array.Float32:
		return a.Value(idx), nil
	case *array.Float64:
		return a.Value(idx), nil
	case *array.String:
		return a.Value(idx), nil
	case *array.Binary:
		return a.Value(idx), nil
	case *array.Timestamp:
		ts, err := getTimestampValue(arr, idx)
		if err != nil {
			return nil, err
		}
		return ts, nil
	case *array.List:
		// Get the list's value array
		values := a.ListValues()
		// Get the offsets for the list at idx
		start, end := a.ValueOffsets(idx)

		// Create a slice to hold the values
		list := make([]interface{}, end-start)

		// Extract each value in the list
		for i := start; i < end; i++ {
			val, err := getColumnValue(values, int(i-start))
			if err != nil {
				return nil, err
			}
			list[int(i-start)] = val
		}

		return list, nil
	default:
		return nil, fmt.Errorf("unsupported array type: %T", arr)
	}
}
