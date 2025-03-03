package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/core"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/marcboeker/go-duckdb"
	"go.uber.org/zap"
)

// Storage implements core.FeatureStore interface using DuckDB
type Storage struct {
	db      *sql.DB
	config  *config.DuckDBConfig
	log     *logger.Logger
	metrics *metrics.Metrics

	// Connection pool
	connPool   []*sql.Conn
	connPoolMu sync.Mutex

	// Feature set schemas
	schemas   map[string]*arrow.Schema
	schemasMu sync.RWMutex
}

// NewStorage creates a new DuckDB storage instance
func NewStorage(cfg *config.DuckDBConfig, log *logger.Logger, m *metrics.Metrics) (*Storage, error) {
	// Open DuckDB database
	connector, err := duckdb.NewConnector(fmt.Sprintf("file:%s", cfg.Path), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}

	db := sql.OpenDB(connector)

	// Set connection limits
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxConnections)

	storage := &Storage{
		db:      db,
		config:  cfg,
		log:     log,
		metrics: m,
		schemas: make(map[string]*arrow.Schema),
	}

	// Initialize connection pool
	if err := storage.initConnectionPool(cfg.MaxConnections); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize connection pool: %w", err)
	}

	return storage, nil
}

// initConnectionPool creates a pool of DuckDB connections
func (s *Storage) initConnectionPool(size int) error {
	s.connPoolMu.Lock()
	defer s.connPoolMu.Unlock()

	s.connPool = make([]*sql.Conn, size)

	for i := 0; i < size; i++ {
		conn, err := s.db.Conn(context.Background())
		if err != nil {
			return fmt.Errorf("failed to create DuckDB connection: %w", err)
		}
		s.connPool[i] = conn
	}

	return nil
}

// getConnection gets a connection from the pool
func (s *Storage) getConnection() (*sql.Conn, error) {
	s.connPoolMu.Lock()
	defer s.connPoolMu.Unlock()

	if len(s.connPool) == 0 {
		return nil, fmt.Errorf("no available connections in the pool")
	}

	// Get a connection from the pool
	conn := s.connPool[len(s.connPool)-1]
	s.connPool = s.connPool[:len(s.connPool)-1]

	// Update active connections metric
	if s.metrics != nil {
		s.metrics.SetDuckDBConnectionsActive(s.config.MaxConnections - len(s.connPool))
	}

	return conn, nil
}

// releaseConnection returns a connection to the pool
func (s *Storage) releaseConnection(conn *sql.Conn) {
	s.connPoolMu.Lock()
	defer s.connPoolMu.Unlock()

	s.connPool = append(s.connPool, conn)

	// Update active connections metric
	if s.metrics != nil {
		s.metrics.SetDuckDBConnectionsActive(s.config.MaxConnections - len(s.connPool))
	}
}

// CreateFeatureSet creates a new feature set table in DuckDB
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

	// Create table in DuckDB
	conn, err := s.getConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer s.releaseConnection(conn)

	createTableSQL := buildCreateTableSQL(featureSet.Name, featureSet.Schema)

	start := time.Now()
	_, err = conn.ExecContext(ctx, createTableSQL)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("create_table", duration)
	}

	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Store schema in memory
	s.schemasMu.Lock()
	s.schemas[featureSet.Name] = featureSet.Schema
	s.schemasMu.Unlock()

	s.log.Info("Created feature set",
		zap.String("feature_set", featureSet.Name),
		zap.Int("num_features", len(featureSet.Features)),
		zap.Duration("duration", duration),
	)

	return nil
}

// GetFeatureSet retrieves a feature set by name
func (s *Storage) GetFeatureSet(ctx context.Context, name string) (*core.FeatureSet, error) {
	conn, err := s.getConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer s.releaseConnection(conn)

	// Check if table exists
	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"

	start := time.Now()
	row := conn.QueryRowContext(ctx, query, name)
	var count int
	err = row.Scan(&count)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("check_table_exists", duration)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to check if table exists: %w", err)
	}

	if count == 0 {
		return nil, core.ErrFeatureSetNotFound
	}

	// Get schema from memory if available
	s.schemasMu.RLock()
	schema, ok := s.schemas[name]
	s.schemasMu.RUnlock()

	// If schema is not in memory, retrieve it from the database
	if !ok {
		schema, err = s.getTableSchema(ctx, conn, name)
		if err != nil {
			return nil, fmt.Errorf("failed to get table schema: %w", err)
		}

		// Store schema in memory
		s.schemasMu.Lock()
		s.schemas[name] = schema
		s.schemasMu.Unlock()
	}

	// Build features from schema
	features := make([]core.Feature, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		if field.Name == "entity_id" || field.Name == "timestamp" {
			continue
		}

		feature := core.Feature{
			Name:        field.Name,
			DataType:    field.Type,
			Description: "", // Not stored in DuckDB schema
			Tags:        make(map[string]string),
		}

		features = append(features, feature)
	}

	// Create feature set
	featureSet := &core.FeatureSet{
		Name:        name,
		Features:    features,
		Schema:      schema,
		Description: "", // Not stored in DuckDB schema
		Tags:        make(map[string]string),
		CreatedAt:   time.Time{}, // Not available from DuckDB
		UpdatedAt:   time.Time{}, // Not available from DuckDB
	}

	return featureSet, nil
}

// getTableSchema retrieves the Arrow schema for a DuckDB table
func (s *Storage) getTableSchema(ctx context.Context, conn *sql.Conn, tableName string) (*arrow.Schema, error) {
	// Get Arrow schema from table structure
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName)

	start := time.Now()
	rows, err := conn.QueryContext(ctx, query)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("get_table_schema", duration)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	// Create arrow schema from column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build arrow schema from column information
	fields := make([]arrow.Field, 0, len(columnTypes))
	for _, col := range columnTypes {
		// Convert SQL type to Arrow type
		arrowType, err := sqlTypeToArrowType(col.DatabaseTypeName())
		if err != nil {
			return nil, fmt.Errorf("failed to convert SQL type to Arrow type: %w", err)
		}

		// Add field to schema
		fields = append(fields, arrow.Field{
			Name: col.Name(),
			Type: arrowType,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

// sqlTypeToArrowType converts a SQL type name to an Arrow data type
func sqlTypeToArrowType(sqlType string) (arrow.DataType, error) {
	switch sqlType {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, nil
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nil
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case "INTEGER", "INT":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "UTINYINT":
		return arrow.PrimitiveTypes.Uint8, nil
	case "USMALLINT":
		return arrow.PrimitiveTypes.Uint16, nil
	case "UINTEGER", "UINT":
		return arrow.PrimitiveTypes.Uint32, nil
	case "UBIGINT":
		return arrow.PrimitiveTypes.Uint64, nil
	case "REAL", "FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "VARCHAR", "TEXT", "CHAR", "CHARACTER":
		return arrow.BinaryTypes.String, nil
	case "BLOB", "VARBINARY":
		return arrow.BinaryTypes.Binary, nil
	case "TIMESTAMP", "DATETIME":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "TIME":
		return arrow.FixedWidthTypes.Time32s, nil
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String, nil
	}
}

// ListFeatureSets returns all feature sets
func (s *Storage) ListFeatureSets(ctx context.Context) ([]*core.FeatureSet, error) {
	conn, err := s.getConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer s.releaseConnection(conn)

	// Query for all tables that aren't system tables
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
	`

	start := time.Now()
	rows, err := conn.QueryContext(ctx, query)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("list_tables", duration)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, name)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table names: %w", err)
	}

	// Get each feature set
	featureSets := make([]*core.FeatureSet, 0, len(tableNames))
	for _, name := range tableNames {
		featureSet, err := s.GetFeatureSet(ctx, name)
		if err != nil {
			s.log.Warn("Failed to get feature set",
				zap.String("feature_set", name),
				zap.Error(err),
			)
			continue
		}
		featureSets = append(featureSets, featureSet)
	}

	return featureSets, nil
}

// DeleteFeatureSet removes a feature set and all its data
func (s *Storage) DeleteFeatureSet(ctx context.Context, name string) error {
	// Check if feature set exists
	if _, err := s.GetFeatureSet(ctx, name); err != nil {
		return err
	}

	conn, err := s.getConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer s.releaseConnection(conn)

	// Drop the table
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)

	start := time.Now()
	_, err = conn.ExecContext(ctx, query)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("drop_table", duration)
	}

	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Remove schema from memory
	s.schemasMu.Lock()
	delete(s.schemas, name)
	s.schemasMu.Unlock()

	s.log.Info("Deleted feature set",
		zap.String("feature_set", name),
		zap.Duration("duration", duration),
	)

	return nil
}

// IngestBatch ingests a batch of features into DuckDB
func (s *Storage) IngestBatch(ctx context.Context, featureSetName string, batch arrow.Record) error {
	// Check if feature set exists and get schema
	_, err := s.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		return err
	}

	// Validate batch schema against feature set schema
	schema := batch.Schema()
	if schema == nil {
		return fmt.Errorf("batch schema is nil")
	}

	conn, err := s.getConnection()
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer s.releaseConnection(conn)

	// Append the batch to the table
	start := time.Now()

	// For larger batches, use SQL transaction with prepared statements
	if batch.NumRows() > 0 {
		// Start a transaction
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer func() {
			if err != nil {
				tx.Rollback()
			}
		}()

		// Build the insert statement with placeholders
		fields := batch.Schema().Fields()
		columns := make([]string, len(fields))
		placeholders := make([]string, len(fields))

		for i, f := range fields {
			columns[i] = f.Name
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			featureSetName,
			joinStrings(columns, ", "),
			joinStrings(placeholders, ", "),
		)

		// Prepare the statement
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()

		// Insert each row
		for i := int64(0); i < batch.NumRows(); i++ {
			// Extract values for this row
			values := make([]interface{}, len(fields))
			for j := 0; j < len(fields); j++ {
				col := batch.Column(j)
				values[j] = getValueFromArray(col, i)
			}

			// Execute the statement
			_, err = stmt.ExecContext(ctx, values...)
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", i, err)
			}
		}

		// Commit the transaction
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("insert_batch", duration)
		s.metrics.ObserveFeatureIngestion(
			featureSetName,
			int(batch.NumRows()),
			estimateBatchSize(batch),
			duration,
		)
	}

	s.log.Info("Ingested feature batch",
		zap.String("feature_set", featureSetName),
		zap.Int64("num_rows", batch.NumRows()),
		zap.Duration("duration", duration),
	)

	return nil
}

// getValueFromArray extracts a value from an Arrow array at the specified index
func getValueFromArray(arr arrow.Array, index int64) driver.Value {
	if arr.IsNull(int(index)) {
		return nil
	}

	switch arr := arr.(type) {
	case *array.Boolean:
		return arr.Value(int(index))
	case *array.Int8:
		return arr.Value(int(index))
	case *array.Int16:
		return arr.Value(int(index))
	case *array.Int32:
		return arr.Value(int(index))
	case *array.Int64:
		return arr.Value(int(index))
	case *array.Uint8:
		return arr.Value(int(index))
	case *array.Uint16:
		return arr.Value(int(index))
	case *array.Uint32:
		return arr.Value(int(index))
	case *array.Uint64:
		return arr.Value(int(index))
	case *array.Float32:
		return arr.Value(int(index))
	case *array.Float64:
		return arr.Value(int(index))
	case *array.String:
		return arr.Value(int(index))
	case *array.Binary:
		return arr.Value(int(index))
	case *array.Timestamp:
		return arr.Value(int(index)).ToTime(arr.DataType().(*arrow.TimestampType).Unit)
	case *array.Date32:
		return arr.Value(int(index)).ToTime()
	case *array.Date64:
		return arr.Value(int(index)).ToTime()
	default:
		// For unsupported types, convert to string
		return arr.ValueStr(int(index))
	}
}

// GetFeatures retrieves features for specific entities from DuckDB
func (s *Storage) GetFeatures(ctx context.Context, featureSetName string, entityIDs []string) (arrow.Record, error) {
	if len(entityIDs) == 0 {
		return nil, fmt.Errorf("no entity IDs provided")
	}

	// Check if feature set exists
	_, err := s.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		return nil, err
	}

	conn, err := s.getConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer s.releaseConnection(conn)

	// Build query with placeholders
	placeholders := make([]string, len(entityIDs))
	args := make([]interface{}, len(entityIDs))

	for i, id := range entityIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	// Query to get latest features for each entity ID
	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT *, ROW_NUMBER() OVER(PARTITION BY entity_id ORDER BY timestamp DESC) as rn
			FROM %s
			WHERE entity_id IN (%s)
		)
		SELECT * EXCLUDE(rn) FROM ranked WHERE rn = 1
	`, featureSetName, joinStrings(placeholders, ","))

	// Execute query
	start := time.Now()
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query features: %w", err)
	}
	defer rows.Close()

	// Convert SQL rows to Arrow record batch
	batch, err := convertRowsToArrowRecord(rows, s.db)
	duration := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("failed to convert to Arrow batch: %w", err)
	}

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("get_features", duration)
		s.metrics.ObserveFeatureRetrieval(
			featureSetName,
			int(batch.NumRows()),
			estimateBatchSize(batch),
			duration,
		)
	}

	s.log.Info("Retrieved features",
		zap.String("feature_set", featureSetName),
		zap.Int("num_entities", len(entityIDs)),
		zap.Int64("num_rows", batch.NumRows()),
		zap.Duration("duration", duration),
	)

	return batch, nil
}

// GetFeatureHistory retrieves historical features for a specific entity
func (s *Storage) GetFeatureHistory(
	ctx context.Context,
	featureSetName string,
	entityID string,
	startTime, endTime time.Time,
) (arrow.Record, error) {
	// Check if feature set exists
	_, err := s.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		return nil, err
	}

	conn, err := s.getConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer s.releaseConnection(conn)

	// Query to get features for entity ID within time range
	query := fmt.Sprintf(`
		SELECT * FROM %s
		WHERE entity_id = ?
		AND timestamp >= ?
		AND timestamp <= ?
		ORDER BY timestamp DESC
	`, featureSetName)

	// Execute query
	start := time.Now()
	rows, err := conn.QueryContext(ctx, query, entityID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query feature history: %w", err)
	}
	defer rows.Close()

	// Convert SQL rows to Arrow record batch
	batch, err := convertRowsToArrowRecord(rows, s.db)
	duration := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("failed to convert to Arrow batch: %w", err)
	}

	if s.metrics != nil {
		s.metrics.ObserveDuckDBQuery("get_feature_history", duration)
		s.metrics.ObserveFeatureRetrieval(
			featureSetName,
			int(batch.NumRows()),
			estimateBatchSize(batch),
			duration,
		)
	}

	s.log.Info("Retrieved feature history",
		zap.String("feature_set", featureSetName),
		zap.String("entity_id", entityID),
		zap.Time("start_time", startTime),
		zap.Time("end_time", endTime),
		zap.Int64("num_rows", batch.NumRows()),
		zap.Duration("duration", duration),
	)

	return batch, nil
}

// convertRowsToArrowRecord converts SQL rows to an Arrow record batch
func convertRowsToArrowRecord(rows *sql.Rows, db *sql.DB) (arrow.Record, error) {
	// Get column information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}

	// Create arrow schema
	fields := make([]arrow.Field, len(columnTypes))
	for i, col := range columnTypes {
		arrowType, err := sqlTypeToArrowType(col.DatabaseTypeName())
		if err != nil {
			return nil, fmt.Errorf("failed to convert SQL type to Arrow type: %w", err)
		}

		fields[i] = arrow.Field{
			Name: columnNames[i],
			Type: arrowType,
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// Create builders for each column
	builders := make([]array.Builder, len(fields))
	for i, field := range fields {
		builders[i] = createBuilderForType(field.Type, memory.NewGoAllocator())
	}

	// Read all rows into memory
	values := make([]interface{}, len(fields))
	scanArgs := make([]interface{}, len(fields))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Scan rows and append to builders
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Append values to builders
		for i, val := range values {
			appendValueToBuilder(builders[i], val)
		}
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
		defer builder.Release()
	}

	// Create record batch
	batch := array.NewRecord(schema, arrays, -1)
	return batch, nil
}

// createBuilderForType creates an appropriate builder for the given Arrow data type
func createBuilderForType(dt arrow.DataType, mem memory.Allocator) array.Builder {
	switch dt.ID() {
	case arrow.BOOL:
		return array.NewBooleanBuilder(mem)
	case arrow.INT8:
		return array.NewInt8Builder(mem)
	case arrow.INT16:
		return array.NewInt16Builder(mem)
	case arrow.INT32:
		return array.NewInt32Builder(mem)
	case arrow.INT64:
		return array.NewInt64Builder(mem)
	case arrow.UINT8:
		return array.NewUint8Builder(mem)
	case arrow.UINT16:
		return array.NewUint16Builder(mem)
	case arrow.UINT32:
		return array.NewUint32Builder(mem)
	case arrow.UINT64:
		return array.NewUint64Builder(mem)
	case arrow.FLOAT32:
		return array.NewFloat32Builder(mem)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(mem)
	case arrow.STRING:
		return array.NewStringBuilder(mem)
	case arrow.BINARY:
		return array.NewBinaryBuilder(mem, dt.(*arrow.BinaryType))
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(mem, dt.(*arrow.TimestampType))
	case arrow.DATE32:
		return array.NewDate32Builder(mem)
	case arrow.DATE64:
		return array.NewDate64Builder(mem)
	default:
		// Default to string for unsupported types
		return array.NewStringBuilder(mem)
	}
}

// appendValueToBuilder appends a value to the appropriate builder
func appendValueToBuilder(builder array.Builder, val interface{}) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		b.Append(val.(bool))
	case *array.Int8Builder:
		b.Append(int8(val.(int64)))
	case *array.Int16Builder:
		b.Append(int16(val.(int64)))
	case *array.Int32Builder:
		b.Append(int32(val.(int64)))
	case *array.Int64Builder:
		b.Append(val.(int64))
	case *array.Uint8Builder:
		b.Append(uint8(val.(uint64)))
	case *array.Uint16Builder:
		b.Append(uint16(val.(uint64)))
	case *array.Uint32Builder:
		b.Append(uint32(val.(uint64)))
	case *array.Uint64Builder:
		b.Append(val.(uint64))
	case *array.Float32Builder:
		b.Append(float32(val.(float64)))
	case *array.Float64Builder:
		b.Append(val.(float64))
	case *array.StringBuilder:
		b.Append(fmt.Sprintf("%v", val))
	case *array.BinaryBuilder:
		if byteVal, ok := val.([]byte); ok {
			b.Append(byteVal)
		} else {
			byteVal := []byte(fmt.Sprintf("%v", val))
			b.Append(byteVal)
		}
	case *array.TimestampBuilder:
		if timeVal, ok := val.(time.Time); ok {
			b.Append(arrow.Timestamp(timeVal.UnixNano()))
		}
	case *array.Date32Builder:
		if timeVal, ok := val.(time.Time); ok {
			b.Append(arrow.Date32FromTime(timeVal))
		}
	case *array.Date64Builder:
		if timeVal, ok := val.(time.Time); ok {
			b.Append(arrow.Date64FromTime(timeVal))
		}
	default:
		// Try to append as string for unsupported types
		if strBuilder, ok := builder.(*array.StringBuilder); ok {
			strBuilder.Append(fmt.Sprintf("%v", val))
		} else {
			builder.AppendNull()
		}
	}
}

// Helper function to estimate the size of a record batch in bytes
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

// Helper function to build SQL for creating a table
func buildCreateTableSQL(tableName string, schema *arrow.Schema) string {
	sql := fmt.Sprintf("CREATE TABLE %s (", tableName)

	columns := make([]string, 0, len(schema.Fields()))
	for i, field := range schema.Fields() {
		sqlType := arrowTypeToSQLType(field.Type)

		// Add column definition
		columns = append(columns, fmt.Sprintf("%s %s", field.Name, sqlType))

		// Add primary key for entity_id if it's the first column
		if field.Name == "entity_id" {
			columns[i] += " NOT NULL"
		}
	}

	sql += joinStrings(columns, ", ")

	// Add timestamp index if it exists
	hasTimestamp := false
	for _, field := range schema.Fields() {
		if field.Name == "timestamp" {
			hasTimestamp = true
			break
		}
	}

	sql += ")"

	// Add indexing statement
	if hasTimestamp {
		sql += fmt.Sprintf("; CREATE INDEX idx_%s_entity_timestamp ON %s(entity_id, timestamp DESC)",
			tableName, tableName)
	} else {
		sql += fmt.Sprintf("; CREATE INDEX idx_%s_entity ON %s(entity_id)",
			tableName, tableName)
	}

	return sql
}

// Helper function to convert Arrow data type to SQL type
func arrowTypeToSQLType(dataType arrow.DataType) string {
	switch dataType.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.UINT8:
		return "UTINYINT"
	case arrow.UINT16:
		return "USMALLINT"
	case arrow.UINT32:
		return "UINTEGER"
	case arrow.UINT64:
		return "UBIGINT"
	case arrow.FLOAT32:
		return "REAL"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "VARCHAR"
	case arrow.BINARY:
		return "BLOB"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.DATE32:
		return "DATE"
	case arrow.TIME32:
		return "TIME"
	case arrow.LIST:
		return "LIST"
	case arrow.MAP:
		return "MAP"
	case arrow.STRUCT:
		return "STRUCT"
	default:
		return "VARCHAR" // Default to varchar for unknown types
	}
}

// Helper function to check if two schemas are compatible
func schemasCompatible(schema1, schema2 *arrow.Schema) bool {
	// Get field names from both schemas
	fields1 := make(map[string]arrow.DataType)
	for _, field := range schema1.Fields() {
		fields1[field.Name] = field.Type
	}

	// Check if all fields in schema2 exist in schema1 with compatible types
	for _, field := range schema2.Fields() {
		if dataType, ok := fields1[field.Name]; ok {
			// Check if types are compatible
			if !typesCompatible(dataType, field.Type) {
				return false
			}
		} else {
			// Field in schema2 doesn't exist in schema1
			return false
		}
	}

	return true
}

// Helper function to check if two data types are compatible
func typesCompatible(type1, type2 arrow.DataType) bool {
	// For now, just check if the types are the same
	// Could be extended to handle type conversions
	return type1.ID() == type2.ID()
}

// Helper function to join strings with a separator
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}

	return result
}

// BuildSchemaFromFields creates an Arrow schema from a list of field definitions
func BuildSchemaFromFields(fields []arrow.Field) *arrow.Schema {
	return arrow.NewSchema(fields, nil)
}

// NewArrowAllocator creates a new Arrow memory allocator
func NewArrowAllocator() *memory.GoAllocator {
	return memory.NewGoAllocator()
}

// Close closes the database and releases all resources
func (s *Storage) Close() error {
	s.log.Info("Closing DuckDB storage")

	// Close all connections in the pool
	s.connPoolMu.Lock()
	for _, conn := range s.connPool {
		if err := conn.Close(); err != nil {
			s.log.Error("Error closing connection", zap.Error(err))
		}
	}
	s.connPool = nil
	s.connPoolMu.Unlock()

	// Close the database
	return s.db.Close()
}
