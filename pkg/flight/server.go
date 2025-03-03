package flight

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/TFMV/featherstore/pkg/config"
	"github.com/TFMV/featherstore/pkg/core"
	"github.com/TFMV/featherstore/pkg/logger"
	"github.com/TFMV/featherstore/pkg/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	flight "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
)

// ActionType defines the supported Arrow Flight actions
type ActionType string

const (
	// CreateFeatureSetAction creates a new feature set
	CreateFeatureSetAction ActionType = "CreateFeatureSet"
	// GetFeatureSetAction retrieves a feature set definition
	GetFeatureSetAction ActionType = "GetFeatureSet"
	// ListFeatureSetsAction lists all feature sets
	ListFeatureSetsAction ActionType = "ListFeatureSets"
	// DeleteFeatureSetAction deletes a feature set
	DeleteFeatureSetAction ActionType = "DeleteFeatureSet"
)

// Server implements the Arrow Flight server for feature ingestion and retrieval
type Server struct {
	flight.UnimplementedFlightServiceServer
	config    *config.ServerConfig
	log       *logger.Logger
	metrics   *metrics.Metrics
	store     core.FeatureStore
	allocator memory.Allocator
	server    *grpc.Server
	listener  net.Listener
}

// NewServer creates a new Arrow Flight server
func NewServer(
	cfg *config.ServerConfig,
	log *logger.Logger,
	metrics *metrics.Metrics,
	store core.FeatureStore,
) (*Server, error) {
	allocator := memory.NewGoAllocator()

	server := &Server{
		config:    cfg,
		log:       log,
		metrics:   metrics,
		store:     store,
		allocator: allocator,
	}

	return server, nil
}

// Start starts the Arrow Flight server
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.config.FlightPort)

	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(int(s.config.MaxRequestSize)),
		grpc.MaxSendMsgSize(int(s.config.MaxRequestSize)),
		grpc.Creds(insecure.NewCredentials()),
	)

	flight.RegisterFlightServiceServer(s.server, s)

	s.log.Info("Starting Arrow Flight server", zap.String("address", addr))

	go func() {
		if err := s.server.Serve(s.listener); err != nil {
			s.log.Error("Arrow Flight server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop stops the Arrow Flight server gracefully
func (s *Server) Stop() error {
	if s.server != nil {
		s.server.GracefulStop()
		s.log.Info("Arrow Flight server stopped")
	}

	return nil
}

// ListFlights returns a list of available data streams (feature sets)
func (s *Server) ListFlights(
	criteria *flight.Criteria,
	server flight.FlightService_ListFlightsServer,
) error {
	start := time.Now()
	defer func() {
		s.metrics.ObserveFlightRequest("ListFlights", "success", time.Since(start))
	}()

	ctx := server.Context()
	featureSets, err := s.store.ListFeatureSets(ctx)
	if err != nil {
		s.metrics.ObserveFlightRequest("ListFlights", "error", time.Since(start))
		return err
	}

	for _, fs := range featureSets {
		descriptor := &flight.FlightDescriptor{
			Type: flight.FlightDescriptor_PATH,
			Path: []string{fs.Name},
		}

		info, err := s.getFlightInfoForFeatureSet(ctx, descriptor, fs)
		if err != nil {
			s.log.Warn("Error creating flight info for feature set",
				zap.String("feature_set", fs.Name),
				zap.Error(err),
			)
			continue
		}

		if err := server.Send(info); err != nil {
			return err
		}
	}

	return nil
}

// GetFlightInfo returns information about a specific data stream (feature set)
func (s *Server) GetFlightInfo(
	ctx context.Context,
	request *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	start := time.Now()
	defer func() {
		s.metrics.ObserveFlightRequest("GetFlightInfo", "success", time.Since(start))
	}()

	if request.Type != flight.FlightDescriptor_PATH || len(request.Path) == 0 {
		s.metrics.ObserveFlightRequest("GetFlightInfo", "error", time.Since(start))
		return nil, fmt.Errorf("invalid flight descriptor")
	}

	featureSetName := request.Path[0]

	featureSet, err := s.store.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		s.metrics.ObserveFlightRequest("GetFlightInfo", "error", time.Since(start))
		return nil, err
	}

	return s.getFlightInfoForFeatureSet(ctx, request, featureSet)
}

// getFlightInfoForFeatureSet creates FlightInfo for a feature set
func (s *Server) getFlightInfoForFeatureSet(
	ctx context.Context,
	descriptor *flight.FlightDescriptor,
	featureSet *core.FeatureSet,
) (*flight.FlightInfo, error) {
	// Create flight endpoint
	endpoint := &flight.FlightEndpoint{
		Ticket: &flight.Ticket{
			Ticket: []byte(featureSet.Name),
		},
		Location: []*flight.Location{
			{Uri: fmt.Sprintf("grpc://localhost:%d", s.config.FlightPort)},
		},
	}

	// Serialize the feature set schema
	schemaBytes, err := json.Marshal(featureSet)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal feature set: %w", err)
	}

	// Create flight info
	info := &flight.FlightInfo{
		Schema:           schemaBytes,
		FlightDescriptor: descriptor,
		Endpoint:         []*flight.FlightEndpoint{endpoint},
		// Note: TotalRecords and TotalBytes are set to -1 as we don't know the exact count
		TotalRecords: -1,
		TotalBytes:   -1,
	}

	return info, nil
}

// GetSchema returns the schema for a specific data stream (feature set)
func (s *Server) GetSchema(
	ctx context.Context,
	request *flight.FlightDescriptor,
) (*flight.SchemaResult, error) {
	start := time.Now()
	defer func() {
		s.metrics.ObserveFlightRequest("GetSchema", "success", time.Since(start))
	}()

	if request.Type != flight.FlightDescriptor_PATH || len(request.Path) == 0 {
		s.metrics.ObserveFlightRequest("GetSchema", "error", time.Since(start))
		return nil, fmt.Errorf("invalid flight descriptor")
	}

	featureSetName := request.Path[0]

	featureSet, err := s.store.GetFeatureSet(ctx, featureSetName)
	if err != nil {
		s.metrics.ObserveFlightRequest("GetSchema", "error", time.Since(start))
		return nil, err
	}

	// Serialize the schema
	payload := ipc.GetSchemaPayload(featureSet.Schema, s.allocator)
	schemaBytes := payload.Meta().Bytes()

	return &flight.SchemaResult{Schema: schemaBytes}, nil
}

// DoGet handles requests to retrieve feature data
func (s *Server) DoGet(
	request *flight.Ticket,
	server flight.FlightService_DoGetServer,
) error {
	ctx := server.Context()
	start := time.Now()

	// Parse the ticket to determine what feature set and entities to retrieve
	var ticketData struct {
		FeatureSet string   `json:"feature_set"`
		EntityIDs  []string `json:"entity_ids,omitempty"`
		EntityID   string   `json:"entity_id,omitempty"`
		StartTime  string   `json:"start_time,omitempty"`
		EndTime    string   `json:"end_time,omitempty"`
	}

	if err := json.Unmarshal(request.Ticket, &ticketData); err != nil {
		s.metrics.ObserveFlightRequest("DoGet", "error", time.Since(start))
		return fmt.Errorf("invalid ticket: %w", err)
	}

	var batch arrow.Record
	var err error

	// Determine the type of retrieval based on the ticket data
	if ticketData.EntityID != "" && ticketData.StartTime != "" && ticketData.EndTime != "" {
		// Get feature history for a single entity
		startTime, parseErr := time.Parse(time.RFC3339, ticketData.StartTime)
		if parseErr != nil {
			s.metrics.ObserveFlightRequest("DoGet", "error", time.Since(start))
			return fmt.Errorf("invalid start time: %w", parseErr)
		}

		endTime, parseErr := time.Parse(time.RFC3339, ticketData.EndTime)
		if parseErr != nil {
			s.metrics.ObserveFlightRequest("DoGet", "error", time.Since(start))
			return fmt.Errorf("invalid end time: %w", parseErr)
		}

		batch, err = s.store.GetFeatureHistory(ctx, ticketData.FeatureSet, ticketData.EntityID, startTime, endTime)
	} else if len(ticketData.EntityIDs) > 0 {
		// Get features for multiple entities
		batch, err = s.store.GetFeatures(ctx, ticketData.FeatureSet, ticketData.EntityIDs)
	} else {
		s.metrics.ObserveFlightRequest("DoGet", "error", time.Since(start))
		return fmt.Errorf("invalid ticket data: must specify either entity_ids or entity_id with time range")
	}

	if err != nil {
		s.metrics.ObserveFlightRequest("DoGet", "error", time.Since(start))
		return err
	}

	// Serialize the schema
	payload := ipc.GetSchemaPayload(batch.Schema(), s.allocator)
	schemaBytes := payload.Meta().Bytes()

	// Send the schema
	if err := server.Send(&flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.FlightDescriptor_PATH,
			Path: []string{ticketData.FeatureSet},
		},
		DataHeader: schemaBytes,
	}); err != nil {
		s.metrics.ObserveFlightRequest("DoGet", "error", time.Since(start))
		return fmt.Errorf("failed to send schema: %w", err)
	}

	// Create a record batch writer
	writer := ipc.NewWriter(
		&flightDataWriter{server: server},
		ipc.WithSchema(batch.Schema()),
		ipc.WithAllocator(s.allocator),
	)
	defer writer.Close()

	// Write the batch
	if err := writer.Write(batch); err != nil {
		s.metrics.ObserveFlightRequest("DoGet", "error", time.Since(start))
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	duration := time.Since(start)
	s.metrics.ObserveFlightRequest("DoGet", "success", duration)
	s.metrics.ObserveFeatureRetrieval(
		ticketData.FeatureSet,
		int(batch.NumRows()),
		estimateBatchSize(batch),
		duration,
	)

	return nil
}

// flightDataWriter implements the io.Writer interface for sending FlightData
type flightDataWriter struct {
	server flight.FlightService_DoGetServer
}

func (w *flightDataWriter) Write(p []byte) (int, error) {
	if err := w.server.Send(&flight.FlightData{
		DataBody: p,
	}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// DoPut handles feature ingestion requests
func (s *Server) DoPut(server flight.FlightService_DoPutServer) error {
	ctx := server.Context()
	start := time.Now()

	// Get the first message which should contain the schema
	data, err := server.Recv()
	if err != nil {
		s.metrics.ObserveFlightRequest("DoPut", "error", time.Since(start))
		return fmt.Errorf("failed to receive schema: %w", err)
	}

	// Get the feature set name from the descriptor
	if data.FlightDescriptor == nil || data.FlightDescriptor.Type != flight.FlightDescriptor_PATH || len(data.FlightDescriptor.Path) == 0 {
		s.metrics.ObserveFlightRequest("DoPut", "error", time.Since(start))
		return fmt.Errorf("invalid flight descriptor")
	}

	featureSetName := data.FlightDescriptor.Path[0]

	// Create a record batch reader
	reader, err := ipc.NewReader(&flightDataReader{server: server, first: data.DataBody}, ipc.WithAllocator(s.allocator))
	if err != nil {
		s.metrics.ObserveFlightRequest("DoPut", "error", time.Since(start))
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	// Read all record batches and ingest them
	var totalRows int64
	for reader.Next() {
		batch := reader.Record()

		// Ingest the batch
		if err := s.store.IngestBatch(ctx, featureSetName, batch); err != nil {
			s.metrics.ObserveFlightRequest("DoPut", "error", time.Since(start))
			return fmt.Errorf("failed to ingest batch: %w", err)
		}

		totalRows += batch.NumRows()
	}

	if reader.Err() != nil {
		s.metrics.ObserveFlightRequest("DoPut", "error", time.Since(start))
		return fmt.Errorf("error reading record batch: %w", reader.Err())
	}

	duration := time.Since(start)
	s.metrics.ObserveFlightRequest("DoPut", "success", duration)

	s.log.Info("Ingested features via Flight",
		zap.String("feature_set", featureSetName),
		zap.Int64("total_rows", totalRows),
		zap.Duration("duration", duration),
	)

	return nil
}

// flightDataReader implements the io.Reader interface for receiving FlightData
type flightDataReader struct {
	server flight.FlightService_DoPutServer
	first  []byte
	pos    int
}

func (r *flightDataReader) Read(p []byte) (int, error) {
	// First use any data we already have
	if r.first != nil && r.pos < len(r.first) {
		n := copy(p, r.first[r.pos:])
		r.pos += n
		return n, nil
	}

	// If we've used all the first data, clear it
	if r.first != nil {
		r.first = nil
		r.pos = 0
	}

	// Get the next message
	data, err := r.server.Recv()
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, err
	}

	// Copy the data
	n := copy(p, data.DataBody)
	if n < len(data.DataBody) {
		// If we couldn't copy all the data, save the rest for later
		r.first = data.DataBody
		r.pos = n
	}
	return n, nil
}

// ListActions returns a list of available actions
func (s *Server) ListActions(
	request *flight.Empty,
	server flight.FlightService_ListActionsServer,
) error {
	start := time.Now()
	defer func() {
		s.metrics.ObserveFlightRequest("ListActions", "success", time.Since(start))
	}()

	actions := []*flight.ActionType{
		{Type: string(CreateFeatureSetAction), Description: "Create a new feature set"},
		{Type: string(GetFeatureSetAction), Description: "Get a feature set by name"},
		{Type: string(ListFeatureSetsAction), Description: "List all feature sets"},
		{Type: string(DeleteFeatureSetAction), Description: "Delete a feature set"},
	}

	for _, action := range actions {
		if err := server.Send(action); err != nil {
			return err
		}
	}

	return nil
}

// DoAction handles various feature store management actions
func (s *Server) DoAction(
	request *flight.Action,
	server flight.FlightService_DoActionServer,
) error {
	ctx := server.Context()
	start := time.Now()
	actionType := ActionType(request.Type)

	var results []*flight.Result
	var err error

	switch actionType {
	case CreateFeatureSetAction:
		results, err = s.doCreateFeatureSet(ctx, request.Body)
	case GetFeatureSetAction:
		results, err = s.doGetFeatureSet(ctx, request.Body)
	case ListFeatureSetsAction:
		results, err = s.doListFeatureSets(ctx)
	case DeleteFeatureSetAction:
		results, err = s.doDeleteFeatureSet(ctx, request.Body)
	default:
		s.metrics.ObserveFlightRequest("DoAction", "error", time.Since(start))
		return fmt.Errorf("unknown action type: %s", actionType)
	}

	if err != nil {
		return err
	}

	// Send results
	for _, result := range results {
		if err := server.Send(result); err != nil {
			return err
		}
	}

	return nil
}

// doCreateFeatureSet handles creation of a new feature set
func (s *Server) doCreateFeatureSet(
	ctx context.Context,
	body []byte,
) ([]*flight.Result, error) {
	start := time.Now()

	var featureSet core.FeatureSet
	if err := json.Unmarshal(body, &featureSet); err != nil {
		s.metrics.ObserveFlightRequest("CreateFeatureSet", "error", time.Since(start))
		return nil, fmt.Errorf("invalid feature set: %w", err)
	}

	if err := s.store.CreateFeatureSet(ctx, &featureSet); err != nil {
		s.metrics.ObserveFlightRequest("CreateFeatureSet", "error", time.Since(start))
		return nil, err
	}

	result := &flight.Result{
		Body: []byte(fmt.Sprintf("Feature set '%s' created successfully", featureSet.Name)),
	}

	s.metrics.ObserveFlightRequest("CreateFeatureSet", "success", time.Since(start))

	return []*flight.Result{result}, nil
}

// doGetFeatureSet handles retrieval of a feature set definition
func (s *Server) doGetFeatureSet(
	ctx context.Context,
	body []byte,
) ([]*flight.Result, error) {
	start := time.Now()

	var request struct {
		Name string `json:"name"`
	}

	if err := json.Unmarshal(body, &request); err != nil {
		s.metrics.ObserveFlightRequest("GetFeatureSet", "error", time.Since(start))
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	featureSet, err := s.store.GetFeatureSet(ctx, request.Name)
	if err != nil {
		s.metrics.ObserveFlightRequest("GetFeatureSet", "error", time.Since(start))
		return nil, err
	}

	featureSetBytes, err := json.Marshal(featureSet)
	if err != nil {
		s.metrics.ObserveFlightRequest("GetFeatureSet", "error", time.Since(start))
		return nil, fmt.Errorf("failed to marshal feature set: %w", err)
	}

	result := &flight.Result{
		Body: featureSetBytes,
	}

	s.metrics.ObserveFlightRequest("GetFeatureSet", "success", time.Since(start))

	return []*flight.Result{result}, nil
}

// doListFeatureSets handles listing all feature sets
func (s *Server) doListFeatureSets(
	ctx context.Context,
) ([]*flight.Result, error) {
	start := time.Now()

	featureSets, err := s.store.ListFeatureSets(ctx)
	if err != nil {
		s.metrics.ObserveFlightRequest("ListFeatureSets", "error", time.Since(start))
		return nil, err
	}

	featureSetsBytes, err := json.Marshal(featureSets)
	if err != nil {
		s.metrics.ObserveFlightRequest("ListFeatureSets", "error", time.Since(start))
		return nil, fmt.Errorf("failed to marshal feature sets: %w", err)
	}

	result := &flight.Result{
		Body: featureSetsBytes,
	}

	s.metrics.ObserveFlightRequest("ListFeatureSets", "success", time.Since(start))

	return []*flight.Result{result}, nil
}

// doDeleteFeatureSet handles deletion of a feature set
func (s *Server) doDeleteFeatureSet(
	ctx context.Context,
	body []byte,
) ([]*flight.Result, error) {
	start := time.Now()

	var request struct {
		Name string `json:"name"`
	}

	if err := json.Unmarshal(body, &request); err != nil {
		s.metrics.ObserveFlightRequest("DeleteFeatureSet", "error", time.Since(start))
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if err := s.store.DeleteFeatureSet(ctx, request.Name); err != nil {
		s.metrics.ObserveFlightRequest("DeleteFeatureSet", "error", time.Since(start))
		return nil, err
	}

	result := &flight.Result{
		Body: []byte(fmt.Sprintf("Feature set '%s' deleted successfully", request.Name)),
	}

	s.metrics.ObserveFlightRequest("DeleteFeatureSet", "success", time.Since(start))

	return []*flight.Result{result}, nil
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
		// Get the data buffer length as an estimate of size
		if len(col.Data().Buffers()) > 1 && col.Data().Buffers()[1] != nil {
			size += int64(col.Data().Buffers()[1].Len())
		}
	}

	return size
}
