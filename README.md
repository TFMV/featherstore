# FeatherStore

A high-performance, real-time ML feature store optimized for low-latency serving and efficient storage, built using modern Go.

## Features

- **Real-time Feature Ingestion**: Accept Apache Arrow RecordBatches via Arrow Flight RPC
- **Efficient Storage**: Multiple storage backends including DuckDB and Parquet
- **Low-latency Serving**: High-throughput feature retrieval via Arrow Flight
- **ML Framework Integration**: Compatible with TensorFlow, PyTorch, and Ray
- **Observability**: Prometheus metrics, structured logging, and OpenTelemetry tracing

## Architecture

FeatherStore follows a clean architecture approach with these primary components:

- **API Layer**: HTTP/REST API and Arrow Flight RPC for feature ingestion and retrieval
- **Core Layer**: Core domain logic for feature handling, validation, and transformation
- **Storage Layer**: Pluggable storage backends including:
  - **DuckDB**: Fast, in-memory SQL database for high-performance queries
  - **Parquet**: Native Parquet file storage for efficient columnar data storage
- **Metrics & Observability**: Prometheus metrics, logging, and tracing

### System Architecture Diagram

```mermaid
graph TD
    %% Client Components
    Client[Client Applications]
    PyClient[Python Client]
    
    %% Server Components
    subgraph FeatherStore["FeatherStore Server"]
        %% API Layer
        subgraph APILayer["API Layer"]
            HTTP[HTTP REST API]
            Flight[Arrow Flight RPC]
        end
        
        %% Core Layer
        subgraph CoreLayer["Core Layer"]
            FeatureLogic[Feature Processing Logic]
            SchemaManagement[Schema Management]
            Validation[Data Validation]
        end
        
        %% Storage Layer
        subgraph StorageLayer["Storage Layer"]
            StorageFactory[Storage Factory]
            
            subgraph Backends["Storage Backends"]
                DuckDB[DuckDB Storage]
                Parquet[Parquet Storage]
            end
        end
        
        %% Observability
        subgraph Observability["Observability"]
            Metrics[Prometheus Metrics]
            Logging[Structured Logging]
        end
    end
    
    %% External Systems
    Prometheus[Prometheus]
    MLFrameworks[ML Frameworks]
    
    %% Connections - Client to API
    Client -->|HTTP Requests| HTTP
    Client -->|Arrow Flight Protocol| Flight
    PyClient -->|HTTP Requests| HTTP
    PyClient -->|Arrow Flight Protocol| Flight
    
    %% Connections - API to Core
    HTTP -->|Feature Set Operations| FeatureLogic
    Flight -->|Batch Ingestion/Retrieval| FeatureLogic
    
    %% Connections - Core to Storage
    FeatureLogic -->|Schema Validation| SchemaManagement
    FeatureLogic -->|Data Validation| Validation
    FeatureLogic -->|Storage Operations| StorageFactory
    
    %% Storage Factory to Backends
    StorageFactory -->|Create/Select Backend| DuckDB
    StorageFactory -->|Create/Select Backend| Parquet
    
    %% Observability Connections
    HTTP -->|Record Metrics| Metrics
    Flight -->|Record Metrics| Metrics
    FeatureLogic -->|Log Operations| Logging
    DuckDB -->|Log Operations| Logging
    Parquet -->|Log Operations| Logging
    Metrics -->|Export Metrics| Prometheus
    
    %% External Connections
    MLFrameworks -->|Consume Features| Client
    
    %% Data Flow
    classDef apiLayer fill:#f9f,stroke:#333,stroke-width:2px
    classDef coreLayer fill:#bbf,stroke:#333,stroke-width:2px
    classDef storageLayer fill:#bfb,stroke:#333,stroke-width:2px
    classDef observability fill:#fbb,stroke:#333,stroke-width:2px
    
    class HTTP,Flight apiLayer
    class FeatureLogic,SchemaManagement,Validation coreLayer
    class StorageFactory,DuckDB,Parquet,Backends storageLayer
    class Metrics,Logging,Observability observability
```

### Data Flow Diagram

```mermaid
sequenceDiagram
    participant Client as Client Application
    participant API as API Layer (HTTP/Flight)
    participant Core as Core Layer
    participant Storage as Storage Layer
    participant Backend as Storage Backend
    
    %% Feature Set Creation
    Client->>API: Create Feature Set
    API->>Core: Validate Feature Set Schema
    Core->>Storage: Create Feature Set
    Storage->>Backend: Initialize Storage
    Backend-->>Storage: Storage Initialized
    Storage-->>Core: Feature Set Created
    Core-->>API: Success Response
    API-->>Client: Feature Set Created
    
    %% Feature Ingestion
    Client->>API: Ingest Feature Batch (Arrow Record)
    API->>Core: Process Feature Batch
    Core->>Core: Validate Schema Compatibility
    Core->>Storage: Store Feature Batch
    Storage->>Backend: Write Data
    Backend-->>Storage: Write Confirmed
    Storage-->>Core: Ingestion Complete
    Core-->>API: Success Response
    API-->>Client: Batch Ingested
    
    %% Feature Retrieval
    Client->>API: Get Features (Entity IDs)
    API->>Core: Process Feature Request
    Core->>Storage: Retrieve Features
    Storage->>Backend: Read Data
    Backend-->>Storage: Return Arrow Record
    Storage-->>Core: Features Retrieved
    Core-->>API: Arrow Record Batch
    API-->>Client: Feature Data (Arrow Record)
```

## Storage Options

FeatherStore supports multiple storage backends, each with its own strengths:

### DuckDB Storage

DuckDB provides an in-memory SQL database that excels at analytical queries and offers:

- Fast query performance with SQL capabilities
- Low latency for feature retrieval
- Integration with Arrow for efficient data exchange

### Parquet Storage

The Parquet storage backend provides:

- Efficient columnar storage with high compression ratios
- Excellent performance for analytical workloads
- Schema evolution capabilities
- Compatibility with big data ecosystems
- Time-based partitioning of feature data
- Support for various compression algorithms (Snappy, GZIP, ZSTD, etc.)

## Getting Started

### Prerequisites

- Go 1.24+
- Apache Arrow libraries

### Installation

```bash
go get github.com/TFMV/featherstore
```

### Configuration

FeatherStore is configured via environment variables or a configuration file:

```yaml
storage:
  # Choose your storage backend
  backend: "duckdb"  # or "parquet"
  
  duckdb:
    path: ./data/features.db
    memory_limit: 4GB
    
  parquet:
    directory: ./data/features
    row_group_size: 8192
    compression: "snappy"  # Options: snappy, gzip, zstd, brotli, lz4
    
server:
  http_port: 8080
  flight_port: 8081
  
metrics:
  prometheus_port: 9090
```

## Usage Examples

### Ingesting Features

```go
// Go client example
client := featherstore.NewClient("localhost:8081")
batch := arrow.NewRecordBatch(...)
err := client.IngestFeatures(ctx, "user_features", batch)
```

### Retrieving Features

```go
// Go client example
client := featherstore.NewClient("localhost:8081")
features, err := client.GetFeatures(ctx, "user_features", []string{"user_123"})
```

### Using Historical Features

```go
// Go client example
client := featherstore.NewClient("localhost:8081")
startTime := time.Now().Add(-24 * time.Hour) // 1 day ago
endTime := time.Now()
features, err := client.GetFeatureHistory(ctx, "user_features", "user_123", startTime, endTime)
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
