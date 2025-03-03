# FeatherStore Python Client

A Python client for interacting with FeatherStore, a high-performance feature store for machine learning.

## Installation

```bash
# Install from PyPI
pip install featherstore-client

# Or install from source
pip install -e .
```

## Dependencies

The client requires the following dependencies:
- pandas
- numpy
- pyarrow
- requests

## Usage

### Basic Usage

```python
from featherstore_client import FeatherStoreClient

# Connect to FeatherStore
client = FeatherStoreClient(
    http_url="http://localhost:8080",
    flight_url="grpc://localhost:8081"
)

# Check if the server is healthy
health = client.health_check()
print(health)
```

### Creating a Feature Set

```python
# Define and create a feature set
feature_set = client.create_feature_set(
    name="user_features",
    features=[
        {"name": "age", "type": "int", "description": "User age"},
        {"name": "active_days", "type": "int", "description": "Days since last activity"},
        {"name": "score", "type": "float", "description": "User score"},
        {"name": "is_premium", "type": "bool", "description": "Premium status"}
    ],
    description="User features for recommendation model"
)
```

### Ingesting Features

```python
import pandas as pd

# Create a DataFrame with features
df = pd.DataFrame({
    "entity_id": ["user_1", "user_2", "user_3"],
    "age": [25, 34, 45],
    "active_days": [5, 12, 0],
    "score": [0.85, 0.92, 0.75],
    "is_premium": [False, True, False],
    "timestamp": pd.Timestamp.now()
})

# Ingest the features
client.ingest_batch("user_features", df)
```

### Retrieving Features

```python
# Get features for specific users
features = client.get_features("user_features", ["user_1", "user_2"])
print(features)

# Get feature history for a specific user
history = client.get_feature_history(
    "user_features",
    "user_1",
    start_time="2023-01-01T00:00:00Z",
    end_time="2023-12-31T23:59:59Z"
)
print(history)
```

### Managing Feature Sets

```python
# List all feature sets
feature_sets = client.list_feature_sets()
print(feature_sets)

# Get details of a specific feature set
feature_set = client.get_feature_set("user_features")
print(feature_set)

# Delete a feature set
client.delete_feature_set("user_features")
```

## Integration with ML Frameworks

### PyTorch

```python
import torch
from torch.utils.data import Dataset, DataLoader

class FeatureDataset(Dataset):
    def __init__(self, client, feature_set_name, entity_ids):
        self.features = client.get_features(feature_set_name, entity_ids)
        
    def __len__(self):
        return len(self.features)
        
    def __getitem__(self, idx):
        row = self.features.iloc[idx]
        # Convert feature values to tensors
        x = torch.tensor([
            row['age'],
            row['active_days'],
            row['score'],
            1.0 if row['is_premium'] else 0.0
        ], dtype=torch.float32)
        return x

# Create a PyTorch DataLoader
dataset = FeatureDataset(client, "user_features", ["user_1", "user_2", "user_3"])
dataloader = DataLoader(dataset, batch_size=2, shuffle=True)
```

### TensorFlow

```python
import tensorflow as tf

# Get features as a pandas DataFrame
features_df = client.get_features("user_features", ["user_1", "user_2", "user_3"])

# Convert to TensorFlow Dataset
feature_columns = ['age', 'active_days', 'score', 'is_premium']
features = features_df[feature_columns].values
dataset = tf.data.Dataset.from_tensor_slices(features)
dataset = dataset.batch(2)
```

## Error Handling

The client raises exceptions with descriptive error messages when API requests fail:

```python
try:
    # Try to get a non-existent feature set
    client.get_feature_set("nonexistent_set")
except Exception as e:
    print(f"Error: {e}")
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 