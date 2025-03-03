import json
from typing import Dict, List, Optional, Union, Any
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import requests
from datetime import datetime


class FeatherStoreClient:
    """Client for interacting with the FeatherStore feature store."""

    def __init__(
        self,
        http_url: str = "http://localhost:8080",
        flight_url: str = "grpc://localhost:8081",
    ):
        """Initialize FeatherStore client.

        Args:
            http_url: URL for the HTTP API
            flight_url: URL for the Arrow Flight API
        """
        self.http_url = http_url
        self.flight_url = flight_url
        self._flight_client = flight.FlightClient(flight_url)

    def health_check(self) -> Dict[str, str]:
        """Check if the FeatherStore server is healthy."""
        response = requests.get(f"{self.http_url}/health")
        return response.json()

    def create_feature_set(
        self, name: str, features: List[Dict[str, Any]], description: str = ""
    ) -> Dict[str, str]:
        """Create a new feature set.

        Args:
            name: Name of the feature set
            features: List of feature definitions
            description: Optional description of the feature set

        Returns:
            Response from the server
        """
        # Create fields for the Arrow schema
        fields = [
            pa.field("entity_id", pa.string()),
            pa.field("timestamp", pa.timestamp("ns")),
        ]

        # Add feature fields
        for feature in features:
            data_type = self._get_arrow_data_type(feature["type"])
            fields.append(pa.field(feature["name"], data_type))

        # Create the schema
        schema = pa.schema(fields)

        # Create the feature set
        feature_set = {
            "name": name,
            "features": [
                {"name": f["name"], "description": f.get("description", "")}
                for f in features
            ],
            "schema": json.loads(schema.serialize().to_pybytes().decode("utf-8")),
            "description": description,
            "tags": {},
        }

        response = requests.post(
            f"{self.http_url}/api/v1/feature_sets", json=feature_set
        )

        if response.status_code != 201:
            raise Exception(f"Failed to create feature set: {response.json()}")

        return response.json()

    def get_feature_set(self, name: str) -> Dict[str, Any]:
        """Get a feature set by name.

        Args:
            name: Name of the feature set

        Returns:
            Feature set details
        """
        response = requests.get(f"{self.http_url}/api/v1/feature_sets/{name}")

        if response.status_code != 200:
            raise Exception(f"Failed to get feature set: {response.json()}")

        return response.json()

    def list_feature_sets(self) -> List[Dict[str, Any]]:
        """List all feature sets.

        Returns:
            List of feature sets
        """
        response = requests.get(f"{self.http_url}/api/v1/feature_sets")

        if response.status_code != 200:
            raise Exception(f"Failed to list feature sets: {response.json()}")

        return response.json()

    def delete_feature_set(self, name: str) -> Dict[str, str]:
        """Delete a feature set.

        Args:
            name: Name of the feature set

        Returns:
            Response from the server
        """
        response = requests.delete(f"{self.http_url}/api/v1/feature_sets/{name}")

        if response.status_code != 200:
            raise Exception(f"Failed to delete feature set: {response.json()}")

        return response.json()

    def ingest_batch(self, feature_set_name: str, df: pd.DataFrame) -> None:
        """Ingest a batch of features using Arrow Flight.

        Args:
            feature_set_name: Name of the feature set
            df: Pandas DataFrame with the features to ingest
        """
        # Ensure the DataFrame has entity_id and timestamp columns
        if "entity_id" not in df.columns:
            raise ValueError("DataFrame must have an 'entity_id' column")

        if "timestamp" not in df.columns:
            # Add current timestamp if not provided
            df["timestamp"] = pd.Timestamp.now()
        elif not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            # Convert timestamp to datetime if it's not already
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Convert DataFrame to Arrow table
        table = pa.Table.from_pandas(df)

        # Create a FlightDescriptor
        descriptor = flight.FlightDescriptor.for_path(feature_set_name)

        # Upload the batch
        writer, _ = self._flight_client.do_put(descriptor, table.schema)
        writer.write_table(table)
        writer.close()

    def get_features(
        self, feature_set_name: str, entity_ids: List[str]
    ) -> pd.DataFrame:
        """Get features for specific entities using Arrow Flight.

        Args:
            feature_set_name: Name of the feature set
            entity_ids: List of entity IDs

        Returns:
            DataFrame with the features
        """
        # Create ticket
        ticket_data = {"feature_set": feature_set_name, "entity_ids": entity_ids}

        ticket = flight.Ticket(json.dumps(ticket_data).encode())

        # Get the data
        reader = self._flight_client.do_get(ticket)

        # Read all batches and concatenate
        data = reader.read_all()

        # Convert to DataFrame
        return data.to_pandas()

    def get_feature_history(
        self,
        feature_set_name: str,
        entity_id: str,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
    ) -> pd.DataFrame:
        """Get feature history for a specific entity.

        Args:
            feature_set_name: Name of the feature set
            entity_id: Entity ID
            start_time: Start time (ISO format string or datetime)
            end_time: End time (ISO format string or datetime)

        Returns:
            DataFrame with the feature history
        """
        # Format times if provided
        start_time_str = None
        end_time_str = None

        if start_time:
            if isinstance(start_time, datetime):
                start_time_str = start_time.isoformat()
            else:
                start_time_str = start_time

        if end_time:
            if isinstance(end_time, datetime):
                end_time_str = end_time.isoformat()
            else:
                end_time_str = end_time

        # Create ticket
        ticket_data = {"feature_set": feature_set_name, "entity_id": entity_id}

        if start_time_str:
            ticket_data["start_time"] = start_time_str

        if end_time_str:
            ticket_data["end_time"] = end_time_str

        ticket = flight.Ticket(json.dumps(ticket_data).encode())

        # Get the data
        reader = self._flight_client.do_get(ticket)

        # Read all batches and concatenate
        data = reader.read_all()

        # Convert to DataFrame
        return data.to_pandas()

    def _get_arrow_data_type(self, type_name: str) -> pa.DataType:
        """Convert type name to Arrow data type.

        Args:
            type_name: Type name (int, float, bool, string, etc.)

        Returns:
            Arrow data type
        """
        type_map = {
            "int": pa.int64(),
            "integer": pa.int64(),
            "int64": pa.int64(),
            "int32": pa.int32(),
            "int16": pa.int16(),
            "int8": pa.int8(),
            "uint64": pa.uint64(),
            "uint32": pa.uint32(),
            "uint16": pa.uint16(),
            "uint8": pa.uint8(),
            "float": pa.float64(),
            "float64": pa.float64(),
            "double": pa.float64(),
            "float32": pa.float32(),
            "bool": pa.bool_(),
            "boolean": pa.bool_(),
            "string": pa.string(),
            "str": pa.string(),
            "binary": pa.binary(),
            "timestamp": pa.timestamp("ns"),
            "date": pa.date32(),
            "time": pa.time64("ns"),
        }

        if type_name.lower() in type_map:
            return type_map[type_name.lower()]

        raise ValueError(f"Unsupported data type: {type_name}")


# Example usage
if __name__ == "__main__":
    # Create client
    client = FeatherStoreClient()

    # Check if server is healthy
    health = client.health_check()
    print("Server health:", health)

    # Create a feature set
    try:
        feature_set = client.create_feature_set(
            name="user_features",
            features=[
                {"name": "age", "type": "int", "description": "User age"},
                {
                    "name": "active_days",
                    "type": "int",
                    "description": "Days since last activity",
                },
                {"name": "score", "type": "float", "description": "User score"},
                {"name": "is_premium", "type": "bool", "description": "Premium status"},
            ],
            description="User features for recommendation model",
        )
        print("Created feature set:", feature_set)
    except Exception as e:
        print("Feature set might already exist:", e)

    # List feature sets
    feature_sets = client.list_feature_sets()
    print("Feature sets:", feature_sets)

    # Ingest some features
    df = pd.DataFrame(
        {
            "entity_id": ["user_1", "user_2", "user_3"],
            "age": [25, 34, 45],
            "active_days": [5, 12, 0],
            "score": [0.85, 0.92, 0.75],
            "is_premium": [False, True, False],
            "timestamp": pd.Timestamp.now(),
        }
    )

    client.ingest_batch("user_features", df)
    print("Ingested batch")

    # Get features
    result = client.get_features("user_features", ["user_1", "user_2"])
    print("Retrieved features:")
    print(result)

    # Add historical features for user_1
    historical_df = pd.DataFrame(
        {
            "entity_id": ["user_1", "user_1", "user_1"],
            "age": [24, 24, 25],
            "active_days": [30, 15, 5],
            "score": [0.70, 0.78, 0.85],
            "is_premium": [False, False, False],
            "timestamp": [
                pd.Timestamp.now() - pd.Timedelta(days=60),
                pd.Timestamp.now() - pd.Timedelta(days=30),
                pd.Timestamp.now(),
            ],
        }
    )

    client.ingest_batch("user_features", historical_df)
    print("Ingested historical batch")

    # Get feature history
    history = client.get_feature_history(
        "user_features",
        "user_1",
        start_time=(pd.Timestamp.now() - pd.Timedelta(days=90)).isoformat(),
        end_time=pd.Timestamp.now().isoformat(),
    )
    print("Feature history:")
    print(history)
