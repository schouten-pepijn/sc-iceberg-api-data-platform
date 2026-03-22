"""Bootstrap Iceberg namespace and table contracts for all layers."""

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    StringType,
    DateType,
    LongType,
    NestedField,
    TimestamptzType,
)

catalog = load_catalog("local")

namespace = "lakehouse"

bronze_weather_feed_schema = Schema(
    NestedField(1, "timestamp", TimestamptzType(), required=False),
    NestedField(2, "temperature", DoubleType(), required=False),
    NestedField(3, "precipitation", DoubleType(), required=False),
    NestedField(4, "wind_speed_10m", DoubleType(), required=False),
    NestedField(5, "_ingest_ts", TimestamptzType(), required=False),
    NestedField(6, "_source_api", StringType(), required=False),
    NestedField(7, "_batch_id", StringType(), required=False),
    NestedField(8, "location_id", StringType(), required=False),
)

silver_weather_feed_hourly_schema = Schema(
    NestedField(1, "timestamp", TimestamptzType(), required=False),
    NestedField(2, "day", DateType(), required=False),
    NestedField(3, "temperature_c", DoubleType(), required=False),
    NestedField(4, "temperature_f", DoubleType(), required=False),
    NestedField(5, "precipitation_mm", DoubleType(), required=False),
    NestedField(6, "wind_speed_10m_max", DoubleType(), required=False),
    NestedField(7, "location_id", StringType(), required=False),
)

gold_fact_weather_schema = Schema(
    NestedField(1, "day", DateType(), required=False),
    NestedField(2, "avg_temperature_c", DoubleType(), required=False),
    NestedField(3, "avg_temperature_f", DoubleType(), required=False),
    NestedField(4, "total_precipitation_mm", DoubleType(), required=False),
    NestedField(5, "max_wind_speed_10m", DoubleType(), required=False),
    NestedField(6, "hour_count", LongType(), required=False),
    NestedField(7, "location_id", StringType(), required=False),
)

bronze_forecast_weather_schema = Schema(
    NestedField(1, "location_id", StringType(), required=False),
    NestedField(2, "target_timestamp", TimestamptzType(), required=False),
    NestedField(3, "forecast_generated_at", TimestamptzType(), required=False),
    NestedField(4, "temperature", DoubleType(), required=False),
    NestedField(5, "precipitation", DoubleType(), required=False),
    NestedField(6, "wind_speed_10m", DoubleType(), required=False),
    NestedField(7, "_ingest_ts", TimestamptzType(), required=False),
    NestedField(8, "_source_api", StringType(), required=False),
    NestedField(9, "_batch_id", StringType(), required=False),
)

bronze_observed_weather_schema = Schema(
    NestedField(1, "location_id", StringType(), required=False),
    NestedField(2, "timestamp", TimestamptzType(), required=False),
    NestedField(3, "temperature", DoubleType(), required=False),
    NestedField(4, "precipitation", DoubleType(), required=False),
    NestedField(5, "wind_speed_10m", DoubleType(), required=False),
    NestedField(6, "_ingest_ts", TimestamptzType(), required=False),
    NestedField(7, "_source_api", StringType(), required=False),
    NestedField(8, "_batch_id", StringType(), required=False),
)

silver_observed_weather_schema = Schema(
    NestedField(1, "location_id", StringType(), required=False),
    NestedField(2, "timestamp", TimestamptzType(), required=False),
    NestedField(3, "day", DateType(), required=False),
    NestedField(4, "temperature", DoubleType(), required=False),
    NestedField(5, "precipitation", DoubleType(), required=False),
    NestedField(6, "wind_speed_10m", DoubleType(), required=False),
)

silver_forecast_latest_schema = Schema(
    NestedField(1, "location_id", StringType(), required=False),
    NestedField(2, "target_timestamp", TimestamptzType(), required=False),
    NestedField(3, "forecast_generated_at", TimestamptzType(), required=False),
    NestedField(4, "temperature", DoubleType(), required=False),
    NestedField(5, "precipitation", DoubleType(), required=False),
    NestedField(6, "wind_speed_10m", DoubleType(), required=False),
    NestedField(7, "day", DateType(), required=False),
)

dim_location_schema = Schema(
    NestedField(1, "location_id", StringType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "latitude", DoubleType(), required=False),
    NestedField(4, "longitude", DoubleType(), required=False),
    NestedField(5, "elevation", DoubleType(), required=False),
    NestedField(6, "timezone", StringType(), required=False),
    NestedField(7, "country_code", StringType(), required=False),
    NestedField(8, "country", StringType(), required=False),
    NestedField(9, "admin1", StringType(), required=False),
    NestedField(10, "_ingest_ts", TimestamptzType(), required=False),
    NestedField(11, "_source_api", StringType(), required=False),
    NestedField(12, "_batch_id", StringType(), required=False),
)

pipeline_state_schema = Schema(
    NestedField(1, "pipeline_name", StringType(), required=False),
    NestedField(2, "location_id", StringType(), required=False),
    NestedField(3, "last_bronze_ingest_ts", TimestamptzType(), required=False),
    NestedField(4, "last_silver_processed_day", DateType(), required=False),
    NestedField(5, "updated_at", TimestamptzType(), required=False),
)


def create_namespace_if_missing() -> None:
    """Create the target namespace once so all tables share one domain."""
    existing_namespaces = set(catalog.list_namespaces())
    if (namespace,) not in existing_namespaces:
        catalog.create_namespace(namespace)


def create_table_if_missing(identifier: str, schema: Schema) -> None:
    """Create a table only when it does not already exist."""
    existing_tables = set(catalog.list_tables(namespace))
    table_name = identifier.split(".")[-1]
    if (namespace, table_name) not in existing_tables:

        catalog.create_table(identifier=identifier, schema=schema)


def run() -> None:
    """Create all required Iceberg tables for Bronze, Silver, Gold, and state."""
    create_namespace_if_missing()

    create_table_if_missing("lakehouse.pipeline_state", pipeline_state_schema)

    create_table_if_missing("lakehouse.dim_location", dim_location_schema)

    create_table_if_missing("lakehouse.bronze_weather_feed", bronze_weather_feed_schema)
    create_table_if_missing(
        "lakehouse.bronze_forecast_weather", bronze_forecast_weather_schema
    )
    create_table_if_missing(
        "lakehouse.bronze_observed_weather", bronze_observed_weather_schema
    )
    create_table_if_missing(
        "lakehouse.silver_weather_feed_hourly", silver_weather_feed_hourly_schema
    )
    create_table_if_missing(
        "lakehouse.silver_forecast_latest", silver_forecast_latest_schema
    )
    create_table_if_missing(
        "lakehouse.silver_observed_weather", silver_observed_weather_schema
    )
    create_table_if_missing("lakehouse.fact_weather", gold_fact_weather_schema)


if __name__ == "__main__":
    run()
