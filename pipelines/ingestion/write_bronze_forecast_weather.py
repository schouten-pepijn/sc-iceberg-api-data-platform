import pyarrow as pa
from pyiceberg.catalog import load_catalog

from pipelines.ingestion.ingest_forecast_weather import run as ingest_forecast_weather


def to_arrow_table(df):
    """Convert a pandas weather frame to Arrow for Iceberg writes."""
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("target_timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field(
                "forecast_generated_at", pa.timestamp("us", tz="UTC"), nullable=True
            ),
            pa.field("temperature", pa.float64(), nullable=True),
            pa.field("precipitation", pa.float64(), nullable=True),
            pa.field("wind_speed_10m", pa.float64(), nullable=True),
            pa.field("_ingest_ts", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("_source_api", pa.string(), nullable=True),
            pa.field("_batch_id", pa.string(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def append_to_bronze_forecast_weather(arrow_table) -> None:
    """Append Arrow rows to the Bronze forecast weather table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_forecast_weather")
    table.append(arrow_table)


def run(location_name: str = "Amsterdam") -> None:
    """Execute Bronze forecast weather ingestion and append it to Iceberg."""
    df = ingest_forecast_weather(location_name=location_name)
    arrow_table = to_arrow_table(df)
    append_to_bronze_forecast_weather(arrow_table)
    print(f"Appended {len(df)} records to lakehouse.bronze_forecast_weather")


if __name__ == "__main__":
    run()
