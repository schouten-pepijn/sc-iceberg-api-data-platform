"""Persist ingested raw weather rows into the Bronze table."""

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from pipelines.ingestion.ingest_weather_feed import run as ingest_weather


def to_arrow_table(df):
    """Convert a pandas weather frame to Arrow for Iceberg writes."""
    return pa.Table.from_pandas(df, preserve_index=False)


def append_to_bronze_weather(arrow_table) -> None:
    """Append Arrow rows to the Bronze weather table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_weather_feed")
    table.append(arrow_table)


def run(location_name: str = "Amsterdam") -> None:
    """Execute Bronze weather ingestion and append it to Iceberg."""
    df = ingest_weather(location_name=location_name)
    arrow_table = to_arrow_table(df)
    append_to_bronze_weather(arrow_table)
    print(f"Appended {len(df)} records to lakehouse.bronze_weather_feed")


if __name__ == "__main__":
    run()
