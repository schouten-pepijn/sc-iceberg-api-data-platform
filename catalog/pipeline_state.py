from datetime import date
from typing import Any

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import DateType, NestedField, StringType, TimestamptzType

TABLE_NAME = "lakehouse.pipeline_state"

STATE_SCHEMA = Schema(
    NestedField(1, "pipeline_name", StringType(), required=False),
    NestedField(2, "location_id", StringType(), required=False),
    NestedField(3, "last_bronze_ingest_ts", TimestamptzType(), required=False),
    NestedField(4, "last_silver_processed_day", DateType(), required=False),
    NestedField(5, "updated_at", TimestamptzType(), required=False),
)


def _empty_pipeline_state_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "pipeline_name",
            "location_id",
            "last_bronze_ingest_ts",
            "last_silver_processed_day",
            "updated_at",
        ]
    )


def _load_pipeline_state_table() -> pd.DataFrame:
    catalog = load_catalog("local")
    try:
        table = catalog.load_table(TABLE_NAME)
        return table.scan().to_pandas()
    except NoSuchTableError:
        return _empty_pipeline_state_df()


def _load_or_create_pipeline_state_table():
    catalog = load_catalog("local")
    try:
        return catalog.load_table(TABLE_NAME)
    except NoSuchTableError:
        return catalog.create_table(identifier=TABLE_NAME, schema=STATE_SCHEMA)


def load_pipeline_state(
    pipeline_name: str,
    location_id: str,
) -> dict[str, Any] | None:
    df = _load_pipeline_state_table()

    if df.empty:
        return None

    matches = df[
        (df["pipeline_name"] == pipeline_name) & (df["location_id"] == location_id)
    ]

    if matches.empty:
        return None

    # State is append-only, so the latest update is the active watermark.
    latest_record = matches.sort_values("updated_at", ascending=False).iloc[0]

    return {
        "pipeline_name": latest_record["pipeline_name"],
        "location_id": latest_record["location_id"],
        "last_bronze_ingest_ts": latest_record["last_bronze_ingest_ts"],
        "last_silver_processed_day": latest_record["last_silver_processed_day"],
        "updated_at": latest_record["updated_at"],
    }


def write_pipeline_state(
    pipeline_name: str,
    location_id: str,
    last_bronze_ingest_ts: pd.Timestamp | None = None,
    last_silver_processed_day: date | None = None,
) -> None:
    table = _load_or_create_pipeline_state_table()

    df = pd.DataFrame(
        [
            {
                "pipeline_name": pipeline_name,
                "location_id": location_id,
                "last_bronze_ingest_ts": last_bronze_ingest_ts,
                "last_silver_processed_day": last_silver_processed_day,
                "updated_at": pd.Timestamp.now(tz="UTC"),
            }
        ]
    )

    schema = pa.schema(
        [
            pa.field("pipeline_name", pa.string(), nullable=True),
            pa.field("location_id", pa.string(), nullable=True),
            pa.field(
                "last_bronze_ingest_ts", pa.timestamp("us", tz="UTC"), nullable=True
            ),
            pa.field("last_silver_processed_day", pa.date32(), nullable=True),
            pa.field("updated_at", pa.timestamp("us", tz="UTC"), nullable=True),
        ]
    )

    arrow_table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    # Keep state history for auditability instead of updating rows in place.
    table.append(arrow_table)


if __name__ == "__main__":
    write_pipeline_state(
        pipeline_name="silver_weather_hourly",
        location_id="test-location",
        last_bronze_ingest_ts=pd.Timestamp.now(tz="UTC"),
    )
    print(load_pipeline_state("silver_weather_hourly", "test-location"))
