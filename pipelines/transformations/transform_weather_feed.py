"""Transform Bronze weather feed rows into hourly Silver aggregates."""

import pandas as pd
from pyiceberg.catalog import load_catalog

from catalog.location import load_location as _load_location
from catalog.pipeline_state import load_pipeline_state
from pipelines.validation.ingest_weather_feed import validate


def load_bronze_weather_feed() -> pd.DataFrame:
    """Load raw Bronze weather rows from Iceberg."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_weather_feed")
    return table.scan().to_pandas()


def run(location_name: str = "Amsterdam") -> tuple[pd.DataFrame, pd.Timestamp | None]:
    """Build location-scoped hourly Silver aggregates and return watermark metadata."""
    location = _load_location(location_name=location_name)
    bronze_df = load_bronze_weather_feed()
    validation_result = validate(bronze_df)

    if not validation_result["success"]:
        raise ValueError(f"Validation failed: {validation_result['errors']}")

    df = validation_result["validated_df"].copy()
    # Silver is location-partitioned in orchestration, so filter early for efficiency.
    df = df[df["location_id"] == location["location_id"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["_ingest_ts"] = pd.to_datetime(df["_ingest_ts"], utc=True)

    state = load_pipeline_state(
        pipeline_name="silver_weather_feed_hourly",
        location_id=location["location_id"],
    )
    # Only process newly ingested Bronze batches for this location.
    if state is not None and state["last_bronze_ingest_ts"] is not None:
        df = df[df["_ingest_ts"] > state["last_bronze_ingest_ts"]].copy()

    if df.empty:
        return (
            pd.DataFrame(
                columns=[
                    "location_id",
                    "timestamp",
                    "day",
                    "temperature_c",
                    "temperature_f",
                    "precipitation_mm",
                    "wind_speed_10m_max",
                ]
            ),
            None,
        )

    max_ingest_ts = df["_ingest_ts"].max()

    # Bronze is append-only, so keep the newest record per raw weather timestamp.
    latest_bronze_df = (
        df.sort_values(["location_id", "timestamp", "_ingest_ts"])
        .drop_duplicates(subset=["location_id", "timestamp"], keep="last")
        .copy()
    )

    silver_df = (
        latest_bronze_df.assign(hour_bucket=latest_bronze_df["timestamp"].dt.floor("h"))
        .groupby(["location_id", "hour_bucket"], as_index=False)
        .agg(
            temperature_c=("temperature", "mean"),
            precipitation_mm=("precipitation", "sum"),
            wind_speed_10m_max=("wind_speed_10m", "max"),
        )
        .rename(columns={"hour_bucket": "timestamp"})
    )

    silver_df["day"] = silver_df["timestamp"].dt.date
    silver_df["temperature_f"] = silver_df["temperature_c"] * 9 / 5 + 32

    silver_df = silver_df[
        [
            "location_id",
            "timestamp",
            "day",
            "temperature_c",
            "temperature_f",
            "precipitation_mm",
            "wind_speed_10m_max",
        ]
    ].sort_values(["location_id", "timestamp"])

    return silver_df, max_ingest_ts


if __name__ == "__main__":
    df, max_ingest_ts = run()
    print(df.head())
    print(df.dtypes)
    print(max_ingest_ts)
