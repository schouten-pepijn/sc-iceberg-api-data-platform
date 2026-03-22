"""Transform Bronze observed weather rows into canonical Silver actuals."""

import pandas as pd
from pyiceberg.catalog import load_catalog

from catalog.location import load_location
from pipelines.validation.ingest_observed_weather import validate


def load_bronze_observed_weather() -> pd.DataFrame:
    """Load all rows from the Bronze observed weather table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_observed_weather")
    return table.scan().to_pandas()


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    """Build the latest observed weather rows for a single location."""
    location = load_location(location_name=location_name)
    bronze_df = load_bronze_observed_weather()
    validation_result = validate(bronze_df)

    if not validation_result["success"]:
        raise ValueError(f"Validation failed: {validation_result['errors']}")

    df = validation_result["validated_df"].copy()
    df = df[df["location_id"] == location["location_id"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["_ingest_ts"] = pd.to_datetime(df["_ingest_ts"], utc=True)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "location_id",
                "timestamp",
                "day",
                "temperature",
                "precipitation",
                "wind_speed_10m",
            ]
        )

    # Bronze observed weather is append-only, so keep the newest record per timestamp.
    silver_df = (
        df.sort_values(["location_id", "timestamp", "_ingest_ts"])
        .drop_duplicates(subset=["location_id", "timestamp"], keep="last")
        .copy()
    )

    silver_df["day"] = silver_df["timestamp"].dt.date

    silver_df = silver_df[
        [
            "location_id",
            "timestamp",
            "day",
            "temperature",
            "precipitation",
            "wind_speed_10m",
        ]
    ].sort_values(["location_id", "timestamp"])

    return silver_df


if __name__ == "__main__":
    result_df = run()
    print(result_df.head())
