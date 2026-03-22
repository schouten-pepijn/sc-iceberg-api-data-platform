import pandas as pd
from pyiceberg.catalog import load_catalog

from catalog.location import load_location
from pipelines.validation.ingest_forecast_weather import validate


def load_bronze_forecast_weather() -> pd.DataFrame:
    """Load all rows from the Bronze forecast weather feed."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_forecast_weather")
    return table.scan().to_pandas()


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    """Build Silver forecast weather feed rows for a location."""
    location = load_location(location_name=location_name)
    bronze_df = load_bronze_forecast_weather()
    validation_result = validate(bronze_df)

    if not validation_result["success"]:
        raise ValueError(f"Validation failed: {validation_result['errors']}")

    df = validation_result["validated_df"].copy()
    df = df[df["location_id"] == location["location_id"]].copy()
    df["target_timestamp"] = pd.to_datetime(df["target_timestamp"], utc=True)
    df["forecast_generated_at"] = pd.to_datetime(df["forecast_generated_at"], utc=True)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "location_id",
                "target_timestamp",
                "forecast_generated_at",
                "temperature",
                "precipitation",
                "wind_speed_10m",
                "day",
            ]
        )

    silver_df = (
        df.sort_values(["location_id", "target_timestamp", "forecast_generated_at"])
        # Keep the latest forecast per location and target timestamp.
        .drop_duplicates(subset=["location_id", "target_timestamp"], keep="last").copy()
    )

    silver_df["day"] = silver_df["target_timestamp"].dt.date

    silver_df = silver_df[
        [
            "location_id",
            "target_timestamp",
            "forecast_generated_at",
            "temperature",
            "precipitation",
            "wind_speed_10m",
            "day",
        ]
    ].sort_values(["location_id", "target_timestamp"])

    return silver_df


if __name__ == "__main__":
    result_df = run("Amsterdam")
    print(result_df.head())
