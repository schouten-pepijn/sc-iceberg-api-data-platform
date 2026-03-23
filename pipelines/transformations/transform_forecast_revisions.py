"""Transform Bronze forecast snapshots into revision-history Silver rows."""

import pandas as pd
from pyiceberg.catalog import load_catalog

from catalog.location import load_location
from pipelines.validation.ingest_forecast_weather import validate



def load_bronze_forecast_weather() -> pd.DataFrame:
    """Load all rows from the Bronze forecast weather table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_forecast_weather")
    return table.scan().to_pandas()



def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    """Build revision-history forecast rows for a single location."""
    location = load_location(location_name=location_name)
    bronze_df = load_bronze_forecast_weather()
    validation_result = validate(bronze_df)

    if not validation_result["success"]:
        raise ValueError(f"Validation failed: {validation_result['errors']}")

    df = validation_result["validated_df"].copy()
    df = df[df["location_id"] == location["location_id"]].copy()
    df["target_timestamp"] = pd.to_datetime(df["target_timestamp"], utc=True)
    df["forecast_generated_at"] = pd.to_datetime(
        df["forecast_generated_at"], utc=True
    )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "location_id",
                "target_timestamp",
                "forecast_generated_at",
                "revision_number",
                "previous_forecast_generated_at",
                "temperature",
                "previous_temperature",
                "temperature_delta_from_previous",
                "precipitation",
                "previous_precipitation",
                "precipitation_delta_from_previous",
                "wind_speed_10m",
                "previous_wind_speed_10m",
                "wind_speed_delta_from_previous",
                "day",
            ]
        )

    revisions_df = df.sort_values(
        ["location_id", "target_timestamp", "forecast_generated_at"]
    ).copy()

    group_keys = ["location_id", "target_timestamp"]
    revisions_df["revision_number"] = revisions_df.groupby(group_keys).cumcount() + 1
    revisions_df["previous_forecast_generated_at"] = revisions_df.groupby(group_keys)[
        "forecast_generated_at"
    ].shift(1)
    revisions_df["previous_temperature"] = revisions_df.groupby(group_keys)[
        "temperature"
    ].shift(1)
    revisions_df["previous_precipitation"] = revisions_df.groupby(group_keys)[
        "precipitation"
    ].shift(1)
    revisions_df["previous_wind_speed_10m"] = revisions_df.groupby(group_keys)[
        "wind_speed_10m"
    ].shift(1)

    revisions_df["temperature_delta_from_previous"] = (
        revisions_df["temperature"] - revisions_df["previous_temperature"]
    )
    revisions_df["precipitation_delta_from_previous"] = (
        revisions_df["precipitation"] - revisions_df["previous_precipitation"]
    )
    revisions_df["wind_speed_delta_from_previous"] = (
        revisions_df["wind_speed_10m"] - revisions_df["previous_wind_speed_10m"]
    )
    revisions_df["day"] = revisions_df["target_timestamp"].dt.date

    revisions_df = revisions_df[
        [
            "location_id",
            "target_timestamp",
            "forecast_generated_at",
            "revision_number",
            "previous_forecast_generated_at",
            "temperature",
            "previous_temperature",
            "temperature_delta_from_previous",
            "precipitation",
            "previous_precipitation",
            "precipitation_delta_from_previous",
            "wind_speed_10m",
            "previous_wind_speed_10m",
            "wind_speed_delta_from_previous",
            "day",
        ]
    ].sort_values(["location_id", "target_timestamp", "forecast_generated_at"])

    return revisions_df


if __name__ == "__main__":
    result_df = run()
    print(result_df.head())
