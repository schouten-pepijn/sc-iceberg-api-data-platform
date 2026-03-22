"""Transform forecast and observed Silver rows into forecast-accuracy facts."""

import pandas as pd
from pyiceberg.catalog import load_catalog

from catalog.location import load_location


def load_silver_forecast_latest() -> pd.DataFrame:
    """Load the latest forecast rows from Iceberg."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_forecast_latest")
    return table.scan().to_pandas()


def load_silver_observed_weather() -> pd.DataFrame:
    """Load the canonical observed weather rows from Iceberg."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_observed_weather")
    return table.scan().to_pandas()


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    """Build forecast-vs-observed accuracy facts for a single location."""
    location = load_location(location_name=location_name)

    forecast_df = load_silver_forecast_latest().copy()
    forecast_df = forecast_df[
        forecast_df["location_id"] == location["location_id"]
    ].copy()
    forecast_df["target_timestamp"] = pd.to_datetime(
        forecast_df["target_timestamp"], utc=True
    )
    forecast_df["forecast_generated_at"] = pd.to_datetime(
        forecast_df["forecast_generated_at"], utc=True
    )

    observed_df = load_silver_observed_weather().copy()
    observed_df = observed_df[
        observed_df["location_id"] == location["location_id"]
    ].copy()
    observed_df["timestamp"] = pd.to_datetime(observed_df["timestamp"], utc=True)

    if forecast_df.empty or observed_df.empty:
        return pd.DataFrame(
            columns=[
                "location_id",
                "target_timestamp",
                "day",
                "forecast_generated_at",
                "forecast_temperature",
                "observed_temperature",
                "temperature_error",
                "forecast_precipitation",
                "observed_precipitation",
                "precipitation_error",
                "forecast_wind_speed_10m",
                "observed_wind_speed_10m",
                "wind_speed_error",
            ]
        )

    # Forecast is at 15-minute grain while observed data is hourly.
    forecast_df["forecast_hour"] = forecast_df["target_timestamp"].dt.floor("h")

    fact_df = forecast_df.merge(
        observed_df,
        left_on=["location_id", "forecast_hour"],
        right_on=["location_id", "timestamp"],
        how="inner",
        suffixes=("_forecast", "_observed"),
    )

    if fact_df.empty:
        return pd.DataFrame(
            columns=[
                "location_id",
                "target_timestamp",
                "day",
                "forecast_generated_at",
                "forecast_temperature",
                "observed_temperature",
                "temperature_error",
                "forecast_precipitation",
                "observed_precipitation",
                "precipitation_error",
                "forecast_wind_speed_10m",
                "observed_wind_speed_10m",
                "wind_speed_error",
            ]
        )

    fact_df["day"] = fact_df["target_timestamp"].dt.date
    fact_df["temperature_error"] = (
        fact_df["temperature_forecast"] - fact_df["temperature_observed"]
    )
    fact_df["precipitation_error"] = (
        fact_df["precipitation_forecast"] - fact_df["precipitation_observed"]
    )
    fact_df["wind_speed_error"] = (
        fact_df["wind_speed_10m_forecast"] - fact_df["wind_speed_10m_observed"]
    )

    fact_df = fact_df.rename(
        columns={
            "temperature_forecast": "forecast_temperature",
            "temperature_observed": "observed_temperature",
            "precipitation_forecast": "forecast_precipitation",
            "precipitation_observed": "observed_precipitation",
            "wind_speed_10m_forecast": "forecast_wind_speed_10m",
            "wind_speed_10m_observed": "observed_wind_speed_10m",
        }
    )

    fact_df = fact_df[
        [
            "location_id",
            "target_timestamp",
            "day",
            "forecast_generated_at",
            "forecast_temperature",
            "observed_temperature",
            "temperature_error",
            "forecast_precipitation",
            "observed_precipitation",
            "precipitation_error",
            "forecast_wind_speed_10m",
            "observed_wind_speed_10m",
            "wind_speed_error",
        ]
    ].sort_values(["location_id", "target_timestamp", "forecast_generated_at"])

    return fact_df


if __name__ == "__main__":
    result_df = run()
    print(result_df.head())
