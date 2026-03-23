import pandas as pd
from pyiceberg.catalog import load_catalog

from catalog.location import load_location


def load_fact_forecast_accuracy() -> pd.DataFrame:
    """Load the latest forecast-accuracy facts from Iceberg."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_forecast_accuracy")
    return table.scan().to_pandas()


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    location = load_location(location_name=location_name)
    df = load_fact_forecast_accuracy().copy()
    df = df[df["location_id"] == location["location_id"]].copy()

    if df.empty:
        return pd.DataFrame(
            columns=[
                "location_id",
                "day",
                "mean_absolute_temperature_error",
                "mean_absolute_precipitation_error",
                "mean_absolute_wind_speed_error",
                "avg_forecast_horizon_hours",
                "row_count",
            ]
        )

    df["day"] = pd.to_datetime(df["day"]).dt.date
    df["abs_temperature_error"] = df["temperature_error"].abs()
    df["abs_precipitation_error"] = df["precipitation_error"].abs()
    df["abs_wind_speed_error"] = df["wind_speed_error"].abs()

    daily_df = (
        df.groupby(["location_id", "day"], as_index=False)
        .agg(
            mean_absolute_temperature_error=("abs_temperature_error", "mean"),
            mean_absolute_precipitation_error=("abs_precipitation_error", "mean"),
            mean_absolute_wind_speed_error=("abs_wind_speed_error", "mean"),
            avg_forecast_horizon_hours=("forecast_horizon_hours", "mean"),
            row_count=("target_timestamp", "count"),
        )
        .sort_values(["location_id", "day"])
    )

    return daily_df
