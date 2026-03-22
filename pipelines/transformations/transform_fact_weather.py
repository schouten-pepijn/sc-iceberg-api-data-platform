from datetime import date

import pandas as pd
from pyiceberg.catalog import load_catalog

from catalog.pipeline_state import load_pipeline_state


def _load_location(location_name: str = "Amsterdam") -> dict:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.dim_location")
    df = table.scan().to_pandas()

    match = df[df["name"] == location_name].sort_values("_ingest_ts").tail(1)
    if match.empty:
        raise ValueError(f"No location found with name '{location_name}'")

    row = match.iloc[0]
    return {
        "location_id": row["location_id"],
        "name": row["name"],
    }


def load_silver_weather() -> pd.DataFrame:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_weather_hourly")
    return table.scan().to_pandas()


def run(location_name: str = "Amsterdam") -> tuple[pd.DataFrame, date | None]:
    location = _load_location(location_name=location_name)
    silver_df = load_silver_weather().copy()
    silver_df = silver_df[silver_df["location_id"] == location["location_id"]].copy()
    silver_df["day"] = pd.to_datetime(silver_df["day"]).dt.date

    state = load_pipeline_state(
        pipeline_name="fact_weather",
        location_id=location["location_id"],
    )
    # Gold only needs to rebuild days that were not processed in earlier runs.
    if state is not None and state["last_silver_processed_day"] is not None:
        silver_df = silver_df[
            silver_df["day"] > state["last_silver_processed_day"]
        ].copy()

    if silver_df.empty:
        return (
            pd.DataFrame(
                columns=[
                    "location_id",
                    "day",
                    "avg_temperature_c",
                    "avg_temperature_f",
                    "total_precipitation_mm",
                    "max_wind_speed_10m",
                    "hour_count",
                ]
            ),
            None,
        )

    # Persist the newest processed day as the Gold watermark after a successful write.
    max_processed_day = silver_df["day"].max()

    fact_weather_df = (
        silver_df.groupby(["location_id", "day"], as_index=False)
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            avg_temperature_f=("temperature_f", "mean"),
            total_precipitation_mm=("precipitation_mm", "sum"),
            max_wind_speed_10m=("wind_speed_10m_max", "max"),
            hour_count=("timestamp", "count"),
        )
        .sort_values(["location_id", "day"])
    )

    return fact_weather_df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
