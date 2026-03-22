"""Ingest weather observations for a resolved location."""

import uuid

import pandas as pd

from apis.open_meteo_forecast_weather import fetch_weather_data
from catalog.location import load_location


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    """Fetch weather data and shape it into the Bronze weather contract."""
    location = load_location(location_name=location_name)
    data = fetch_weather_data(lat=location["latitude"], lon=location["longitude"])

    batch_id = str(uuid.uuid4())
    ingest_ts = pd.Timestamp.now("UTC")

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(data["minutely_15"]["time"], utc=True),
            "temperature": data["minutely_15"]["temperature_2m"],
            "precipitation": data["minutely_15"]["precipitation"],
            "wind_speed_10m": data["minutely_15"]["wind_speed_10m"],
        }
    )

    # Add ingestion metadata used for lineage and incremental watermarking downstream.
    df["location_id"] = location["location_id"]
    df["_ingest_ts"] = ingest_ts
    df["_source_api"] = "open_meteo_feed"
    df["_batch_id"] = batch_id

    return df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
