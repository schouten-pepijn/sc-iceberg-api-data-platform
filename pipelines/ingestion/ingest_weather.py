import uuid

import pandas as pd

from apis.open_meteo_weather import fetch_weather_data
from catalog.location import load_location as _load_location


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    location = _load_location(location_name=location_name)
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

    df["location_id"] = location["location_id"]
    df["_ingest_ts"] = ingest_ts
    df["_source_api"] = "open_meteo"
    df["_batch_id"] = batch_id

    return df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
