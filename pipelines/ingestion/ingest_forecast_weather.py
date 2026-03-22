import uuid

import pandas as pd

from catalog.location import load_location
from apis.open_meteo_weather import fetch_weather_data


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    location = load_location(location_name=location_name)
    data = fetch_weather_data(lat=location["latitude"], lon=location["longitude"])

    batch_id = str(uuid.uuid4())
    ingest_ts = pd.Timestamp.now("UTC")

    df = pd.DataFrame(
        {
            "location_id": location["location_id"],
            "target_timestamp": pd.to_datetime(data["minutely_15"]["time"], utc=True),
            "forecast_generated_at": ingest_ts,  # not a true model-run timestamp due to API limitations, but serves a similar purpose for incremental processing
            "temperature": data["minutely_15"]["temperature_2m"],
            "precipitation": data["minutely_15"]["precipitation"],
            "wind_speed_10m": data["minutely_15"]["wind_speed_10m"],
            "_ingest_ts": ingest_ts,
            "_source_api": "open_meteo_forecast",
            "_batch_id": batch_id,
        }
    )

    return df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
