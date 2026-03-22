"""Ingest historical observed weather snapshots into a Bronze-shaped dataframe."""

import uuid
from datetime import date, timedelta

import pandas as pd

from apis.open_meteo_observed_weather import fetch_observed_weather_data
from catalog.location import load_location


def _resolve_observed_window(
    start_date: date | None,
    end_date: date | None,
) -> tuple[date, date]:
    """Default to yesterday so the observed window is a closed actuals slice."""
    if start_date is None and end_date is None:
        yesterday = date.today() - timedelta(days=1)
        return yesterday, yesterday

    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date must be provided together")

    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")

    return start_date, end_date


def run(
    location_name: str = "Amsterdam",
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """Load observed weather for one location and shape it for Bronze writes."""
    resolved_start_date, resolved_end_date = _resolve_observed_window(
        start_date=start_date,
        end_date=end_date,
    )
    location = load_location(location_name=location_name)
    data = fetch_observed_weather_data(
        lat=location["latitude"],
        lon=location["longitude"],
        start_date=resolved_start_date,
        end_date=resolved_end_date,
    )

    batch_id = str(uuid.uuid4())
    ingest_ts = pd.Timestamp.now("UTC")

    df = pd.DataFrame(
        {
            "location_id": location["location_id"],
            "timestamp": pd.to_datetime(data["hourly"]["time"], utc=True),
            "temperature": data["hourly"]["temperature_2m"],
            "precipitation": data["hourly"]["precipitation"],
            "wind_speed_10m": data["hourly"]["wind_speed_10m"],
            "_ingest_ts": ingest_ts,
            "_source_api": "open_meteo_archive",
            "_batch_id": batch_id,
        }
    )

    return df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
