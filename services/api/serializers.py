"""Dataframe-to-response serializers for API payloads."""

from typing import Any, Callable

import pandas as pd

from services.api.models import DailyWeatherResponse
from services.api.models import LocationResponse


def serialize_daily_weather(df: pd.DataFrame) -> list[DailyWeatherResponse]:
    """Convert a daily weather dataframe into typed API response objects."""
    optional_float: Callable[[Any], float | None] = lambda x: (
        None if pd.isna(x) else float(x)
    )
    optional_int: Callable[[Any], int | None] = lambda x: None if pd.isna(x) else int(x)

    normalized_df = df.copy()
    # Normalize to python date objects to match API schema exactly.
    normalized_df["day"] = pd.to_datetime(normalized_df["day"]).dt.date

    return [
        DailyWeatherResponse(
            location_id=row["location_id"],
            day=row["day"],
            avg_temperature_c=optional_float(row["avg_temperature_c"]),
            avg_temperature_f=optional_float(row["avg_temperature_f"]),
            total_precipitation_mm=optional_float(row["total_precipitation_mm"]),
            max_wind_speed_10m=optional_float(row["max_wind_speed_10m"]),
            hour_count=optional_int(row["hour_count"]),
        )
        for row in normalized_df.to_dict(orient="records")
    ]


def serialize_locations(df: pd.DataFrame) -> list[LocationResponse]:
    """Convert a location dataframe into typed API response objects."""
    optional_str: Callable[[Any], str | None] = lambda x: None if pd.isna(x) else str(x)

    normalized_df = df.copy()

    return [
        LocationResponse(
            location_id=str(row["location_id"]),
            name=optional_str(row["name"]),
            country_code=optional_str(row["country_code"]),
            country=optional_str(row["country"]),
            admin1=optional_str(row["admin1"]),
            timezone=optional_str(row["timezone"]),
        )
        for row in normalized_df.to_dict(orient="records")
    ]
