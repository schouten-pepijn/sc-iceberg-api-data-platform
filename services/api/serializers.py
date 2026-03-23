"""Dataframe-to-response serializers for API payloads."""

from typing import Any, Callable

import pandas as pd

from services.api.models import DailyWeatherResponse
from services.api.models import ForecastAccuracyResponse
from services.api.models import ForecastAccuracyDailyResponse
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


def serialize_forecast_accuracy(df: pd.DataFrame) -> list[ForecastAccuracyResponse]:
    """Convert a forecast-accuracy dataframe into typed API response objects."""
    optional_float: Callable[[Any], float | None] = lambda x: (
        None if pd.isna(x) else float(x)
    )

    normalized_df = df.copy()
    normalized_df["day"] = pd.to_datetime(normalized_df["day"]).dt.date
    normalized_df["target_timestamp"] = pd.to_datetime(
        normalized_df["target_timestamp"], utc=True
    ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    normalized_df["forecast_generated_at"] = pd.to_datetime(
        normalized_df["forecast_generated_at"], utc=True
    ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return [
        ForecastAccuracyResponse(
            location_id=str(row["location_id"]),
            target_timestamp=row["target_timestamp"],
            day=row["day"],
            forecast_generated_at=row["forecast_generated_at"],
            forecast_temperature=optional_float(row["forecast_temperature"]),
            observed_temperature=optional_float(row["observed_temperature"]),
            temperature_error=optional_float(row["temperature_error"]),
            forecast_precipitation=optional_float(row["forecast_precipitation"]),
            observed_precipitation=optional_float(row["observed_precipitation"]),
            precipitation_error=optional_float(row["precipitation_error"]),
            forecast_wind_speed_10m=optional_float(row["forecast_wind_speed_10m"]),
            observed_wind_speed_10m=optional_float(row["observed_wind_speed_10m"]),
            wind_speed_error=optional_float(row["wind_speed_error"]),
        )
        for row in normalized_df.to_dict(orient="records")
    ]



def serialize_forecast_accuracy_daily(
    df: pd.DataFrame,
) -> list[ForecastAccuracyDailyResponse]:
    """Convert a daily forecast-accuracy dataframe into typed API response objects."""
    optional_float: Callable[[Any], float | None] = lambda x: (
        None if pd.isna(x) else float(x)
    )
    optional_int: Callable[[Any], int | None] = lambda x: None if pd.isna(x) else int(x)

    normalized_df = df.copy()
    normalized_df["day"] = pd.to_datetime(normalized_df["day"]).dt.date

    return [
        ForecastAccuracyDailyResponse(
            location_id=str(row["location_id"]),
            day=row["day"],
            mean_absolute_temperature_error=optional_float(
                row["mean_absolute_temperature_error"]
            ),
            mean_absolute_precipitation_error=optional_float(
                row["mean_absolute_precipitation_error"]
            ),
            mean_absolute_wind_speed_error=optional_float(
                row["mean_absolute_wind_speed_error"]
            ),
            avg_forecast_horizon_hours=optional_float(
                row["avg_forecast_horizon_hours"]
            ),
            row_count=optional_int(row["row_count"]),
        )
        for row in normalized_df.to_dict(orient="records")
    ]
