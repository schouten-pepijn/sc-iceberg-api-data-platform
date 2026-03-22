"""Pydantic response models for API endpoints."""

from datetime import date
from pydantic import BaseModel, Field


class DailyWeatherResponse(BaseModel):
    """Serialized Gold weather aggregate returned by the daily endpoint."""

    location_id: str = Field(
        description="Identifier for the location of the weather data"
    )
    day: date = Field(description="Calendar day for the weather aggregate")
    avg_temperature_c: float | None = Field(
        default=None, description="Average hourly temperature in Celsius"
    )
    avg_temperature_f: float | None = Field(
        default=None, description="Average hourly temperature in Fahrenheit"
    )
    total_precipitation_mm: float | None = Field(
        default=None, description="Total precipitation for the day in millimeters"
    )
    max_wind_speed_10m: float | None = Field(
        default=None, description="Maximum 10 meter wind speed observed during the day"
    )
    hour_count: int | None = Field(
        default=None, description="Number of hourly records contributing to the day"
    )


class LocationResponse(BaseModel):
    """Serialized latest location record returned by the locations endpoint."""

    location_id: str = Field(description="Identifier for the location")
    name: str | None = Field(default=None, description="Location name")
    country_code: str | None = Field(default=None, description="ISO country code")
    country: str | None = Field(default=None, description="Country name")
    admin1: str | None = Field(
        default=None, description="First-level administrative area"
    )
    timezone: str | None = Field(default=None, description="IANA timezone name")


class ForecastAccuracyResponse(BaseModel):
    """Serialized forecast-vs-observed accuracy row returned by the API."""

    location_id: str = Field(description="Identifier for the location")
    target_timestamp: str = Field(
        description="Forecast target timestamp in ISO 8601 UTC format"
    )
    day: date = Field(description="Calendar day of the forecast target")
    forecast_generated_at: str = Field(
        description="Timestamp when the forecast snapshot was generated"
    )
    forecast_temperature: float | None = Field(
        default=None, description="Forecast temperature in Celsius"
    )
    observed_temperature: float | None = Field(
        default=None, description="Observed temperature in Celsius"
    )
    temperature_error: float | None = Field(
        default=None, description="Forecast minus observed temperature"
    )
    forecast_precipitation: float | None = Field(
        default=None, description="Forecast precipitation in millimeters"
    )
    observed_precipitation: float | None = Field(
        default=None, description="Observed precipitation in millimeters"
    )
    precipitation_error: float | None = Field(
        default=None, description="Forecast minus observed precipitation"
    )
    forecast_wind_speed_10m: float | None = Field(
        default=None, description="Forecast 10 meter wind speed"
    )
    observed_wind_speed_10m: float | None = Field(
        default=None, description="Observed 10 meter wind speed"
    )
    wind_speed_error: float | None = Field(
        default=None, description="Forecast minus observed wind speed"
    )
