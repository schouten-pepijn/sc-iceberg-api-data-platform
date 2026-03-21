from datetime import date
from pydantic import BaseModel, Field


class DailyWeatherResponse(BaseModel):
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
