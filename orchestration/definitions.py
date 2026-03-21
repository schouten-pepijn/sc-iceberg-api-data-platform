from dagster import Definitions

from orchestration.assets.weather import (
    dim_location_by_location,
    bronze_weather_by_location,
    fact_weather_by_location,
    silver_weather_hourly_by_location,
)

defs = Definitions(
    assets=[
        dim_location_by_location,
        bronze_weather_by_location,
        silver_weather_hourly_by_location,
        fact_weather_by_location,
    ]
)
