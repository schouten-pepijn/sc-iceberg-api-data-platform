from dagster import Definitions

from orchestration.assets.weather import (
    dim_location,
    bronze_weather,
    fact_weather,
    silver_weather_hourly,
)

defs = Definitions(
    assets=[
        dim_location,
        bronze_weather,
        silver_weather_hourly,
        fact_weather,
    ]
)
