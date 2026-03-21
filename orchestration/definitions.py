from dagster import Definitions

from orchestration.assets.weather import bronze_weather, silver_weather

defs = Definitions(assets=[bronze_weather, silver_weather])
