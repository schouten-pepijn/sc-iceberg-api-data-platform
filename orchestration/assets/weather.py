import dagster as dg

from pipelines.ingestion.ingest_locations import run as ingest_locations
from pipelines.ingestion.write_bronze_weather import run as write_bronze_weather
from pipelines.transformations.write_fact_weather import run as write_fact_weather
from pipelines.transformations.write_silver_weather import run as write_silver_weather


@dg.asset
def dim_location(context: dg.AssetExecutionContext) -> None:
    context.log.info("Ingesting location data into dimension table...")
    ingest_locations()


@dg.asset(deps=[dim_location])
def bronze_weather(context: dg.AssetExecutionContext) -> None:
    context.log.info("Ingesting weather data into bronze layer...")
    write_bronze_weather()


@dg.asset(deps=[bronze_weather])
def silver_weather_hourly(context: dg.AssetExecutionContext) -> None:
    context.log.info("Transforming weather data into silver layer...")
    write_silver_weather()


@dg.asset(deps=[silver_weather_hourly])
def fact_weather(context: dg.AssetExecutionContext) -> None:
    context.log.info("Aggregating weather data into gold layer...")
    write_fact_weather()
