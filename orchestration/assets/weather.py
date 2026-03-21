import dagster as dg

from pipelines.ingestion.write_bronze_weather import run as write_bronze_weather
from pipelines.transformations.write_silver_weather import run as write_silver_weather


@dg.asset
def bronze_weather(context: dg.AssetExecutionContext) -> None:
    context.log.info("Ingesting weather data into bronze layer...")
    write_bronze_weather()


@dg.asset(deps=[bronze_weather])
def silver_weather(context: dg.AssetExecutionContext) -> None:
    context.log.info("Transforming weather data into silver layer...")
    write_silver_weather()


import dagster as dg
