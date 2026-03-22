import dagster as dg

from pipelines.ingestion.write_bronze_weather import run as write_bronze_weather
from pipelines.ingestion.write_locations import run as write_locations
from pipelines.transformations.write_fact_weather import run as write_fact_weather
from pipelines.transformations.write_silver_weather import run as write_silver_weather

location_partitions = dg.StaticPartitionsDefinition(
    [
        "Amsterdam",
        "Berlin",
        "Paris",
        "Madrid",
        "Rome",
    ]
)


@dg.asset(partitions_def=location_partitions)
def dim_location_by_location(context: dg.AssetExecutionContext) -> None:
    location_name = context.partition_key
    context.log.info(f"Ingesting location data for {location_name}...")
    write_locations(query=location_name)


@dg.asset(partitions_def=location_partitions, deps=[dim_location_by_location])
def bronze_weather_by_location(context: dg.AssetExecutionContext) -> None:
    location_name = context.partition_key
    context.log.info(f"Ingesting weather data into bronze for {location_name}...")
    write_bronze_weather(location_name=location_name)


@dg.asset(partitions_def=location_partitions, deps=[bronze_weather_by_location])
def silver_weather_hourly_by_location(context: dg.AssetExecutionContext) -> None:
    location_name = context.partition_key
    context.log.info(f"Transforming weather data into silver for {location_name}...")
    write_silver_weather(location_name=location_name)


@dg.asset(partitions_def=location_partitions, deps=[silver_weather_hourly_by_location])
def fact_weather_by_location(context: dg.AssetExecutionContext) -> None:
    location_name = context.partition_key
    context.log.info(f"Aggregating weather data into gold for {location_name}...")
    write_fact_weather(location_name=location_name)
