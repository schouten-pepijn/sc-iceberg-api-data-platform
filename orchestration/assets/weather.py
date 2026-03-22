"""Dagster asset graph for location-partitioned weather pipelines."""

import dagster as dg

from pipelines.ingestion.write_bronze_weather_feed import (
    run as write_bronze_weather_feed,
)
from pipelines.ingestion.write_locations import run as write_locations
from pipelines.loading.write_fact_weather import run as write_fact_weather
from pipelines.loading.write_silver_weather_feed import run as write_silver_weather_feed

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
    """Materialize the location dimension slice for one city partition."""
    location_name = context.partition_key
    context.log.info(f"Ingesting location data for {location_name}...")
    write_locations(query=location_name)


@dg.asset(partitions_def=location_partitions, deps=[dim_location_by_location])
def bronze_weather_feed_by_location(context: dg.AssetExecutionContext) -> None:
    """Ingest Bronze weather rows for one city partition."""
    location_name = context.partition_key
    context.log.info(f"Ingesting weather data into bronze for {location_name}...")
    write_bronze_weather_feed(location_name=location_name)


@dg.asset(partitions_def=location_partitions, deps=[bronze_weather_feed_by_location])
def silver_weather_feed_hourly_by_location(context: dg.AssetExecutionContext) -> None:
    """Build Silver hourly aggregates for one city partition."""
    location_name = context.partition_key
    context.log.info(f"Transforming weather data into silver for {location_name}...")
    result = write_silver_weather_feed(location_name=location_name)
    context.add_output_metadata(result)


@dg.asset(partitions_def=location_partitions, deps=[silver_weather_feed_hourly_by_location])
def fact_weather_by_location(context: dg.AssetExecutionContext) -> None:
    """Build Gold daily facts for one city partition."""
    location_name = context.partition_key
    context.log.info(f"Aggregating weather data into gold for {location_name}...")
    result = write_fact_weather(location_name=location_name)
    context.add_output_metadata(result)
