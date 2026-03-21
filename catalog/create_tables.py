from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, StringType, TimestampType, DateType, NestedField

catalog = load_catalog("local")

namespace = "lakehouse"

bronze_weather_schema = Schema(
    NestedField(1, "timestamp", TimestampType(), required=True),
    NestedField(2, "temperature", DoubleType(), required=False),
    NestedField(3, "_ingest_ts", TimestampType(), required=True),
    NestedField(4, "_source_api", StringType(), required=True),
    NestedField(5, "_batch_id", StringType(), required=True),
)

silver_weather_hourly_schema = Schema(
    NestedField(1, "timestamp", TimestampType(), required=True),
    NestedField(2, "day", DateType(), required=True),
    NestedField(3, "temperature_c", DoubleType(), required=False),
    NestedField(4, "temperature_f", DoubleType(), required=False),
)


def create_namespace_if_missing() -> None:
    existing_namespaces = set(catalog.list_namespaces())
    print(existing_namespaces)

    if (namespace,) not in existing_namespaces:
        catalog.create_namespace(namespace)


def create_table_if_missing(identifier: str, schema: Schema) -> None:
    existing_tables = set(catalog.list_tables(namespace))
    print(existing_tables)

    table_name = identifier.split(".")[-1]
    if (namespace, table_name) not in existing_tables:

        catalog.create_table(identifier=identifier, schema=schema)


def run() -> None:
    create_namespace_if_missing()
    create_table_if_missing("lakehouse.bronze_weather", bronze_weather_schema)
    create_table_if_missing(
        "lakehouse.silver_weather_hourly", silver_weather_hourly_schema
    )


if __name__ == "__main__":
    run()
