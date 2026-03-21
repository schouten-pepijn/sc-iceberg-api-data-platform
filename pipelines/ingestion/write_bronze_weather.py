import pyarrow as pa
from pyiceberg.catalog import load_catalog

from pipelines.ingestion.ingest_weather import run as ingest_weather


def to_arrow_table(df):
    return pa.Table.from_pandas(df, preserve_index=False)


def append_to_bronze_weather(arrow_table) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_weather")
    table.append(arrow_table)


def run(location_name: str = "Amsterdam") -> None:
    df = ingest_weather(location_name=location_name)
    arrow_table = to_arrow_table(df)
    append_to_bronze_weather(arrow_table)
    print(f"Appended {len(df)} records to lakehouse.bronze_weather")


if __name__ == "__main__":
    run()
