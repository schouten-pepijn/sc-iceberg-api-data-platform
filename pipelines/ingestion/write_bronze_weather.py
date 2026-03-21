import pyarrow as pa
from pyiceberg.catalog import load_catalog

from pipelines.ingestion.ingest_weather import run as ingest_weather


def to_arrow_table(df):
    return pa.Table.from_pandas(df, preserve_index=False)


def full_load_bronze_weather(arrow_table) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_weather")
    table.overwrite(arrow_table)


def run() -> None:
    df = ingest_weather()
    arrow_table = to_arrow_table(df)
    full_load_bronze_weather(arrow_table)
    print(f"Overwrote {len(df)} records in lakehouse.bronze_weather")


if __name__ == "__main__":
    run()
