import pyarrow as pa
from pyiceberg.catalog import load_catalog

from pipelines.ingestion.ingest_locations import run as ingest_locations


def to_arrow_table(df):
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
            pa.field("latitude", pa.float64(), nullable=True),
            pa.field("longitude", pa.float64(), nullable=True),
            pa.field("elevation", pa.float64(), nullable=True),
            pa.field("timezone", pa.string(), nullable=True),
            pa.field("country_code", pa.string(), nullable=True),
            pa.field("country", pa.string(), nullable=True),
            pa.field("admin1", pa.string(), nullable=True),
            pa.field("_ingest_ts", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("_source_api", pa.string(), nullable=True),
            pa.field("_batch_id", pa.string(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def append_to_dim_location(arrow_table) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.dim_location")
    table.append(arrow_table)


def run(query: str = "Amsterdam") -> None:
    df = ingest_locations(query=query)
    arrow_table = to_arrow_table(df)
    append_to_dim_location(arrow_table)
    print(f"Appended {len(df)} records to lakehouse.dim_location")


if __name__ == "__main__":
    run()
