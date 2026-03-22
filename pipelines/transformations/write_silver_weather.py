import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.pipeline_state import write_pipeline_state
from pipelines.transformations.transform_weather import run as transform_weather


def to_arrow_table(df: object) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("day", pa.date32(), nullable=True),
            pa.field("temperature_c", pa.float64(), nullable=True),
            pa.field("temperature_f", pa.float64(), nullable=True),
            pa.field("precipitation_mm", pa.float64(), nullable=True),
            pa.field("wind_speed_10m_max", pa.float64(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def _load_location(location_name: str = "Amsterdam") -> dict:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.dim_location")
    df = table.scan().to_pandas()

    match = df[df["name"] == location_name].sort_values("_ingest_ts").tail(1)
    if match.empty:
        raise ValueError(f"No location found with name '{location_name}'")

    row = match.iloc[0]
    return {
        "location_id": row["location_id"],
        "name": row["name"],
    }


def overwrite_location_silver_weather(arrow_table: pa.Table, location_id: str) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_weather_hourly")
    # Replace only the canonical Silver slice for the selected location.
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo(
            term=Reference("location_id"), value=literal(location_id)
        ),
    )


def run(location_name: str = "Amsterdam") -> None:
    location = _load_location(location_name=location_name)
    df = transform_weather(location_name=location_name)
    arrow_table = to_arrow_table(df)
    overwrite_location_silver_weather(arrow_table, location["location_id"])
    print(
        f"Overwrote {len(df)} records in lakehouse.silver_weather_hourly for {location_name}"
    )


if __name__ == "__main__":
    run()
