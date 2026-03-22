import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.pipeline_state import write_pipeline_state
from pipelines.transformations.transform_fact_weather import (
    run as transform_fact_weather,
)


def to_arrow_table(df: object) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("day", pa.date32(), nullable=True),
            pa.field("avg_temperature_c", pa.float64(), nullable=True),
            pa.field("avg_temperature_f", pa.float64(), nullable=True),
            pa.field("total_precipitation_mm", pa.float64(), nullable=True),
            pa.field("max_wind_speed_10m", pa.float64(), nullable=True),
            pa.field("hour_count", pa.int64(), nullable=True),
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


def overwrite_location_fact_weather(arrow_table: pa.Table, location_id: str) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_weather")
    # Replace only the canonical Gold slice for the selected location.
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo(
            term=Reference("location_id"), value=literal(location_id)
        ),
    )


def run(location_name: str = "Amsterdam") -> None:
    location = _load_location(location_name=location_name)
    df, max_processed_day = transform_fact_weather(location_name=location_name)

    # A missing day watermark means there was no new Silver data to roll up.
    if df.empty or max_processed_day is None:
        print(
            f"No new Silver data for {location_name}; skipped lakehouse.fact_weather refresh"
        )
        return

    arrow_table = to_arrow_table(df)
    overwrite_location_fact_weather(arrow_table, location["location_id"])
    print(f"Overwrote {len(df)} records in lakehouse.fact_weather for {location_name}")
    # Persist the latest processed Silver day for the next incremental Gold run.
    write_pipeline_state(
        pipeline_name="fact_weather",
        location_id=location["location_id"],
        last_silver_processed_day=max_processed_day,
    )
    print(
        f"Updated pipeline state for location_id {location['location_id']} with last_silver_processed_day {max_processed_day}"
    )


if __name__ == "__main__":
    run()
