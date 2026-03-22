"""Load transformed Silver hourly data into Iceberg with watermark tracking."""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.location import load_location as _load_location
from catalog.pipeline_state import load_pipeline_state, write_pipeline_state
from pipelines.transformations.transform_weather import run as transform_weather


def _format_metadata_value(value: object) -> str | None:
    """Normalize metadata values for structured output logs."""
    return None if value is None else str(value)


def to_arrow_table(df: object) -> pa.Table:
    """Convert Silver dataframe rows to the typed Arrow table contract."""
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


def overwrite_location_silver_weather(arrow_table: pa.Table, location_id: str) -> None:
    """Overwrite only one location slice to keep each run idempotent per location."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_weather_hourly")
    # Replace only the canonical Silver slice for the selected location.
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo(
            term=Reference("location_id"), value=literal(location_id)
        ),
    )


def run(location_name: str = "Amsterdam") -> dict[str, object]:
    """Materialize Silver data for one location and persist new watermark state."""
    location = _load_location(location_name=location_name)
    previous_state = load_pipeline_state(
        pipeline_name="silver_weather_hourly",
        location_id=location["location_id"],
    )
    previous_watermark = (
        previous_state["last_bronze_ingest_ts"] if previous_state is not None else None
    )

    df, max_ingest_ts = transform_weather(location_name=location_name)

    # A missing watermark advance means there was no new Bronze batch to process.
    if df.empty or max_ingest_ts is None:
        result = {
            "location_name": location_name,
            "location_id": location["location_id"],
            "previous_watermark": _format_metadata_value(previous_watermark),
            "new_watermark": None,
            "rows_written": 0,
            "status": "no_op",
        }
        print(
            "Silver refresh skipped: "
            f"location_name={location_name}, "
            f"location_id={location['location_id']}, "
            f"previous_watermark={previous_watermark}, "
            "new_watermark=None, "
            "rows_written=0, "
            "status=no_op"
        )
        return result

    arrow_table = to_arrow_table(df)
    overwrite_location_silver_weather(arrow_table, location["location_id"])
    result = {
        "location_name": location_name,
        "location_id": location["location_id"],
        "previous_watermark": _format_metadata_value(previous_watermark),
        "new_watermark": _format_metadata_value(max_ingest_ts),
        "rows_written": len(df),
        "status": "written",
    }
    print(
        "Silver refresh completed: "
        f"location_name={location_name}, "
        f"location_id={location['location_id']}, "
        f"previous_watermark={previous_watermark}, "
        f"new_watermark={max_ingest_ts}, "
        f"rows_written={len(df)}, "
        "status=written"
    )

    # Persist the latest processed Bronze ingest timestamp for the next incremental run.
    write_pipeline_state(
        pipeline_name="silver_weather_hourly",
        location_id=location["location_id"],
        last_bronze_ingest_ts=max_ingest_ts,
    )
    return result


if __name__ == "__main__":
    run()
