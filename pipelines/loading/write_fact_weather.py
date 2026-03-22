import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.location import load_location as _load_location
from catalog.pipeline_state import load_pipeline_state, write_pipeline_state
from pipelines.transformations.transform_fact_weather import (
    run as transform_fact_weather,
)


def _format_metadata_value(value: object) -> str | None:
    return None if value is None else str(value)


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


def run(location_name: str = "Amsterdam") -> dict[str, object]:
    location = _load_location(location_name=location_name)
    previous_state = load_pipeline_state(
        pipeline_name="fact_weather",
        location_id=location["location_id"],
    )
    previous_watermark = (
        previous_state["last_silver_processed_day"]
        if previous_state is not None
        else None
    )

    df, max_processed_day = transform_fact_weather(location_name=location_name)

    # A missing day watermark means there was no new Silver data to roll up.
    if df.empty or max_processed_day is None:
        result = {
            "location_name": location_name,
            "location_id": location["location_id"],
            "previous_watermark": _format_metadata_value(previous_watermark),
            "new_watermark": None,
            "rows_written": 0,
            "status": "no_op",
        }
        print(
            "Gold refresh skipped: "
            f"location_name={location_name}, "
            f"location_id={location['location_id']}, "
            f"previous_watermark={previous_watermark}, "
            "new_watermark=None, "
            "rows_written=0, "
            "status=no_op"
        )
        return result

    arrow_table = to_arrow_table(df)
    overwrite_location_fact_weather(arrow_table, location["location_id"])
    result = {
        "location_name": location_name,
        "location_id": location["location_id"],
        "previous_watermark": _format_metadata_value(previous_watermark),
        "new_watermark": _format_metadata_value(max_processed_day),
        "rows_written": len(df),
        "status": "written",
    }
    print(
        "Gold refresh completed: "
        f"location_name={location_name}, "
        f"location_id={location['location_id']}, "
        f"previous_watermark={previous_watermark}, "
        f"new_watermark={max_processed_day}, "
        f"rows_written={len(df)}, "
        "status=written"
    )

    # Persist the latest processed Silver day for the next incremental Gold run.
    write_pipeline_state(
        pipeline_name="fact_weather",
        location_id=location["location_id"],
        last_silver_processed_day=max_processed_day,
    )
    return result


if __name__ == "__main__":
    run()
