import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.pipeline_state import load_pipeline_state, write_pipeline_state
from pipelines.transformations.transform_weather import run as transform_weather


def _format_metadata_value(value: object) -> str | None:
    return None if value is None else str(value)


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


def run(location_name: str = "Amsterdam") -> dict[str, object]:
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
