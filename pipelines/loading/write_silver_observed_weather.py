"""Load transformed Silver observed weather data into Iceberg."""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.location import load_location
from pipelines.transformations.transform_observed_weather import (
    run as transform_observed_weather,
)


def to_arrow_table(df: object) -> pa.Table:
    """Convert Silver observed-weather rows to the typed Arrow contract."""
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("day", pa.date32(), nullable=True),
            pa.field("temperature", pa.float64(), nullable=True),
            pa.field("precipitation", pa.float64(), nullable=True),
            pa.field("wind_speed_10m", pa.float64(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def overwrite_location_silver_observed_weather(
    arrow_table: pa.Table,
    location_id: str,
) -> None:
    """Overwrite only one location slice to keep observed refreshes idempotent."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_observed_weather")
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo(
            term=Reference("location_id"),
            value=literal(location_id),
        ),
    )


def run(location_name: str = "Amsterdam") -> None:
    """Materialize Silver observed weather for one location."""
    location = load_location(location_name=location_name)
    df = transform_observed_weather(location_name=location_name)

    if df.empty:
        print(
            f"No observed rows found for {location_name}; "
            "skipped lakehouse.silver_observed_weather refresh."
        )
        return

    arrow_table = to_arrow_table(df)
    overwrite_location_silver_observed_weather(
        arrow_table=arrow_table,
        location_id=location["location_id"],
    )
    print(
        f"Overwrote silver_observed_weather for {location_name} "
        f"with {len(df)} records."
    )


if __name__ == "__main__":
    run()
