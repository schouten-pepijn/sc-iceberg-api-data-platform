"""Load transformed forecast-accuracy facts into Iceberg."""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.location import load_location
from pipelines.transformations.transform_forecast_accuracy import (
    run as transform_forecast_accuracy,
)


def to_arrow_table(df: object) -> pa.Table:
    """Convert forecast-accuracy rows to the typed Arrow contract."""
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("target_timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("day", pa.date32(), nullable=True),
            pa.field("forecast_generated_at", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("forecast_temperature", pa.float64(), nullable=True),
            pa.field("observed_temperature", pa.float64(), nullable=True),
            pa.field("temperature_error", pa.float64(), nullable=True),
            pa.field("forecast_precipitation", pa.float64(), nullable=True),
            pa.field("observed_precipitation", pa.float64(), nullable=True),
            pa.field("precipitation_error", pa.float64(), nullable=True),
            pa.field("forecast_wind_speed_10m", pa.float64(), nullable=True),
            pa.field("observed_wind_speed_10m", pa.float64(), nullable=True),
            pa.field("wind_speed_error", pa.float64(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def overwrite_location_fact_forecast_accuracy(
    arrow_table: pa.Table,
    location_id: str,
) -> None:
    """Overwrite one location slice so accuracy refreshes remain idempotent."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_forecast_accuracy")
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo(
            term=Reference("location_id"),
            value=literal(location_id),
        ),
    )


def run(location_name: str = "Amsterdam") -> None:
    """Materialize forecast-accuracy facts for one location."""
    location = load_location(location_name=location_name)
    df = transform_forecast_accuracy(location_name=location_name)

    if df.empty:
        print(
            f"No forecast accuracy rows found for {location_name}; "
            "skipped lakehouse.fact_forecast_accuracy refresh."
        )
        return

    arrow_table = to_arrow_table(df)
    overwrite_location_fact_forecast_accuracy(
        arrow_table=arrow_table,
        location_id=location["location_id"],
    )
    print(
        f"Overwrote fact_forecast_accuracy for {location_name} "
        f"with {len(df)} records."
    )


if __name__ == "__main__":
    run()
