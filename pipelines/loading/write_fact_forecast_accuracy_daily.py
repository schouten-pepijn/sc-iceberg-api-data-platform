"""Load transformed daily forecast-accuracy facts into Iceberg."""

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.location import load_location
from pipelines.transformations.transform_forecast_accuracy_daily import (
    run as transform_forecast_accuracy_daily,
)


def to_arrow_table(df: object) -> pa.Table:
    """Convert daily forecast-accuracy rows to the typed Arrow contract."""
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("day", pa.date32(), nullable=True),
            pa.field(
                "mean_absolute_temperature_error",
                pa.float64(),
                nullable=True,
            ),
            pa.field(
                "mean_absolute_precipitation_error",
                pa.float64(),
                nullable=True,
            ),
            pa.field(
                "mean_absolute_wind_speed_error",
                pa.float64(),
                nullable=True,
            ),
            pa.field("avg_forecast_horizon_hours", pa.float64(), nullable=True),
            pa.field("row_count", pa.int64(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def overwrite_location_fact_forecast_accuracy_daily(
    arrow_table: pa.Table,
    location_id: str,
) -> None:
    """Overwrite one location slice so daily accuracy refreshes stay idempotent."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_forecast_accuracy_daily")
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo(
            term=Reference("location_id"),
            value=literal(location_id),
        ),
    )


def run(location_name: str = "Amsterdam") -> None:
    """Materialize daily forecast-accuracy facts for one location."""
    location = load_location(location_name=location_name)
    df = transform_forecast_accuracy_daily(location_name=location_name)

    if df.empty:
        print(
            f"No daily forecast accuracy rows found for {location_name}; "
            "skipped lakehouse.fact_forecast_accuracy_daily refresh."
        )
        return

    arrow_table = to_arrow_table(df)
    overwrite_location_fact_forecast_accuracy_daily(
        arrow_table=arrow_table,
        location_id=location["location_id"],
    )
    print(
        f"Overwrote fact_forecast_accuracy_daily for {location_name} "
        f"with {len(df)} records."
    )


if __name__ == "__main__":
    run()
