import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal

from catalog.location import load_location
from pipelines.transformations.transform_forecast_weather import (
    run as transform_forecast_weather,
)


def to_arrow_table(df: object) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("location_id", pa.string(), nullable=True),
            pa.field("target_timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field(
                "forecast_generated_at", pa.timestamp("us", tz="UTC"), nullable=True
            ),
            pa.field("temperature", pa.float64(), nullable=True),
            pa.field("precipitation", pa.float64(), nullable=True),
            pa.field("wind_speed_10m", pa.float64(), nullable=True),
            pa.field("day", pa.date32(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def overwrite_location_silver_forecast_latest(
    arrow_table: pa.Table, location_id: str
) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_forecast_latest")
    table.overwrite(
        arrow_table,
        overwrite_filter=EqualTo(
            term=Reference("location_id"),
            value=literal(location_id),
        ),
    )


def run(location_name: str = "Amsterdam") -> None:
    location = load_location(location_name=location_name)
    df = transform_forecast_weather(location_name=location_name)

    if df.empty:
        print(
            f"No forecast rows found for {location_name}; "
            "skipped lakehouse.silver_forecast_latest refresh."
        )
        return

    arrow_table = to_arrow_table(df)
    overwrite_location_silver_forecast_latest(
        arrow_table=arrow_table, location_id=location["location_id"]
    )
    print(
        f"Overwrote silver_forecast_latest for {location_name} with {len(df)} records."
    )


if __name__ == "__main__":
    run()
