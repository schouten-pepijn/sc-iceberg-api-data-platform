import pyarrow as pa
from pyiceberg.catalog import load_catalog

from pipelines.transformations.transform_fact_weather import (
    run as transform_fact_weather,
)


def to_arrow_table(df):
    schema = pa.schema(
        [
            pa.field("day", pa.date32(), nullable=True),
            pa.field("avg_temperature_c", pa.float64(), nullable=True),
            pa.field("avg_temperature_f", pa.float64(), nullable=True),
            pa.field("total_precipitation_mm", pa.float64(), nullable=True),
            pa.field("max_wind_speed_10m", pa.float64(), nullable=True),
            pa.field("hour_count", pa.int64(), nullable=True),
        ]
    )
    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


def full_load_fact_weather(arrow_table) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_weather")
    table.overwrite(arrow_table)


def run() -> None:
    df = transform_fact_weather()
    arrow_table = to_arrow_table(df)
    full_load_fact_weather(arrow_table)
    print(f"Overwrote {len(df)} records in lakehouse.fact_weather")


if __name__ == "__main__":
    run()
