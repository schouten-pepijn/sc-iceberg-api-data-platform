import pyarrow as pa
from pyiceberg.catalog import load_catalog

from pipelines.transformations.transform_weather import run as transform_weather


def to_arrow_table(df):
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


def full_load_silver_weather(arrow_table) -> None:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_weather_hourly")
    table.overwrite(arrow_table)


def run() -> None:
    df = transform_weather()
    arrow_table = to_arrow_table(df)
    full_load_silver_weather(arrow_table)
    print(f"Overwrote {len(df)} records in lakehouse.silver_weather_hourly")


if __name__ == "__main__":
    run()
