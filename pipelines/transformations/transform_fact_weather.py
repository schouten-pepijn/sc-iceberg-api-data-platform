import pandas as pd
from pyiceberg.catalog import load_catalog


def load_silver_weather() -> pd.DataFrame:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.silver_weather_hourly")
    return table.scan().to_pandas()


def run() -> pd.DataFrame:
    silver_df = load_silver_weather().copy()
    silver_df["day"] = pd.to_datetime(silver_df["day"]).dt.date

    fact_weather_df = (
        silver_df.groupby(["location_id", "day"], as_index=False)
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            avg_temperature_f=("temperature_f", "mean"),
            total_precipitation_mm=("precipitation_mm", "sum"),
            max_wind_speed_10m=("wind_speed_10m_max", "max"),
            hour_count=("timestamp", "count"),
        )
        .sort_values(["location_id", "day"])
    )

    return fact_weather_df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
