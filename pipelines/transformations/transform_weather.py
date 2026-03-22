import pandas as pd
from pyiceberg.catalog import load_catalog

from pipelines.validation.ingest_weather import validate


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


def load_bronze_weather() -> pd.DataFrame:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_weather")
    return table.scan().to_pandas()


def run(location_name: str = "Amsterdam") -> pd.DataFrame:
    location = _load_location(location_name=location_name)
    bronze_df = load_bronze_weather()
    validation_result = validate(bronze_df)

    if not validation_result["success"]:
        raise ValueError(f"Validation failed: {validation_result['errors']}")

    df = validation_result["validated_df"].copy()
    df = df[df["location_id"] == location["location_id"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["_ingest_ts"] = pd.to_datetime(df["_ingest_ts"], utc=True)

    latest_bronze_df = (
        df.sort_values(["location_id", "timestamp", "_ingest_ts"])
        .drop_duplicates(subset=["location_id", "timestamp"], keep="last")
        .copy()
    )

    silver_df = (
        latest_bronze_df.assign(hour_bucket=latest_bronze_df["timestamp"].dt.floor("h"))
        .groupby(["location_id", "hour_bucket"], as_index=False)
        .agg(
            temperature_c=("temperature", "mean"),
            precipitation_mm=("precipitation", "sum"),
            wind_speed_10m_max=("wind_speed_10m", "max"),
        )
        .rename(columns={"hour_bucket": "timestamp"})
    )

    silver_df["day"] = silver_df["timestamp"].dt.date
    silver_df["temperature_f"] = silver_df["temperature_c"] * 9 / 5 + 32

    silver_df = silver_df[
        [
            "location_id",
            "timestamp",
            "day",
            "temperature_c",
            "temperature_f",
            "precipitation_mm",
            "wind_speed_10m_max",
        ]
    ].sort_values(["location_id", "timestamp"])

    return silver_df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
