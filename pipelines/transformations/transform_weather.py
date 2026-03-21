import pandas as pd
from pyiceberg.catalog import load_catalog

from pipelines.validation.ingest_weather import validate


def load_bronze_weather() -> pd.DataFrame:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.bronze_weather")
    return table.scan().to_pandas()


def run() -> pd.DataFrame:
    bronze_df = load_bronze_weather()
    validation_result = validate(bronze_df)

    if not validation_result["success"]:
        raise ValueError(f"Validation failed: {validation_result['errors']}")

    df = validation_result["validated_df"].copy()

    silver_df = df[["timestamp", "temperature"]].copy()
    silver_df = silver_df.drop_duplicates(subset=["timestamp"])
    silver_df = silver_df.rename(columns={"temperature": "temperature_c"})

    silver_df["timestamp"] = pd.to_datetime(silver_df["timestamp"], utc=True)
    silver_df["day"] = silver_df["timestamp"].dt.date
    silver_df["temperature_f"] = silver_df["temperature_c"] * 9 / 5 + 32

    silver_df = silver_df[
        ["timestamp", "day", "temperature_c", "temperature_f"]
    ].sort_values("timestamp")

    return silver_df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
