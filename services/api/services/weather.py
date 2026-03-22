import pandas as pd

from pyiceberg.catalog import load_catalog


def load_fact_weather() -> pd.DataFrame:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_weather")
    return table.scan().to_pandas()
