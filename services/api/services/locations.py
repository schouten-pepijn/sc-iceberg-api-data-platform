import pandas as pd

from pyiceberg.catalog import load_catalog


def load_dim_location() -> pd.DataFrame:
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.dim_location")
    return table.scan().to_pandas()
