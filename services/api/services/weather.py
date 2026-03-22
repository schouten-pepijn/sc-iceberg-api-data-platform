"""Fact-weather read service used by API endpoints."""

import pandas as pd

from pyiceberg.catalog import load_catalog


def load_fact_weather() -> pd.DataFrame:
    """Load all rows from the daily fact weather table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_weather")
    return table.scan().to_pandas()
