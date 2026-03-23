"""Fact-weather read service used by API endpoints."""

import pandas as pd

from pyiceberg.catalog import load_catalog


def load_fact_weather() -> pd.DataFrame:
    """Load all rows from the daily fact weather table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_weather")
    return table.scan().to_pandas()


def load_fact_forecast_accuracy() -> pd.DataFrame:
    """Load all rows from the forecast-accuracy fact table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_forecast_accuracy")
    return table.scan().to_pandas()



def load_fact_forecast_accuracy_daily() -> pd.DataFrame:
    """Load all rows from the daily forecast-accuracy fact table."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_forecast_accuracy_daily")
    return table.scan().to_pandas()
