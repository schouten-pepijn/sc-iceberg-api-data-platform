"""Catalog helpers for location dimension lookups."""

from pyiceberg.catalog import load_catalog


def load_location(location_name: str = "Amsterdam") -> dict:
    """Load the latest dimension row for a location name."""
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.dim_location")
    df = table.scan().to_pandas()

    # The dimension is append-only; latest ingest timestamp is the active record.
    match = df[df["name"] == location_name].sort_values("_ingest_ts").tail(1)
    if match.empty:
        raise ValueError(f"No location found with name '{location_name}'")

    row = match.iloc[0]
    return {
        "location_id": row["location_id"],
        "name": row["name"],
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
    }
