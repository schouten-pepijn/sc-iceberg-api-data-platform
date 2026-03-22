"""Ingest location search results into the raw location shape."""

import uuid
import pandas as pd

from apis.open_meteo_geocoding import search_locations


def _build_location_id(row: dict) -> str:
    """Build a deterministic business key from stable location attributes."""
    return (
        "open_meteo:"
        f"{row.get('name')}:"
        f"{row.get('country_code')}:"
        f"{row.get('latitude')}:"
        f"{row.get('longitude')}"
    )


def run(query: str = "Amsterdam") -> pd.DataFrame:
    """Fetch location candidates and normalize them for append-only Bronze ingestion."""
    results = search_locations(query)

    batch_id = str(uuid.uuid4())
    ingest_ts = pd.Timestamp.now("UTC")

    rows = []
    for result in results:
        row = {
            "name": result.get("name"),
            "latitude": result.get("latitude"),
            "longitude": result.get("longitude"),
            "elevation": result.get("elevation"),
            "timezone": result.get("timezone"),
            "country_code": result.get("country_code"),
            "country": result.get("country"),
            "admin1": result.get("admin1"),
        }
        # Keep id generation deterministic so repeated fetches map to one entity key.
        row["location_id"] = _build_location_id(row)
        row["_ingest_ts"] = ingest_ts
        row["_source_api"] = "open_meteo_geocoding"
        row["_batch_id"] = batch_id
        rows.append(row)

    return pd.DataFrame(rows)[
        [
            "location_id",
            "name",
            "latitude",
            "longitude",
            "elevation",
            "timezone",
            "country_code",
            "country",
            "admin1",
            "_ingest_ts",
            "_source_api",
            "_batch_id",
        ]
    ]


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
