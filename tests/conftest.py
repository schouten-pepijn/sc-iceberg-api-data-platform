import pandas as pd
import pytest
from fastapi.testclient import TestClient

from services.api.main import app


@pytest.fixture
def api_client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def valid_weather_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-03-22T00:00:00Z", "2026-03-22T00:15:00Z"], utc=True
            ),
            "temperature": [10.0, 11.0],
            "precipitation": [0.1, 0.0],
            "wind_speed_10m": [5.0, 6.0],
            "location_id": ["loc-amsterdam", "loc-amsterdam"],
            "_ingest_ts": pd.to_datetime(
                ["2026-03-22T01:00:00Z", "2026-03-22T01:00:00Z"], utc=True
            ),
            "_source_api": ["open_meteo", "open_meteo"],
            "_batch_id": ["batch-123", "batch-123"],
        }
    )


@pytest.fixture
def bronze_weather_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-03-22T00:00:00Z",
                    "2026-03-22T00:15:00Z",
                    "2026-03-22T00:15:00Z",
                    "2026-03-22T00:30:00Z",
                    "2026-03-22T00:00:00Z",
                ],
                utc=True,
            ),
            "temperature": [10.0, 12.0, 14.0, 16.0, 99.0],
            "precipitation": [0.1, 0.2, 0.4, 0.3, 9.9],
            "wind_speed_10m": [5.0, 6.0, 8.0, 7.0, 50.0],
            "location_id": [
                "loc-amsterdam",
                "loc-amsterdam",
                "loc-amsterdam",
                "loc-amsterdam",
                "loc-berlin",
            ],
            "_ingest_ts": pd.to_datetime(
                [
                    "2026-03-22T01:00:00Z",
                    "2026-03-22T01:00:00Z",
                    "2026-03-22T01:05:00Z",
                    "2026-03-22T01:00:00Z",
                    "2026-03-22T01:00:00Z",
                ],
                utc=True,
            ),
            "_source_api": ["open_meteo"] * 5,
            "_batch_id": ["batch-123"] * 5,
        }
    )

