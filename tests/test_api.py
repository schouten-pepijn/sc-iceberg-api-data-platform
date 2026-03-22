"""API behavior tests for health, locations, and daily weather endpoints."""

import pandas as pd


def test_health_returns_ok(api_client) -> None:
    response = api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_locations_returns_latest_record_per_location(monkeypatch, api_client) -> None:
    from services.api import main

    monkeypatch.setattr(
        main,
        "load_dim_location",
        lambda: pd.DataFrame(
            {
                "location_id": ["loc-a", "loc-a", "loc-b"],
                "name": ["Amsterdam", "Amsterdam", "Berlin"],
                "country_code": ["NL", "NL", "DE"],
                "country": ["Netherlands", "Netherlands", "Germany"],
                "admin1": ["North Holland", "North Holland", "Berlin"],
                "timezone": [
                    "Europe/Amsterdam",
                    "Europe/Amsterdam",
                    "Europe/Berlin",
                ],
                "_ingest_ts": pd.to_datetime(
                    [
                        "2026-03-22T00:00:00Z",
                        "2026-03-22T01:00:00Z",
                        "2026-03-22T00:30:00Z",
                    ],
                    utc=True,
                ),
            }
        ),
    )

    response = api_client.get("/locations")

    assert response.status_code == 200
    body = response.json()
    # Expect one row per location id after selecting latest ingested records.
    assert len(body) == 2
    assert {row["location_id"] for row in body} == {"loc-a", "loc-b"}


def test_daily_weather_rejects_invalid_date_range(api_client) -> None:
    response = api_client.get(
        "/weather/daily",
        params={"start_date": "2026-03-23", "end_date": "2026-03-22"},
    )

    assert response.status_code == 400
    assert (
        response.json()["detail"] == "start_date must be less than or equal to end_date"
    )


def test_daily_weather_filters_by_location(monkeypatch, api_client) -> None:
    from services.api import main

    monkeypatch.setattr(
        main,
        "load_fact_weather",
        lambda: pd.DataFrame(
            {
                "location_id": ["loc-a", "loc-b"],
                "day": pd.to_datetime(["2026-03-22", "2026-03-22"]),
                "avg_temperature_c": [10.0, 20.0],
                "avg_temperature_f": [50.0, 68.0],
                "total_precipitation_mm": [1.0, 2.0],
                "max_wind_speed_10m": [5.0, 6.0],
                "hour_count": [24, 24],
            }
        ),
    )

    response = api_client.get("/weather/daily", params={"location_id": "loc-b"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["location_id"] == "loc-b"
