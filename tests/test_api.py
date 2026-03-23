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


def test_forecast_accuracy_filters_by_location(monkeypatch, api_client) -> None:
    from services.api import main

    monkeypatch.setattr(
        main,
        "load_fact_forecast_accuracy",
        lambda: pd.DataFrame(
            {
                "location_id": ["loc-a", "loc-b"],
                "target_timestamp": pd.to_datetime(
                    ["2026-03-22T10:00:00Z", "2026-03-22T10:00:00Z"], utc=True
                ),
                "day": pd.to_datetime(["2026-03-22", "2026-03-22"]),
                "forecast_generated_at": pd.to_datetime(
                    ["2026-03-22T08:00:00Z", "2026-03-22T08:00:00Z"], utc=True
                ),
                "forecast_temperature": [10.0, 20.0],
                "observed_temperature": [9.0, 21.0],
                "temperature_error": [1.0, -1.0],
                "forecast_precipitation": [0.5, 0.2],
                "observed_precipitation": [0.0, 0.4],
                "precipitation_error": [0.5, -0.2],
                "forecast_wind_speed_10m": [5.0, 6.0],
                "observed_wind_speed_10m": [4.0, 7.0],
                "wind_speed_error": [1.0, -1.0],
            }
        ),
    )

    response = api_client.get("/forecast/accuracy", params={"location_id": "loc-b"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["location_id"] == "loc-b"
    assert body[0]["target_timestamp"] == "2026-03-22T10:00:00Z"



def test_forecast_accuracy_daily_filters_by_location(monkeypatch, api_client) -> None:
    from services.api import main

    monkeypatch.setattr(
        main,
        "load_fact_forecast_accuracy_daily",
        lambda: pd.DataFrame(
            {
                "location_id": ["loc-a", "loc-b"],
                "day": pd.to_datetime(["2026-03-22", "2026-03-22"]),
                "mean_absolute_temperature_error": [1.2, 0.8],
                "mean_absolute_precipitation_error": [0.3, 0.1],
                "mean_absolute_wind_speed_error": [1.5, 0.9],
                "avg_forecast_horizon_hours": [12.0, 18.0],
                "row_count": [24, 24],
            }
        ),
    )

    response = api_client.get(
        "/forecast/accuracy/daily", params={"location_id": "loc-b"}
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["location_id"] == "loc-b"
    assert body[0]["avg_forecast_horizon_hours"] == 18.0
