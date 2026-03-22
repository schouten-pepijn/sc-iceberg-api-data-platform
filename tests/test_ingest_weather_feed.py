"""Unit tests for weather ingestion dataframe mapping."""

import uuid


from pipelines.ingestion import ingest_weather_feed


def test_ingest_weather_maps_api_payload_to_bronze_dataframe(monkeypatch) -> None:
    monkeypatch.setattr(
        ingest_weather_feed,
        "_load_location",
        lambda location_name="Amsterdam": {
            "location_id": "loc-amsterdam",
            "name": "Amsterdam",
            "latitude": 52.37,
            "longitude": 4.90,
        },
    )
    monkeypatch.setattr(
        ingest_weather_feed,
        "fetch_weather_data",
        lambda lat, lon: {
            "minutely_15": {
                "time": ["2026-03-22T00:00", "2026-03-22T00:15"],
                "temperature_2m": [10.0, 11.0],
                "precipitation": [0.1, 0.0],
                "wind_speed_10m": [5.0, 6.0],
            }
        },
    )
    monkeypatch.setattr(uuid, "uuid4", lambda: "batch-123")

    df = ingest_weather_feed.run()

    assert list(df.columns) == [
        "timestamp",
        "temperature",
        "precipitation",
        "wind_speed_10m",
        "location_id",
        "_ingest_ts",
        "_source_api",
        "_batch_id",
    ]
    assert df["location_id"].tolist() == ["loc-amsterdam", "loc-amsterdam"]
    assert df["_source_api"].tolist() == ["open_meteo_feed", "open_meteo_feed"]
    assert df["_batch_id"].tolist() == ["batch-123", "batch-123"]
    assert str(df["timestamp"].dtype).startswith("datetime64")
    assert str(df["_ingest_ts"].dtype).startswith("datetime64")
