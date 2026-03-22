"""Unit tests for Bronze-to-Silver weather feed transformation behavior."""

import pandas as pd

from pipelines.transformations import transform_weather_feed


def test_transform_weather_feed_aggregates_hourly_by_location(
    monkeypatch, bronze_weather_feed_df
) -> None:
    monkeypatch.setattr(
        transform_weather_feed,
        "_load_location",
        lambda location_name="Amsterdam": {
            "location_id": "loc-amsterdam",
            "name": "Amsterdam",
        },
    )
    monkeypatch.setattr(
        transform_weather_feed,
        "load_bronze_weather_feed",
        lambda: bronze_weather_feed_df,
    )
    monkeypatch.setattr(
        transform_weather_feed,
        "validate",
        lambda df: {"success": True, "errors": [], "validated_df": df},
    )
    monkeypatch.setattr(
        transform_weather_feed,
        "load_pipeline_state",
        lambda pipeline_name, location_id: None,
    )

    silver_df, max_ingest_ts = transform_weather_feed.run("Amsterdam")

    # Duplicate raw points collapse into one hourly aggregate with latest values.
    assert len(silver_df) == 1
    row = silver_df.iloc[0]
    assert row["location_id"] == "loc-amsterdam"
    assert row["temperature_c"] == 13.333333333333334
    assert row["temperature_f"] == 56.0
    assert row["precipitation_mm"] == 0.8
    assert row["wind_speed_10m_max"] == 8.0
    assert max_ingest_ts == pd.Timestamp("2026-03-22T01:05:00Z")


def test_transform_weather_feed_returns_noop_when_watermark_filters_all_rows(
    monkeypatch, valid_weather_df
) -> None:
    monkeypatch.setattr(
        transform_weather_feed,
        "_load_location",
        lambda location_name="Amsterdam": {
            "location_id": "loc-amsterdam",
            "name": "Amsterdam",
        },
    )
    monkeypatch.setattr(
        transform_weather_feed, "load_bronze_weather_feed", lambda: valid_weather_df
    )
    monkeypatch.setattr(
        transform_weather_feed,
        "validate",
        lambda df: {"success": True, "errors": [], "validated_df": df},
    )
    monkeypatch.setattr(
        transform_weather_feed,
        "load_pipeline_state",
        lambda pipeline_name, location_id: {
            "last_bronze_ingest_ts": pd.Timestamp("2026-03-22T02:00:00Z")
        },
    )

    silver_df, max_ingest_ts = transform_weather_feed.run("Amsterdam")

    # New runs should no-op when watermark excludes all available Bronze rows.
    assert silver_df.empty
    assert max_ingest_ts is None
