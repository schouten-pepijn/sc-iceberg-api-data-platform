"""Unit tests for ingestion weather validation rules."""

from pipelines.validation.ingest_weather_feed import validate


def test_validate_accepts_valid_weather_dataframe(valid_weather_df) -> None:
    result = validate(valid_weather_df)

    assert result["success"] is True
    assert result["validated_df"] is not None
    assert result["errors"] == []


def test_validate_rejects_invalid_precipitation(valid_weather_df) -> None:
    df = valid_weather_df.copy()
    df.loc[0, "precipitation"] = -1.0

    result = validate(df)

    assert result["success"] is False
    assert result["validated_df"] is None
    assert any(error["column"] == "precipitation" for error in result["errors"])
