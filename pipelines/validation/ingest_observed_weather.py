"""Validate ingested Bronze observed weather rows before downstream transforms."""

import pandas as pd
import pandera.pandas as pa
from pandera import Check
from pandera.errors import SchemaErrors

OBSERVED_WEATHER_SCHEMA = pa.DataFrameSchema(
    {
        "location_id": pa.Column(str, nullable=False),
        "timestamp": pa.Column(pa.DateTime, nullable=False),
        "temperature": pa.Column(
            float,
            checks=Check.in_range(min_value=-80, max_value=60),
            nullable=True,
        ),
        "precipitation": pa.Column(
            float,
            checks=Check.ge(0),
            nullable=True,
        ),
        "wind_speed_10m": pa.Column(
            float,
            checks=Check.ge(0),
            nullable=True,
        ),
        "_ingest_ts": pa.Column(pa.DateTime, nullable=False),
        "_source_api": pa.Column(str, nullable=False),
        "_batch_id": pa.Column(str, nullable=False),
    },
    coerce=True,
)


def validate(df: pd.DataFrame):
    """Validate an observed weather dataframe and return a structured payload."""
    try:
        validated_df = OBSERVED_WEATHER_SCHEMA.validate(df, lazy=True)
        return {"success": True, "errors": [], "validated_df": validated_df}
    except SchemaErrors as exc:
        errors = exc.failure_cases[["column", "check", "failure_case"]].to_dict(
            "records"
        )
        return {"success": False, "errors": errors, "validated_df": None}


if __name__ == "__main__":
    from pipelines.ingestion.ingest_observed_weather import run as ingest_observed_weather

    df = ingest_observed_weather()
    result = validate(df)

    print(result["success"])
    if not result["success"]:
        print(result["errors"])
