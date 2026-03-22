"""FastAPI application exposing curated location and daily weather datasets."""

from datetime import date
import pandas as pd

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import RedirectResponse

from pyiceberg.exceptions import NoSuchTableError

from services.api.models import DailyWeatherResponse
from services.api.models import LocationResponse
from services.api.serializers import serialize_daily_weather
from services.api.serializers import serialize_locations
from services.api.services.locations import load_dim_location
from services.api.services.weather import load_fact_weather

app = FastAPI(title="Iceberg API Data Platform")


@app.get("/")
def read_root():
    """Redirect root traffic to interactive API docs."""
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    """Return a lightweight liveness signal for probes."""
    return {"status": "ok"}


@app.get("/locations", response_model=list[LocationResponse])
def get_locations(limit: int = Query(default=100, ge=1, le=1000)):
    """Return latest known dimension record per location."""
    try:
        df = load_dim_location().copy()
    except NoSuchTableError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="dim_location table does not exist. Materialize the location dimension first.",
        ) from exc

    if df.empty:
        return []

    df["_ingest_ts"] = pd.to_datetime(df["_ingest_ts"], utc=True)
    # Dimension rows are append-only; pick the newest row per business key.
    latest_locations = (
        df.sort_values(["location_id", "_ingest_ts"])
        .drop_duplicates(subset=["location_id"], keep="last")
        .sort_values(["country_code", "name"])
        .head(limit)
    )

    if latest_locations.empty:
        return []

    return serialize_locations(latest_locations)


@app.get("/weather/daily", response_model=list[DailyWeatherResponse])
def get_daily_weather(
    limit: int = Query(default=100, ge=1, le=1000),
    start_date: date | None = None,
    end_date: date | None = None,
    location_id: str | None = None,
):
    """Return Gold daily weather facts with optional date/location filters."""
    if start_date is not None and end_date is not None and start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date must be less than or equal to end_date",
        )

    try:
        df = load_fact_weather().copy()
    except NoSuchTableError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="fact_weather table does not exist. Materialize the Gold layer first.",
        ) from exc

    if df.empty:
        return []

    df["day"] = pd.to_datetime(df["day"]).dt.date

    if start_date is not None:
        df = df[df["day"] >= start_date]

    if end_date is not None:
        df = df[df["day"] <= end_date]

    if location_id is not None:
        df = df[df["location_id"] == location_id]

    # Keep deterministic ordering for stable API pagination behavior.
    df = df.sort_values(["location_id", "day"]).head(limit)

    if df.empty:
        return []

    return serialize_daily_weather(df)
