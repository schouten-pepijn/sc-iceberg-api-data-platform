from datetime import date
import pandas as pd

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import RedirectResponse

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from services.api.models import DailyWeatherResponse
from services.api.serializers import serialize_daily_weather

app = FastAPI(title="Iceberg API Data Platform")


def load_fact_weather():
    catalog = load_catalog("local")
    table = catalog.load_table("lakehouse.fact_weather")
    return table.scan().to_pandas()


@app.get("/")
def read_root():
    return RedirectResponse(url="/docs")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/weather/daily", response_model=list[DailyWeatherResponse])
def get_daily_weather(
    limit: int = Query(default=100, ge=1, le=1000),
    start_date: date | None = None,
    end_date: date | None = None,
):
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

    df = df.sort_values("day").head(limit)

    if df.empty:
        return []

    return serialize_daily_weather(df)
