"""HTTP client for Open-Meteo historical observed weather data."""

from datetime import date

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
REQUEST_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def _is_retryable_error(exc: BaseException) -> bool:
    """Return True when an exception should trigger a retry."""
    if isinstance(exc, httpx.RequestError):
        return True

    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in {429, 500, 502, 503, 504}

    return False


@retry(
    retry=retry_if_exception(_is_retryable_error),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=True,
)
def _fetch_observed_weather_response(
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
) -> httpx.Response:
    """Fetch the raw historical weather response for a date range."""
    response = httpx.get(
        BASE_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": "temperature_2m,precipitation,wind_speed_10m",
            "timezone": "UTC",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response


def fetch_observed_weather_data(
    lat: float = 52.37,
    lon: float = 4.90,
    start_date: date | None = None,
    end_date: date | None = None,
):
    """Fetch historical observed weather for a closed date window."""
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date are required for observed weather")

    response = _fetch_observed_weather_response(
        lat=lat,
        lon=lon,
        start_date=start_date,
        end_date=end_date,
    )
    return response.json()


if __name__ == "__main__":
    from datetime import timedelta

    yesterday = date.today() - timedelta(days=1)
    print(
        fetch_observed_weather_data(
            start_date=yesterday,
            end_date=yesterday,
        )
    )
