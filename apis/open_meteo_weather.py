import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

BASE_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def _is_retryable_error(exc: BaseException) -> bool:
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
def _fetch_weather_response(lat: float, lon: float) -> httpx.Response:
    response = httpx.get(
        BASE_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "minutely_15": "temperature_2m,precipitation,wind_speed_10m",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response


def fetch_weather_data(
    lat: float = 52.37,
    lon: float = 4.90,
):
    response = _fetch_weather_response(lat=lat, lon=lon)
    return response.json()


if __name__ == "__main__":
    print(fetch_weather_data())
