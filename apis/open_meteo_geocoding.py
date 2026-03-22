"""HTTP client for Open-Meteo geocoding search data."""

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

BASE_URL = "https://geocoding-api.open-meteo.com/v1/search"
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
def _search_locations_response(
    name: str,
    count: int,
    language: str,
    country_code: str | None,
) -> httpx.Response:
    """Call the geocoding endpoint and return the raw HTTP response."""
    params = {
        "name": name,
        "count": count,
        "language": language,
        "format": "json",
    }

    if country_code is not None:
        params["countryCode"] = country_code

    response = httpx.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response


def search_locations(
    name: str,
    count: int = 10,
    language: str = "en",
    country_code: str | None = None,
):
    """Search candidate locations and return only the results payload list."""
    response = _search_locations_response(
        name=name,
        count=count,
        language=language,
        country_code=country_code,
    )
    payload = response.json()
    return payload.get("results", [])


if __name__ == "__main__":
    print(search_locations("Amsterdam"))
