import httpx

BASE_URL = "https://geocoding-api.open-meteo.com/v1/search"


def search_locations(
    name: str,
    count: int = 10,
    language: str = "en",
    country_code: str | None = None,
):
    params = {
        "name": name,
        "count": count,
        "language": language,
        "format": "json",
    }

    if country_code is not None:
        params["countryCode"] = country_code

    response = httpx.get(BASE_URL, params=params, timeout=30.0)
    response.raise_for_status()
    payload = response.json()

    return payload.get("results", [])


if __name__ == "__main__":
    locations = search_locations("Amsterdam")
    print(locations)
