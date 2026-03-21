import httpx

BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(
    lat=52.37,
    lon=4.90,
):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
    }

    r = httpx.get(BASE_URL, params=params)
    r.raise_for_status()

    return r.json()


if __name__ == "__main__":
    weather = fetch_weather()
    print(weather)
