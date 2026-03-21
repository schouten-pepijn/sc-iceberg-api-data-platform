import pandas as pd
from apis.open_meteo import fetch_weather_data


def run():

    data = fetch_weather_data()

    df = pd.DataFrame(
        {
            "timestamp": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"],
        }
    )

    return df


if __name__ == "__main__":
    df = run()
    print(df.head())
