import uuid

import pandas as pd
from apis.open_meteo import fetch_weather_data


def run():

    data = fetch_weather_data()

    batch_id = str(uuid.uuid4())
    ingest_ts = pd.Timestamp.utcnow()

    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(data["hourly"]["time"], utc=True),
            "temperature": data["hourly"]["temperature_2m"],
        }
    )

    # metadata columns
    df["_ingest_ts"] = ingest_ts
    df["_source_api"] = "open_meteo"
    df["_batch_id"] = batch_id

    return df


if __name__ == "__main__":
    df = run()
    print(df.head())
    print(df.dtypes)
