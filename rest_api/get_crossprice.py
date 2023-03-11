import requests
import pandas as pd
import datetime
import os

kaiko_api_key = os.environ['KAIKO_API_KEY']

if __name__ == '__main__':

    url = "https://us.market-api.kaiko.io/v2/data/trades.v1/spot_exchange_rate/reth/eth?page_size=100&sort=desc" \
          "&interval=5m&start_time=2023-02-20T00:00:00Z&end_time=2023-02-21T00:00:00Z&sources=false"
    headers = {"X-Api-Key": kaiko_api_key, "Accept": "application/json", "Accept-Encoding": "gzip"}


    a = requests.get(url, headers=headers, stream=True).json()["data"]

    while "continuation_token" in requests.get(url, headers=headers, stream=True).json():
        url = requests.get(url, headers=headers, stream=True).json()["next_url"]
        a = a + requests.get(url, headers=headers, stream=True).json()["data"]

    print(a)
    df_a = pd.DataFrame(a)
    df_a["timestamp"] = pd.to_datetime(df_a["timestamp"], unit="ms")
    df_a.to_csv("test.csv")
    print(df_a)
    df_a = df_a.fillna(0)

    import matplotlib.pyplot as plt
    plt.plot(df_a["timestamp"], df_a["price"])
    plt.show()


