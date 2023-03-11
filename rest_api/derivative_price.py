import requests
import os
import pandas as pd


if __name__ == '__main__':
    url = 'https://us.market-api.kaiko.io/v2/data/derivatives.v2/price?interval=1h&exchange=bopt' \
          '&instrument=eth2303101900c&instrument_class=option&start_time=2023-02-27T08:00:00.000Z' \
          '&end_time=2023-03-10T08:00:00.000Z'

    kaiko_api_key = os.environ['KAIKO_API_KEY']
    headers = {"X-Api-Key": kaiko_api_key, "Accept": "application/json", "Accept-Encoding": "gzip"}

    a = requests.get(url, headers=headers, stream=True).json()["data"]

    while "continuation_token" in requests.get(url, headers=headers, stream=True).json():
        url = requests.get(url, headers=headers, stream=True).json()["next_url"]
        a = a + requests.get(url, headers=headers, stream=True).json()["data"]

    df_a = pd.DataFrame(a)
    df_a["timestamp"] = pd.to_datetime(df_a["timestamp"], unit="ms")
    print(df_a)
    df_a.to_csv("eth2303101900c.csv")
