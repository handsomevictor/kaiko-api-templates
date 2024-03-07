"""
This is for getting Cross Price V2 data from Kaiko API using single thread
"""

import requests
import pandas as pd

api_key = "xxxx"  # Replace it with your own key


def get_crossprice_single(base, quote, start_time, end_time, interval, time_label='timestamp',
                     extrapolate_missing_values='true'):
    url_ohlcv = (f"https://us.market-api.kaiko.io/v2/data/trades.v2/spot_exchange_rate"
                 f"/{base}/{quote}"
                 f"?interval={interval}&"
                 f"extrapolate_missing_values={extrapolate_missing_values}"
                 f"&start_time={start_time}"
                 f"&end_time={end_time}")

    headers = {
        'Accept': 'application/json',
        'X-Api-Key': api_key,
    }

    response = requests.get(url_ohlcv, headers=headers)
    res = response.json()

    res_data = res['data']

    # Loop for pagination, does not retry if there is an error
    while True:
        if (res.get('next_url') is not None) & (res['data'] != []):
            response = requests.get(res['next_url'], headers=headers)
            res = response.json()
            res_data = res_data + res['data']
        else:
            break
    try:
        df_ = pd.DataFrame.from_dict(res_data, dtype='float')
        df_[time_label] = pd.to_datetime(df_[time_label], unit='ms')
        df_.index = df_[time_label]
        df_ = df_.drop(columns=time_label)
        df_['pair'] = f'{base}-{quote}'

        return df_

    except KeyError:
        df_ = pd.DataFrame(columns=['price', 'extrapolated', 'pair'])
        return df_


if __name__ == '__main__':
    start_time_str = '2024-03-01T00:00:00.000Z'
    end_time_str = '2024-03-01T00:10:00.000Z'
    time_label = 'timestamp'
    base = 'btc'
    quote = 'usd'
    interval = '1m'
    df = get_crossprice_single(base, quote, start_time_str, end_time_str, interval, time_label)
    print(df)
