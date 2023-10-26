"""
For Getting Trades Data, several steps:

1. Go to our instrument page, and find the ticker that you want: https://instruments.kaiko.com/#/instruments
2. Run the script, and save to local etc.
"""

import requests
import pandas as pd

api_key = 'xxxxxxx'  # Replace it with your own key


def get_ohlcv_single(exch, pair, start_time, end_time, aclass='spot', time_label='timestamp'):
    url_ohlcv = f"https://us.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exch}/{aclass}/{pair}/trades/" \
                f"?start_time={start_time}&end_time={end_time}&page_size=100000"

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
        df_['pair'] = pair
        df_['exchange'] = exch
        return df_

    except KeyError:
        df_ = pd.DataFrame(columns=['pair', 'exchange', 'open', 'high', 'low', 'close', 'volume', 'vwap'])
        return df_


if __name__ == '__main__':
    start_time_str = '2020-01-01T00:00:00.000Z'
    end_time_str = '2020-01-02T00:00:00.000Z'
    aclass = 'spot'
    time_label = 'timestamp'
    exch = 'binc'
    pair = 'btc-usdt'
    df = get_ohlcv_single(exch, pair, start_time_str, end_time_str, aclass, time_label)
    print(df.columns)
    print(df)
