"""
For Getting Trades Data, several steps:

1. Go to our instrument page, and find the ticker that you want: https://instruments.kaiko.com/#/instruments
2. Run the script, and save to local etc.
"""
import os
import requests
import pandas as pd

api_key = os.getenv('KAIKO_API_KEY')


def get_trades_single(exch, pair, start_time, end_time, aclass='spot', time_label='timestamp'):
    url_trades = f"https://us.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exch}/{aclass}/{pair}/trades/" \
                f"?start_time={start_time}&end_time={end_time}&page_size=100000"

    headers = {
        'Accept': 'application/json',
        'X-Api-Key': api_key,
    }

    response = requests.get(url_trades, headers=headers)
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
        df_ = pd.DataFrame.from_dict(res_data)
        df_[time_label] = pd.to_datetime(df_[time_label], unit='ms')
        df_['pair'] = pair
        df_['exchange'] = exch
        return df_

    except KeyError:
        return pd.DataFrame()


if __name__ == '__main__':
    start_time_str = '2023-05-01T00:00:00.000Z'
    end_time_str = '2023-05-01T02:00:00.000Z'
    aclass = 'perpetual-future'
    time_label = 'timestamp'
    exch = 'binc'
    pair = 'btc-usdt'
    df = get_trades_single(exch, pair, start_time_str, end_time_str, aclass, time_label)
    print(df.columns)
    print(df)
