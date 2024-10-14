import os
import requests
import pandas as pd


def get_ohlcv_single(exch, pair, start_time, end_time, interval='1d', aclass='spot', time_label='timestamp'):
    url_ohlcv = f"https://us.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exch}/{aclass}/{pair}/aggregations/" \
                f"count_ohlcv_vwap?start_time={start_time}&end_time={end_time}&interval={interval}&page_size=100000"

    headers = {
        'Accept': 'application/json',
        'X-Api-Key': os.environ['KAIKO_API_KEY'],
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
    start_time_str = '2024-09-10T00:00:00.000Z'
    end_time_str = '2024-09-20T00:00:00.000Z'
    interval = '1m'
    aclass = 'spot'
    time_label = 'timestamp'
    exch = 'usp3'
    pair = 'dai-usdc'
    df = get_ohlcv_single(exch, pair, start_time_str, end_time_str, interval, aclass, time_label)
    print(df)
    df.to_csv(f'{exch}_{pair}_{start_time_str}_{end_time_str}.csv')
