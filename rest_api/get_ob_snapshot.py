import os
import requests
import pandas as pd


def get_ob_agg_single(exch, pair, start_time, end_time, aclass='spot'):
    url_ob = (f"https://us.market-api.kaiko.io/v2/data/order_book_snapshots.v1/exchanges/{exch}/{aclass}/{pair}"
              f"/snapshots/raw?page_size=100&start_time={start_time}&end_time={end_time}")

    headers = {
        'Accept': 'application/json',
        'X-Api-Key': os.environ['KAIKO_API_KEY'],
    }

    response = requests.get(url_ob, headers=headers)
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
        df_['poll_timestamp'] = pd.to_datetime(df_['poll_timestamp'], unit='ms')
        return df_

    except KeyError:
        df_ = pd.DataFrame()
        return df_


if __name__ == '__main__':
    start_time_str = '2024-12-11T00:00:00.000Z'
    end_time_str = '2024-12-11T00:01:00.000Z'
    aclass = 'perpetual-future'
    time_label = 'timestamp'
    exch = 'drbt'
    pair = 'btc-usdt'
    df = get_ob_agg_single(exch, pair, start_time_str, end_time_str, aclass)

    df.to_csv(f'{exch}_{pair}_{start_time_str}_{end_time_str}.csv')
