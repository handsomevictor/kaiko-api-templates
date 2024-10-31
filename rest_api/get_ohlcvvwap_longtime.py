import os
import requests
import datetime
import pandas as pd
from tqdm import tqdm

from itertools import repeat
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from general_tools import time_convert

api_key = os.environ['KAIKO_API_KEY']


def get_ohlcv_single(exch, pair, start_time, end_time, interval='1d', aclass='spot', time_label='timestamp'):
    url_ohlcv = f"https://us.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exch}/{aclass}/{pair}/aggregations/" \
                f"count_ohlcv_vwap?start_time={start_time}&end_time={end_time}&interval={interval}&page_size=100000"

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

        # order by timestamp
        df_ = df_.sort_values(by='timestamp')
        # df_.to_csv(os.path.join(os.getcwd(), 'database', f'{exch}_{pair}_{start_time}_{end_time}.csv'))
        return df_

    except KeyError:
        df_ = pd.DataFrame(columns=['pair', 'exchange', 'open', 'high', 'low', 'close', 'volume', 'vwap'])
        return df_


def get_ohlcv_single_pair_long_duration(start_time: str, end_time: str,
                                        exch, pair, interval, aclass, time_label):
    # split the start and end time into 1 day interval
    start_time_str, start_time = time_convert(start_time)
    end_time_str, end_time = time_convert(end_time)
    start_time_list = [start_time + datetime.timedelta(days=i) for i in range((end_time - start_time).days + 1)]
    end_time_list = [start_time + datetime.timedelta(days=i + 1) for i in range((end_time - start_time).days + 1)]

    start_time_list = [start_time.strftime('%Y-%m-%dT%H:%M:%SZ') for start_time in start_time_list]
    end_time_list = [end_time.strftime('%Y-%m-%dT%H:%M:%SZ') for end_time in end_time_list]

    # get ohlcv data for each day
    with ThreadPoolExecutor(max_workers=20) as pool:
        res_temp = list(tqdm(pool.map(get_ohlcv_single, repeat(exch), repeat(pair),
                                      start_time_list, end_time_list,
                                      repeat(interval), repeat(aclass), repeat(time_label)),
                             total=len(start_time_list)))

    res = pd.concat(res_temp)
    return res


if __name__ == '__main__':
    start_time = '2023-01-01T00:00:00Z'
    end_time = '2023-12-31T00:00:00Z'

    start_time_str, start_time_dt = time_convert(start_time)
    end_time_str, end_time_dt = time_convert(end_time)

    data = get_ohlcv_single_pair_long_duration(start_time=start_time_str,
                                               end_time=end_time_str,
                                               exch='kcon',
                                               pair='btc-usdt',
                                               interval='1s',
                                               aclass='spot',
                                               time_label='timestamp')
    data.to_csv('ohlcvvwap_kcon_btcusdt_1s.csv')
