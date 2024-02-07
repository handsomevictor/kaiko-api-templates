import requests
import pandas as pd
import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import repeat
from tqdm import tqdm

from credentials import api_key
from general_tools import time_convert


# noinspection SpellCheckingInspection,PyShadowingNames
class GetOhlcv:
    def __init__(self, exches: list, pairs: list, start_time, end_time,
                 interval='1d', aclass='spot', time_label='timestamp'):
        self.exches = exches
        self.pairs = pairs
        self.start_time = start_time
        self.end_time = end_time
        self.interval = interval
        self.aclass = aclass
        self.time_label = time_label

        self.conc_exch = True if len(exches) > 1 else False
        self.conc_exch_num = len(exches) if len(exches) < 40 else 40

        self.conc_pair = True if len(pairs) > 1 else False
        self.conc_pair_num = len(pairs) if len(pairs) < 40 else 40

        if self.conc_pair_num * self.conc_exch_num > 600:  # try to balance the number of concurrent threads
            self.conc_pair_num = 600 // self.conc_exch_num

        self.start_time_str, self.start_time_dt = time_convert(self.start_time)
        self.end_time_str, self.end_time_dt = time_convert(self.end_time)

        # when more than one exch or pair & interval is using minutes or seconds, use multi threads for spliting dates

    @staticmethod
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
            return df_

        except KeyError:
            df_ = pd.DataFrame(columns=['pair', 'exchange', 'open', 'high', 'low', 'close', 'volume', 'vwap'])
            return df_

    def get_ohlcv_conc_exch(self, exches, pair):
        """
        Getting ohlcv data from multiple exchanges but only one pair
        :return: DataFrame
        """
        res = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.conc_exch_num) as pool:
            if len(self.pairs) == 1:
                res_temp = list(tqdm(pool.map(self.get_ohlcv_single, exches, repeat(pair),
                                              repeat(self.start_time_str), repeat(self.end_time_str),
                                              repeat(self.interval),
                                              repeat(self.aclass), repeat(self.time_label)),
                                     total=len(self.pairs)))
            else:
                res_temp = list(pool.map(self.get_ohlcv_single, exches, repeat(pair),
                                         repeat(self.start_time_str), repeat(self.end_time_str),
                                         repeat(self.interval),
                                         repeat(self.aclass), repeat(self.time_label)))

            for df in res_temp:
                res = pd.concat([res, df])
        return res

    def get_ohlcv_conc_pair(self):
        """
        Getting ohlcv data from multiple pairs (each pair all exchanges)
        :return: DataFrame
        """

        res = pd.DataFrame()
        with ProcessPoolExecutor(max_workers=self.conc_pair_num) as pool:
            res_temp = list(tqdm(pool.map(self.get_ohlcv_conc_exch, repeat(self.exches), self.pairs),
                                 total=len(self.pairs)))

            for df in res_temp:
                res = pd.concat([res, df])
        return res

    def get_ohlcv(self):
        return self.get_ohlcv_conc_pair()


if __name__ == '__main__':
    params = {
        'exches': ['huob'],
        'pairs': ['btc-usdt'],
        'start_time': '2023-01-01T00:00:00Z',
        'end_time': '2024-01-01T00:00:00Z',
        'interval': '1s',
        'aclass': 'spot',
        'time_label': 'timestamp',
    }

    data = GetOhlcv(**params)
    df = data.get_ohlcv()
    print(df)
    df.to_csv('ohlcvvwap_binc.csv')
    #
    # import matplotlib.pyplot as plt
    # # line chart
    # df.reset_index(inplace=True)
    # plt.plot(df['timestamp'], df['close'], label='close')
    # plt.show()
    #
