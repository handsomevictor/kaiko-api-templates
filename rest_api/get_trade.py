import requests
import pandas as pd
import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import repeat
from tqdm import tqdm

from credentials import api_key
from general_tools import time_convert


# noinspection SpellCheckingInspection,PyShadowingNames
class GetTrade:
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
    def get_trade_single(exch, pair, start_time, end_time, aclass='spot', time_label='timestamp'):
        trade_url = f'https://us.market-api.kaiko.io/v2/data/trades.v1/exchanges/{exch}/{aclass}/{pair}/trades?' \
              f'start_time={start_time}&end_time={end_time}&page_size=100000'

        headers = {
            'Accept': 'application/json',
            'X-Api-Key': api_key,
        }

        response = requests.get(trade_url, headers=headers)
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
            df_.index = df_[time_label]
            df_ = df_.drop(columns=time_label)
            df_['pair'] = pair
            df_['exchange'] = exch
            return df_

        except KeyError:
            df_ = pd.DataFrame(columns=['pair', 'exchange', 'trade_id', 'price', 'amount', 'taker_side_sell'])
            return df_

    def get_trade_conc_exch(self, exches, pair, start_time_str, end_time_str):
        """
        Getting ohlcv data from multiple exchanges but only one pair
        :return: DataFrame
        """
        res = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.conc_exch_num) as pool:
            if len(self.pairs) == 1:
                res_temp = list(tqdm(pool.map(self.get_trade_single, exches, repeat(pair),
                                              repeat(start_time_str), repeat(end_time_str),
                                              repeat(self.aclass), repeat(self.time_label)),
                                     total=len(self.pairs)))
            else:
                res_temp = list(pool.map(self.get_trade_single, exches, repeat(pair),
                                         repeat(start_time_str), repeat(end_time_str),
                                         repeat(self.aclass), repeat(self.time_label)))
            for df in res_temp:
                res = pd.concat([res, df])
        return res

    def get_trade_conc_pair(self, start_time_str, end_time_str):
        """
        Getting ohlcv data from multiple pairs (each pair all exchanges)
        :return: DataFrame
        """

        res = pd.DataFrame()
        with ProcessPoolExecutor(max_workers=self.conc_pair_num) as pool:
            res_temp = list(tqdm(pool.map(self.get_trade_conc_exch, repeat(self.exches), self.pairs,
                                          repeat(start_time_str), repeat(end_time_str)),
                                 total=len(self.pairs)))

            for df in res_temp:
                res = pd.concat([res, df])
        return res

    def get_trade(self):
        """
        Since trade data is often very large, for efficiency purpose, I recommend splitting the date range into smaller
        chunks then combine.

        Usually for Coinbase (the one with very large data), I split the date range into 1 hour chunk.
        """
        if not self.end_time_dt - self.start_time_dt >= datetime.timedelta(hours=1):
            return self.get_trade_conc_pair(self.start_time_str, self.end_time_str).sort_values(by=['pair', 'exchange',
                                                                                                    self.time_label])

        # split the dates into 1 hour chunks
        start_time, end_time = self.start_time_dt, self.start_time_dt + datetime.timedelta(hours=1)
        start_time_lst, end_time_lst = [], []

        while end_time < self.end_time_dt:
            start_time_lst.append(start_time)
            end_time_lst.append(end_time)
            start_time = end_time
            end_time = start_time + datetime.timedelta(hours=1)
        start_time_lst.append(start_time)
        end_time_lst.append(self.end_time_dt)

        start_time_lst, end_time_lst = [x.strftime('%Y-%m-%dT%H:%M:%SZ') for x in start_time_lst], \
                                          [x.strftime('%Y-%m-%dT%H:%M:%SZ') for x in end_time_lst]

        res = pd.DataFrame()
        with ProcessPoolExecutor(max_workers=len(start_time_lst) if len(start_time_lst) < 10 else 10) as pool:
            res_temp = list(tqdm(pool.map(self.get_trade_conc_pair, start_time_lst, end_time_lst),
                                 total=len(self.pairs)))
            for df in res_temp:
                res = pd.concat([res, df])
        res = res.sort_values(by=['pair', 'exchange', self.time_label])
        return res


if __name__ == '__main__':
    params = {
        'exches': ['cbse', 'krkn'],
        'pairs': ['btc-usd', 'eth-usd'],
        'start_time': '2023-02-10T06:00:00Z',
        'end_time': '2023-02-10T12:30:00Z',
        'aclass': 'spot',
        'time_label': 'timestamp',
    }

    data = GetTrade(**params)
    df = data.get_trade()
    print(df)
    df.to_csv('trade_test.csv')
