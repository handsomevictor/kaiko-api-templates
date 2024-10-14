import os
import json
import datetime
import requests
import pandas as pd
from tqdm import tqdm

from itertools import repeat
from concurrent.futures import ThreadPoolExecutor


def get_rates_data_single(rate_ticker, start_time, end_time):
    url_rates = ('https://us.market-api.kaiko.io/v2/data/index.v1/digital_asset_rates_price_public'
                 f'/{rate_ticker}'
                 '?parameters=true'
                 f'&start_time={start_time}'
                 f'&end_time={end_time}'
                 '&detail=false&page_size=20000')
    print(url_rates)
    headers = {
        'Accept': 'application/json',
        'X-Api-Key': os.environ['KAIKO_API_KEY'],
    }
    try:
        response = requests.get(url_rates, headers=headers)
        res = response.json()
        res_data = res['data']

        while True:
            if (res.get('next_url') is not None) & (res['data'] != []):
                response = requests.get(res['next_url'], headers=headers)
                res = response.json()
                res_data = res_data + res['data']
            else:
                break

        df_ = pd.DataFrame.from_dict(res_data)
        return df_

    except KeyError:
        print(f'Error in {rate_ticker}')
        return None


def get_data_main():
    start_date = datetime.datetime(2024, 4, 3)
    end_date = datetime.datetime(2024, 10, 10)
    start_date_list = [start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    end_date_list = [date + datetime.timedelta(days=1) for date in start_date_list]
    start_date_list = [date.strftime('%Y-%m-%dT%H:%M:%S.000Z') for date in start_date_list]
    end_date_list = [date.strftime('%Y-%m-%dT%H:%M:%S.000Z') for date in end_date_list]

    rate_ticker = 'KK_BRR_BTCUSD_1S'

    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(get_rates_data_single, repeat(rate_ticker), start_date_list, end_date_list))

    return results


if __name__ == '__main__':
    rate_ticker = 'KK_BRR_BTCUSD_1S'
    get_data_main()

