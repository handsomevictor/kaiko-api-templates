import os
import time
import requests
import datetime
import pandas as pd
from tqdm import tqdm

from itertools import repeat
from concurrent.futures import ThreadPoolExecutor


kaiko_api_key = os.environ['KAIKO_API_KEY']


def get_crossprice_one_instrument_one_periods(base, quote, interval, extrapolate_missing_values, start_time, end_time):
    print(f"base: {base}, quote: {quote}, interval: {interval}, start_time: {start_time}, end_time: {end_time}")
    url = ('https://us.market-api.kaiko.io/v2/data/trades.v2/spot_exchange_rate'
           f'/{base}/{quote}?interval={interval}&extrapolate_missing_values={extrapolate_missing_values}'
           f'&start_time={start_time}&end_time={end_time}&page_size=100')
    headers = {"X-Api-Key": kaiko_api_key,
               "Accept": "application/json", "Accept-Encoding": "gzip"}

    try:
        a = requests.get(url, headers=headers, stream=True).json()["data"]
        page = 1
        while "continuation_token" in requests.get(url, headers=headers, stream=True).json():
            url = requests.get(url, headers=headers, stream=True).json()["next_url"]
            a = a + requests.get(url, headers=headers, stream=True).json()["data"]
            print(f"base: {base}, quote: {quote}, interval: {interval}, start_time: {start_time}, end_time: {end_time},"
                  f"page: {page}")
            page += 1
        df_a = pd.DataFrame(a)
        df_a["timestamp"] = pd.to_datetime(df_a["timestamp"], unit="ms")

        start_time_file_name = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
        end_time_file_name = datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
        df_a.to_csv(os.path.join('database',
                                 f'{base}-{quote}-{interval}',
                                 f'{base}_{quote}_{interval}_{start_time_file_name}_{end_time_file_name}.csv'),
                    index=False)
    except:
        pass


def get_crossprice_one_instrument(base, quote, interval, extrapolate_missing_values, start_time_list, end_time_list,
                                  max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(get_crossprice_one_instrument_one_periods, repeat(base), repeat(quote),
                                    repeat(interval), repeat(extrapolate_missing_values), start_time_list, end_time_list))
        tqdm(results, total=len(start_time_list))


def split_date(start_time: datetime.datetime, end_time: datetime.datetime, split_interval=1):
    """
    This function is to split the original start_time and end_time into a list of start_time and end_time, each
    start_time and end_time is split_interval apart

    split_interval is in days
    """
    start_time_list = []
    end_time_list = []
    while start_time < end_time:
        start_time_list.append(start_time)
        end_time_list.append(start_time + datetime.timedelta(days=split_interval))
        start_time = start_time + datetime.timedelta(days=split_interval)

    start_time_list = [i.strftime("%Y-%m-%dT%H:%M:%S.%fZ") for i in start_time_list]
    end_time_list = [i.strftime("%Y-%m-%dT%H:%M:%S.%fZ") for i in end_time_list]
    return start_time_list, end_time_list


def check_missing_days(start_time: str, end_time: str, file_dir):
    """
    Just for checking which days are missing, and rerun the function only for missing days
    """
    start_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    end_time = datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    all_dates = pd.date_range(start=start_time, end=end_time, freq='D')
    all_dates = [i.strftime("%Y-%m-%d") for i in all_dates][:-1]

    # all files, use os.walk
    all_files = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if file.endswith(".csv"):
                temp_name = file.split("_")[3]
                all_files.append(temp_name)

    missing_dates = list(set(all_dates) - set(all_files))
    print(f"Missing dates: {missing_dates}")
    return missing_dates


if __name__ == '__main__':
    current_start_time = time.time()
    start_time = "2023-01-01T00:00:00.000Z"
    end_time = "2023-01-31T00:00:00.000Z"
    base = "btc"
    quote = "usd"
    interval = "1m"

    start_time_list, end_time_list = split_date(datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ"),
                                                datetime.datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S.%fZ"))

    if not os.path.exists(os.path.join('database', f'{base}-{quote}-{interval}')):
        os.makedirs(os.path.join('database', f'{base}-{quote}-{interval}'))

    get_crossprice_one_instrument(base='btc', quote='usd', interval='1m', extrapolate_missing_values='true',
                                  start_time_list=start_time_list, end_time_list=end_time_list,
                                  max_workers=len(start_time_list) if len(start_time_list) < 80 else 80)
    print(f"Time taken: {time.time() - current_start_time}")

    check_missing_days(start_time, end_time, file_dir=os.path.join(os.getcwd(), 'database', 'btc-usd-1m'))
