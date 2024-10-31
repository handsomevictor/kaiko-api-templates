import os
import requests
import pandas as pd

kaiko_api_key = os.environ['KAIKO_API_KEY']


def get_asset_metrics(asset, start_time, end_time, interval='1d'):
    url_ohlcv = (f'https://us.market-api.kaiko.io/v2/data/analytics.v2/asset_metrics'
                 f'?interval={interval}'
                 f'&asset={asset}'
                 f'&start_time={start_time}'
                 f'&end_time={end_time}'
                 f'&page_size=100'
                 f'&sources=true')

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

    df_ = pd.DataFrame.from_dict(res_data)

    ########### This only keeps the depth data ############
    # process the df
    df_['buy_market_depths'] = df_['off_chain_liquidity_data'].apply(lambda x: x['buy_market_depths'])
    df_['sell_market_depths'] = df_['off_chain_liquidity_data'].apply(lambda x: x['sell_market_depths'])

    # only keep the one that has exchange = binc
    exchange_wanted = 'binc'
    df_['buy_market_depths'] = df_['buy_market_depths'].apply(lambda x:
                                                              [y for y in x if y['exchange'] == exchange_wanted])
    df_['sell_market_depths'] = df_['sell_market_depths'].apply(lambda x:
                                                                [y for y in x if y['exchange'] == exchange_wanted])

    df_['buy_market_depths'] = df_['buy_market_depths'].apply(lambda x: x[0] if x else {})
    df_['sell_market_depths'] = df_['sell_market_depths'].apply(lambda x: x[0] if x else {})

    df_['exchange'] = exchange_wanted
    df_['asset'] = asset

    print(df_['buy_market_depths'])
    all_depths = ['0_1', '0_2', '0_3', '0_4', '0_5', '0_6', '0_7', '0_8', '0_9', '1', '1_5', '2', '4', '6', '8', '10']
    bid_ask_volume = ['bid_volume_', 'ask_volume_']

    for depth in all_depths:
        df_[f'{bid_ask_volume[0]}{depth}'] = df_['buy_market_depths'].apply(
            lambda x: x['volume_assets'][bid_ask_volume[0] + depth] if x else None)
        df_[f'{bid_ask_volume[1]}{depth}'] = df_['sell_market_depths'].apply(
            lambda x: x['volume_assets'][bid_ask_volume[1] + depth] if x else None)

    df_ = df_.drop(
        columns=['off_chain_liquidity_data', 'on_chain_liquidity_data', 'buy_market_depths', 'sell_market_depths'])
    return df_


if __name__ == '__main__':
    start_time_str = '2024-08-10T00:00:00.000Z'
    end_time_str = '2024-08-27T00:00:00.000Z'
    interval = '1d'
    asset = 'doge'
    df = get_asset_metrics(asset, start_time_str, end_time_str, interval)
    print(df)
    df.to_csv(f'{asset}_{start_time_str}_{end_time_str}.csv', index=False)
