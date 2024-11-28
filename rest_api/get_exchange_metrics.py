import os
import requests
import pandas as pd

kaiko_api_key = os.environ['KAIKO_API_KEY']


def get_exchange_metrics(exchange, start_time, end_time, interval='1d'):
    url_ohlcv = (f'https://us.market-api.kaiko.io/v2/data/analytics.v2/exchange_metrics'
                 f'?interval={interval}'
                 f'&exchange={exchange}'
                 f'&start_time={start_time}'
                 f'&end_time={end_time}'
                 f'&page_size=100'
                 f'&sources=true'
                 f'&sort=desc')

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
    # df_.to_csv(f'{exchange}.csv', index=False)

    # save to csv for each column -- if needed
    # for col in df_.columns:
    #     df_[col].to_csv(f'{exchange}_{col}.csv', index=False)

    # investigate assets_volumes colume
    # iterate each row in assets_volumes column
    for i, row in df_.iterrows():
        # if i != 1:
        #     continue
        tmp_row = row['assets_volumes']

        final_res = pd.DataFrame()
        for asset in tmp_row:
            asset_df = pd.DataFrame.from_dict(asset)
            asset_df = asset_df[['asset_code', 'asset_total_volume_usd']]
            final_res = pd.concat([final_res, asset_df])

    # sort by asset_total_volume_usd
    final_res = final_res.sort_values(by='asset_total_volume_usd', ascending=False)
    final_res.to_csv(f'{exchange}_assets_volumes_detail.csv', index=False)

    # extract details of toshi -- or any weird looking asset
    # for i, row in df_.iterrows():
    #     if i != 1:
    #         continue
    #     tmp_row = row['assets_volumes']
    #
    #     for asset in tmp_row:
    #         if asset['asset_code'] != 'toshi':
    #             continue
    #         asset_df = pd.DataFrame.from_dict(asset)
    #         asset_df.to_csv(f'{exchange}_toshi_detail.csv', index=False)



if __name__ == '__main__':
    start_time_str = '2024-11-21T00:00:00.000Z'
    end_time_str = '2024-11-29T00:00:00.000Z'
    interval = '1d'
    exchange = 'gate'
    get_exchange_metrics(exchange, start_time_str, end_time_str, interval)
