import os
import requests
import pandas as pd


kaiko_api_key = os.environ['KAIKO_API_KEY']


def get_derivatives_metrics(url_derivatives_metrics):
    headers = {
        'Accept': 'application/json',
        'X-Api-Key': kaiko_api_key,
    }

    response = requests.get(url_derivatives_metrics, headers=headers)

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
        res_data = pd.DataFrame(res_data)
        if 'timestamp' in res_data.columns:
            res_data['timestamp'] = pd.to_datetime(res_data['timestamp'], unit='ms')
        return res_data
    except KeyError:
        return pd.DataFrame()


if __name__ == '__main__':
    exch = 'binc'
    start_time = '2024-06-10T00:00:00.000Z'
    end_time = '2024-06-10T01:00:00.000Z'
    base_assets = 'usdc'
    quote_assets = 'usdt'
    instrument_class = 'perpetual-future'
    instrument = "usdc-usdt"  # This info can be found in derivatives reference result for options

    url_derivatives_reference = (f"https://us.market-api.kaiko.io/v2/data/derivatives.v2/reference"
                                 f"?exchange={exch}"
                                 f"&instrument_class={instrument_class}"
                                 f"&base_assets={base_assets}"
                                 f"&quote_assets={quote_assets}")

    url_derivatives_risk = ("https://us.market-api.kaiko.io/v2/data/derivatives.v2/risk"
                            f"?exchange={exch}"
                            f"&instrument_class={instrument_class}"
                            f"&instrument={instrument}"
                            f"&page_size=10")

    url_derivatives_price = ("https://us.market-api.kaiko.io/v2/data/derivatives.v2/price"
                             f"?exchange={exch}"
                             f"&instrument_class={instrument_class}"
                             f"&instrument={instrument}"
                             "&page_size=10"
                             f"&start_time={start_time}"
                             f"&end_time={end_time}")

    df_ref = get_derivatives_metrics(url_derivatives_metrics=url_derivatives_reference)
    print(df_ref.head())

    df_risk = get_derivatives_metrics(url_derivatives_metrics=url_derivatives_risk)
    print(df_risk.head())

    df_price = get_derivatives_metrics(url_derivatives_metrics=url_derivatives_price)
    print(df_price.head())
