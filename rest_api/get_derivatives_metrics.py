import requests
import os
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
        return pd.DataFrame(res_data)
    except KeyError:
        return pd.DataFrame()


if __name__ == '__main__':
    exch = 'drbt'
    start_time = '2024-06-01T00:00:00.000Z'
    end_time = '2024-06-10T00:00:00.000Z'
    base_assets = 'btc'
    quote_assets = 'usd'
    time_label = 'timestamp'
    instrument_class = 'option'
    instrument = "btc28jun2410000c"  # This info can be found in derivatives reference result

    url_derivatives_reference = (f"https://us.market-api.kaiko.io/v2/data/derivatives.v2/reference"
                                 f"?exchange={exch}"
                                 f"&instrument_class={instrument_class}"
                                 f"&base_assets={base_assets}"
                                 f"&quote_assets={quote_assets}"
                                 f"&start_time={start_time}"
                                 f"&end_time={end_time}")

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
    df_risk = get_derivatives_metrics(url_derivatives_metrics=url_derivatives_risk)
    df_price = get_derivatives_metrics(url_derivatives_metrics=url_derivatives_price)

    print(df_ref.head())
    print(df_risk.head())
    print(df_price.head())
