import requests
import pandas as pd

from credentials import api_key


def request_df(url, df_format=pd.DataFrame):
    headers = {
        'Accept': 'application/json',
        'X-Api-Key': api_key,
    }

    response = requests.get(url, headers=headers)
    res = response.json()

    try:
        if res['result'] == 'success':
            res = res['data']
    except KeyError:
        raise ValueError('Data request failed \n%s' % res)

    df = df_format(res)
    return res, df


if __name__ == '__main__':
    _, list_instruments = request_df('https://reference-data-api.kaiko.io/v1/instruments')
    print(list_instruments)
    list_instruments.to_csv('list_instruments.csv', index=False)

